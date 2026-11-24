import os
import signal
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from threading import Event
from typing import Annotated, Final
from urllib.parse import parse_qsl, unquote

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from psycopg import Connection
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from litellm._logging import verbose_proxy_logger
from litellm.proxy.db.fleet_budget_spend import replicated_budget_entity_id
from litellm.repositories.spend_log_insert import replicated_spend_log_insert

_DEFAULT_TOPIC: Final = "litellm-spend-logs"
_BATCH_SIZE: Final = 100
_MAX_RECORD_BYTES: Final = 256 * 1024
_MAX_DISPLAY_FIELD_BYTES: Final = 2048
_KAFKA_SIZE_ERRORS: Final = frozenset({KafkaError.MSG_SIZE_TOO_LARGE, KafkaError.RECORD_LIST_TOO_LARGE})
_DELIVERY_TIMEOUT_SECONDS: Final = 10
_KAFKA_CONFIG: Final = TypeAdapter(dict[str, str | int | float | bool])
_OUTBOX_BATCH: Final = TypeAdapter(tuple[tuple[int, str], ...])
_REQUEST_ID_KEY_SETTING: Final = "LITELLM_SPEND_LOG_KAFKA_REQUEST_ID_KEY"
_PRISMA_QUERY_OPTIONS: Final = frozenset(
    {
        "connection_limit",
        "schema",
        "pool_timeout",
        "socket_timeout",
        "pgbouncer",
        "max_idle_connection_lifetime",
        "max_connection_lifetime",
        "statement_cache_size",
    }
)


class SpendLogMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    request_id: Annotated[str, Field(min_length=1)]
    call_type: str
    api_key: Annotated[str, Field(pattern=r"^(?:[a-fA-F0-9]{64})?$")] = ""
    spend: float = 0
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    startTime: str
    endTime: str
    request_duration_ms: int | None = None
    completionStartTime: str | None = None
    model: str = ""
    model_id: str | None = None
    model_group: str | None = None
    custom_llm_provider: str | None = None
    user: str | None = None
    end_user: str | None = None
    team_id: str | None = None
    organization_id: str | None = None
    status: str | None = None
    created_at: str
    updated_at: str


class SpendLogEvent(BaseModel):
    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    region: Annotated[str, Field(min_length=1)]
    row: SpendLogMetadata


class KafkaDeliveryFailed(RuntimeError):
    pass


@contextmanager
def connect_regional_db(database_url: str) -> Generator[Connection[TupleRow]]:
    base, query_separator, query = database_url.partition("?")
    uri_query: Final = query if base.startswith(("postgresql://", "postgres://")) else ""
    schema: Final = dict(parse_qsl(uri_query, keep_blank_values=True)).get("schema")
    libpq_query: Final = "&".join(
        field for field in uri_query.split("&") if unquote(field.partition("=")[0]) not in _PRISMA_QUERY_OPTIONS
    )
    libpq_url: Final = base + (query_separator + libpq_query if libpq_query else "") if uri_query else database_url
    dsn: Final = make_conninfo(libpq_url, connect_timeout=5)
    with psycopg.connect(dsn, autocommit=True, prepare_threshold=None) as db:
        if schema is not None:
            db.execute("SELECT set_config('search_path', quote_ident(%s), false)", (schema,))
        yield db


def enable_capture(db: Connection[TupleRow], region: str, request_id_key: str) -> None:
    try:
        key: Final = bytes.fromhex(request_id_key)
    except ValueError:
        raise ValueError(f"{_REQUEST_ID_KEY_SETTING} must be a 32-byte hex key") from None
    if len(key) != 32:
        raise ValueError(f"{_REQUEST_ID_KEY_SETTING} must be a 32-byte hex key")
    if db.info.server_version < 110000 and db.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pgcrypto')"
    ).fetchone() != (True,):
        raise ValueError("Spend log replication on PostgreSQL 10 requires the pgcrypto extension")
    block: Final = key.ljust(64, b"\0")
    hmac_pads: Final = bytes(byte ^ 0x36 for byte in block) + bytes(byte ^ 0x5C for byte in block)
    with db.transaction():
        db.execute(
            'INSERT INTO "LiteLLM_SpendLogReplication" (id, region, request_id_hmac_pads) '
            "VALUES (1, %s, %s) ON CONFLICT (id) DO NOTHING",
            (region, hmac_pads),
        )
        current: Final = db.execute(
            'SELECT region = %s, request_id_hmac_pads = %s FROM "LiteLLM_SpendLogReplication" WHERE id = 1',
            (region, hmac_pads),
        ).fetchone()
        if current != (True, True):
            raise ValueError(
                "The regional database is already configured with a different Kafka region or request ID key"
            )


def _send_record(
    producer: Producer,
    topic: str,
    record: tuple[int, str, str],
    report_oversized: Callable[[int], None],
    report_batch_rejection: Callable[[int], None],
    failed: Event,
) -> None:
    record_id, key, value = record
    if len(key.encode()) + len(value.encode()) > _MAX_RECORD_BYTES:
        report_oversized(record_id)
        return

    def delivered(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            if error.code() in _KAFKA_SIZE_ERRORS:
                report_batch_rejection(record_id)
            else:
                failed.set()

    try:
        producer.produce(topic, key=key, value=value, on_delivery=delivered)
    except KafkaException as error:
        args: Final[tuple[object, ...]] = error.args
        reason: Final = args[0] if args else None
        if not isinstance(reason, KafkaError) or reason.code() not in _KAFKA_SIZE_ERRORS:
            raise
        report_oversized(record_id)


def _deliver_records(
    producer: Producer, topic: str, records: Sequence[tuple[int, str, str]]
) -> tuple[bool, frozenset[int]]:
    failed: Final = Event()
    oversized: Final[set[int]] = set()  # mutable-ok: librdkafka reports oversized deliveries through callbacks
    retry_alone: Final[set[int]] = set()  # mutable-ok: size errors can apply to an entire Kafka batch
    try:
        for record in records:
            _send_record(producer, topic, record, oversized.add, retry_alone.add, failed)
        if producer.flush(_DELIVERY_TIMEOUT_SECONDS) != 0:
            failed.set()
        if not failed.is_set():
            for record in records:
                if record[0] not in retry_alone:
                    continue
                _send_record(producer, topic, record, oversized.add, oversized.add, failed)
                if producer.flush(_DELIVERY_TIMEOUT_SECONDS) != 0 or failed.is_set():
                    failed.set()
                    break
    except (BufferError, KafkaException):
        failed.set()
    return not failed.is_set(), frozenset(oversized)


def _kafka_record(record_id: int, payload: str, region: str) -> tuple[int, str, str]:
    row: Final = SpendLogMetadata.model_validate_json(payload)
    bounded_row: Final = row.model_copy(
        update={
            "team_id": replicated_budget_entity_id(row.team_id),
            "user": replicated_budget_entity_id(row.user),
            "end_user": replicated_budget_entity_id(row.end_user),
            "model": _bounded_display_field(row.model) or "",
            "model_id": _bounded_display_field(row.model_id),
            "model_group": _bounded_display_field(row.model_group),
            "custom_llm_provider": _bounded_display_field(row.custom_llm_provider),
            "organization_id": _bounded_display_field(row.organization_id),
            "status": _bounded_display_field(row.status),
        }
    )
    return record_id, row.request_id, SpendLogEvent(region=region, row=bounded_row).model_dump_json()


def _bounded_display_field(value: str | None) -> str | None:
    return value if value is not None and len(value.encode()) <= _MAX_DISPLAY_FIELD_BYTES else None


def publish_batch(db: Connection[TupleRow], producer: Producer, region: str, topic: str) -> bool:
    with db.transaction():
        rows: Final = _OUTBOX_BATCH.validate_python(
            db.execute(
                'SELECT id, "row"::text FROM "LiteLLM_SpendLogOutbox" ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED',
                (_BATCH_SIZE,),
            ).fetchall()
        )
        if not rows:
            return False

        records: Final = tuple(_kafka_record(record_id, payload, region) for record_id, payload in rows)
        delivered, oversized = _deliver_records(producer, topic, records)
        removable: Final = [record_id for record_id, _, _ in records] if delivered else list(oversized)
        if removable:
            db.execute('DELETE FROM "LiteLLM_SpendLogOutbox" WHERE id = ANY(%s)', (removable,))
    if oversized:
        verbose_proxy_logger.warning("Removed %s oversized Kafka spend-log events from the outbox", len(oversized))
    if not delivered:
        raise KafkaDeliveryFailed("Kafka did not acknowledge the spend-log batch")
    return True


def consume_batch(db: Connection[TupleRow], consumer: Consumer, messages: Sequence[Message], region: str) -> None:
    if not messages:
        return
    with db.transaction():
        for message in messages:
            if (error := message.error()) is not None:
                raise KafkaException(error)
            value = message.value()
            if value is None:
                raise ValueError("Kafka spend-log events require a row")
            event = SpendLogEvent.model_validate_json(value)
            if event.region == region:
                continue
            if message.key() != event.row.request_id.encode():
                raise ValueError("A Kafka spend-log event has a missing or inconsistent request ID")
            query, params = replicated_spend_log_insert(
                event.row.model_dump(mode="json", exclude_unset=True), region=event.region
            )
            db.execute(query, (params,))

    offsets: Final = consumer.commit(asynchronous=False)
    if any(partition.error is not None for partition in offsets):
        raise KafkaDeliveryFailed("Kafka did not commit the spend-log consumer offsets")


def run_replication(db: Connection[TupleRow], settings: Mapping[str, str], stop: Event) -> None:
    region: Final = settings["LITELLM_SPEND_LOG_KAFKA_REGION"].strip()
    topic: Final = settings.get("LITELLM_SPEND_LOG_KAFKA_TOPIC", _DEFAULT_TOPIC).strip()
    kafka: Final = _KAFKA_CONFIG.validate_json(settings["LITELLM_SPEND_LOG_KAFKA_CONFIG"])
    if not region or not topic or not kafka.get("bootstrap.servers"):
        raise ValueError("Kafka spend-log replication needs a region, topic, and bootstrap.servers")
    protocol: Final = str(kafka.get("security.protocol", "SSL")).upper()
    if protocol not in {"SSL", "SASL_SSL", "PLAINTEXT"}:
        raise ValueError("Kafka TLS requires SSL or SASL_SSL. Set PLAINTEXT explicitly for local development")
    if protocol != "PLAINTEXT" and (
        str(kafka.get("enable.ssl.certificate.verification", True)).lower() != "true"
        or str(kafka.get("ssl.endpoint.identification.algorithm", "https")).lower() != "https"
    ):
        raise ValueError("Kafka TLS requires certificate and hostname verification")
    kafka_config: Final = {**kafka, "security.protocol": protocol}
    producer: Final = Producer(
        {
            **kafka_config,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": _DELIVERY_TIMEOUT_SECONDS * 1000,
            "allow.auto.create.topics": False,
        }
    )
    consumer_config: Final = {
        **kafka_config,
        "group.id": f"litellm-spend-logs.{topic}.{region}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": True,
        "enable.partition.eof": False,
        "allow.auto.create.topics": False,
    }
    consumer: Consumer | None = None  # rebind-ok: recreate after Kafka failures to rewind to committed offsets
    try:
        enable_capture(db, region, settings.get(_REQUEST_ID_KEY_SETTING, ""))
        verbose_proxy_logger.info("Kafka spend-log replication started")
        while not stop.is_set():
            if consumer is None:
                consumer = Consumer(consumer_config)
                consumer.subscribe([topic])
            try:
                messages = consumer.consume(num_messages=_BATCH_SIZE, timeout=0)
                consume_batch(db, consumer, messages, region)
            except (KafkaException, KafkaDeliveryFailed):
                consumer.close()
                consumer = None
                messages = []
                verbose_proxy_logger.warning("Kafka spend-log consumption failed. The consumer will rejoin its group")
            try:
                published: bool = publish_batch(db, producer, region, topic)
                if consumer is None:
                    stop.wait(1)
                elif not published and not messages:
                    stop.wait(0.1)
            except KafkaDeliveryFailed:
                verbose_proxy_logger.warning("Kafka spend-log delivery failed. The outbox batch will retry")
                stop.wait(1)
    finally:
        if consumer is not None:
            consumer.close()


def main() -> None:
    stop: Final = Event()

    def stop_replication(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, stop_replication)
    signal.signal(signal.SIGINT, stop_replication)
    try:
        with connect_regional_db(os.environ["DATABASE_URL"]) as db:
            run_replication(db, os.environ, stop)
    except (KeyError, ValueError, psycopg.Error, KafkaException, KafkaDeliveryFailed) as error:
        raise SystemExit(f"Kafka spend-log replication stopped ({type(error).__name__})") from None


if __name__ == "__main__":
    main()
