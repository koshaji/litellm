import hmac
import json
import os
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from threading import Event
from typing import (
    Final,
    cast,  # noqa: TID251  # psycopg requires LiteralString for trusted checked-in SQL files
)
from unittest.mock import Mock
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from psycopg import Connection, sql
from psycopg.rows import TupleRow
from pydantic import JsonValue, TypeAdapter
from typing_extensions import LiteralString, TypedDict

from litellm.exceptions import BudgetExceededError
from litellm.models.end_user import LiteLLM_EndUserTable
from litellm.proxy import proxy_server
from litellm.proxy._types import LitellmUserRoles, UserAPIKeyAuth
from litellm.proxy.auth.auth_checks import check_end_user_budget
from litellm.proxy.auth.user_api_key_auth import user_api_key_auth
from litellm.proxy.common_utils.user_api_key_cache import UserApiKeyCache
from litellm.proxy.db import spend_log_replication
from litellm.proxy.db.db_transaction_queue.spend_logs_partition_manager import SpendLogsPartitionManager
from litellm.proxy.db.fleet_budget_spend import (
    read_replica_budget_spend,
    record_fleet_key_budget_reset,
    replicated_budget_entity_id,
)
from litellm.proxy.db.spend_log_replication import (
    KafkaDeliveryFailed,
    SpendLogEvent,
    connect_regional_db,
    consume_batch,
    publish_batch,
)
from litellm.proxy.spend_tracking.spend_management_endpoints import _ui_session_grouped_spend_logs, router
from litellm.proxy.utils import PrismaClient, ProxyLogging

_ROOT: Final = Path(__file__).resolve().parents[3]
_MIGRATION: Final = (
    _ROOT / "litellm/proxy-extras/litellm_proxy_extras/migrations/20261124040000_spend_log_replication/migration.sql"
)
_PARTITION_SCRIPT: Final = _ROOT / "litellm/scripts/db/partition_spend_logs.sql"
_UNPARTITION_SCRIPT: Final = _ROOT / "litellm/scripts/db/unpartition_spend_logs.sql"
_KAFKA_RECORD: Final = TypeAdapter(dict[str, str])
_KAFKA_OPTIONS: Final = TypeAdapter(Mapping[str, str | bool | int | float])
_JSON_VALUE: Final[TypeAdapter[JsonValue]] = TypeAdapter(JsonValue)
_REQUEST_ID_KEY: Final = "a3" * 32


def enable_capture(db: Connection[TupleRow], region: str) -> None:
    spend_log_replication.enable_capture(db, region, _REQUEST_ID_KEY)


def execute_sql_file(db: Connection[TupleRow], path: Path) -> None:
    db.execute(cast("LiteralString", path.read_text()))  # cast-ok: checked-in SQL file


@pytest.fixture
def regional_db() -> Iterator[Connection[TupleRow]]:
    schema: Final = sql.Identifier(f"Kafka_Replication_{uuid4().hex}")
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
        try:
            db.execute(sql.SQL("SET search_path TO {}").format(schema))
            db.execute(
                'CREATE TABLE "LiteLLM_SpendLogs" (request_id text PRIMARY KEY, spend double precision NOT NULL, '
                """metadata jsonb NOT NULL DEFAULT '{}', created_at timestamp(3) NOT NULL,
                updated_at timestamp(3) NOT NULL, call_type text NOT NULL DEFAULT 'acompletion',
                api_key text NOT NULL DEFAULT '', total_tokens int NOT NULL DEFAULT 5,
                prompt_tokens int NOT NULL DEFAULT 2, completion_tokens int NOT NULL DEFAULT 3,
                "startTime" timestamp(3) NOT NULL DEFAULT '2026-11-23 12:00:00',
                "endTime" timestamp(3) NOT NULL DEFAULT '2026-11-23 12:00:01',
                "completionStartTime" timestamp(3), request_duration_ms int,
                model text NOT NULL DEFAULT '', model_id text, model_group text, custom_llm_provider text,
                "user" text, team_id text, organization_id text, status text, litellm_call_id text,
                end_user text, session_id text, api_base text, cache_hit text, cache_key text,
                requester_ip_address text, mcp_namespaced_tool_name text, billing_agent_id text, agent_id text,
                messages jsonb NOT NULL DEFAULT '{}', response jsonb NOT NULL DEFAULT '{}',
                proxy_server_request jsonb NOT NULL DEFAULT '{}', request_tags jsonb NOT NULL DEFAULT '[]')"""
            )
            execute_sql_file(db, _MIGRATION)
            yield db
        finally:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))


def record_spend(db: Connection[TupleRow], request_id: str, spend: float) -> None:
    db.execute(
        'INSERT INTO "LiteLLM_SpendLogs" (request_id, spend, created_at, updated_at) '
        "VALUES (%s, %s, '2026-11-23 12:00:00', '2026-11-23 12:00:00')",
        (request_id, spend),
    )


def event(spend: float, updated_at: str, *, region: str = "us", start_time: str = "2026-11-23T12:00:00") -> Message:
    message: Final = Mock(spec=Message)
    message.error = Mock(return_value=None)
    message.key = Mock(return_value=b"remote-request")
    payload: Final = json.dumps(
        {
            "region": region,
            "row": {
                "request_id": "remote-request",
                "call_type": "acompletion",
                "spend": spend,
                "model": "fleet-model",
                "api_key": "a" * 64,
                "user": "fleet-user",
                "team_id": "fleet-team",
                "end_user": "fleet-end-user",
                "startTime": start_time,
                "endTime": "2026-11-23T12:00:01",
                "created_at": "2026-11-23T12:00:00",
                "updated_at": updated_at,
            },
        }
    ).encode()
    message.value = Mock(return_value=payload)
    return message


def test_replay_after_a_commit_failure_keeps_one_latest_row(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "eu")
    original: Final = event(0.13, "2026-11-23T12:00:00")
    updated: Final = event(0.27, "2026-11-23T12:00:01")
    consumer: Final = Mock(spec=Consumer)

    def interrupted_commit(*, asynchronous: bool) -> None:
        assert not asynchronous
        assert regional_db.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        raise KafkaDeliveryFailed("consumer stopped before committing its Kafka offset")

    consumer.commit = Mock(side_effect=interrupted_commit)
    with pytest.raises(KafkaDeliveryFailed, match="before committing"):
        consume_batch(regional_db, consumer, (original, updated), "eu")
    assert regional_db.execute('SELECT spend FROM "LiteLLM_SpendLogReporting"').fetchall() == [(0.27,)]

    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (updated, updated, original), "eu")
    assert regional_db.execute(
        'SELECT request_id, spend, model, metadata, source_region FROM "LiteLLM_SpendLogReporting"'
    ).fetchall() == [("remote-request", 0.27, "fleet-model", None, "us")]
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []


@pytest_asyncio.fixture(loop_scope="function")
async def fleet_prisma(
    regional_db: Connection[TupleRow], monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[PrismaClient]:
    enable_capture(regional_db, "eu")
    current: Final = regional_db.execute("SELECT current_schema()").fetchone()
    assert current is not None and isinstance(current[0], str)
    parsed: Final = urlsplit(os.environ["DATABASE_URL"])
    scoped_url: Final = urlunsplit(
        parsed._replace(query=urlencode({**dict(parse_qsl(parsed.query)), "schema": current[0]}))
    )
    monkeypatch.setenv("DATABASE_URL", scoped_url)
    prisma: Final = PrismaClient(scoped_url, ProxyLogging(user_api_key_cache=UserApiKeyCache()))
    await prisma.connect()
    monkeypatch.setattr(proxy_server, "prisma_client", prisma)
    try:
        yield prisma
    finally:
        await prisma.disconnect()


@pytest.mark.parametrize(
    "counter_key",
    ("spend:key:" + "a" * 64, "spend:team:fleet-team", "spend:user:fleet-user", "spend:end_user:fleet-end-user"),
)
async def test_fleet_budget_combines_local_and_remote_spend_once(
    regional_db: Connection[TupleRow], fleet_prisma: PrismaClient, counter_key: str
) -> None:
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    remote: Final = event(4.0, "2026-11-23T12:00:00")
    duplicate: Final = event(4.0, "2026-11-23T12:00:00", region="ap")
    assert await proxy_server.get_current_spend(counter_key, 7.0, max_budget=10.0) == 7.0
    consume_batch(regional_db, consumer, (remote, remote, duplicate), "eu")
    assert await proxy_server.get_current_spend(counter_key, 7.0, max_budget=10.0) == 11.0
    assert await proxy_server.get_current_spend(counter_key, 7.0) == 7.0
    assert (
        await read_replica_budget_spend(
            fleet_prisma, counter_key, window_start=datetime(2026, 11, 24, tzinfo=timezone.utc)
        )
        == 0.0
    )

    regional_db.execute('DELETE FROM "LiteLLM_ReplicatedSpendLogs"')
    consume_batch(regional_db, consumer, (remote,), "eu")
    assert await proxy_server.get_current_spend(counter_key, 7.0, max_budget=10.0) == 11.0
    assert regional_db.execute('SELECT spend FROM "LiteLLM_SpendLogs"').fetchall() == []


@pytest.mark.usefixtures("fleet_prisma")
async def test_end_user_budget_excludes_replica_spend_from_previous_window(
    regional_db: Connection[TupleRow],
) -> None:
    end_user: Final = LiteLLM_EndUserTable.model_validate(
        {
            "user_id": "fleet-end-user",
            "blocked": False,
            "spend": 7.0,
            "litellm_budget_table": {
                "max_budget": 10.0,
                "budget_duration": "1d",
                "budget_reset_at": "2026-11-25T00:00:00Z",
            },
        }
    )
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (event(9.0, "2026-11-23T12:00:01"),), "eu")
    await check_end_user_budget(end_user, "/v1/chat/completions")

    current: Final = event(4.0, "2026-11-24T12:00:01", start_time="2026-11-24T12:00:00")
    consume_batch(regional_db, consumer, (current,), "eu")
    with pytest.raises(BudgetExceededError) as exc:
        await check_end_user_budget(end_user, "/v1/chat/completions")
    assert exc.value.current_cost == 11.0


async def test_key_reset_excludes_replayed_spend_without_resetting_the_team(
    regional_db: Connection[TupleRow], fleet_prisma: PrismaClient
) -> None:
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    previous: Final = event(9.0, "2026-11-23T12:00:01")
    current: Final = event(4.0, "2026-11-24T12:00:01", start_time="2026-11-24T12:00:00")
    key: Final = "a" * 64
    consume_batch(regional_db, consumer, (previous,), "eu")
    assert await read_replica_budget_spend(fleet_prisma, f"spend:key:{key}") == 9.0
    async with fleet_prisma.writer_db.tx() as tx:
        await record_fleet_key_budget_reset(tx, key, datetime(2026, 11, 24, tzinfo=timezone.utc))
    consume_batch(regional_db, consumer, (previous, current), "eu")
    assert await read_replica_budget_spend(fleet_prisma, f"spend:key:{key}") == 4.0
    assert await read_replica_budget_spend(fleet_prisma, "spend:team:fleet-team") == 13.0


def test_remote_logs_do_not_reserve_local_request_ids_or_count_as_local_spend(
    regional_db: Connection[TupleRow],
) -> None:
    enable_capture(regional_db, "eu")
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (event(9.0, "2026-11-23T12:00:00"),), "eu")
    assert regional_db.execute('SELECT request_id, spend FROM "LiteLLM_SpendLogs"').fetchall() == []
    assert regional_db.execute('SELECT request_id, spend FROM "LiteLLM_SpendLogReporting"').fetchall() == [
        ("remote-request", 9.0)
    ]

    record_spend(regional_db, "remote-request", 0.04)
    consume_batch(regional_db, consumer, (event(19.0, "2026-11-23T12:00:01"),), "eu")

    assert regional_db.execute('SELECT request_id, spend FROM "LiteLLM_SpendLogs"').fetchall() == [
        ("remote-request", 0.04)
    ]
    assert regional_db.execute(
        'SELECT request_id, spend, source_region FROM "LiteLLM_SpendLogReporting"'
    ).fetchall() == [("remote-request", 0.04, None)]
    assert regional_db.execute('SELECT "row"->>\'request_id\' FROM "LiteLLM_SpendLogOutbox"').fetchall() == [
        ("remote-request",)
    ]
    regional_db.execute('DELETE FROM "LiteLLM_SpendLogs"')
    consume_batch(regional_db, consumer, (event(29.0, "2026-11-23T12:00:02"),), "eu")
    assert regional_db.execute('SELECT source_region, spend FROM "LiteLLM_FleetBudgetSpend"').fetchall() == [
        (None, 0.0)
    ]


def test_worker_rejoins_after_offset_commit_failure_and_replays_the_committed_row(
    regional_db: Connection[TupleRow], monkeypatch: pytest.MonkeyPatch
) -> None:
    stop: Final = Event()
    remote: Final = event(0.27, "2026-11-23T12:00:01")
    failed_consumer: Final = Mock(spec=Consumer)
    replacement: Final = Mock(spec=Consumer)
    failed_consumer.consume = Mock(return_value=[remote])
    failed_consumer.commit = Mock(side_effect=KafkaException(KafkaError(KafkaError.REBALANCE_IN_PROGRESS)))
    replacement.consume = Mock(return_value=[remote])

    def commit(*, asynchronous: bool) -> list[object]:
        assert not asynchronous
        stop.set()
        return []

    replacement.commit = Mock(side_effect=commit)
    monkeypatch.setattr(spend_log_replication, "Consumer", Mock(side_effect=[failed_consumer, replacement]))
    producer: Final = Mock(spec=Producer)
    producer.flush = Mock(return_value=0)
    monkeypatch.setattr(spend_log_replication, "Producer", Mock(return_value=producer))
    enable_capture(regional_db, "eu")
    record_spend(regional_db, "local-request", 0.04)

    spend_log_replication.run_replication(
        regional_db,
        {
            "LITELLM_SPEND_LOG_KAFKA_REGION": "eu",
            "LITELLM_SPEND_LOG_KAFKA_REQUEST_ID_KEY": _REQUEST_ID_KEY,
            "LITELLM_SPEND_LOG_KAFKA_CONFIG": '{"bootstrap.servers":"kafka.example:9092"}',
        },
        stop,
    )

    assert regional_db.execute('SELECT request_id, spend FROM "LiteLLM_ReplicatedSpendLogs"').fetchall() == [
        ("remote-request", 0.27)
    ]
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []
    failed_consumer.close.assert_called_once()
    replacement.subscribe.assert_called_once_with(["litellm-spend-logs"])


def test_replay_keeps_distinct_origins_and_request_times(regional_db: Connection[TupleRow]) -> None:
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    original: Final = event(0.1, "2026-11-23T12:00:00")
    another_region: Final = event(0.2, "2026-11-23T12:00:00", region="ap")
    another_request: Final = event(0.3, "2026-11-23T12:00:01", start_time="2026-11-23T12:00:01")
    consume_batch(regional_db, consumer, (original, another_region, another_request, original), "eu")

    assert regional_db.execute(
        'SELECT source_region, spend FROM "LiteLLM_SpendLogReporting" ORDER BY spend'
    ).fetchall() == [("us", 0.1), ("ap", 0.2), ("us", 0.3)]


def test_local_event_is_skipped_even_when_its_row_is_absent(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "us")
    consumer: Final = Mock(spec=Consumer)
    commit: Final = Mock(return_value=[])
    consumer.commit = commit
    consume_batch(regional_db, consumer, (event(0.13, "2026-11-23T12:00:00"),), "us")
    assert regional_db.execute('SELECT request_id FROM "LiteLLM_SpendLogReporting"').fetchall() == []
    commit.assert_called_once_with(asynchronous=False)


def test_replay_after_retention_preserves_pre_activation_local_budget_precedence(
    regional_db: Connection[TupleRow],
) -> None:
    record_spend(regional_db, "remote-request", 0.13)
    enable_capture(regional_db, "eu")
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (event(0.13, "2026-11-23T12:00:00"),), "eu")

    regional_db.execute('DELETE FROM "LiteLLM_SpendLogs" WHERE request_id = %s', ("remote-request",))
    consume_batch(regional_db, consumer, (event(0.27, "2026-11-23T12:00:01"),), "eu")

    assert regional_db.execute(
        'SELECT request_id, source_region, spend FROM "LiteLLM_FleetBudgetSpend"'
    ).fetchall() == [("remote-request", None, 0.0)]


def test_first_import_after_retention_preserves_pre_activation_local_budget_precedence(
    regional_db: Connection[TupleRow],
) -> None:
    record_spend(regional_db, "remote-request", 0.13)
    enable_capture(regional_db, "eu")
    regional_db.execute('DELETE FROM "LiteLLM_SpendLogs" WHERE request_id = %s', ("remote-request",))
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (event(0.13, "2026-11-23T12:00:00"),), "eu")

    assert regional_db.execute(
        'SELECT request_id, source_region, spend FROM "LiteLLM_FleetBudgetSpend"'
    ).fetchall() == [("remote-request", None, 0.0)]
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []


async def test_first_import_after_partition_retention_preserves_local_budget_precedence(
    regional_db: Connection[TupleRow], fleet_prisma: PrismaClient
) -> None:
    execute_sql_file(regional_db, _PARTITION_SCRIPT)
    regional_db.execute('ALTER TABLE "LiteLLM_SpendLogs" DETACH PARTITION "LiteLLM_SpendLogs_pdefault"')
    regional_db.execute(
        'CREATE TABLE "old_partition" PARTITION OF "LiteLLM_SpendLogs" '
        "FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')"
    )
    with regional_db.transaction():
        regional_db.execute("SET LOCAL litellm.spend_log_replica = 'on'")
        regional_db.execute(
            'INSERT INTO "LiteLLM_SpendLogs" (request_id, spend, "startTime", created_at, updated_at) '
            "VALUES ('remote-request', 0.13, '2025-01-01', '2025-01-01', '2025-01-01')"
        )

    dropped: Final = await SpendLogsPartitionManager().drop_partitions_older_than(
        fleet_prisma, datetime(2025, 3, 1, tzinfo=timezone.utc), lambda: 5000
    )
    assert dropped == ["old_partition"]
    assert regional_db.execute("SELECT to_regclass('old_partition')").fetchone() == (None,)
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(
        regional_db,
        consumer,
        (event(0.13, "2025-01-01T00:00:00", start_time="2025-01-01T00:00:00"),),
        "eu",
    )
    assert regional_db.execute(
        'SELECT request_id, source_region, spend FROM "LiteLLM_FleetBudgetSpend"'
    ).fetchall() == [("remote-request", None, 0.0)]


class _GroupedLog(TypedDict):
    source_region: str | None
    spend: float


async def test_grouped_reporting_pages_preserve_region_and_timestamp_identity(
    regional_db: Connection[TupleRow], monkeypatch: pytest.MonkeyPatch
) -> None:
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(
        regional_db,
        consumer,
        (
            event(1.0, "2026-11-23T12:00:00", region="us"),
            event(2.0, "2026-11-23T12:00:00", region="apac"),
            event(3.0, "2026-11-23T12:00:00", region="us", start_time="2026-11-23T11:00:00"),
        ),
        "eu",
    )
    current: Final = regional_db.execute("SELECT current_schema()").fetchone()
    assert current is not None and isinstance(current[0], str)
    parsed: Final = urlsplit(os.environ["DATABASE_URL"])
    scoped_url: Final = urlunsplit(
        parsed._replace(query=urlencode({**dict(parse_qsl(parsed.query)), "schema": current[0]}))
    )
    monkeypatch.setenv("DATABASE_URL", scoped_url)
    prisma: Final = PrismaClient(scoped_url, ProxyLogging(user_api_key_cache=UserApiKeyCache()))
    await prisma.connect()
    try:

        async def page(cursor: str | None) -> Mapping[str, object]:
            return await _ui_session_grouped_spend_logs(
                prisma, (), (), 1, "success", 1, 1, True, cursor, relation='"LiteLLM_SpendLogReporting"'
            )

        first: Final = await page(None)
        assert first["total"] == 3
        first_cursor: Final = first["next_session_cursor"]
        assert isinstance(first_cursor, str)
        second: Final = await page(first_cursor)
        second_cursor: Final = second["next_session_cursor"]
        assert isinstance(second_cursor, str)
        third: Final = await page(second_cursor)
        assert third["next_session_cursor"] is None
        logs: Final = TypeAdapter(list[_GroupedLog])
        assert [logs.validate_python(result["data"]) for result in (first, second, third)] == [
            [{"source_region": "us", "spend": 1.0}],
            [{"source_region": "apac", "spend": 2.0}],
            [{"source_region": "us", "spend": 3.0}],
        ]
    finally:
        await prisma.disconnect()


async def test_admin_log_filters_match_local_and_encoded_imported_ids(
    regional_db: Connection[TupleRow], fleet_prisma: PrismaClient
) -> None:
    entity_id: Final = "sha256:customer"
    api_key: Final = "a" * 64
    record_spend(regional_db, "local-request", 1.0)
    regional_db.execute(
        """UPDATE "LiteLLM_SpendLogs" SET "user" = %s, team_id = %s, end_user = %s, api_key = %s""",
        (entity_id, entity_id, entity_id, api_key),
    )
    remote: Final = event(2.0, "2026-11-23T12:00:00")
    remote_payload: Final = SpendLogEvent.model_validate_json(remote.value())
    encoded: Final = replicated_budget_entity_id(entity_id)
    remote.value = Mock(
        return_value=remote_payload.model_copy(
            update={
                "row": remote_payload.row.model_copy(update={"user": encoded, "team_id": encoded, "end_user": encoded})
            }
        )
        .model_dump_json()
        .encode()
    )
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (remote,), "eu")

    app: Final = FastAPI()
    app.include_router(router)
    app.dependency_overrides[user_api_key_auth] = lambda: UserAPIKeyAuth(user_role=LitellmUserRoles.PROXY_ADMIN)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for field in ("team_id", "user_id", "end_user", "search"):
            response = await client.get(
                "/spend/logs/ui",
                params={
                    field: entity_id,
                    "api_key": api_key,
                    "start_date": "2026-11-23 00:00:00",
                    "end_date": "2026-11-24 00:00:00",
                },
            )
            assert response.status_code == 200, response.text
            assert {row["request_id"] for row in response.json()["data"]} == {"local-request", "remote-request"}, field


def test_worker_connects_to_the_prisma_schema(regional_db: Connection[TupleRow]) -> None:
    current: Final = regional_db.execute("SELECT current_schema()").fetchone()
    assert current is not None and isinstance(current[0], str)
    parsed: Final = urlsplit(os.environ["DATABASE_URL"])
    query: Final = {
        **dict(parse_qsl(parsed.query)),
        "schema": current[0],
        "connection_limit": "4",
        "pool_timeout": "15",
        "options": "-c statement_timeout=3000",
    }
    database_url: Final = urlunsplit(parsed._replace(query=urlencode(query, quote_via=quote)))
    with connect_regional_db(database_url) as worker_db:
        enable_capture(worker_db, "eu")
        assert worker_db.execute("SHOW statement_timeout").fetchone() == ("3s",)
    assert regional_db.execute('SELECT region FROM "LiteLLM_SpendLogReplication"').fetchall() == [("eu",)]


@pytest.mark.parametrize("protocol", [None, "sasl_ssl", "PLAINTEXT"], ids=["default", "sasl", "development"])
def test_worker_uses_tls_unless_plaintext_is_explicit(
    regional_db: Connection[TupleRow], monkeypatch: pytest.MonkeyPatch, protocol: str | None
) -> None:
    producer: Final = Mock(spec=Producer)
    consumer: Final = Mock(spec=Consumer)
    new_producer: Final = Mock(return_value=producer)
    new_consumer: Final = Mock(return_value=consumer)
    monkeypatch.setattr(spend_log_replication, "Producer", new_producer)
    monkeypatch.setattr(spend_log_replication, "Consumer", new_consumer)
    kafka: Final = {"bootstrap.servers": "kafka.example:9092"}
    stop: Final = Event()

    def consume(*, num_messages: int, timeout: float) -> list[Message]:
        stop.set()
        return []

    consumer.consume = Mock(side_effect=consume)
    spend_log_replication.run_replication(
        regional_db,
        {
            "LITELLM_SPEND_LOG_KAFKA_REGION": "us",
            "LITELLM_SPEND_LOG_KAFKA_REQUEST_ID_KEY": _REQUEST_ID_KEY,
            "LITELLM_SPEND_LOG_KAFKA_CONFIG": json.dumps(
                kafka if protocol is None else {**kafka, "security.protocol": protocol}
            ),
        },
        stop,
    )
    expected: Final = "SSL" if protocol is None else protocol.upper()
    producer_config: Final = _KAFKA_OPTIONS.validate_python(new_producer.call_args.args[0])
    consumer_config: Final = _KAFKA_OPTIONS.validate_python(new_consumer.call_args.args[0])
    assert producer_config["security.protocol"] == expected
    assert consumer_config["security.protocol"] == expected


@pytest.mark.parametrize(
    "options",
    [
        {"security.protocol": "SASL_PLAINTEXT"},
        {"enable.ssl.certificate.verification": False},
        {"ssl.endpoint.identification.algorithm": "none"},
    ],
    ids=["plaintext-password", "certificate", "hostname"],
)
def test_worker_rejects_unsafe_kafka_tls_settings(
    regional_db: Connection[TupleRow], monkeypatch: pytest.MonkeyPatch, options: Mapping[str, str | bool]
) -> None:
    monkeypatch.setattr(spend_log_replication, "Producer", Mock(return_value=Mock(spec=Producer)))
    monkeypatch.setattr(spend_log_replication, "Consumer", Mock(return_value=Mock(spec=Consumer)))
    stop: Final = Event()
    stop.set()
    with pytest.raises(ValueError, match="Kafka TLS"):
        spend_log_replication.run_replication(
            regional_db,
            {
                "LITELLM_SPEND_LOG_KAFKA_REGION": "us",
                "LITELLM_SPEND_LOG_KAFKA_REQUEST_ID_KEY": _REQUEST_ID_KEY,
                "LITELLM_SPEND_LOG_KAFKA_CONFIG": json.dumps({"bootstrap.servers": "localhost:1", **options}),
            },
            stop,
        )


def test_replay_survives_a_partition_swap_on_the_same_connection(regional_db: Connection[TupleRow]) -> None:
    record_spend(regional_db, "local-before-setup", 0.12)
    enable_capture(regional_db, "eu")
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    original: Final = event(0.13, "2026-11-23T12:00:00")
    updated: Final = event(0.27, "2026-11-23T12:00:01")
    consume_batch(regional_db, consumer, (original,), "eu")
    assert regional_db.execute('SELECT spend FROM "LiteLLM_ReplicatedSpendLogs"').fetchall() == [(0.13,)]

    execute_sql_file(regional_db, _PARTITION_SCRIPT)
    with regional_db.transaction():
        regional_db.execute("SET LOCAL litellm.spend_log_replica = 'on'")
        regional_db.execute('INSERT INTO "LiteLLM_SpendLogs" SELECT * FROM "LiteLLM_SpendLogs_legacy"')
    consume_batch(regional_db, consumer, (original, updated, original), "eu")
    execute_sql_file(regional_db, _UNPARTITION_SCRIPT)
    consume_batch(regional_db, consumer, (original,), "eu")

    assert regional_db.execute(
        'SELECT request_id, spend FROM "LiteLLM_SpendLogReporting" ORDER BY request_id'
    ).fetchall() == [("local-before-setup", 0.12), ("remote-request", 0.27)]
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []
    record_spend(regional_db, "local-after-swap", 0.04)
    assert regional_db.execute('SELECT "row"->>\'request_id\' FROM "LiteLLM_SpendLogOutbox"').fetchall() == [
        ("local-after-swap",)
    ]


def test_pause_survives_worker_restart_and_allows_remote_rows(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "eu")
    record_spend(regional_db, "before-pause", 0.05)
    regional_db.execute('UPDATE "LiteLLM_SpendLogReplication" SET enabled = false WHERE id = 1')
    enable_capture(regional_db, "eu")
    record_spend(regional_db, "during-pause", 0.13)

    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (event(0.13, "2026-11-23T12:00:00"),), "eu")
    regional_db.execute('UPDATE "LiteLLM_SpendLogReplication" SET enabled = true WHERE id = 1')
    record_spend(regional_db, "after-resuming", 0.27)

    assert regional_db.execute('SELECT request_id FROM "LiteLLM_SpendLogReporting" ORDER BY request_id').fetchall() == [
        ("after-resuming",),
        ("before-pause",),
        ("during-pause",),
        ("remote-request",),
    ]
    assert regional_db.execute(
        'SELECT "row"->>\'request_id\' FROM "LiteLLM_SpendLogOutbox" ORDER BY id'
    ).fetchall() == [
        ("before-pause",),
        ("after-resuming",),
    ]


def test_unreachable_broker_leaves_committed_spend_and_retries_outbox(regional_db: Connection[TupleRow]) -> None:
    record_spend(regional_db, "before-enabling", 0.05)
    enable_capture(regional_db, "us")
    record_spend(regional_db, "after-enabling", 0.13)
    producer: Final = Mock(spec=Producer)
    flush: Final = Mock(return_value=1)
    producer.flush = flush
    produce: Final = Mock()
    producer.produce = produce
    with pytest.raises(KafkaDeliveryFailed, match="did not acknowledge"):
        publish_batch(regional_db, producer, "us", "spend")

    assert regional_db.execute('SELECT request_id, spend FROM "LiteLLM_SpendLogs" ORDER BY request_id').fetchall() == [
        ("after-enabling", 0.13),
        ("before-enabling", 0.05),
    ]
    assert regional_db.execute('SELECT "row"->>\'request_id\' FROM "LiteLLM_SpendLogOutbox"').fetchall() == [
        ("after-enabling",)
    ]

    flush.return_value = 0
    assert publish_batch(regional_db, producer, "us", "spend")
    sent: Final = tuple(
        _KAFKA_RECORD.validate_python({"key": call.kwargs["key"], "value": call.kwargs["value"]})
        for call in produce.call_args_list
    )
    assert len(sent) == 2
    assert (sent[0]["key"], sent[0]["value"]) == (sent[1]["key"], sent[1]["value"])
    assert _JSON_VALUE.validate_json(sent[1]["value"]) == expected_payload("after-enabling")
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []


@pytest.mark.parametrize("reject_at_produce", [True, False], ids=["client", "broker"])
def test_oversized_event_is_removed_from_outbox_without_blocking_other_rows(
    regional_db: Connection[TupleRow], reject_at_produce: bool
) -> None:
    enable_capture(regional_db, "us")
    record_spend(regional_db, "oversized", 0.13)
    record_spend(regional_db, "normal", 0.27)
    producer: Final = Mock(spec=Producer)
    flush: Final = Mock(return_value=0)
    producer.flush = flush

    def produce(topic: str, *, key: str, value: str, on_delivery: Callable[[KafkaError | None, Message], None]) -> None:
        if key == "oversized" or (not reject_at_produce and flush.call_count == 0):
            error: Final = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
            if reject_at_produce:
                raise KafkaException(error)
            on_delivery(error, Mock(spec=Message))

    sent: Final = Mock(side_effect=produce)
    producer.produce = sent
    assert publish_batch(regional_db, producer, "us", "spend")
    assert not publish_batch(regional_db, producer, "us", "spend")
    assert sent.call_count == (2 if reject_at_produce else 4)
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []
    assert regional_db.execute('SELECT COUNT(*) FROM "LiteLLM_SpendLogs"').fetchone() == (2,)


def test_serialized_kafka_event_size_is_bounded(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "us")
    record_spend(regional_db, "oversized", 0.13)
    regional_db.execute("UPDATE \"LiteLLM_SpendLogs\" SET model = repeat('é', 150000)")
    producer: Final = Mock(spec=Producer)
    sent: Final = Mock()
    producer.produce = sent
    producer.flush = Mock(return_value=0)

    assert publish_batch(regional_db, producer, "us", "spend")
    recorded: Final = tuple(SpendLogEvent.model_validate_json(call.kwargs["value"]).row for call in sent.call_args_list)
    assert [(row.model, row.spend) for row in recorded] == [("", 0.13), ("", 0.13)]
    assert regional_db.execute('SELECT id FROM "LiteLLM_SpendLogOutbox"').fetchall() == []


async def test_large_end_user_identifier_preserves_all_fleet_budget_charges(
    regional_db: Connection[TupleRow], fleet_prisma: PrismaClient
) -> None:
    end_user: Final = "large-end-user-" + "é" * 150_000
    api_key: Final = "a" * 64
    regional_db.execute(
        """INSERT INTO "LiteLLM_SpendLogOutbox" ("row") VALUES (
            %s::jsonb || jsonb_build_object('end_user', %s::text, 'api_key', %s::text,
                'team_id', 'fleet-team', 'user', 'fleet-user', 'spend', 4)
        )""",
        (json.dumps(expected_payload("large-id")["row"]), end_user, api_key),
    )
    producer: Final = Mock(spec=Producer)
    producer.flush = Mock(return_value=0)
    produce: Final = Mock()
    producer.produce = produce
    assert publish_batch(regional_db, producer, "us", "spend")
    produce.assert_called_once()
    sent: Final = _KAFKA_RECORD.validate_python(
        {"key": produce.call_args.kwargs["key"], "value": produce.call_args.kwargs["value"]}
    )
    message: Final = Mock(spec=Message)
    message.error = Mock(return_value=None)
    message.key = Mock(return_value=sent["key"].encode())
    message.value = Mock(return_value=sent["value"].encode())
    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    consume_batch(regional_db, consumer, (message, message), "eu")
    assert await proxy_server.get_current_spend(f"spend:key:{api_key}", 7.0, max_budget=10.0) == 11.0
    assert await proxy_server.get_current_spend("spend:team:fleet-team", 7.0, max_budget=10.0) == 11.0
    assert await proxy_server.get_current_spend("spend:user:fleet-user", 7.0, max_budget=10.0) == 11.0
    assert await read_replica_budget_spend(fleet_prisma, f"spend:end_user:{end_user}") == 4.0


def test_retryable_rows_remain_queued_when_an_oversized_event_is_removed(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "us")
    record_spend(regional_db, "oversized", 0.13)
    record_spend(regional_db, "retryable", 0.27)
    producer: Final = Mock(spec=Producer)
    producer.produce = Mock(side_effect=[KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)), None])
    producer.flush = Mock(return_value=1)

    with pytest.raises(KafkaDeliveryFailed, match="did not acknowledge"):
        publish_batch(regional_db, producer, "us", "spend")

    assert regional_db.execute('SELECT "row"->>\'request_id\' FROM "LiteLLM_SpendLogOutbox"').fetchall() == [
        ("retryable",)
    ]
    assert regional_db.execute('SELECT request_id FROM "LiteLLM_SpendLogs" ORDER BY request_id').fetchall() == [
        ("oversized",),
        ("retryable",),
    ]


def expected_payload(request_id: str) -> Mapping[str, JsonValue]:
    return {
        "region": "us",
        "row": {
            "request_id": request_id,
            "call_type": "acompletion",
            "api_key": "",
            "spend": 0.13,
            "total_tokens": 5,
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "startTime": "2026-11-23T12:00:00",
            "endTime": "2026-11-23T12:00:01",
            "request_duration_ms": None,
            "completionStartTime": None,
            "model": "",
            "model_id": None,
            "model_group": None,
            "custom_llm_provider": None,
            "user": None,
            "end_user": None,
            "team_id": None,
            "organization_id": None,
            "status": None,
            "created_at": "2026-11-23T12:00:00",
            "updated_at": "2026-11-23T12:00:00",
        },
    }


def test_kafka_payload_excludes_all_request_and_response_content(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "us")
    record_spend(regional_db, "content-probe", 0.13)
    regional_db.execute(
        """UPDATE "LiteLLM_SpendLogs"
           SET messages = '{"content":"private prompt"}', response = '{"output":"private output"}',
               proxy_server_request = '{"body":{"input":"private input"},"headers":{"private":"header"}}',
               metadata = '{"arbitrary":"private metadata"}', request_tags = '["private tag"]',
               api_key = 'private credential', litellm_call_id = 'client correlation ID'
           WHERE request_id = 'content-probe'"""
    )
    assert regional_db.execute(
        'SELECT litellm_call_id FROM "LiteLLM_SpendLogs" WHERE request_id = %s', ("content-probe",)
    ).fetchall() == [("client correlation ID",)]
    expected: Final = expected_payload("content-probe")
    assert regional_db.execute('SELECT "row" FROM "LiteLLM_SpendLogOutbox" ORDER BY id').fetchall() == [
        (expected["row"],),
        (expected["row"],),
    ]

    regional_db.execute(
        """UPDATE "LiteLLM_SpendLogOutbox" SET "row" = "row" ||
           '{"messages":{"content":"stale outbox prompt"},"litellm_call_id":"client correlation ID"}'"""
    )
    producer: Final = Mock(spec=Producer)
    producer.flush = Mock(return_value=0)
    produce: Final = Mock()
    producer.produce = produce
    assert publish_batch(regional_db, producer, "us", "spend")
    sent: Final = tuple(
        _KAFKA_RECORD.validate_python({"key": call.kwargs["key"], "value": call.kwargs["value"]})
        for call in produce.call_args_list
    )
    assert tuple(_JSON_VALUE.validate_json(record["value"]) for record in sent) == (
        expected,
        expected,
    )


def test_header_derived_request_id_stays_local(regional_db: Connection[TupleRow]) -> None:
    enable_capture(regional_db, "us")
    regional_db.execute(
        """INSERT INTO "LiteLLM_SpendLogs" (request_id, litellm_call_id, spend, created_at, updated_at)
           VALUES ('caller header_video_cost', 'caller header', 0.13, '2026-11-23', '2026-11-23')"""
    )
    regional_db.execute('UPDATE "LiteLLM_SpendLogs" SET spend = 0.23')
    producer: Final = Mock(spec=Producer)
    producer.flush = Mock(return_value=0)
    produce: Final = Mock()
    producer.produce = produce
    assert publish_batch(regional_db, producer, "us", "spend")
    records: Final = tuple(
        _KAFKA_RECORD.validate_python({"key": call.kwargs["key"], "value": call.kwargs["value"]})
        for call in produce.call_args_list
    )
    assert len(records) == 2
    digest: Final = hmac.digest(bytes.fromhex(_REQUEST_ID_KEY), b"caller header_video_cost", "sha256")
    replica_id: Final = f"replica:{digest.hex()}"
    assert tuple(record["key"] for record in records) == (replica_id, replica_id)
    published: Final = tuple(SpendLogEvent.model_validate_json(record["value"]).row for record in records)
    assert tuple((row.request_id, row.spend) for row in published) == ((replica_id, 0.13), (replica_id, 0.23))
    assert all("caller header" not in record["value"] for record in records)

    consumer: Final = Mock(spec=Consumer)
    consumer.commit = Mock(return_value=[])
    replicas: Final = tuple(
        Mock(
            spec=Message,
            error=Mock(return_value=None),
            key=Mock(return_value=replica_id.encode()),
            value=Mock(return_value=SpendLogEvent(region=region, row=published[1]).model_dump_json().encode()),
        )
        for region in ("eu", "ap")
    )
    consume_batch(regional_db, consumer, replicas, "us")
    assert regional_db.execute(
        'SELECT request_id, source_region, spend FROM "LiteLLM_FleetBudgetSpend"'
    ).fetchall() == [(replica_id, None, 0.0)]
    assert regional_db.execute('SELECT request_id, source_region FROM "LiteLLM_SpendLogReporting"').fetchall() == [
        ("caller header_video_cost", None)
    ]
    assert regional_db.execute('SELECT request_id, litellm_call_id, spend FROM "LiteLLM_SpendLogs"').fetchall() == [
        ("caller header_video_cost", "caller header", 0.23)
    ]
