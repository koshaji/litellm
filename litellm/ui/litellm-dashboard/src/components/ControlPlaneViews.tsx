"use client";

import { useTranslation } from "react-i18next";
import { useState, type FormEvent, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  type ControlPlaneApi,
  type ControlPlaneDataplane,
  type ControlPlanePrincipal,
  type DataplaneRoute,
  hasScope,
} from "@/lib/http/controlPlane";
import { ErrorNotice, ResultsTable, ScopedButton } from "./ControlPlaneShared";
import { ControlPlaneAdmin } from "./ControlPlaneAdmin";
import { ControlPlaneLegacyActions } from "./ControlPlaneLegacyActions";

const management = {
  keys: { route: "key/list", label: "virtualKeys", fields: ["key_alias", "team_id", "user_id", "spend"] },
  teams: { route: "team/list", label: "teams", fields: ["team_id", "team_alias", "spend", "max_budget"] },
  users: { route: "user/list", label: "users", fields: ["user_id", "user_email", "user_role", "spend"] },
  models: { route: "v1/model/info", label: "models", fields: ["model_name", "model_id"] },
  budgets: { route: "budget/list", label: "budgets", fields: ["budget_id", "max_budget", "budget_duration"] },
  organizations: {
    route: "organization/list",
    label: "organizations",
    fields: ["organization_id", "organization_alias", "spend"],
  },
  guardrails: { route: "v2/guardrails/list", label: "guardrails", fields: ["guardrail_id", "guardrail_name"] },
  mcp: { route: "v1/mcp/server", label: "mcpServers", fields: ["server_id", "server_name", "transport"] },
  credentials: { route: "credentials", label: "credentials", fields: ["credential_name"] },
} as const satisfies Record<string, { route: DataplaneRoute; label: string; fields: readonly string[] }>;

type ManagementSection = keyof typeof management;

export const navigation = [
  { id: "overview", label: "overview" },
  ...(Object.keys(management) as ManagementSection[]).map(
    (id) =>
      ({
        id,
        label: management[id].label,
        scope: "proxy:read",
        dataplane: true,
      }) as const,
  ),
  { id: "fleet-models", label: "fleetModels", scope: "fleet:read" },
  { id: "fleet-teams", label: "fleetTeams", scope: "fleet:read" },
  { id: "usage", label: "fleetUsage", scope: "usage:read" },
  { id: "dataplanes", label: "dataplanes", scope: "dataplanes:read" },
  { id: "roles", label: "roles", scope: "roles:write" },
  { id: "audit", label: "auditLog", scope: "audit:read" },
  { id: "playground", label: "playground", reason: "noInference" },
  { id: "settings", label: "proxySettings", reason: "noProxySettings" },
] as const;

export type Section = (typeof navigation)[number]["id"];

function Coverage({
  result,
}: {
  result: { partial: boolean; unreachable: { id: string; reason: string }[]; truncated_dataplanes?: string[] };
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  if (!result.partial && !result.unreachable.length && !result.truncated_dataplanes?.length) return null;
  return (
    <div role="status" className="space-y-2 rounded border border-amber-500 p-3">
      {result.partial && <p>{t("partialResultsTotalsDoNotCoverEveryDataplane")}</p>}
      {result.unreachable.length > 0 && (
        <ul>
          {result.unreachable.map(({ id, reason }) => (
            <li key={id}>
              {t("unreachableLabel")} {id} ({reason})
            </li>
          ))}
        </ul>
      )}
      {!!result.truncated_dataplanes?.length && (
        <p>
          {t("truncatedLabel")} {result.truncated_dataplanes.join(", ")}
        </p>
      )}
    </div>
  );
}

function Fleet({ api, section }: { api: ControlPlaneApi; section: "fleet-models" | "fleet-teams" }) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const models = useQuery({ queryKey: ["fleet", "models"], queryFn: api.models, enabled: section === "fleet-models" });
  const teams = useQuery({ queryKey: ["fleet", "teams"], queryFn: api.teams, enabled: section === "fleet-teams" });
  const result = section === "fleet-models" ? models : teams;
  return (
    <>
      <ErrorNotice error={result.error} />
      {result.isPending && <p role="status">{t("loadingFleet")}</p>}
      {result.data && <Coverage result={result.data} />}
      {section === "fleet-models" && models.data && (
        <ResultsTable
          columns={[t("model"), t("dataplane"), t("provider"), t("mode")]}
          rows={models.data.items.map((row) => [row.model_name, row.dataplane_id, row.provider, row.mode])}
        />
      )}
      {section === "fleet-teams" && teams.data && (
        <ResultsTable
          columns={[t("team"), t("dataplane"), t("members"), t("spend")]}
          rows={teams.data.items.map((row) => [
            row.team_alias ?? row.team_id,
            row.dataplane_id,
            row.members,
            row.spend,
          ])}
        />
      )}
    </>
  );
}

const usageViews = ["summary", "daily", "top"] as const;

function Usage({ api }: { api: ControlPlaneApi }) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const [view, setView] = useState<"summary" | "daily" | "top">("summary");
  const [dates, setDates] = useState({ start: "", end: "" });
  const search = new URLSearchParams();
  if (dates.start) search.set("start_date", dates.start);
  if (dates.end) search.set("end_date", dates.end);
  const summary = useQuery({
    queryKey: ["usage", "summary", dates],
    queryFn: () => api.usageSummary(search),
    enabled: view === "summary",
  });
  const daily = useQuery({
    queryKey: ["usage", "daily", dates],
    queryFn: () => api.usageDaily(search),
    enabled: view === "daily",
  });
  const top = useQuery({
    queryKey: ["usage", "top", dates],
    queryFn: () => api.usageTop(search),
    enabled: view === "top",
  });
  const result = { summary, daily, top }[view];
  const metrics = (row: { spend: number; api_requests: number; total_tokens: number }) => [
    row.spend,
    row.api_requests,
    row.total_tokens,
  ];
  function applyDates(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const fields = new FormData(event.currentTarget);
    setDates({ start: String(fields.get("start") ?? ""), end: String(fields.get("end") ?? "") });
  }
  return (
    <>
      <div className="flex gap-2" aria-label={t("usageViews")}>
        {usageViews.map((item) => (
          <Button
            key={item}
            variant={view === item ? "secondary" : "outline"}
            aria-pressed={view === item}
            onClick={() => setView(item)}
          >
            {t(item)}
          </Button>
        ))}
      </div>
      <form className="flex flex-wrap items-end gap-3" onSubmit={applyDates}>
        <label>
          {t("startDate")}
          <Input type="date" name="start" />
        </label>
        <label>
          {t("endDate")}
          <Input type="date" name="end" />
        </label>
        <Button type="submit">{t("applyDates")}</Button>
      </form>
      <ErrorNotice error={result.error} />
      {result.isPending && <p role="status">{t("loadingUsage")}</p>}
      {result.data && (
        <>
          <p>
            {result.data.window.start_date} {t("to")} {result.data.window.end_date}
          </p>
          <Coverage result={result.data} />
        </>
      )}
      {view === "summary" && summary.data && (
        <ResultsTable
          columns={[t("dataplane"), t("spendUsd"), t("requests"), t("tokens")]}
          rows={[
            [t("fleetTotal"), ...metrics(summary.data.totals)],
            ...summary.data.by_dataplane.map((row) => [row.id, ...metrics(row.metrics)]),
          ]}
        />
      )}
      {view === "daily" && daily.data && (
        <ResultsTable
          columns={[t("date"), t("group"), t("spendUsd"), t("requests"), t("tokens")]}
          rows={daily.data.rows.map((row) => [row.date, row.group, ...metrics(row.metrics)])}
        />
      )}
      {view === "top" && top.data && (
        <ResultsTable
          columns={[t("group"), t("spendUsd"), t("requests"), t("tokens")]}
          rows={top.data.items.map((row) => [row.group, ...metrics(row.metrics)])}
        />
      )}
    </>
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function records(value: unknown, section: ManagementSection): Record<string, unknown>[] {
  if (Array.isArray(value)) return value.filter(isRecord);
  if (!isRecord(value)) return [];
  return [value.data, value.keys, value[section], value.credentials].find(Array.isArray)?.filter(isRecord) ?? [];
}

type Mutation = "create" | "edit" | "delete";
const rowActions = ["edit", "delete"] as const;
type ManagedResource = {
  id: string;
  field: string;
  deleteField: string;
  routes: Record<Mutation, DataplaneRoute>;
  numeric?: boolean;
  deleteOne?: boolean;
};

const managedResources: Partial<Record<ManagementSection, ManagedResource>> = {
  keys: {
    id: "token",
    field: "key_alias",
    deleteField: "keys",
    routes: { create: "key/generate", edit: "key/update", delete: "key/delete" },
  },
  teams: {
    id: "team_id",
    field: "team_alias",
    deleteField: "team_ids",
    routes: { create: "team/new", edit: "team/update", delete: "team/delete" },
  },
  users: {
    id: "user_id",
    field: "user_email",
    deleteField: "user_ids",
    routes: { create: "user/new", edit: "user/update", delete: "user/delete" },
  },
  budgets: {
    id: "budget_id",
    field: "max_budget",
    deleteField: "id",
    routes: { create: "budget/new", edit: "budget/update", delete: "budget/delete" },
    numeric: true,
    deleteOne: true,
  },
  organizations: {
    id: "organization_id",
    field: "organization_alias",
    deleteField: "organization_ids",
    routes: { create: "organization/new", edit: "organization/update", delete: "organization/delete" },
  },
  models: {
    id: "model_id",
    field: "model_name",
    deleteField: "id",
    routes: { create: "model/new", edit: "model/{model_id}/update", delete: "model/delete" },
    deleteOne: true,
  },
};

function rowValue(row: Record<string, unknown> | undefined, field: string) {
  const value = row?.[field];
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (field === "model_id" && isRecord(row?.model_info) && typeof row.model_info.id === "string")
    return row.model_info.id;
}

function providerModel(row: Record<string, unknown> | undefined) {
  return isRecord(row?.litellm_params) ? rowValue(row.litellm_params, "model") : undefined;
}

function mutationBody({
  section,
  resource,
  action,
  fields,
  id,
  row,
}: {
  section: ManagementSection;
  resource: ManagedResource;
  action: Mutation;
  fields: FormData;
  id?: string;
  row?: Record<string, unknown>;
}) {
  if (action === "delete") return { [resource.deleteField]: resource.deleteOne ? id : [id] };
  const value = String(fields.get("value") ?? "").trim();
  if (section === "models") {
    const model = String(fields.get("deployment") ?? "").trim();
    if (action === "edit") {
      return {
        ...(value !== rowValue(row, resource.field) ? { model_name: value } : {}),
        ...(model && model !== providerModel(row) ? { litellm_params: { model } } : {}),
      };
    }
    return {
      model_name: value,
      ...(model ? { litellm_params: { model } } : {}),
    };
  }
  return {
    [resource.field]: resource.numeric ? Number(value) : value,
    ...(id ? { [resource.id === "token" ? "key" : resource.id]: id } : {}),
    ...(section === "users" && action === "create" ? { auto_create_key: false } : {}),
  };
}

function ManagementFields({
  section,
  resource,
  row,
  action,
}: {
  section: ManagementSection;
  resource: ManagedResource;
  row?: Record<string, unknown>;
  action: Mutation;
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const value = rowValue(row, resource.field);
  if (action === "delete") return <p>{t("confirmDeleteResource", { id: value ?? rowValue(row, resource.id) })}</p>;
  return (
    <>
      <label>
        {section === "keys" ? t("keyAlias") : resource.field}
        <Input
          name="value"
          type={resource.numeric ? "number" : "text"}
          step={resource.numeric ? "any" : undefined}
          defaultValue={value}
          required
        />
      </label>
      {section === "models" && (
        <label>
          {t("providerModel")}
          <Input name="deployment" defaultValue={providerModel(row)} required={action === "create"} />
        </label>
      )}
    </>
  );
}

function managementPagination(section: ManagementSection, page: number) {
  if (section === "keys") {
    const params = { return_full_object: "true", include_team_keys: "true", page: String(page), size: "50" };
    return new URLSearchParams(params);
  }
  if (section === "users") return new URLSearchParams({ page: String(page), page_size: "25" });
}

function Management({
  api,
  principal,
  plane,
  section,
}: {
  api: ControlPlaneApi;
  principal: ControlPlanePrincipal;
  plane: ControlPlaneDataplane;
  section: ManagementSection;
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const client = useQueryClient();
  const [action, setAction] = useState<Mutation>();
  const [row, setRow] = useState<Record<string, unknown>>();
  const [newKey, setNewKey] = useState<string>();
  const [error, setError] = useState<unknown>();
  const [pending, setPending] = useState(false);
  const [page, setPage] = useState(1);
  const isKeys = section === "keys";
  const isPaginated = isKeys || section === "users";
  const config = management[section];
  const resource = managedResources[section];
  const queryKey = ["management", plane.id, section, page];
  const result = useQuery({
    queryKey,
    queryFn: () => {
      const query = managementPagination(section, page);
      return api.dataplane(principal, plane.id, config.route, query ? { query } : undefined);
    },
  });
  const scope = "proxy:write";
  const scopeReason = hasScope(principal, scope) ? undefined : t("requiresScope", { scope });
  const writeReason = resource ? scopeReason : t("managementWriteUnavailable");
  const id = resource && rowValue(row, resource.id);
  const resourceLabel = isKeys ? t("keyResource") : t(config.label).toLowerCase();
  const actionLabel = (action: Mutation) => t(`${action}Resource`, { label: resourceLabel });
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!resource || !action) return;
    if (writeReason || pending) return;
    if (action !== "create" && !id) return;
    const input = { section, resource, action, fields: new FormData(event.currentTarget), id, row };
    const body = mutationBody(input);
    if (Object.keys(body).length === 0) {
      setAction(undefined);
      return;
    }
    setPending(true);
    setError(undefined);
    try {
      const route = resource.routes[action];
      const response = await api.dataplane(principal, plane.id, route, { body, modelId: id });
      const createdKey = isKeys && action === "create" && isRecord(response) ? response.key : undefined;
      if (typeof createdKey === "string") {
        setNewKey(createdKey);
      }
      setAction(undefined);
      if (isPaginated && action === "delete") setPage(1);
      await client.invalidateQueries({ queryKey: ["management", plane.id, section] });
      if (section === "models" || section === "teams") {
        await client.invalidateQueries({ queryKey: ["fleet", section] });
      }
    } catch (error) {
      setError(error);
    } finally {
      setPending(false);
    }
  }
  const rows = records(result.data, section);
  const pageCount = isRecord(result.data) && typeof result.data.total_pages === "number" ? result.data.total_pages : 1;
  const hasOtherPages = Math.max(page, pageCount) > 1;
  const showPagination = isPaginated && hasOtherPages && !result.isPending;
  return (
    <>
      <p className="text-muted-foreground">
        {t("dataplaneLabel")} {plane.name}
      </p>
      <div className="flex flex-wrap gap-2">
        <ScopedButton
          reason={writeReason}
          disabled={pending}
          onClick={() => {
            setRow(undefined);
            setAction("create");
          }}
        >
          {actionLabel("create")}
        </ScopedButton>
        <ControlPlaneLegacyActions section={section} placement="toolbar" />
      </div>
      <ErrorNotice error={result.error} />
      <ErrorNotice error={error} />
      {newKey && (
        <div className="grid max-w-xl gap-2 rounded border p-4">
          <label>
            {t("copyThisKeyNowItWillNotBeShownAgain")}
            <Input readOnly value={newKey} />
          </label>
          <Button variant="outline" onClick={() => setNewKey(undefined)}>
            {t("dismissKey")}
          </Button>
        </div>
      )}
      {action && resource && (
        <form key={`${action}:${id ?? ""}`} className="grid max-w-sm gap-3 rounded border p-4" onSubmit={submit}>
          <h3 className="font-semibold">{actionLabel(action)}</h3>
          <fieldset disabled={pending} className="grid gap-3">
            <ManagementFields section={section} resource={resource} row={row} action={action} />
          </fieldset>
          <div className="flex gap-2">
            <Button type="submit" disabled={pending}>
              {action === "delete" ? t("confirmDelete") : t(isKeys ? "saveKey" : "save")}
            </Button>
            <Button variant="outline" disabled={pending} onClick={() => setAction(undefined)}>
              {t("cancel")}
            </Button>
          </div>
        </form>
      )}
      {result.isPending && <p role="status">{t("loadingView", { view: t(config.label) })}</p>}
      {result.data !== undefined && (
        <ResultsTable
          columns={[...config.fields, t("actions")]}
          rowKeys={resource ? rows.map((row) => rowValue(row, resource.id)) : undefined}
          rows={rows.map((row) => {
            const cells: ReactNode[] = config.fields.map((field) => rowValue(row, field));
            const hasId = resource && rowValue(row, resource.id);
            cells.push(
              <div className="flex flex-wrap gap-2">
                {rowActions.map((item) => (
                  <ScopedButton
                    key={item}
                    variant="outline"
                    reason={writeReason ?? (!hasId ? t("missingResourceId") : undefined)}
                    disabled={pending}
                    onClick={() => {
                      setRow(row);
                      setAction(item);
                    }}
                  >
                    {actionLabel(item)}
                  </ScopedButton>
                ))}
                <ControlPlaneLegacyActions section={section} placement="row" />
              </div>,
            );
            return cells;
          })}
        />
      )}
      {showPagination && (
        <div className="flex items-center gap-2">
          <Button variant="outline" disabled={page === 1} onClick={() => setPage(page - 1)}>
            {t("previous")}
          </Button>
          <span>
            {t("page")} {page} {t("of")} {pageCount}
          </span>
          <Button variant="outline" disabled={page >= pageCount} onClick={() => setPage(page + 1)}>
            {t("next")}
          </Button>
        </div>
      )}
    </>
  );
}

export function ControlPlaneViews({
  section,
  api,
  principal,
  selected,
}: {
  section: Section;
  api: ControlPlaneApi;
  principal: ControlPlanePrincipal;
  selected?: ControlPlaneDataplane;
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const label = navigation.find((item) => item.id === section)?.label;
  const isAdmin = section === "dataplanes" || section === "roles" || section === "audit";
  return (
    <>
      <h2 className="text-2xl font-semibold">{label && t(label)}</h2>
      {section === "overview" && <p>{t("selectADataplaneToManageItsResourcesOrOpenAFleetView")}</p>}
      {(section === "fleet-models" || section === "fleet-teams") && <Fleet section={section} api={api} />}
      {section === "usage" && <Usage api={api} />}
      {isAdmin && <ControlPlaneAdmin section={section} api={api} principal={principal} />}
      {selected && Object.hasOwn(management, section) && (
        <Management section={section as ManagementSection} api={api} principal={principal} plane={selected} />
      )}
    </>
  );
}
