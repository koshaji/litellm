"use client";

import { useTranslation } from "react-i18next";
import { productNames } from "@/i18n/untranslated/productNames";
import { useEffect, useMemo, useState, type FormEvent } from "react";
import { QueryClient, QueryClientProvider, useQueries, useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { TooltipProvider } from "@/components/ui/tooltip";
import {
  controlPlaneApi,
  controlPlaneSessionKey,
  hasDataplane,
  hasScope,
  type ControlPlaneApi,
  type ControlPlanePrincipal,
} from "@/lib/http/controlPlane";
import { ControlPlaneViews, navigation, type Section } from "./ControlPlaneViews";
import { ErrorNotice, ScopedButton } from "./ControlPlaneShared";

function Login({ prefix, token, onLogin }: { prefix: string; token: string | null; onLogin: (token: string) => void }) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<unknown>();
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const fields = new FormData(event.currentTarget);
    setPending(true);
    setError(undefined);
    try {
      const response = await controlPlaneApi(prefix).login(
        String(fields.get("username") ?? ""),
        String(fields.get("password") ?? ""),
      );
      onLogin(response.access_token);
    } catch (error) {
      setError(error);
    } finally {
      setPending(false);
    }
  }
  return (
    <main className="mx-auto mt-24 max-w-sm rounded-xl border bg-card p-8 shadow-sm">
      <p className="text-sm font-semibold text-muted-foreground">{productNames.liteLLM}</p>
      <h1 className="mb-6 text-2xl font-semibold">{t("controlPlaneLogin")}</h1>
      <form className="grid gap-4" onSubmit={submit}>
        <label className="grid gap-1">
          {t("username")}
          <Input name="username" autoComplete="username" required />
        </label>
        <label className="grid gap-1">
          {t("password")}
          <Input name="password" type="password" autoComplete="current-password" required />
        </label>
        <ErrorNotice
          error={error ?? (token ? new Error("Your control plane session expired. Sign in again.") : undefined)}
        />
        <Button type="submit" disabled={pending}>
          {pending ? t("signingIn") : t("signIn")}
        </Button>
      </form>
    </main>
  );
}

const unavailablePages = [
  "apps.title",
  "apps.groups.title",
  "apps.internalAccess.title",
  "customerPortal.budgetGroups.title",
  "shell.nav.items.agents",
  "shell.nav.items.workflowRuns",
  "shell.nav.items.memory",
  "shell.nav.items.skills",
  "shell.nav.items.policies",
  "shell.nav.items.searchTools",
  "shell.nav.items.vectorStores",
  "shell.nav.items.toolPolicies",
  "shell.nav.items.alerts",
  "shell.nav.items.costOptimization",
  "shell.nav.items.logs",
  "shell.nav.items.guardrailsMonitor",
  "shell.nav.items.projects",
  "shell.nav.items.accessGroups",
  "shell.nav.items.aiHub",
  "shell.nav.items.responseCache",
  "shell.nav.items.prompts",
  "shell.nav.items.apiPlayground",
  "shell.nav.items.tagManagement",
  "shell.nav.items.routerSettings",
  "shell.nav.items.loggingAndAlerts",
  "shell.nav.items.adminSettings",
  "shell.nav.items.costTracking",
  "shell.nav.items.uiTheme",
  "shell.nav.items.telemetry",
] as const;

function UnavailablePages() {
  const { t } = useTranslation();
  return unavailablePages.map((key) => (
    <ScopedButton key={key} reason={t("controlPlane.pageUnavailable")}>
      {t(key)}
    </ScopedButton>
  ));
}

function Workspace({
  api,
  principal,
  logout,
}: {
  api: ControlPlaneApi;
  principal: ControlPlanePrincipal;
  logout: () => void;
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const [section, setSection] = useState<Section>("overview");
  const [selection, setSelection] = useState("");
  const listEnabled = hasScope(principal, "dataplanes:read");
  const planes = useQuery({ queryKey: ["control-plane", "dataplanes"], queryFn: api.dataplanes, enabled: listEnabled });
  const visible = (planes.data?.items ?? []).filter((plane) => hasDataplane(principal, plane.id));
  const selected = visible.find((plane) => plane.id === selection) ?? visible[0];
  const health = useQueries({
    queries: visible.map((plane) => ({
      queryKey: ["control-plane", "health", plane.id],
      queryFn: () => api.health(plane.id),
      refetchInterval: 15000,
    })),
  });
  function unavailable(item: (typeof navigation)[number]) {
    if ("reason" in item) return t(item.reason);
    if ("scope" in item && !hasScope(principal, item.scope)) return t("requiresScope", { scope: item.scope });
    if ("dataplane" in item && !selected) return t("selectADataplaneToOpenThisPage");
  }
  const active = navigation.find((item) => item.id === section);
  const reason = active && unavailable(active);
  return (
    <TooltipProvider>
      <div className="min-h-screen bg-background text-foreground">
        <header className="flex flex-wrap items-center justify-between gap-4 border-b bg-card px-6 py-4">
          <div>
            <p className="text-sm text-muted-foreground">{productNames.liteLLM}</p>
            <h1 className="text-xl font-semibold">{t("globalControlPlane")}</h1>
          </div>
          <div className="flex flex-wrap items-center gap-4">
            <label className="flex items-center gap-2 text-sm font-medium">
              {t("dataplane")}
              <select
                className="rounded border bg-background px-3 py-2"
                value={selected?.id ?? ""}
                disabled={!listEnabled || !visible.length}
                onChange={(event) => setSelection(event.target.value)}
              >
                {!visible.length && (
                  <option value="">{planes.isFetching ? t("loadingDataplanes") : t("noDataplanesAvailable")}</option>
                )}
                {visible.map((plane, index) => (
                  <option value={plane.id} key={plane.id}>
                    {plane.name} (
                    {health[index].data?.status ??
                      (health[index].isError ? t("healthUnavailable") : t("checkingHealth"))}
                    )
                  </option>
                ))}
              </select>
            </label>
            <span className="text-sm text-muted-foreground">{principal.email ?? principal.subject}</span>
            <Button variant="outline" onClick={logout}>
              {t("signOut")}
            </Button>
          </div>
        </header>
        {visible.length > 0 && (
          <ul aria-label={t("dataplaneHealth")} className="flex flex-wrap gap-3 border-b px-6 py-3 text-sm">
            {visible.map((plane, index) => (
              <li key={plane.id} className="rounded-full border px-3 py-1">
                {plane.name} (
                {health[index].data?.status ?? (health[index].isError ? t("healthUnavailable") : t("checkingHealth"))})
              </li>
            ))}
          </ul>
        )}
        <div className="grid min-h-screen md:grid-cols-[14rem_1fr]">
          <nav aria-label={t("controlPlanePages")} className="grid content-start gap-2 border-r p-4">
            {navigation.map((item) => {
              const reason = unavailable(item);
              return (
                <ScopedButton
                  key={item.id}
                  variant={section === item.id ? "secondary" : "ghost"}
                  aria-current={section === item.id ? "page" : undefined}
                  reason={reason}
                  onClick={() => setSection(item.id)}
                >
                  {t(item.label)}
                </ScopedButton>
              );
            })}
            <UnavailablePages />
          </nav>
          <main className="min-w-0 space-y-4 p-6">
            <ErrorNotice error={planes.error} />
            {reason ? (
              <p role="status">{reason}</p>
            ) : (
              <ControlPlaneViews
                key={`${section}:${selected?.id ?? ""}`}
                section={section}
                api={api}
                principal={principal}
                selected={selected}
              />
            )}
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}

function Session({
  prefix,
  token,
  saveToken,
}: {
  prefix: string;
  token: string | null;
  saveToken: (value: string | null) => void;
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const [expired, setExpired] = useState(false);
  const apiResult = useMemo(() => {
    try {
      return { api: controlPlaneApi(prefix, token ?? undefined, () => setExpired(true)) };
    } catch (error) {
      return { error };
    }
  }, [prefix, token]);
  const principalOptions = {
    queryKey: ["control-plane", "principal"],
    queryFn: () => {
      if (!apiResult.api) throw apiResult.error;
      return apiResult.api.me();
    },
    enabled: Boolean(token && apiResult.api && !expired),
  };
  const principal = useQuery(principalOptions);
  useEffect(() => {
    if (expired) sessionStorage.removeItem(controlPlaneSessionKey(prefix));
  }, [expired, prefix]);
  if (!apiResult.api)
    return (
      <main className="p-8">
        <ErrorNotice error={apiResult.error} />
      </main>
    );
  if (!token || expired) return <Login prefix={prefix} token={token} onLogin={saveToken} />;
  if (principal.isPending)
    return (
      <main className="p-8" role="status">
        {t("checkingSession")}
      </main>
    );
  if (!principal.data)
    return (
      <main className="grid max-w-md gap-3 p-8">
        <ErrorNotice error={principal.error} />
        <Button onClick={() => void principal.refetch()}>{t("retry")}</Button>
        <Button variant="outline" onClick={() => saveToken(null)}>
          {t("signOut")}
        </Button>
      </main>
    );
  return <Workspace api={apiResult.api} principal={principal.data} logout={() => saveToken(null)} />;
}

function sessionQueryClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } });
}

export default function ControlPlaneDashboard({ prefix }: { prefix: string }) {
  const storageKey = controlPlaneSessionKey(prefix);
  const [token, setToken] = useState(() => sessionStorage.getItem(storageKey));
  const [client, setClient] = useState(sessionQueryClient);
  const [session, setSession] = useState(0);
  const saveToken = (value: string | null) => {
    if (value) sessionStorage.setItem(storageKey, value);
    else sessionStorage.removeItem(storageKey);
    client.clear();
    setClient(sessionQueryClient());
    setSession((revision) => revision + 1);
    setToken(value);
  };
  return (
    <QueryClientProvider key={session} client={client}>
      <Session prefix={prefix} token={token} saveToken={saveToken} />
    </QueryClientProvider>
  );
}
