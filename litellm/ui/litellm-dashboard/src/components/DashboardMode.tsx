"use client";

import { useTranslation } from "react-i18next";
import { useEffect, useState, type ReactNode } from "react";
import dynamic from "next/dynamic";
import { discoverControlPlane } from "@/lib/http/controlPlane";

const ControlPlaneDashboard = dynamic(() => import("./ControlPlaneDashboard"), { ssr: false });

export default function DashboardMode({ children }: { children: ReactNode }) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  const [mode, setMode] = useState<Awaited<ReturnType<typeof discoverControlPlane>> | Error | undefined>();

  useEffect(() => {
    let active = true;
    discoverControlPlane()
      .catch(() => new Error("Could not determine dashboard mode. Reload to retry."))
      .then((result) => {
        if (active) setMode(result);
      });
    return () => {
      active = false;
    };
  }, []);

  if (mode === undefined)
    return (
      <main className="p-8" role="status">
        {t("loadingDashboard")}
      </main>
    );
  if (mode instanceof Error)
    return (
      <main className="p-8" role="alert">
        {mode.message}
      </main>
    );
  if (mode?.disabled) return <main className="p-8">{t("theAdminUiIsDisabled")}</main>;
  if (mode) return <ControlPlaneDashboard key={mode.prefix} prefix={mode.prefix} />;
  return children;
}
