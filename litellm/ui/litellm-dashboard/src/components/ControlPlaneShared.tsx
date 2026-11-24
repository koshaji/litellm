import { useTranslation } from "react-i18next";
import type { ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

export function ErrorNotice({ error }: { error: unknown }) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  if (!error) return null;
  return (
    <p role="alert" className="rounded border border-destructive p-3 text-destructive">
      {error instanceof Error ? error.message : t("requestFailed")}
    </p>
  );
}

export function ScopedButton({
  reason,
  children,
  ...props
}: React.ComponentProps<typeof Button> & { reason?: string }) {
  if (!reason) return <Button {...props}>{children}</Button>;
  return (
    <Tooltip>
      <TooltipTrigger render={<span tabIndex={0} className="inline-flex cursor-not-allowed rounded-md" />}>
        <button
          type="button"
          disabled
          aria-disabled="true"
          className="pointer-events-none w-full rounded-md border px-3 py-2 text-sm text-muted-foreground"
        >
          {children}
        </button>
      </TooltipTrigger>
      <TooltipContent role="tooltip">{reason}</TooltipContent>
    </Tooltip>
  );
}

export function ResultsTable({
  columns,
  rows,
  rowKeys,
}: {
  columns: readonly string[];
  rows: readonly (readonly ReactNode[])[];
  rowKeys?: readonly (string | undefined)[];
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  if (!rows.length) return <p className="text-muted-foreground">{t("noResults")}</p>;
  return (
    <div className="overflow-x-auto rounded border">
      <table className="w-full text-left text-sm">
        <thead className="border-b bg-muted">
          <tr>
            {columns.map((column) => (
              <th className="px-4 py-3" scope="col" key={column}>
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={rowKeys?.[index] ?? index} className="border-b last:border-b-0">
              {row.map((cell, column) => (
                <td key={columns[column]} className="px-4 py-3">
                  {cell ?? t("notSet")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
