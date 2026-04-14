"use client";

import { ReactNode, RefObject, useMemo } from "react";
import { Mail, ScanSearch, UsersRound } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/lib/utils";
import { TransformFilters, TransformMapping, useTransformStore } from "@/lib/transform-store";

type FilterEstimate = {
  email: number;
  gender: number;
  deduplication: number;
  totalRemoved: number;
  duplicatesRemoved: number;
  rowsRemaining: number;
};

type FiltersSectionProps = {
  totalRows: number;
  previewRows: Record<string, string>[];
  filtersRef: RefObject<HTMLDivElement>;
};

const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function estimateFromPreview(totalRows: number, previewRows: Record<string, string>[], removedPreviewRows: number) {
  if (totalRows === 0 || previewRows.length === 0 || removedPreviewRows === 0) return 0;
  return Math.min(totalRows, Math.round((removedPreviewRows / previewRows.length) * totalRows));
}

function estimateEmailRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  const { column, mode, domain } = filters.email.config;
  if (!column) return 0;

  const normalizedDomain = domain.trim().replace(/^@/, "").toLowerCase();
  const removedPreviewRows = previewRows.filter((row) => {
    const value = String(row[column] ?? "").trim().toLowerCase();
    if (mode === "keep_domain" && normalizedDomain) {
      return !value.endsWith(`@${normalizedDomain}`);
    }
    return !emailPattern.test(value);
  }).length;

  return estimateFromPreview(totalRows, previewRows, removedPreviewRows);
}

function estimateGenderRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  const { column, allowedValues, normalizeValues } = filters.gender.config;
  if (!column || allowedValues.length === 0) return 0;

  const normalize = (value: string) => normalizeValues ? value.trim().toLowerCase() : value.trim();
  const allowed = new Set(allowedValues.map(normalize).filter(Boolean));
  const removedPreviewRows = previewRows.filter((row) => {
    const value = normalize(String(row[column] ?? ""));
    return !allowed.has(value);
  }).length;

  return estimateFromPreview(totalRows, previewRows, removedPreviewRows);
}

function estimateDedupeRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  const { columns } = filters.deduplication.config;
  if (columns.length === 0) return 0;

  const seen = new Set<string>();
  let duplicates = 0;

  previewRows.forEach((row) => {
    const key = columns.map((column) => String(row[column] ?? "").trim().toLowerCase()).join("::");
    if (seen.has(key)) {
      duplicates += 1;
      return;
    }
    seen.add(key);
  });

  return estimateFromPreview(totalRows, previewRows, duplicates);
}

function useFilterEstimate(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  return useMemo<FilterEstimate>(() => {
    const email = estimateEmailRemoval(totalRows, previewRows, filters);
    const gender = estimateGenderRemoval(totalRows, previewRows, filters);
    const deduplication = estimateDedupeRemoval(totalRows, previewRows, filters);
    const totalRemoved = Math.min(
      totalRows,
      (filters.email.enabled ? email : 0) +
      (filters.gender.enabled ? gender : 0) +
      (filters.deduplication.enabled ? deduplication : 0)
    );
    const duplicatesRemoved = filters.deduplication.enabled ? deduplication : 0;

    return {
      email,
      gender,
      deduplication,
      totalRemoved,
      duplicatesRemoved,
      rowsRemaining: Math.max(totalRows - totalRemoved, 0),
    };
  }, [filters, previewRows, totalRows]);
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="min-w-[160px] flex-1 rounded-lg border bg-background p-4">
      <div className="text-2xl font-bold tabular-nums">{value.toLocaleString()}</div>
      <div className="mt-1 text-xs font-medium uppercase text-muted-foreground">{label}</div>
    </div>
  );
}

function FilterCard({
  title,
  icon,
  enabled,
  onEnabledChange,
  miniStat,
  children,
}: {
  title: string;
  icon: React.ReactNode;
  enabled: boolean;
  onEnabledChange: (enabled: boolean) => void;
  miniStat: string;
  children: ReactNode;
}) {
  return (
    <Card className={cn("rounded-lg", enabled && "ring-primary/40")}>
      <CardHeader className="grid-cols-[1fr_auto] gap-4">
        <div className="flex min-w-0 items-center gap-3">
          <div className="rounded-lg bg-muted p-2 text-primary">{icon}</div>
          <div className="min-w-0">
            <CardTitle>{title}</CardTitle>
            <p className="mt-1 text-xs text-muted-foreground">{miniStat}</p>
          </div>
        </div>
        <Switch checked={enabled} onCheckedChange={onEnabledChange} aria-label={`Toggle ${title}`} />
      </CardHeader>
      {enabled ? <CardContent className="space-y-4">{children}</CardContent> : null}
    </Card>
  );
}

function ColumnSelect({
  value,
  onChange,
  columns,
  label,
}: {
  value: string;
  onChange: (value: string) => void;
  columns: string[];
  label: string;
}) {
  return (
    <label className="block space-y-2 text-sm">
      <span className="font-medium">{label}</span>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
      >
        <option value="">Choose column</option>
        {columns.map((column) => (
          <option key={column} value={column}>{column}</option>
        ))}
      </select>
    </label>
  );
}

function DedupeColumnToggle({
  column,
  selected,
  onToggle,
}: {
  column: string;
  selected: boolean;
  onToggle: (column: string, selected: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 rounded-lg border bg-background px-3 py-2 text-sm">
      <input
        type="checkbox"
        checked={selected}
        onChange={(event) => onToggle(column, event.target.checked)}
        className="h-4 w-4 accent-primary"
      />
      <span className="min-w-0 truncate">{column}</span>
    </label>
  );
}

export function FiltersSection({ totalRows, previewRows, filtersRef }: FiltersSectionProps) {
  const mapping = useTransformStore((state) => state.mapping);
  const filters = useTransformStore((state) => state.filters);
  const setFilterEnabled = useTransformStore((state) => state.setFilterEnabled);
  const setEmailConfig = useTransformStore((state) => state.setEmailConfig);
  const setGenderConfig = useTransformStore((state) => state.setGenderConfig);
  const setDedupeConfig = useTransformStore((state) => state.setDedupeConfig);
  const estimate = useFilterEstimate(totalRows, previewRows, filters);

  const mappedColumns = useMemo(
    () => mapping.map((item: TransformMapping) => item.outputColumn),
    [mapping]
  );

  const toggleDedupeColumn = (column: string, selected: boolean) => {
    const columns = selected
      ? [...filters.deduplication.config.columns, column]
      : filters.deduplication.config.columns.filter((item) => item !== column);
    setDedupeConfig({ columns });
  };

  const runDisabled = mapping.length === 0;

  return (
    <section ref={filtersRef} className="scroll-mt-8 space-y-6">
      <div className="sticky top-0 z-10 rounded-lg border bg-card/95 p-4 shadow-sm backdrop-blur">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-xl font-semibold">Filters</h2>
            <p className="mt-1 text-sm text-muted-foreground">Configure row filters before running the transform.</p>
          </div>
        </div>
        <div className="flex flex-col gap-3 lg:flex-row">
          <StatCard label="Total Rows" value={totalRows} />
          <StatCard label="Rows Remaining" value={estimate.rowsRemaining} />
          <StatCard label="Rows Removed" value={estimate.totalRemoved} />
          <StatCard label="Duplicates Removed" value={estimate.duplicatesRemoved} />
        </div>
      </div>

      <div className="grid gap-4">
        <FilterCard
          title="Email Filter"
          icon={<Mail className="h-4 w-4" />}
          enabled={filters.email.enabled}
          onEnabledChange={(enabled) => setFilterEnabled("email", enabled)}
          miniStat={`Will remove ${estimate.email.toLocaleString()} rows`}
        >
          <ColumnSelect
            label="Email column"
            value={filters.email.config.column}
            onChange={(column) => setEmailConfig({ column })}
            columns={mappedColumns}
          />
          <label className="block space-y-2 text-sm">
            <span className="font-medium">Mode</span>
            <select
              value={filters.email.config.mode}
              onChange={(event) => setEmailConfig({ mode: event.target.value as "remove_invalid" | "keep_domain" })}
              className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
            >
              <option value="remove_invalid">Remove invalid emails</option>
              <option value="keep_domain">Keep only a domain</option>
            </select>
          </label>
          {filters.email.config.mode === "keep_domain" ? (
            <label className="block space-y-2 text-sm">
              <span className="font-medium">Allowed domain</span>
              <input
                value={filters.email.config.domain}
                onChange={(event) => setEmailConfig({ domain: event.target.value })}
                placeholder="example.com"
                className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
              />
            </label>
          ) : null}
        </FilterCard>

        <FilterCard
          title="Gender Filter"
          icon={<UsersRound className="h-4 w-4" />}
          enabled={filters.gender.enabled}
          onEnabledChange={(enabled) => setFilterEnabled("gender", enabled)}
          miniStat={`Will remove ${estimate.gender.toLocaleString()} rows`}
        >
          <ColumnSelect
            label="Gender column"
            value={filters.gender.config.column}
            onChange={(column) => setGenderConfig({ column })}
            columns={mappedColumns}
          />
          <label className="block space-y-2 text-sm">
            <span className="font-medium">Allowed values</span>
            <input
              value={filters.gender.config.allowedValues.join(", ")}
              onChange={(event) => setGenderConfig({
                allowedValues: event.target.value.split(",").map((value) => value.trim()).filter(Boolean),
              })}
              placeholder="female, male, non-binary"
              className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
            />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-lg border bg-background px-3 py-2 text-sm">
            <span>Normalize values before matching</span>
            <Switch
              checked={filters.gender.config.normalizeValues}
              onCheckedChange={(normalizeValues) => setGenderConfig({ normalizeValues })}
              aria-label="Normalize gender values"
            />
          </label>
        </FilterCard>

        <FilterCard
          title="Duplicate Remover"
          icon={<ScanSearch className="h-4 w-4" />}
          enabled={filters.deduplication.enabled}
          onEnabledChange={(enabled) => setFilterEnabled("deduplication", enabled)}
          miniStat={`Will remove ${estimate.deduplication.toLocaleString()} rows`}
        >
          <div className="space-y-2">
            <div className="text-sm font-medium">Match duplicate rows by</div>
            <div className="grid gap-2 sm:grid-cols-2">
              {mappedColumns.map((column) => (
                <DedupeColumnToggle
                  key={column}
                  column={column}
                  selected={filters.deduplication.config.columns.includes(column)}
                  onToggle={toggleDedupeColumn}
                />
              ))}
            </div>
            {mappedColumns.length === 0 ? (
              <p className="text-sm text-muted-foreground">Map at least one output column to configure deduplication.</p>
            ) : null}
          </div>
          <label className="block space-y-2 text-sm">
            <span className="font-medium">Keep row</span>
            <select
              value={filters.deduplication.config.strategy}
              onChange={(event) => setDedupeConfig({ strategy: event.target.value as "first" | "last" })}
              className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
            >
              <option value="first">First occurrence</option>
              <option value="last">Last occurrence</option>
            </select>
          </label>
        </FilterCard>
      </div>

      <Button size="lg" className="h-12 w-full text-base" disabled={runDisabled}>
        Run Transform
      </Button>
    </section>
  );
}
