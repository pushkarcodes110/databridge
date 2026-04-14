"use client";

import { ReactNode, RefObject, useEffect, useMemo, useState } from "react";
import { ChevronDown, ChevronRight, Download, Mail, ScanSearch, UsersRound } from "lucide-react";
import {
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";
import { toast } from "sonner";
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
  sourceColumns: string[];
  inputFile: string;
  filtersRef: RefObject<HTMLDivElement>;
};

type EmailAnalysis = {
  total: number;
  breakdown: { domain: string; count: number; percentage: number }[];
  invalidFormat: number;
  emptyEmails: number;
  duplicateEmails: number;
};

type GenderAnalysis = {
  male: number;
  female: number;
  unknown: number;
  sampleSize: number;
};

type DuplicateAnalysis = {
  fullDuplicates: number;
  emailDuplicates: number;
  totalRows: number;
};

type RunStats = {
  inputRows: number;
  outputRows: number;
  rowsRemoved: number;
  rowsRemovedFullDupe: number;
  rowsRemovedEmailDupe: number;
  rowsRemovedInvalidEmail: number;
  emailsTypoFixed: number;
  rowsRemovedWrongGender: number;
  maleCount: number;
  femaleCount: number;
  unknownCount: number;
  skippedRows: number;
};

type RunEvent = {
  step: string;
  progress?: number;
  rowsProcessed?: number;
  outputFile?: string;
  error?: string;
  warning?: string;
  stats?: RunStats;
  rowsRemovedFullDupe?: number;
  rowsRemovedEmailDupe?: number;
  rowsRemovedInvalidEmail?: number;
  emailsTypoFixed?: number;
  rowsRemovedWrongGender?: number;
  maleCount?: number;
  femaleCount?: number;
  unknownCount?: number;
};

type TransformHistoryEntry = {
  date: string;
  inputFile: string;
  outputRows: number;
  inputRows: number;
  filtersApplied: string[];
  outputFile?: string;
};

const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const freeProviders = new Set(["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]);
const chartColors = ["#0ea5e9", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#14b8a6", "#f97316", "#84cc16", "#06b6d4", "#a855f7", "#64748b"];
const historyStorageKey = "databridge-transform-history";

const typoCorrections = [
  ["gmal.com", "gmail.com"],
  ["gmial.com", "gmail.com"],
  ["gmail.co", "gmail.com"],
  ["gmai.com", "gmail.com"],
  ["yaho.com", "yahoo.com"],
  ["yahoo.co", "yahoo.com"],
  ["hotmial.com", "hotmail.com"],
  ["hotmai.com", "hotmail.com"],
  ["outlok.com", "outlook.com"],
  ["outlook.co", "outlook.com"],
];

function estimateFromPreview(totalRows: number, previewRows: Record<string, string>[], removedPreviewRows: number) {
  if (totalRows === 0 || previewRows.length === 0 || removedPreviewRows === 0) return 0;
  return Math.min(totalRows, Math.round((removedPreviewRows / previewRows.length) * totalRows));
}

function getEmailDomain(value: string) {
  const atIndex = value.lastIndexOf("@");
  if (atIndex === -1) return "";
  return value.slice(atIndex + 1).trim().toLowerCase();
}

function correctCommonTypo(email: string) {
  const atIndex = email.lastIndexOf("@");
  if (atIndex === -1) return email;

  const localPart = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1);
  const correction = typoCorrections.find(([from]) => from === domain);
  return correction ? `${localPart}@${correction[1]}` : email;
}

function normalizeEmailForFilter(value: string, filters: TransformFilters) {
  let email = String(value ?? "").trim();
  if (filters.email.config.normalizeLowercase) email = email.toLowerCase();
  if (filters.email.config.fixCommonTypos) email = correctCommonTypo(email);
  return email;
}

function estimateEmailRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  const { column, selectedDomains, removeInvalidFormat } = filters.email.config;
  if (!column) return 0;

  const selectedDomainSet = new Set(selectedDomains);
  const removedPreviewRows = previewRows.filter((row) => {
    const value = normalizeEmailForFilter(String(row[column] ?? ""), filters);
    if (!value) return removeInvalidFormat;
    if (!emailPattern.test(value)) return removeInvalidFormat;

    const domain = getEmailDomain(value);
    return selectedDomainSet.size > 0 && !selectedDomainSet.has(domain);
  }).length;

  return estimateFromPreview(totalRows, previewRows, removedPreviewRows);
}

function estimateGenderRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  if (!filters.gender.config.nameColumn || filters.gender.config.mode === "all") return 0;
  return estimateFromPreview(totalRows, previewRows, 0);
}

function estimateDedupeRemoval(totalRows: number, previewRows: Record<string, string>[], filters: TransformFilters) {
  if (!filters.deduplication.config.removeFullDuplicates && !filters.deduplication.config.removeDuplicateEmails) return 0;

  const seenRows = new Set<string>();
  const seenEmails = new Set<string>();
  let duplicates = 0;

  previewRows.forEach((row) => {
    let isDuplicate = false;

    if (filters.deduplication.config.removeFullDuplicates) {
      const rowKey = JSON.stringify(row);
      if (seenRows.has(rowKey)) {
        isDuplicate = true;
      } else {
        seenRows.add(rowKey);
      }
    }

    if (filters.deduplication.config.removeDuplicateEmails && filters.deduplication.config.emailColumn) {
      const email = String(row[filters.deduplication.config.emailColumn] ?? "").trim().toLowerCase();
      if (email) {
        if (seenEmails.has(email)) {
          isDuplicate = true;
        } else {
          seenEmails.add(email);
        }
      }
    }

    if (isDuplicate) duplicates += 1;
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
    <div className="min-w-[160px] shrink-0 flex-1 rounded-lg border bg-background p-4">
      <div className="text-2xl font-bold tabular-nums">{value.toLocaleString()}</div>
      <div className="mt-1 text-xs font-medium uppercase text-muted-foreground">{label}</div>
    </div>
  );
}

function AnalysisSkeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div className="space-y-3 rounded-lg border bg-background p-4" aria-label="Loading analysis">
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="flex animate-pulse items-center gap-3">
          <div className="h-8 w-8 rounded-lg bg-muted" />
          <div className="min-w-0 flex-1 space-y-2">
            <div className="h-3 w-2/3 rounded bg-muted" />
            <div className="h-3 w-1/3 rounded bg-muted" />
          </div>
        </div>
      ))}
    </div>
  );
}

function filtersAppliedLabel(filters: TransformFilters) {
  const applied: string[] = [];
  if (filters.email.enabled) applied.push("Email Filter");
  if (filters.gender.enabled) applied.push("Gender Filter");
  if (filters.deduplication.enabled) applied.push("Duplicate Remover");
  return applied;
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
  icon: ReactNode;
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

function GenderFilterConfigPanel({
  sourceColumns,
  enabled,
}: {
  sourceColumns: string[];
  enabled: boolean;
}) {
  const uploadId = useTransformStore((state) => state.uploadId);
  const genderConfig = useTransformStore((state) => state.filters.gender.config);
  const setGenderConfig = useTransformStore((state) => state.setGenderConfig);
  const [analysis, setAnalysis] = useState<GenderAnalysis | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!enabled || !uploadId || !genderConfig.nameColumn) {
      setAnalysis(null);
      return;
    }

    const controller = new AbortController();
    setIsLoading(true);
    setError("");

    fetch("/api/transform/analyze/gender", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify({
        uploadId,
        nameColumn: genderConfig.nameColumn,
        sampleSize: 200,
      }),
    })
      .then(async (response) => {
        const data = await response.json();
        if (!response.ok) throw new Error(data?.error || "Failed to analyze gender.");
        return data as GenderAnalysis;
      })
      .then(setAnalysis)
      .catch((fetchError) => {
        if (fetchError instanceof DOMException && fetchError.name === "AbortError") return;
        setError(fetchError instanceof Error ? fetchError.message : "Failed to analyze gender.");
        setAnalysis(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setIsLoading(false);
      });

    return () => controller.abort();
  }, [enabled, genderConfig.nameColumn, uploadId]);

  return (
    <div className="space-y-4">
      <ColumnSelect
        label="Name source column"
        value={genderConfig.nameColumn}
        onChange={(nameColumn) => setGenderConfig({ nameColumn })}
        columns={sourceColumns}
      />

      <div className="space-y-2">
        <div className="text-sm font-medium">Gender classification options</div>
        <div className="grid gap-2 sm:grid-cols-3">
          {[
            { value: "male", label: "Male only" },
            { value: "female", label: "Female only" },
            { value: "all", label: "All" },
          ].map((option) => (
            <label key={option.value} className="flex items-center gap-2 rounded-lg border bg-background px-3 py-2 text-sm">
              <input
                type="checkbox"
                checked={genderConfig.mode === option.value}
                onChange={() => setGenderConfig({ mode: option.value as "male" | "female" | "all" })}
                className="h-4 w-4 accent-primary"
              />
              {option.label}
            </label>
          ))}
        </div>
      </div>

      <label className="flex items-center justify-between gap-3 rounded-lg border bg-background px-3 py-3 text-sm">
        <span>Add &apos;gender&apos; column to output</span>
        <Switch
          checked={genderConfig.addGenderColumn}
          onCheckedChange={(addGenderColumn) => setGenderConfig({ addGenderColumn })}
          aria-label="Add gender column to output"
        />
      </label>

      {isLoading ? (
        <AnalysisSkeleton rows={2} />
      ) : null}

      {error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">{error}</div>
      ) : null}

      {analysis ? (
        <div className="rounded-lg border bg-background p-4 text-sm">
          Estimated{" "}
          <span className="font-semibold">{analysis.male.toLocaleString()} male</span>,{" "}
          <span className="font-semibold">{analysis.female.toLocaleString()} female</span>,{" "}
          <span className="font-semibold">{analysis.unknown.toLocaleString()} unknown</span>{" "}
          from {analysis.sampleSize.toLocaleString()} sampled rows.
        </div>
      ) : null}

      <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-200">
        Gender detection is based on first name analysis and may not be 100% accurate.
      </div>
    </div>
  );
}

function buildChartData(breakdown: EmailAnalysis["breakdown"]) {
  const top = breakdown.slice(0, 10);
  const others = breakdown.slice(10).reduce((sum, item) => sum + item.count, 0);
  return others > 0 ? [...top, { domain: "Others", count: others, percentage: 0 }] : top;
}

function ValidationOption({
  label,
  description,
  checked,
  onCheckedChange,
}: {
  label: string;
  description?: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex items-start justify-between gap-4 rounded-lg border bg-background px-3 py-3 text-sm">
      <span>
        <span className="block font-medium">{label}</span>
        {description ? <span className="mt-1 block text-xs text-muted-foreground">{description}</span> : null}
      </span>
      <Switch checked={checked} onCheckedChange={onCheckedChange} aria-label={label} />
    </label>
  );
}

function EmailFilterConfigPanel({
  mappedColumns,
  mapping,
}: {
  mappedColumns: string[];
  mapping: TransformMapping[];
}) {
  const uploadId = useTransformStore((state) => state.uploadId);
  const emailConfig = useTransformStore((state) => state.filters.email.config);
  const setEmailConfig = useTransformStore((state) => state.setEmailConfig);
  const [analysis, setAnalysis] = useState<EmailAnalysis | null>(null);
  const [analysisKey, setAnalysisKey] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const [domainSearch, setDomainSearch] = useState("");

  const selectedMapping = useMemo(
    () => mapping.find((item) => item.outputColumn === emailConfig.column),
    [emailConfig.column, mapping]
  );
  const sourceColumn = selectedMapping?.sourceColumn ?? "";

  useEffect(() => {
    if (!uploadId || !sourceColumn) {
      setAnalysis(null);
      setAnalysisKey("");
      return;
    }

    const key = `${uploadId}:${sourceColumn}`;
    const controller = new AbortController();
    setIsLoading(true);
    setError("");

    fetch(`/api/transform/analyze/email?uploadId=${encodeURIComponent(uploadId)}&sourceColumn=${encodeURIComponent(sourceColumn)}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        const data = await response.json();
        if (!response.ok) throw new Error(data?.error || "Failed to analyze email domains.");
        return data as EmailAnalysis;
      })
      .then((data) => {
        setAnalysis(data);
        setAnalysisKey(key);
        setEmailConfig({ selectedDomains: data.breakdown.map((item) => item.domain) });
      })
      .catch((fetchError) => {
        if (fetchError instanceof DOMException && fetchError.name === "AbortError") return;
        setError(fetchError instanceof Error ? fetchError.message : "Failed to analyze email domains.");
        setAnalysis(null);
        setAnalysisKey("");
      })
      .finally(() => {
        if (!controller.signal.aborted) setIsLoading(false);
      });

    return () => controller.abort();
  }, [sourceColumn, setEmailConfig, uploadId]);

  const chartData = useMemo(() => buildChartData(analysis?.breakdown ?? []), [analysis]);
  const selectedDomainSet = useMemo(() => new Set(emailConfig.selectedDomains), [emailConfig.selectedDomains]);
  const filteredDomains = useMemo(() => {
    const query = domainSearch.trim().toLowerCase();
    return (analysis?.breakdown ?? []).filter((item) => item.domain.includes(query));
  }, [analysis, domainSearch]);
  const keptEmails = useMemo(
    () => (analysis?.breakdown ?? []).reduce((sum, item) => selectedDomainSet.has(item.domain) ? sum + item.count : sum, 0),
    [analysis, selectedDomainSet]
  );
  const domainEmailTotal = useMemo(
    () => (analysis?.breakdown ?? []).reduce((sum, item) => sum + item.count, 0),
    [analysis]
  );

  const setDomainChecked = (domain: string, checked: boolean) => {
    const next = checked
      ? Array.from(new Set([...emailConfig.selectedDomains, domain]))
      : emailConfig.selectedDomains.filter((item) => item !== domain);
    setEmailConfig({ selectedDomains: next });
  };

  const allDomains = analysis?.breakdown.map((item) => item.domain) ?? [];

  return (
    <div className="space-y-5">
      <ColumnSelect
        label="Email column"
        value={emailConfig.column}
        onChange={(column) => setEmailConfig({ column, selectedDomains: [] })}
        columns={mappedColumns}
      />

      {!emailConfig.column ? (
        <div className="rounded-lg border border-dashed p-6 text-sm text-muted-foreground">
          Choose the mapped output column that contains email addresses.
        </div>
      ) : null}

      {emailConfig.column && !sourceColumn ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
          This output column is not mapped to a source column yet.
        </div>
      ) : null}

      {isLoading ? (
        <AnalysisSkeleton rows={4} />
      ) : null}

      {error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">{error}</div>
      ) : null}

      {analysis ? (
        <div className="space-y-5">
          <div className="grid gap-4 lg:grid-cols-[320px_minmax(0,1fr)]">
            <div className="rounded-lg border bg-background p-4">
              <div className="h-[260px]">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={chartData}
                      dataKey="count"
                      nameKey="domain"
                      innerRadius={58}
                      outerRadius={94}
                      paddingAngle={2}
                    >
                      {chartData.map((entry, index) => (
                        <Cell key={entry.domain} fill={chartColors[index % chartColors.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value, name) => [Number(value).toLocaleString(), name]} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div className="rounded-lg bg-muted/40 p-3">
                  <div className="text-lg font-semibold">{analysis.invalidFormat.toLocaleString()}</div>
                  <div className="text-xs text-muted-foreground">Invalid format</div>
                </div>
                <div className="rounded-lg bg-muted/40 p-3">
                  <div className="text-lg font-semibold">{analysis.emptyEmails.toLocaleString()}</div>
                  <div className="text-xs text-muted-foreground">Empty emails</div>
                </div>
                <div className="rounded-lg bg-muted/40 p-3">
                  <div className="text-lg font-semibold">{analysis.duplicateEmails.toLocaleString()}</div>
                  <div className="text-xs text-muted-foreground">Duplicate emails</div>
                </div>
                <div className="rounded-lg bg-muted/40 p-3">
                  <div className="text-lg font-semibold">{analysis.total.toLocaleString()}</div>
                  <div className="text-xs text-muted-foreground">Rows analyzed</div>
                </div>
              </div>
            </div>

            <div className="rounded-lg border bg-background p-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <div>
                  <div className="font-medium">Domain allowlist</div>
                  <div className="mt-1 text-sm text-muted-foreground">
                    Keeping {keptEmails.toLocaleString()} of {domainEmailTotal.toLocaleString()} emails after domain filter.
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">Analysis source: {analysisKey || "current upload"}</div>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setEmailConfig({ selectedDomains: allDomains.filter((domain) => domain === "gmail.com") })}
                  >
                    Keep Gmail Only
                  </Button>
                  <Button type="button" variant="outline" onClick={() => setEmailConfig({ selectedDomains: allDomains })}>
                    Keep All
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setEmailConfig({ selectedDomains: allDomains.filter((domain) => !freeProviders.has(domain)) })}
                  >
                    Remove Free Providers
                  </Button>
                </div>
              </div>

              <input
                value={domainSearch}
                onChange={(event) => setDomainSearch(event.target.value)}
                placeholder="Search domains"
                className="mt-4 w-full rounded-lg border bg-card px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
              />

              <div className="mt-4 max-h-[360px] overflow-auto rounded-lg border">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-muted text-xs uppercase text-muted-foreground">
                    <tr>
                      <th className="px-3 py-2 font-medium">Keep</th>
                      <th className="px-3 py-2 font-medium">Domain</th>
                      <th className="px-3 py-2 text-right font-medium">Count</th>
                      <th className="px-3 py-2 text-right font-medium">Share</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredDomains.map((item) => (
                      <tr key={item.domain} className="border-t">
                        <td className="px-3 py-2">
                          <input
                            type="checkbox"
                            checked={selectedDomainSet.has(item.domain)}
                            onChange={(event) => setDomainChecked(item.domain, event.target.checked)}
                            className="h-4 w-4 accent-primary"
                            aria-label={`Keep ${item.domain}`}
                          />
                        </td>
                        <td className="px-3 py-2 font-medium">{item.domain}</td>
                        <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{item.count.toLocaleString()}</td>
                        <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{item.percentage.toFixed(2)}%</td>
                      </tr>
                    ))}
                    {filteredDomains.length === 0 ? (
                      <tr>
                        <td className="px-3 py-6 text-center text-muted-foreground" colSpan={4}>
                          No domains match your search.
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div className="space-y-3">
            <div className="text-sm font-semibold">Email Validation Options</div>
            <div className="grid gap-3 lg:grid-cols-2">
              <ValidationOption
                label="Fix common typos"
                checked={emailConfig.fixCommonTypos}
                onCheckedChange={(fixCommonTypos) => setEmailConfig({ fixCommonTypos })}
              />
              <ValidationOption
                label="Remove invalid format emails"
                description="Removes values without an @ sign or top-level domain."
                checked={emailConfig.removeInvalidFormat}
                onCheckedChange={(removeInvalidFormat) => setEmailConfig({ removeInvalidFormat })}
              />
              <ValidationOption
                label="Verify mailbox exists via API"
                description="Uses reacher.email or MillionVerifier. This is slow for large files and costs API credits."
                checked={emailConfig.verifyMailboxExists}
                onCheckedChange={(verifyMailboxExists) => setEmailConfig({ verifyMailboxExists })}
              />
              <ValidationOption
                label="Normalize emails to lowercase"
                checked={emailConfig.normalizeLowercase}
                onCheckedChange={(normalizeLowercase) => setEmailConfig({ normalizeLowercase })}
              />
            </div>
          </div>

          <div className="rounded-lg border bg-background">
            <button
              type="button"
              onClick={() => setEmailConfig({ typoRulesExpanded: !emailConfig.typoRulesExpanded })}
              className="flex w-full items-center justify-between px-4 py-3 text-left text-sm font-medium"
            >
              <span>Typo Correction Rules</span>
              {emailConfig.typoRulesExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            </button>
            {emailConfig.typoRulesExpanded ? (
              <div className="grid gap-2 border-t p-4 text-sm sm:grid-cols-2 lg:grid-cols-3">
                {typoCorrections.map(([from, to]) => (
                  <div key={from} className="rounded-lg bg-muted/40 px-3 py-2">
                    <span className="font-mono text-xs">{from}</span>
                    <span className="px-2 text-muted-foreground">→</span>
                    <span className="font-mono text-xs">{to}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function DedupeColumnToggle({
  label,
  description,
  checked,
  onCheckedChange,
}: {
  label: string;
  description: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex items-start justify-between gap-4 rounded-lg border bg-background px-3 py-3 text-sm">
      <span>
        <span className="block font-medium">{label}</span>
        <span className="mt-1 block text-xs text-muted-foreground">{description}</span>
      </span>
      <Switch checked={checked} onCheckedChange={onCheckedChange} aria-label={label} />
    </label>
  );
}

function DuplicateRemoverConfigPanel({
  sourceColumns,
  enabled,
}: {
  sourceColumns: string[];
  enabled: boolean;
}) {
  const uploadId = useTransformStore((state) => state.uploadId);
  const emailConfig = useTransformStore((state) => state.filters.email.config);
  const dedupeConfig = useTransformStore((state) => state.filters.deduplication.config);
  const setDedupeConfig = useTransformStore((state) => state.setDedupeConfig);
  const [analysis, setAnalysis] = useState<DuplicateAnalysis | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const guessedEmailColumn = useMemo(() => {
    if (dedupeConfig.emailColumn) return dedupeConfig.emailColumn;
    if (emailConfig.column && sourceColumns.includes(emailConfig.column)) return emailConfig.column;
    return sourceColumns.find((column) => column.toLowerCase().includes("email")) ?? "";
  }, [dedupeConfig.emailColumn, emailConfig.column, sourceColumns]);

  useEffect(() => {
    if (!dedupeConfig.emailColumn && guessedEmailColumn) {
      setDedupeConfig({ emailColumn: guessedEmailColumn });
    }
  }, [dedupeConfig.emailColumn, guessedEmailColumn, setDedupeConfig]);

  useEffect(() => {
    if (!enabled || !uploadId || !dedupeConfig.emailColumn) {
      setAnalysis(null);
      return;
    }

    const controller = new AbortController();
    setIsLoading(true);
    setError("");

    fetch(`/api/transform/analyze/duplicates?uploadId=${encodeURIComponent(uploadId)}&emailColumn=${encodeURIComponent(dedupeConfig.emailColumn)}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        const data = await response.json();
        if (!response.ok) throw new Error(data?.error || "Failed to analyze duplicates.");
        return data as DuplicateAnalysis;
      })
      .then(setAnalysis)
      .catch((fetchError) => {
        if (fetchError instanceof DOMException && fetchError.name === "AbortError") return;
        setError(fetchError instanceof Error ? fetchError.message : "Failed to analyze duplicates.");
        setAnalysis(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setIsLoading(false);
      });

    return () => controller.abort();
  }, [dedupeConfig.emailColumn, enabled, uploadId]);

  return (
    <div className="space-y-4">
      <ColumnSelect
        label="Email source column"
        value={dedupeConfig.emailColumn}
        onChange={(emailColumn) => setDedupeConfig({ emailColumn })}
        columns={sourceColumns}
      />

      <div className="grid gap-3 lg:grid-cols-2">
        <DedupeColumnToggle
          label="Remove fully duplicate rows"
          description="The entire row must match."
          checked={dedupeConfig.removeFullDuplicates}
          onCheckedChange={(removeFullDuplicates) => setDedupeConfig({ removeFullDuplicates })}
        />
        <DedupeColumnToggle
          label="Remove rows with duplicate emails"
          description="Duplicate email values keep only one occurrence."
          checked={dedupeConfig.removeDuplicateEmails}
          onCheckedChange={(removeDuplicateEmails) => setDedupeConfig({ removeDuplicateEmails })}
        />
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <div className="rounded-lg border bg-background p-4">
          <div className="text-2xl font-semibold tabular-nums">
            {(analysis?.fullDuplicates ?? 0).toLocaleString()}
          </div>
          <div className="mt-1 text-sm text-muted-foreground">fully duplicate rows found</div>
        </div>
        <div className="rounded-lg border bg-background p-4">
          <div className="text-2xl font-semibold tabular-nums">
            {(analysis?.emailDuplicates ?? 0).toLocaleString()}
          </div>
          <div className="mt-1 text-sm text-muted-foreground">duplicate email rows found</div>
        </div>
      </div>

      {isLoading ? (
        <AnalysisSkeleton rows={2} />
      ) : null}

      {error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">{error}</div>
      ) : null}

      <label className="block space-y-2 text-sm">
        <span className="font-medium">Which row to keep on duplicate?</span>
        <select
          value={dedupeConfig.strategy}
          onChange={(event) => setDedupeConfig({ strategy: event.target.value as "first" | "last" })}
          className="w-full rounded-lg border bg-background px-3 py-2 text-sm outline-none transition focus:ring-2 focus:ring-primary"
        >
          <option value="first">First occurrence</option>
          <option value="last">Last occurrence</option>
        </select>
      </label>

      {analysis ? (
        <p className="text-xs text-muted-foreground">
          Scanned {analysis.totalRows.toLocaleString()} rows for duplicate analysis.
        </p>
      ) : null}
    </div>
  );
}

function ProcessingModal({
  open,
  event,
  events,
  onClose,
}: {
  open: boolean;
  event: RunEvent | null;
  events: RunEvent[];
  onClose: () => void;
}) {
  if (!open) return null;

  const isComplete = event?.step === "complete";
  const isError = event?.step === "error";
  const stats = event?.stats;
  const progress = isComplete ? 100 : Math.min(Number(event?.progress ?? 0), 100);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-6 backdrop-blur">
      <div className="max-h-[90vh] w-full max-w-4xl overflow-auto rounded-lg border bg-card p-6 shadow-xl">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-2xl font-bold">Processing Transform</h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {isComplete ? "Transform complete." : isError ? "Transform failed." : `Running ${event?.step ?? "pipeline"} step...`}
            </p>
          </div>
          {(isComplete || isError) ? (
            <Button variant="outline" onClick={onClose}>Close</Button>
          ) : null}
        </div>

        <div className="mt-6">
          <div className="mb-2 flex items-center justify-between text-sm">
            <span className="font-medium">{event?.step ?? "starting"}</span>
            <span className="text-muted-foreground tabular-nums">{progress}%</span>
          </div>
          <div className="h-3 overflow-hidden rounded-full bg-muted">
            <div className="h-full bg-primary transition-all" style={{ width: `${progress}%` }} />
          </div>
        </div>

        {stats ? (
          <div className="mt-6 grid gap-3 sm:grid-cols-3">
            <StatCard label="Input Rows" value={stats.inputRows} />
            <StatCard label="Output Rows" value={stats.outputRows} />
            <StatCard label="Rows Removed" value={stats.rowsRemoved} />
          </div>
        ) : null}

        <div className="mt-6 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard label="Full Duplicates" value={stats?.rowsRemovedFullDupe ?? event?.rowsRemovedFullDupe ?? 0} />
          <StatCard label="Email Duplicates" value={stats?.rowsRemovedEmailDupe ?? event?.rowsRemovedEmailDupe ?? 0} />
          <StatCard label="Email Filter" value={stats?.rowsRemovedInvalidEmail ?? event?.rowsRemovedInvalidEmail ?? 0} />
          <StatCard label="Gender Filter" value={stats?.rowsRemovedWrongGender ?? event?.rowsRemovedWrongGender ?? 0} />
        </div>

        <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard label="Emails Typo-Fixed" value={stats?.emailsTypoFixed ?? event?.emailsTypoFixed ?? 0} />
          <StatCard label="Male" value={stats?.maleCount ?? event?.maleCount ?? 0} />
          <StatCard label="Female" value={stats?.femaleCount ?? event?.femaleCount ?? 0} />
          <StatCard label="Unknown" value={stats?.unknownCount ?? event?.unknownCount ?? 0} />
        </div>

        {stats ? (
          <div className="mt-3 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <StatCard label="Skipped Rows" value={stats.skippedRows} />
          </div>
        ) : null}

        {isError ? (
          <div className="mt-6 rounded-lg border border-destructive/40 bg-destructive/10 p-4 text-sm text-destructive">
            {event?.error || "Transform failed."}
          </div>
        ) : null}

        <div className="mt-6 flex flex-wrap gap-3">
          {event?.outputFile ? (
            <Button onClick={() => window.location.assign(event.outputFile as string)}>
              <Download className="mr-2 h-4 w-4" />
              Download CSV
            </Button>
          ) : null}
          <Button variant="outline" disabled>
            Save to NocoDB
          </Button>
        </div>

        <div className="mt-6 rounded-lg border bg-background p-4">
          <div className="mb-2 text-sm font-medium">Live events</div>
          <div className="max-h-44 space-y-1 overflow-auto font-mono text-xs text-muted-foreground">
            {events.map((item, index) => (
              <div key={index}>{JSON.stringify(item)}</div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export function FiltersSection({ totalRows, previewRows, sourceColumns, inputFile, filtersRef }: FiltersSectionProps) {
  const mapping = useTransformStore((state) => state.mapping);
  const filters = useTransformStore((state) => state.filters);
  const setFilterEnabled = useTransformStore((state) => state.setFilterEnabled);
  const estimate = useFilterEstimate(totalRows, previewRows, filters);
  const [runModalOpen, setRunModalOpen] = useState(false);
  const [runEvents, setRunEvents] = useState<RunEvent[]>([]);
  const [latestRunEvent, setLatestRunEvent] = useState<RunEvent | null>(null);
  const [history, setHistory] = useState<TransformHistoryEntry[]>([]);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(historyStorageKey);
      if (stored) setHistory(JSON.parse(stored) as TransformHistoryEntry[]);
    } catch {
      setHistory([]);
    }
  }, []);

  const mappedColumns = useMemo(
    () => mapping.map((item: TransformMapping) => item.outputColumn),
    [mapping]
  );

  const runDisabled = mapping.length === 0;

  const saveHistoryEntry = (entry: TransformHistoryEntry) => {
    setHistory((current) => {
      const next = [entry, ...current].slice(0, 10);
      window.localStorage.setItem(historyStorageKey, JSON.stringify(next));
      return next;
    });
  };

  const handleRunTransform = async () => {
    const state = useTransformStore.getState();
    const filtersApplied = filtersAppliedLabel(state.filters);
    setRunModalOpen(true);
    setRunEvents([]);
    setLatestRunEvent({ step: "starting", progress: 0 });

    try {
      const response = await fetch("/api/transform/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          uploadId: state.uploadId,
          mapping: state.mapping,
          filters: state.filters,
        }),
      });

      if (!response.ok || !response.body) {
        throw new Error("Transform pipeline failed to start.");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split("\n\n");
        buffer = events.pop() ?? "";

        events.forEach((rawEvent) => {
          const line = rawEvent.split("\n").find((item) => item.startsWith("data: "));
          if (!line) return;
          const parsed = JSON.parse(line.slice("data: ".length)) as RunEvent;
          if (parsed.warning) toast.warning(parsed.warning);
          if (parsed.step === "complete" && parsed.stats) {
            saveHistoryEntry({
              date: new Date().toISOString(),
              inputFile: inputFile || "Uploaded CSV",
              outputRows: parsed.stats.outputRows,
              inputRows: parsed.stats.inputRows,
              filtersApplied,
              outputFile: parsed.outputFile,
            });
          }
          setLatestRunEvent(parsed);
          setRunEvents((current) => [...current, parsed]);
        });
      }
    } catch (error) {
      const event = {
        step: "error",
        error: error instanceof Error ? error.message : "Transform failed.",
      };
      setLatestRunEvent(event);
      setRunEvents((current) => [...current, event]);
    }
  };

  return (
    <section ref={filtersRef} className="scroll-mt-8 space-y-6">
      <ProcessingModal
        open={runModalOpen}
        event={latestRunEvent}
        events={runEvents}
        onClose={() => setRunModalOpen(false)}
      />
      <div className="sticky top-0 z-10 rounded-lg border bg-card/95 p-4 shadow-sm backdrop-blur">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-xl font-semibold">Filters</h2>
            <p className="mt-1 text-sm text-muted-foreground">Configure row filters before running the transform.</p>
          </div>
        </div>
        <div className="flex gap-3 overflow-x-auto pb-2">
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
          <EmailFilterConfigPanel mappedColumns={mappedColumns} mapping={mapping} />
        </FilterCard>

        <FilterCard
          title="Gender Filter"
          icon={<UsersRound className="h-4 w-4" />}
          enabled={filters.gender.enabled}
          onEnabledChange={(enabled) => setFilterEnabled("gender", enabled)}
          miniStat={`Will remove ${estimate.gender.toLocaleString()} rows`}
        >
          <GenderFilterConfigPanel sourceColumns={sourceColumns} enabled={filters.gender.enabled} />
        </FilterCard>

        <FilterCard
          title="Duplicate Remover"
          icon={<ScanSearch className="h-4 w-4" />}
          enabled={filters.deduplication.enabled}
          onEnabledChange={(enabled) => setFilterEnabled("deduplication", enabled)}
          miniStat={`Will remove ${estimate.deduplication.toLocaleString()} rows`}
        >
          <DuplicateRemoverConfigPanel sourceColumns={sourceColumns} enabled={filters.deduplication.enabled} />
        </FilterCard>
      </div>

      <Button size="lg" className="h-12 w-full text-base" disabled={runDisabled} onClick={handleRunTransform}>
        Run Transform
      </Button>

      <Card className="rounded-lg">
        <CardHeader>
          <CardTitle>Transform History</CardTitle>
        </CardHeader>
        <CardContent>
          {history.length > 0 ? (
            <div className="space-y-3">
              {history.map((entry) => (
                <div key={`${entry.date}-${entry.inputFile}`} className="flex flex-col gap-3 rounded-lg border bg-background p-4 sm:flex-row sm:items-center sm:justify-between">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium">{entry.inputFile}</div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      {new Date(entry.date).toLocaleString()} · {entry.outputRows.toLocaleString()} of {entry.inputRows.toLocaleString()} rows kept
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">
                      {entry.filtersApplied.length > 0 ? entry.filtersApplied.join(", ") : "No filters applied"}
                    </div>
                  </div>
                  {entry.outputFile ? (
                    <Button type="button" variant="outline" onClick={() => window.location.assign(entry.outputFile as string)}>
                      <Download className="mr-2 h-4 w-4" />
                      Download
                    </Button>
                  ) : null}
                </div>
              ))}
            </div>
          ) : (
            <div className="rounded-lg border border-dashed p-6 text-sm text-muted-foreground">
              Completed transforms will appear here.
            </div>
          )}
        </CardContent>
      </Card>
    </section>
  );
}
