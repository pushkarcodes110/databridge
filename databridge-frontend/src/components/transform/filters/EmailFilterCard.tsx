import { useEffect, useState, useMemo } from "react";
import { Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { useTransformStore } from "@/lib/transform-store";

export interface EmailFilterCardProps {
  uploadId: string;
  sourceColumn: string;
  onStatsChange: (rowsRemoved: number) => void;
}

type EmailStats = {
  totalEmails: number;
  invalidFormat: number;
  emptyEmails: number;
  duplicates: number;
  domains: { domain: string; count: number }[];
};

export function EmailFilterCard({ uploadId, sourceColumn, onStatsChange }: EmailFilterCardProps) {
  const { filters, setFilterEnabled, setEmailConfig } = useTransformStore();
  const enabled = filters.email.enabled;
  const config = filters.email.config;

  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<EmailStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (enabled && uploadId && sourceColumn) {
      if (config.column !== sourceColumn) {
        setEmailConfig({ column: sourceColumn });
      }

      let isCancelled = false;

      async function fetchAnalysis() {
        setLoading(true);
        setError(null);
        try {
          const res = await fetch(
            `/api/transform/analyze/email?uploadId=${encodeURIComponent(uploadId)}&sourceColumn=${encodeURIComponent(sourceColumn)}`
          );
          if (!res.ok) {
            throw new Error("Failed to fetch email analysis");
          }
          const json = await res.json();
          if (!isCancelled) {
            setData(json);
            // Default all domains to checked if we don't have any selected yet
            if (json.domains && config.selectedDomains.length === 0) {
              setEmailConfig({ selectedDomains: json.domains.map((d: any) => d.domain) });
            }
          }
        } catch (err: any) {
          if (!isCancelled) {
            setError(err.message || "Unknown error");
          }
        } finally {
          if (!isCancelled) {
            setLoading(false);
          }
        }
      }

      // Only fetch if data is not already loaded for this column
      if (!data || config.column !== sourceColumn) {
        fetchAnalysis();
      }

      return () => {
        isCancelled = true;
      };
    } else {
      // If disabled, report 0 rows removed
      onStatsChange(0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, uploadId, sourceColumn]);

  const rowsRemoved = useMemo(() => {
    if (!enabled || !data) return 0;

    let removed = 0;
    if (config.removeInvalidFormat) {
      removed += data.invalidFormat;
    }
    removed += data.emptyEmails;

    const checkedDomainsSet = new Set(config.selectedDomains);
    if (data.domains) {
      for (const d of data.domains) {
        if (!checkedDomainsSet.has(d.domain)) {
          removed += d.count;
        }
      }
    }

    return removed;
  }, [enabled, data, config.removeInvalidFormat, config.selectedDomains]);

  useEffect(() => {
    onStatsChange(rowsRemoved);
  }, [rowsRemoved, onStatsChange]);

  const handleDomainToggle = (domain: string, checked: boolean) => {
    if (checked) {
      setEmailConfig({ selectedDomains: [...config.selectedDomains, domain] });
    } else {
      setEmailConfig({ selectedDomains: config.selectedDomains.filter((d) => d !== domain) });
    }
  };

  const handleKeepAll = () => {
    if (data?.domains) {
      setEmailConfig({ selectedDomains: data.domains.map((d) => d.domain) });
    }
  };

  const handleGmailOnly = () => {
    if (data?.domains) {
      setEmailConfig({
        selectedDomains: data.domains.filter((d) => d.domain === "gmail.com").map((d) => d.domain),
      });
    }
  };

  const handleRemoveFreeProviders = () => {
    if (data?.domains) {
      const freeProviders = new Set(["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com"]);
      setEmailConfig({
        selectedDomains: data.domains.filter((d) => !freeProviders.has(d.domain)).map((d) => d.domain),
      });
    }
  };

  return (
    <Card className="overflow-hidden">
      <CardHeader className="p-4 bg-muted/20 border-b flex flex-row items-center justify-between space-y-0">
        <div className="flex items-center gap-3">
          <Switch
            checked={enabled}
            onCheckedChange={(checked) => setFilterEnabled("email", checked)}
          />
          <h3 className="font-semibold">Email Filter</h3>
        </div>
        {enabled && (
          <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-destructive/10 text-destructive">
            Will remove {rowsRemoved.toLocaleString()} rows
          </span>
        )}
      </CardHeader>

      {enabled && (
        <CardContent className="p-0">
          {loading ? (
            <div className="p-8 flex flex-col items-center justify-center text-muted-foreground">
              <Loader2 className="h-8 w-8 animate-spin mb-4" />
              <p className="text-sm">Analyzing email column...</p>
            </div>
          ) : error ? (
            <div className="p-6 text-sm text-red-500 bg-red-50/50 dark:bg-red-950/20">
              Error: {error}
            </div>
          ) : data ? (
            <div className="grid gap-6 p-6 md:grid-cols-2">
              <div className="space-y-6">
                <div className="grid grid-cols-2 gap-4">
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Total Emails</p>
                    <p className="mt-1 font-semibold">{(data.totalEmails || 0).toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Invalid Format</p>
                    <p className="mt-1 font-semibold text-red-500">{(data.invalidFormat || 0).toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Empty Emails</p>
                    <p className="mt-1 font-semibold text-red-500">{(data.emptyEmails || 0).toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Duplicates</p>
                    <p className="mt-1 font-semibold text-orange-500">{(data.duplicates || 0).toLocaleString()}</p>
                  </div>
                </div>

                <div className="space-y-4">
                  <h4 className="text-sm font-medium">Auto-Formatting</h4>
                  
                  <div className="flex items-center justify-between rounded-lg border p-3 shadow-sm">
                    <div className="space-y-0.5">
                      <label className="text-sm font-medium">Fix common typos</label>
                      <p className="text-xs text-muted-foreground">e.g., gamil.com &rarr; gmail.com</p>
                    </div>
                    <Switch
                      checked={config.fixCommonTypos}
                      onCheckedChange={(checked) => setEmailConfig({ fixCommonTypos: checked })}
                    />
                  </div>

                  <div className="flex items-center justify-between rounded-lg border p-3 shadow-sm">
                    <div className="space-y-0.5">
                      <label className="text-sm font-medium">Remove invalid format</label>
                      <p className="text-xs text-muted-foreground">Drops rows with invalid email syntax</p>
                    </div>
                    <Switch
                      checked={config.removeInvalidFormat}
                      onCheckedChange={(checked) => setEmailConfig({ removeInvalidFormat: checked })}
                    />
                  </div>

                  <div className="flex items-center justify-between rounded-lg border p-3 shadow-sm">
                    <div className="space-y-0.5">
                      <label className="text-sm font-medium">Normalize to lowercase</label>
                      <p className="text-xs text-muted-foreground">Converts all emails to lowercase</p>
                    </div>
                    <Switch
                      checked={config.normalizeLowercase}
                      onCheckedChange={(checked) => setEmailConfig({ normalizeLowercase: checked })}
                    />
                  </div>
                </div>
              </div>

              <div className="space-y-4 flex flex-col h-full">
                <div className="flex items-center justify-between gap-2 flex-wrap">
                  <h4 className="text-sm font-medium whitespace-nowrap">Accepted Domains</h4>
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={handleKeepAll} className="h-7 text-xs">
                      Keep All
                    </Button>
                    <Button variant="outline" size="sm" onClick={handleGmailOnly} className="h-7 text-xs">
                      Gmail Only
                    </Button>
                    <Button variant="outline" size="sm" onClick={handleRemoveFreeProviders} className="h-7 text-xs">
                      No Free
                    </Button>
                  </div>
                </div>

                <div className="rounded-md border bg-muted/20 flex-1 overflow-hidden flex flex-col min-h-[250px] shadow-sm">
                  <div className="p-2 border-b bg-muted/40 text-xs font-medium text-muted-foreground grid grid-cols-[auto_1fr_auto] gap-3">
                    <div className="w-4"></div>
                    <div>Domain</div>
                    <div>Count</div>
                  </div>
                  <div className="overflow-y-auto flex-1 p-2 space-y-1">
                    {data.domains && data.domains.map((d) => (
                      <label
                        key={d.domain}
                        className="flex cursor-pointer items-center justify-between rounded-sm px-2 py-1.5 hover:bg-muted/50"
                      >
                        <div className="flex items-center gap-3 min-w-0">
                          <input
                            type="checkbox"
                            className="h-4 w-4 rounded border-gray-300 text-primary accent-primary cursor-pointer"
                            checked={config.selectedDomains.includes(d.domain)}
                            onChange={(e) => handleDomainToggle(d.domain, e.target.checked)}
                          />
                          <span className="text-sm truncate select-none">{d.domain}</span>
                        </div>
                        <span className="text-xs text-muted-foreground tabular-nums">
                          {d.count.toLocaleString()}
                        </span>
                      </label>
                    ))}
                    {(!data.domains || data.domains.length === 0) && (
                      <div className="p-4 text-center text-sm text-muted-foreground">
                        No domains found
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          ) : null}
        </CardContent>
      )}
    </Card>
  );
}
