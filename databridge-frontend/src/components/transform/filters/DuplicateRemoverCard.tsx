import { useEffect, useState, useMemo } from "react";
import { Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { useTransformStore } from "@/lib/transform-store";

export interface DuplicateRemoverCardProps {
  uploadId: string;
  emailColumn: string;
  onStatsChange: (duplicatesRemoved: number) => void;
}

type DuplicatesStats = {
  fullDuplicates: number;
  emailDuplicates: number;
};

export function DuplicateRemoverCard({ uploadId, emailColumn, onStatsChange }: DuplicateRemoverCardProps) {
  const { filters, setFilterEnabled, setDedupeConfig } = useTransformStore();
  const enabled = filters.deduplication.enabled;
  const config = filters.deduplication.config;

  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<DuplicatesStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [initializedDefaults, setInitializedDefaults] = useState(false);

  // Initialize both checkboxes ON by default on mount if enabled and not done
  useEffect(() => {
    if (enabled && !initializedDefaults) {
      if (!config.removeDuplicateEmails) {
        setDedupeConfig({ removeDuplicateEmails: true });
      }
      setInitializedDefaults(true);
    }
  }, [enabled, initializedDefaults, config.removeDuplicateEmails, setDedupeConfig]);

  useEffect(() => {
    if (enabled && uploadId) {
      if (config.emailColumn !== emailColumn) {
        setDedupeConfig({ emailColumn });
      }

      let isCancelled = false;

      const fetchAnalysis = async () => {
        if (!emailColumn) {
          setError("An 'email' column must be mapped to use deduplication.");
          return;
        }
        setLoading(true);
        setError(null);
        try {
          const colParam = emailColumn ? `&emailColumn=${encodeURIComponent(emailColumn)}` : "";
          const res = await fetch(`/api/transform/analyze/duplicates?uploadId=${encodeURIComponent(uploadId)}${colParam}`);
          if (!res.ok) {
            let errMessage = "Failed to fetch duplicates analysis";
            try {
              const errBody = await res.json();
              if (errBody.error) errMessage = errBody.error;
              else if (errBody.message) errMessage = errBody.message;
            } catch (e) {}
            throw new Error(errMessage);
          }
          const json = await res.json();
          if (!isCancelled) {
            setData(json);
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
      };

      // Fetch on mount or when column changes
      fetchAnalysis();

      return () => {
        isCancelled = true;
      };
    } else {
      onStatsChange(0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, uploadId, emailColumn]);

  const duplicatesRemoved = useMemo(() => {
    if (!enabled || !data) return 0;
    
    let sum = 0;
    if (config.removeFullDuplicates) sum += (data.fullDuplicates || 0);
    if (config.removeDuplicateEmails) sum += (data.emailDuplicates || 0);
    return sum;
  }, [enabled, data, config.removeFullDuplicates, config.removeDuplicateEmails]);

  useEffect(() => {
    onStatsChange(duplicatesRemoved);
  }, [duplicatesRemoved, onStatsChange]);

  return (
    <Card className="overflow-hidden">
      <CardHeader className="p-4 bg-muted/20 border-b flex flex-row items-center justify-between space-y-0">
        <div className="flex items-center gap-3">
          <Switch
            checked={enabled}
            onCheckedChange={(checked) => setFilterEnabled("deduplication", checked)}
          />
          <h3 className="font-semibold">Deduplication Filter</h3>
        </div>
        {enabled && (
          <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-destructive/10 text-destructive">
            Will remove {duplicatesRemoved.toLocaleString()} duplicates
          </span>
        )}
      </CardHeader>

      {enabled && (
        <CardContent className="p-0">
          {loading ? (
            <div className="p-8 flex flex-col items-center justify-center text-muted-foreground">
              <Loader2 className="h-8 w-8 animate-spin mb-4" />
              <p className="text-sm">Finding duplicates...</p>
            </div>
          ) : error ? (
            <div className="p-6 text-sm text-red-500 bg-red-50/50 dark:bg-red-950/20">
              Error: {error}
            </div>
          ) : data ? (
            <div className="grid gap-6 p-6 md:grid-cols-2">
              <div className="space-y-6">
                <div className="grid gap-4">
                  <div className="flex items-center justify-between rounded-lg border p-4 shadow-sm bg-card">
                    <div>
                      <p className="font-medium text-sm">Fully duplicate rows</p>
                      <p className="text-xs text-muted-foreground mt-0.5">Exact copies of another row</p>
                    </div>
                    <div className="text-right">
                      <p className="font-semibold text-orange-500 text-lg">{data.fullDuplicates?.toLocaleString() || 0}</p>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider">Found</p>
                    </div>
                  </div>
                  
                  <div className="flex items-center justify-between rounded-lg border p-4 shadow-sm bg-card">
                    <div>
                      <p className="font-medium text-sm">Duplicate email rows</p>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        Multiple rows sharing the same email{emailColumn ? ` (${emailColumn})` : ""}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="font-semibold text-orange-500 text-lg">{data.emailDuplicates?.toLocaleString() || 0}</p>
                      <p className="text-[10px] text-muted-foreground uppercase tracking-wider">Found</p>
                    </div>
                  </div>
                </div>
              </div>

              <div className="space-y-6 rounded-lg border bg-muted/20 p-4">
                <div>
                  <h4 className="text-sm font-medium mb-3">Removal Rules</h4>
                  <div className="space-y-3">
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input
                        type="checkbox"
                        className="h-4 w-4 rounded border-gray-300 text-primary accent-primary cursor-pointer"
                        checked={config.removeFullDuplicates}
                        onChange={(e) => setDedupeConfig({ removeFullDuplicates: e.target.checked })}
                      />
                      <span className="text-sm font-medium">Remove fully duplicate rows</span>
                    </label>
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input
                        type="checkbox"
                        className="h-4 w-4 rounded border-gray-300 text-primary accent-primary cursor-pointer"
                        checked={config.removeDuplicateEmails}
                        onChange={(e) => setDedupeConfig({ removeDuplicateEmails: e.target.checked })}
                      />
                      <span className="text-sm font-medium">Remove rows with duplicate email</span>
                    </label>
                  </div>
                </div>

                <div className="pt-4 border-t border-muted">
                  <h4 className="text-sm font-medium mb-3">Conflict Resolution Strategy</h4>
                  <div className="space-y-3">
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input 
                        type="radio" 
                        name="dedupeStrategy"
                        checked={config.strategy === "first"} 
                        onChange={() => setDedupeConfig({ strategy: "first" })} 
                        className="h-4 w-4 accent-primary cursor-pointer border-gray-300" 
                      />
                      <span className="text-sm font-medium">Keep first occurrence</span>
                    </label>
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input 
                        type="radio" 
                        name="dedupeStrategy"
                        checked={config.strategy === "last"} 
                        onChange={() => setDedupeConfig({ strategy: "last" })} 
                        className="h-4 w-4 accent-primary cursor-pointer border-gray-300" 
                      />
                      <span className="text-sm font-medium">Keep last occurrence</span>
                    </label>
                  </div>
                  <p className="text-xs text-muted-foreground ml-7 mt-2">
                    Determines which row is preserved when identical matching records exist.
                  </p>
                </div>
              </div>
            </div>
          ) : null}
        </CardContent>
      )}
    </Card>
  );
}
