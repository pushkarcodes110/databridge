import { useEffect, useState, useMemo } from "react";
import { Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { useTransformStore } from "@/lib/transform-store";

export interface GenderFilterCardProps {
  uploadId: string;
  nameColumn: string;
  onStatsChange: (rowsRemoved: number) => void;
}

type GenderStats = {
  totalRows: number;
  sampleSize: number;
  male: number;
  female: number;
  unknown: number;
};

export function GenderFilterCard({ uploadId, nameColumn, onStatsChange }: GenderFilterCardProps) {
  const { filters, setFilterEnabled, setGenderConfig, totalRows } = useTransformStore();
  const enabled = filters.gender.enabled;
  const config = filters.gender.config;

  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<GenderStats | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (enabled && uploadId && nameColumn) {
      if (config.nameColumn !== nameColumn) {
        setGenderConfig({ nameColumn });
      }

      let isCancelled = false;

      async function fetchAnalysis() {
        setLoading(true);
        setError(null);
        try {
          const res = await fetch("/api/transform/analyze/gender", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ uploadId, nameColumn, sampleSize: 200 }),
          });
          if (!res.ok) {
            throw new Error("Failed to fetch gender analysis");
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
      }

      if (!data || config.nameColumn !== nameColumn) {
        fetchAnalysis();
      }

      return () => {
        isCancelled = true;
      };
    } else {
      onStatsChange(0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, uploadId, nameColumn]);

  const { scale, estMale, estFemale, estUnknown } = useMemo(() => {
    if (!data) return { scale: 1, estMale: 0, estFemale: 0, estUnknown: 0 };
    // Prevent division by zero or oversized sample logic
    const s = totalRows > 0 ? (totalRows / Math.min(data.sampleSize || 200, totalRows)) : 1;
    return {
      scale: s,
      estMale: Math.round((data.male || 0) * s),
      estFemale: Math.round((data.female || 0) * s),
      estUnknown: Math.round((data.unknown || 0) * s),
    };
  }, [data]);

  const rowsRemoved = useMemo(() => {
    if (!enabled || !data) return 0;
    
    if (config.mode === "male") return estFemale + estUnknown;
    if (config.mode === "female") return estMale + estUnknown;
    return 0; // "all" mode
  }, [enabled, data, config.mode, estMale, estFemale, estUnknown]);

  useEffect(() => {
    onStatsChange(rowsRemoved);
  }, [rowsRemoved, onStatsChange]);

  return (
    <Card className="overflow-hidden">
      <CardHeader className="p-4 bg-muted/20 border-b flex flex-row items-center justify-between space-y-0">
        <div className="flex items-center gap-3">
          <Switch
            checked={enabled}
            onCheckedChange={(checked) => setFilterEnabled("gender", checked)}
          />
          <h3 className="font-semibold">Gender Inference Filter</h3>
        </div>
        {enabled && (
          <span className="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold border-transparent bg-destructive/10 text-destructive">
            Will remove ~{rowsRemoved.toLocaleString()} rows
          </span>
        )}
      </CardHeader>

      {enabled && (
        <CardContent className="p-0">
          {loading ? (
            <div className="p-8 flex flex-col items-center justify-center text-muted-foreground">
              <Loader2 className="h-8 w-8 animate-spin mb-4" />
              <p className="text-sm">Analyzing names sample...</p>
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
                    <p className="text-xs text-muted-foreground">Est. Male Names</p>
                    <p className="mt-1 font-semibold text-blue-500">~{estMale.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Est. Female Names</p>
                    <p className="mt-1 font-semibold text-pink-500">~{estFemale.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Est. Unknown</p>
                    <p className="mt-1 font-semibold text-orange-500">~{estUnknown.toLocaleString()}</p>
                  </div>
                  <div className="rounded-lg border bg-card p-3">
                    <p className="text-xs text-muted-foreground">Total Rows</p>
                    <p className="mt-1 font-semibold">{totalRows.toLocaleString()}</p>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground italic">
                  Note: Based on a sample of the first {data.sampleSize || 200} rows.
                </p>
              </div>

              <div className="space-y-6 rounded-lg border bg-muted/20 p-4">
                <div>
                  <h4 className="text-sm font-medium mb-3">Filter Mode</h4>
                  <div className="space-y-3">
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input 
                        type="radio" 
                        name="genderMode"
                        checked={config.mode === "all"} 
                        onChange={() => setGenderConfig({ mode: "all" })} 
                        className="h-4 w-4 accent-primary cursor-pointer border-gray-300 text-primary" 
                      />
                      <span className="text-sm font-medium">Keep All</span>
                    </label>
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input 
                        type="radio" 
                        name="genderMode"
                        checked={config.mode === "male"} 
                        onChange={() => setGenderConfig({ mode: "male" })} 
                        className="h-4 w-4 accent-primary cursor-pointer border-gray-300 text-primary" 
                      />
                      <span className="text-sm font-medium">Keep Male Only</span>
                    </label>
                    <label className="flex items-center gap-3 cursor-pointer">
                      <input 
                        type="radio" 
                        name="genderMode"
                        checked={config.mode === "female"} 
                        onChange={() => setGenderConfig({ mode: "female" })} 
                        className="h-4 w-4 accent-primary cursor-pointer border-gray-300 text-primary" 
                      />
                      <span className="text-sm font-medium">Keep Female Only</span>
                    </label>
                  </div>
                </div>

                <div className="pt-4 border-t border-muted">
                  <h4 className="text-sm font-medium mb-3">Enrichment</h4>
                  <label className="flex items-center gap-3 cursor-pointer">
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-gray-300 text-primary accent-primary cursor-pointer"
                      checked={config.addGenderColumn}
                      onChange={(e) => setGenderConfig({ addGenderColumn: e.target.checked })}
                    />
                    <span className="text-sm font-medium">Add 'gender' column to output</span>
                  </label>
                  <p className="text-xs text-muted-foreground ml-7 mt-1">
                    Adds a new column indicating 'male', 'female', or 'unknown'.
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
