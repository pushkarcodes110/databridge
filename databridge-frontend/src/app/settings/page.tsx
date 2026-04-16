"use client";

import { useState, useEffect } from "react";
import { getBases, getSettings, saveSettings, testNocoDBConnection } from "@/lib/api";
import { CheckCircle2, Database, Link2, Lock, RefreshCw, Save, Send, Server, TriangleAlert, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

type HealthCheck = {
  name: string;
  status: "ok" | "warn" | "error";
  message: string;
  latencyMs?: number;
};

export default function SettingsPage() {
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [baseId, setBaseId] = useState("");
  const [bases, setBases] = useState<Array<{ id: string; title: string }>>([]);
  const [webhookEnabled, setWebhookEnabled] = useState(false);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [webhookBatchSize, setWebhookBatchSize] = useState(500);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [healthLoading, setHealthLoading] = useState(false);
  const [healthChecks, setHealthChecks] = useState<HealthCheck[]>([]);
  const [testResult, setTestResult] = useState<{status: string, message: string} | null>(null);

  const loadBases = async () => {
    try {
      const data = await getBases();
      setBases(data);
      setBaseId((current) => current || data[0]?.id || "");
    } catch {
      setBases([]);
    }
  };

  const loadHealth = async () => {
    setHealthLoading(true);
    try {
      const response = await fetch("/api/system/health", { cache: "no-store" });
      const data = await response.json();
      setHealthChecks(Array.isArray(data.checks) ? data.checks : []);
    } catch {
      toast.error("Failed to load system health.");
    } finally {
      setHealthLoading(false);
    }
  };

  useEffect(() => {
    getSettings()
      .then(data => {
        setUrl(data.nocodb_url || "");
        setToken(data.nocodb_api_token || "");
        setBaseId(data.base_id || "");
        setWebhookEnabled(Boolean(data.webhook_enabled));
        setWebhookUrl(data.webhook_url || "");
        setWebhookBatchSize(Number(data.webhook_batch_size || 500));
      })
      .catch(() => toast.error("Failed to load settings."))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!loading) {
      loadBases();
      loadHealth();
    }
  }, [loading]);

  const handleSave = async () => {
    setSaving(true);
    try {
      await saveSettings({
        nocodb_url: url,
        nocodb_api_token: token,
        base_id: baseId || null,
        webhook_enabled: webhookEnabled,
        webhook_url: webhookUrl || null,
        webhook_batch_size: webhookBatchSize,
        default_concurrency: 5,
        table_presets: [],
      });
      toast.success("Settings saved.");
      loadHealth();
    } catch (err) {
      toast.error("Failed to save settings.");
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const res = await testNocoDBConnection({ nocodb_url: url, nocodb_api_token: token, base_id: baseId || null, default_concurrency: 5, table_presets: [] });
      setTestResult(res);
      if (res.status === "success") {
        toast.success(res.message || "NocoDB connection works.");
        loadBases();
      } else {
        toast.error(res.message || "NocoDB connection failed.");
      }
    } catch (err: any) {
      const message = err.message || "Network Error";
      setTestResult({ status: "error", message });
      toast.error(message);
    } finally {
      setTesting(false);
    }
  };

  if (loading) return <div className="p-10 animate-pulse">Loading settings...</div>;
  const healthSummary = healthChecks.reduce(
    (summary, check) => ({
      ok: summary.ok + (check.status === "ok" ? 1 : 0),
      issue: summary.issue + (check.status !== "ok" ? 1 : 0),
    }),
    { ok: 0, issue: 0 }
  );

  return (
    <div className="max-w-4xl space-y-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2">NocoDB Configuration</h1>
        <p className="text-muted-foreground">
          Configure your NocoDB instance details below. These credentials are used globally by DataBridge workers for importing tabular data.
        </p>
      </div>

      <div className="bg-card rounded-2xl border shadow-sm overflow-hidden backdrop-blur-xl">
        <div className="p-6 space-y-6">
          <div className="space-y-4">
            <div>
              <label className="text-sm font-medium mb-1 block">NocoDB Base URL</label>
              <div className="relative">
                <Database className="w-5 h-5 absolute left-3 top-3 text-muted-foreground" />
                <input 
                  type="text" 
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                  placeholder="https://nocodb.yourdomain.com"
                  className="w-full bg-background border rounded-lg pl-10 pr-4 py-2.5 focus:ring-2 focus:ring-primary outline-none transition-all"
                />
              </div>
            </div>

            <div>
              <label className="text-sm font-medium mb-1 block">API Token (xc-token)</label>
              <div className="relative">
                <Lock className="w-5 h-5 absolute left-3 top-3 text-muted-foreground" />
                <input 
                  type="password" 
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  placeholder="Paste your xc-token here..."
                  className="w-full bg-background border rounded-lg pl-10 pr-4 py-2.5 focus:ring-2 focus:ring-primary outline-none transition-all"
                />
              </div>
              <p className="text-xs text-muted-foreground mt-2">
                This token is encrypted at rest in the PostgreSQL database.
              </p>
            </div>

            <div>
              <label className="text-sm font-medium mb-1 block">Default Base</label>
              <select
                value={baseId}
                onChange={(event) => setBaseId(event.target.value)}
                className="w-full bg-background border rounded-lg px-4 py-2.5 text-sm outline-none transition-all focus:ring-2 focus:ring-primary"
              >
                <option value="">Choose a base for auto-imports</option>
                {bases.map((base) => (
                  <option key={base.id} value={base.id}>{base.title}</option>
                ))}
              </select>
              <p className="text-xs text-muted-foreground mt-2">
                Transform auto-imports use this base. Quick Import still uses the base selected during each import.
              </p>
            </div>
          </div>
          
          {testResult && (
            <div className={`p-4 rounded-lg bg-opacity-10 border ${testResult.status === 'success' ? 'bg-green-500 border-green-500/20 text-green-400' : 'bg-red-500 border-red-500/20 text-red-500'}`}>
              <p className="text-sm font-medium">{testResult.message}</p>
            </div>
          )}
        </div>

        <div className="bg-muted/30 px-6 py-4 flex gap-4 border-t items-center justify-between">
          <Button variant="outline" onClick={handleTest} disabled={testing || !url || !token}>
            {testing ? "Testing..." : <><Zap className="w-4 h-4 mr-2" /> Test Connection</>}
          </Button>
          <Button onClick={handleSave} disabled={saving}>
            {saving ? "Saving..." : <><Save className="w-4 h-4 mr-2" /> Save Settings</>}
          </Button>
        </div>
      </div>

      <section className="bg-card rounded-2xl border shadow-sm overflow-hidden backdrop-blur-xl">
        <div className="border-b p-6">
          <h2 className="flex items-center gap-2 text-xl font-semibold">
            <Send className="h-5 w-5 text-primary" />
            Webhook Export
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Send final transformed CSV rows as JSON batches after generation completes.
          </p>
        </div>
        <div className="space-y-5 p-6">
          <label className="flex items-center justify-between gap-4 rounded-xl border bg-muted/20 p-4">
            <span>
              <span className="block text-sm font-semibold">Send to Webhook</span>
              <span className="mt-1 block text-xs text-muted-foreground">Runs in the background and does not block CSV generation.</span>
            </span>
            <input
              type="checkbox"
              checked={webhookEnabled}
              onChange={(event) => setWebhookEnabled(event.target.checked)}
              className="h-5 w-5 accent-primary"
            />
          </label>

          <div>
            <label className="text-sm font-medium mb-1 block">Webhook URL</label>
            <div className="relative">
              <Link2 className="w-5 h-5 absolute left-3 top-3 text-muted-foreground" />
              <input
                type="url"
                value={webhookUrl}
                onChange={(event) => setWebhookUrl(event.target.value)}
                placeholder="https://example.com/webhooks/databridge"
                className="w-full bg-background border rounded-lg pl-10 pr-4 py-2.5 focus:ring-2 focus:ring-primary outline-none transition-all"
              />
            </div>
          </div>

          <div>
            <label className="text-sm font-medium mb-1 block">Batch Size</label>
            <input
              type="number"
              min={1}
              max={2000}
              value={webhookBatchSize}
              onChange={(event) => setWebhookBatchSize(Math.max(1, Math.min(Number(event.target.value || 500), 2000)))}
              className="w-full bg-background border rounded-lg px-4 py-2.5 focus:ring-2 focus:ring-primary outline-none transition-all"
            />
            <p className="text-xs text-muted-foreground mt-2">
              Recommended: 500 to 2,000 rows. Large batches are automatically split if the request body is too large.
            </p>
          </div>

          <div className="flex justify-end border-t pt-5">
            <Button onClick={handleSave} disabled={saving}>
              {saving ? "Saving..." : <><Save className="w-4 h-4 mr-2" /> Save Webhook Settings</>}
            </Button>
          </div>
        </div>
      </section>

      <section className="bg-card rounded-2xl border shadow-sm overflow-hidden backdrop-blur-xl">
        <div className="border-b p-6">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h2 className="flex items-center gap-2 text-xl font-semibold">
                <Server className="h-5 w-5 text-primary" />
                System Health
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                API, storage, NocoDB, email validation, and optional Reacher checks.
              </p>
            </div>
            <div className="flex items-center gap-3">
              <div className="rounded-lg border bg-background px-3 py-2 text-sm">
                <span className="font-semibold text-green-600 dark:text-green-400">{healthSummary.ok}</span> healthy
                <span className="mx-2 text-muted-foreground">/</span>
                <span className={healthSummary.issue ? "font-semibold text-amber-600 dark:text-amber-400" : "font-semibold"}>{healthSummary.issue}</span> attention
              </div>
              <Button variant="outline" onClick={loadHealth} disabled={healthLoading}>
                <RefreshCw className={`mr-2 h-4 w-4 ${healthLoading ? "animate-spin" : ""}`} />
                Refresh
              </Button>
            </div>
          </div>
        </div>
        <div className="grid gap-3 p-6 md:grid-cols-2">
          {healthChecks.length === 0 ? (
            <div className="text-sm text-muted-foreground">{healthLoading ? "Checking services..." : "No health checks loaded."}</div>
          ) : healthChecks.map((check) => (
            <div key={check.name} className="rounded-xl border bg-muted/20 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="font-semibold">{check.name}</div>
                  <div className="mt-1 text-sm text-muted-foreground">{check.message}</div>
                </div>
                {check.status === "ok" ? (
                  <CheckCircle2 className="h-5 w-5 text-green-500" />
                ) : (
                  <TriangleAlert className={check.status === "warn" ? "h-5 w-5 text-amber-500" : "h-5 w-5 text-red-500"} />
                )}
              </div>
              {typeof check.latencyMs === "number" ? (
                <div className="mt-3 text-xs text-muted-foreground">{check.latencyMs}ms</div>
              ) : null}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
