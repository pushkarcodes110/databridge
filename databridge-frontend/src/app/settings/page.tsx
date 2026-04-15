"use client";

import { useState, useEffect } from "react";
import { getSettings, saveSettings, testNocoDBConnection } from "@/lib/api";
import { Database, Lock, Save, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

export default function SettingsPage() {
  const [url, setUrl] = useState("");
  const [token, setToken] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{status: string, message: string} | null>(null);

  useEffect(() => {
    getSettings()
      .then(data => {
        setUrl(data.nocodb_url || "");
        setToken(data.nocodb_api_token || "");
      })
      .catch(() => toast.error("Failed to load settings."))
      .finally(() => setLoading(false));
  }, []);

  const handleSave = async () => {
    setSaving(true);
    try {
      await saveSettings({ nocodb_url: url, nocodb_api_token: token, default_concurrency: 5, table_presets: [] });
      toast.success("Settings saved.");
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
      const res = await testNocoDBConnection({ nocodb_url: url, nocodb_api_token: token, default_concurrency: 5, table_presets: [] });
      setTestResult(res);
      if (res.status === "success") {
        toast.success(res.message || "NocoDB connection works.");
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

  return (
    <div className="max-w-2xl">
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
    </div>
  );
}
