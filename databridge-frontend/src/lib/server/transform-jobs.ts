import { join } from "path";
import { runTransform, TransformConfig, RunStats } from "@/lib/server/transform-runner";

type RunEvent = {
  step: string;
  progress?: number;
  outputFile?: string;
  headers?: string[];
  stats?: RunStats;
  error?: string;
  warning?: string;
  [key: string]: unknown;
};

type AutoImportConfig = {
  enabled: boolean;
  tableName: string;
};

type TransformJob = {
  id: string;
  status: "pending" | "running" | "complete" | "failed";
  events: RunEvent[];
  latestEvent: RunEvent | null;
  createdAt: string;
  completedAt?: string;
  autoImport?: AutoImportConfig;
  importJobId?: string;
  importError?: string;
};

const globalJobs = globalThis as typeof globalThis & {
  __databridgeTransformJobs?: Map<string, TransformJob>;
};

const jobs = globalJobs.__databridgeTransformJobs ?? new Map<string, TransformJob>();
globalJobs.__databridgeTransformJobs = jobs;

function apiBaseUrl() {
  return process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";
}

function cleanTableName(name: string) {
  const cleaned = name.trim().replace(/\.[^.]+$/, "").replace(/[^\w\s-]/g, "").replace(/\s+/g, " ");
  return cleaned || `transform_${new Date().toISOString().slice(0, 10)}`;
}

async function backendJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseUrl()}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(data?.detail || data?.error || `Backend request failed: ${response.status}`);
  }
  return data as T;
}

async function autoImportToNoco(job: TransformJob, uploadId: string, headers: string[], stats: RunStats) {
  if (!job.autoImport?.enabled || stats.outputRows <= 0) return;

  const bases = await backendJson<Array<{ id: string; title: string }>>("/nocodb/bases");
  const defaultBase = bases[0];
  if (!defaultBase?.id) throw new Error("No NocoDB base found for auto import.");

  const tableName = cleanTableName(job.autoImport.tableName);
  const createdTable = await backendJson<{ id: string }>(`/nocodb/tables/${encodeURIComponent(defaultBase.id)}`, {
    method: "POST",
    body: JSON.stringify({
      table_name: tableName,
      columns: headers,
    }),
  });

  const columnMapping = Object.fromEntries(headers.map((header) => [header, header]));
  const importJob = await backendJson<{ id: string }>("/jobs/", {
    method: "POST",
    body: JSON.stringify({
      filename: `${tableName}.csv`,
      file_path: join("/tmp", "databridge", "outputs", uploadId, "output.csv"),
      file_size: stats.outputRows,
      total_rows: stats.outputRows,
      file_format: "csv",
      nocodb_base_id: defaultBase.id,
      nocodb_table_id: createdTable.id,
      column_mapping: columnMapping,
      options: { source: "transform-auto-import", uploadId },
    }),
  });

  job.importJobId = importJob.id;
  const event: RunEvent = {
    step: "noco_import",
    progress: 100,
    importJobId: importJob.id,
    tableName,
    baseId: defaultBase.id,
  };
  job.events.push(event);
  job.latestEvent = event;
}

export function startTransformJob(config: TransformConfig, autoImport?: AutoImportConfig) {
  const jobId = crypto.randomUUID();
  const job: TransformJob = {
    id: jobId,
    status: "pending",
    events: [],
    latestEvent: { step: "queued", progress: 0 },
    createdAt: new Date().toISOString(),
    autoImport,
  };
  jobs.set(jobId, job);

  void (async () => {
    job.status = "running";
    const emit = (event: object) => {
      const parsed = event as RunEvent;
      job.events.push(parsed);
      job.latestEvent = parsed;
    };

    try {
      await runTransform(config, emit);
      const completeEvent = job.events.findLast((event) => event.step === "complete");
      if (completeEvent?.headers && completeEvent.stats) {
        try {
          await autoImportToNoco(job, config.uploadId, completeEvent.headers, completeEvent.stats);
        } catch (error) {
          job.importError = error instanceof Error ? error.message : "Auto import failed.";
          const importErrorEvent: RunEvent = { step: "noco_import", error: job.importError };
          job.events.push(importErrorEvent);
          job.latestEvent = importErrorEvent;
        }
      }
      job.status = "complete";
    } catch (error) {
      const message = error instanceof Error ? error.message : "Transform failed.";
      const errorEvent: RunEvent = { step: "error", error: message };
      job.events.push(errorEvent);
      job.latestEvent = errorEvent;
      job.status = "failed";
    } finally {
      job.completedAt = new Date().toISOString();
    }
  })();

  return job;
}

export function getTransformJob(jobId: string) {
  return jobs.get(jobId) ?? null;
}
