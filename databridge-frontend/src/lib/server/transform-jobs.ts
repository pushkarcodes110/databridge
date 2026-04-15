import { randomUUID } from "crypto";
import { join } from "path";
import { access, mkdir, readFile, readdir, writeFile } from "fs/promises";
import { runTransform, TransformConfig, RunStats } from "@/lib/server/transform-runner";
import { outputFilePath, transformJobsDirPath } from "@/lib/server/storage";

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
  progress: number;
  config: TransformConfig;
  events: RunEvent[];
  latestEvent: RunEvent | null;
  createdAt: string;
  startedAt?: string;
  updatedAt: string;
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

const jobsDir = transformJobsDirPath();
let hydrated = false;

function progressFromEvent(event: RunEvent | null, status: TransformJob["status"]) {
  if (status === "complete") return 100;
  if (status === "failed") return Number(event?.progress ?? 0);

  const step = event?.step || "queued";
  const stepProgress = Math.max(0, Math.min(Number(event?.progress ?? 0), 100));
  const ranges: Record<string, [number, number]> = {
    queued: [0, 1],
    mapping: [1, 20],
    deduplication: [20, 35],
    email: [35, 50],
    email_enrichment: [50, 70],
    gender: [70, 90],
    write: [90, 98],
    noco_import: [98, 99],
  };
  const [start, end] = ranges[step] ?? [1, 98];
  return Math.round((start + ((end - start) * stepProgress / 100)) * 100) / 100;
}

function publicJob(job: TransformJob) {
  return {
    ...job,
    progress: progressFromEvent(job.latestEvent, job.status),
  };
}

async function persistJob(job: TransformJob) {
  job.progress = progressFromEvent(job.latestEvent, job.status);
  job.updatedAt = new Date().toISOString();
  await mkdir(jobsDir, { recursive: true });
  await writeFile(join(jobsDir, `${job.id}.json`), JSON.stringify(job, null, 2));
}

async function hydrateJobs() {
  if (hydrated) return;
  hydrated = true;
  const entries = await readdir(jobsDir).catch(() => []);
  await Promise.all(entries.filter((entry) => entry.endsWith(".json")).map(async (entry) => {
    try {
      const data = JSON.parse(await readFile(join(jobsDir, entry), "utf8")) as TransformJob;
      if (data?.id && !jobs.has(data.id)) jobs.set(data.id, data);
    } catch {
      // Ignore malformed job snapshots.
    }
  }));
}

function remember(job: TransformJob) {
  jobs.set(job.id, job);
  void persistJob(job);
}

function apiBaseUrl() {
  return process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";
}

function cleanTableName(name: string) {
  const cleaned = name.trim().replace(/\.[^.]+$/, "").replace(/[^\w\s-]/g, "").replace(/\s+/g, " ");
  return cleaned || `transform_${new Date().toISOString().slice(0, 10)}`;
}

function autoImportTableName(name: string) {
  const base = cleanTableName(name).slice(0, 48).trim();
  const suffix = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
  return `${base} ${suffix}`;
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
    const detail = data?.detail || data?.error;
    const message = typeof detail === "string"
      ? detail
      : detail
        ? JSON.stringify(detail)
        : `Backend request failed: ${response.status}`;
    throw new Error(message);
  }
  return data as T;
}

async function autoImportToNoco(job: TransformJob, uploadId: string, headers: string[], stats: RunStats) {
  if (!job.autoImport?.enabled || stats.outputRows <= 0) return;

  const outputPath = outputFilePath(uploadId);
  try {
    await access(outputPath);
  } catch {
    throw new Error(`Transform output file is missing at ${outputPath}. Auto-import was not started.`);
  }

  const bases = await backendJson<Array<{ id: string; title: string }>>("/nocodb/bases");
  const defaultBase = bases[0];
  if (!defaultBase?.id) throw new Error("No NocoDB base found for auto import.");

  const tableName = autoImportTableName(job.autoImport.tableName);
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
      file_path: outputPath,
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
      filePath: outputPath,
  };
  job.events.push(event);
  job.latestEvent = event;
}

export function startTransformJob(config: TransformConfig, autoImport?: AutoImportConfig) {
  const jobId = randomUUID();
  const job: TransformJob = {
    id: jobId,
    status: "pending",
    progress: 0,
    config,
    events: [],
    latestEvent: { step: "queued", progress: 0 },
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    autoImport,
  };
  remember(job);

  void (async () => {
    job.status = "running";
    job.startedAt = new Date().toISOString();
    await persistJob(job);
    const emit = (event: object) => {
      const parsed = event as RunEvent;
      job.events.push(parsed);
      job.latestEvent = parsed;
      void persistJob(job);
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
        if (job.importJobId) completeEvent.importJobId = job.importJobId;
        if (job.importError) completeEvent.importError = job.importError;
        completeEvent.progress = 100;
        job.latestEvent = completeEvent;
      }
      job.status = "complete";
      await persistJob(job);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Transform failed.";
      const errorEvent: RunEvent = { step: "error", error: message };
      job.events.push(errorEvent);
      job.latestEvent = errorEvent;
      job.status = "failed";
      await persistJob(job);
    } finally {
      job.completedAt = new Date().toISOString();
      await persistJob(job);
    }
  })();

  return publicJob(job);
}

export async function getTransformJob(jobId: string) {
  await hydrateJobs();
  const job = jobs.get(jobId) ?? null;
  return job ? publicJob(job) : null;
}

export async function listTransformJobs(limit = 100) {
  await hydrateJobs();
  return Array.from(jobs.values())
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .slice(0, Math.min(Math.max(limit, 1), 200))
    .map(publicJob);
}
