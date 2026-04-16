import { randomUUID } from "crypto";
import { createReadStream } from "fs";
import { join } from "path";
import { access, mkdir, readFile, readdir, writeFile } from "fs/promises";
import csv from "csv-parser";
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

type WebhookSettings = {
  webhook_enabled?: boolean;
  webhook_url?: string | null;
  webhook_batch_size?: number | null;
};

type WebhookExportState = {
  status: "idle" | "running" | "complete" | "failed";
  url?: string;
  batchesSent: number;
  batchesFailed: number;
  rowsSent: number;
  error?: string;
  startedAt?: string;
  completedAt?: string;
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
  webhookExport?: WebhookExportState;
};

const globalJobs = globalThis as typeof globalThis & {
  __databridgeTransformJobs?: Map<string, TransformJob>;
};

const jobs = globalJobs.__databridgeTransformJobs ?? new Map<string, TransformJob>();
globalJobs.__databridgeTransformJobs = jobs;

const jobsDir = transformJobsDirPath();
let hydrated = false;
const WEBHOOK_DEFAULT_BATCH_SIZE = 500;
const WEBHOOK_MAX_BATCH_SIZE = 2000;
const WEBHOOK_MAX_BODY_BYTES = Number(process.env.WEBHOOK_MAX_BODY_BYTES || 4 * 1024 * 1024);
const WEBHOOK_TIMEOUT_MS = Number(process.env.WEBHOOK_TIMEOUT_MS || 20000);
const WEBHOOK_MAX_RETRIES = Number(process.env.WEBHOOK_MAX_RETRIES || 3);

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
    webhook_export: [99, 100],
  };
  const [start, end] = ranges[step] ?? [1, 98];
  return Math.round((start + ((end - start) * stepProgress / 100)) * 100) / 100;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clampWebhookBatchSize(value: unknown) {
  const parsed = Number(value || WEBHOOK_DEFAULT_BATCH_SIZE);
  if (!Number.isFinite(parsed)) return WEBHOOK_DEFAULT_BATCH_SIZE;
  return Math.max(1, Math.min(Math.floor(parsed), WEBHOOK_MAX_BATCH_SIZE));
}

function validWebhookUrl(value: string) {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

async function postWebhookBatch(url: string, rows: Record<string, string>[], batchNumber: number) {
  const body = JSON.stringify(rows);
  const bodyBytes = Buffer.byteLength(body);
  if (bodyBytes > WEBHOOK_MAX_BODY_BYTES) {
    throw new Error(`Webhook batch ${batchNumber} exceeds ${WEBHOOK_MAX_BODY_BYTES} bytes.`);
  }

  let lastError: string | null = null;
  for (let attempt = 1; attempt <= WEBHOOK_MAX_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        signal: controller.signal,
      });
      const text = await response.text().catch(() => "");
      if (response.ok) return;
      lastError = `Webhook batch ${batchNumber} failed (${response.status}): ${text.slice(0, 180)}`;
    } catch (error) {
      lastError = error instanceof Error && error.name === "AbortError"
        ? `Webhook batch ${batchNumber} timed out after ${WEBHOOK_TIMEOUT_MS}ms.`
        : error instanceof Error
          ? error.message
          : `Webhook batch ${batchNumber} failed.`;
    } finally {
      clearTimeout(timeout);
    }

    if (attempt < WEBHOOK_MAX_RETRIES) {
      await sleep(750 * Math.pow(2, attempt - 1));
    }
  }

  throw new Error(lastError || `Webhook batch ${batchNumber} failed.`);
}

function splitWebhookBatch(rows: Record<string, string>[]) {
  const chunks: Record<string, string>[][] = [];
  let current: Record<string, string>[] = [];

  for (const row of rows) {
    const candidate = [...current, row];
    if (Buffer.byteLength(JSON.stringify(candidate)) <= WEBHOOK_MAX_BODY_BYTES) {
      current = candidate;
      continue;
    }

    if (current.length > 0) {
      chunks.push(current);
      current = [row];
    }

    if (Buffer.byteLength(JSON.stringify(current)) > WEBHOOK_MAX_BODY_BYTES) {
      throw new Error(`A single webhook row exceeds ${WEBHOOK_MAX_BODY_BYTES} bytes.`);
    }
  }

  if (current.length > 0) chunks.push(current);
  return chunks;
}

async function exportCsvToWebhook(job: TransformJob, uploadId: string, settings: WebhookSettings) {
  if (!settings.webhook_enabled || !settings.webhook_url) return;
  if (!validWebhookUrl(settings.webhook_url)) {
    throw new Error("Webhook URL must start with http:// or https://.");
  }

  const outputPath = outputFilePath(uploadId);
  await access(outputPath);

  const batchSize = clampWebhookBatchSize(settings.webhook_batch_size);
  job.webhookExport = {
    status: "running",
    url: settings.webhook_url,
    batchesSent: 0,
    batchesFailed: 0,
    rowsSent: 0,
    startedAt: new Date().toISOString(),
  };
  job.events.push({ step: "webhook_export", progress: 0, batchSize });
  job.latestEvent = job.events[job.events.length - 1];
  await persistJob(job);

  let batch: Record<string, string>[] = [];
  let batchNumber = 0;
  let rowsRead = 0;

  const sendCurrentBatch = async () => {
    if (batch.length === 0) return;
    const rows = batch;
    batch = [];

    for (const chunk of splitWebhookBatch(rows)) {
      batchNumber += 1;
      try {
        await postWebhookBatch(settings.webhook_url as string, chunk, batchNumber);
        job.webhookExport!.batchesSent += 1;
        job.webhookExport!.rowsSent += chunk.length;
        const event: RunEvent = {
          step: "webhook_export",
          batch: batchNumber,
          rows: chunk.length,
          rowsSent: job.webhookExport!.rowsSent,
          status: "sent",
        };
        job.events.push(event);
        job.latestEvent = event;
        await persistJob(job);
      } catch (error) {
        job.webhookExport!.batchesFailed += 1;
        const message = error instanceof Error ? error.message : `Webhook batch ${batchNumber} failed.`;
        const event: RunEvent = {
          step: "webhook_export",
          batch: batchNumber,
          rows: chunk.length,
          status: "failed",
          error: message,
        };
        job.events.push(event);
        job.latestEvent = event;
        await persistJob(job);
        throw error;
      }
    }
  };

  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(outputPath).pipe(csv());
    stream
      .on("data", (row: Record<string, string>) => {
        stream.pause();
        rowsRead += 1;
        batch.push(row);
        const shouldFlush = batch.length >= batchSize;
        (shouldFlush ? sendCurrentBatch() : Promise.resolve())
          .then(() => stream.resume())
          .catch(reject);
      })
      .on("error", reject)
      .on("end", () => {
        sendCurrentBatch().then(resolve).catch(reject);
      });
  });

  job.webhookExport.status = "complete";
  job.webhookExport.completedAt = new Date().toISOString();
  const event: RunEvent = {
    step: "webhook_export",
    progress: 100,
    status: "complete",
    rowsSent: job.webhookExport.rowsSent,
    batchesSent: job.webhookExport.batchesSent,
    rowsRead,
  };
  job.events.push(event);
  job.latestEvent = event;
  await persistJob(job);
}

function queueWebhookExport(job: TransformJob, uploadId: string) {
  void (async () => {
    try {
      const settings = await backendJson<WebhookSettings>("/settings/");
      if (!settings.webhook_enabled) return;
      await exportCsvToWebhook(job, uploadId, settings);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Webhook export failed.";
      job.webhookExport = {
        ...(job.webhookExport || {
          status: "running",
          batchesSent: 0,
          batchesFailed: 0,
          rowsSent: 0,
          startedAt: new Date().toISOString(),
        }),
        status: "failed",
        error: message,
        completedAt: new Date().toISOString(),
      };
      const event: RunEvent = { step: "webhook_export", status: "failed", error: message };
      job.events.push(event);
      job.latestEvent = event;
      await persistJob(job);
    }
  })();
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
  const text = await response.text();
  let data: unknown = null;
  if (text) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json") || /^[\[{]/.test(text.trim())) {
      try {
        data = JSON.parse(text);
      } catch {
        throw new Error(`Backend returned invalid JSON (${response.status}): ${text.slice(0, 180)}`);
      }
    } else if (!response.ok) {
      throw new Error(`Backend returned non-JSON error (${response.status}): ${text.slice(0, 180)}`);
    }
  }

  if (!response.ok) {
    const detail = data && typeof data === "object" ? ((data as { detail?: unknown; error?: unknown }).detail || (data as { error?: unknown }).error) : null;
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

  const [settings, bases] = await Promise.all([
    backendJson<{ base_id?: string | null }>("/settings/"),
    backendJson<Array<{ id: string; title: string }>>("/nocodb/bases"),
  ]);
  const selectedBase = bases.find((base) => base.id === settings.base_id) || bases[0];
  if (!selectedBase?.id) throw new Error("No NocoDB base found for auto import.");

  const tableName = autoImportTableName(job.autoImport.tableName);
  const createdTable = await backendJson<{ id: string }>(`/nocodb/tables/${encodeURIComponent(selectedBase.id)}`, {
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
      nocodb_base_id: selectedBase.id,
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
      baseId: selectedBase.id,
      baseTitle: selectedBase.title,
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
      queueWebhookExport(job, config.uploadId);
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
