"use client";

import { useEffect, useRef } from "react";
import { toast } from "sonner";
import { getJobs } from "@/lib/api";

type WatchedJob = {
  id: string;
  kind: "import" | "transform";
  status: string;
  filename: string;
  error_summary?: string | null;
  importError?: string | null;
};

type TransformJobApi = {
  id: string;
  status: string;
  autoImport?: { tableName?: string };
  config?: { uploadId?: string };
  importError?: string;
  latestEvent?: { error?: unknown } | null;
};

const activeStatuses = new Set(["pending", "queued", "running"]);
const terminalStatuses = new Set(["complete", "failed", "cancelled"]);

function jobLabel(job: WatchedJob) {
  return job.filename || `${job.kind === "transform" ? "Transform" : "Import"} ${job.id.slice(0, 8)}`;
}

function notifyJob(job: WatchedJob) {
  const label = jobLabel(job);

  if (job.status === "complete") {
    toast.success(`${label} completed.`);
    return;
  }

  if (job.status === "failed") {
    toast.error(`${label} failed.`, {
      description: job.error_summary || job.importError || undefined,
    });
    return;
  }

  if (job.status === "cancelled") {
    toast.warning(`${label} was cancelled.`);
  }
}

async function getTransformJobs(): Promise<WatchedJob[]> {
  const response = await fetch("/api/transform/jobs?limit=100", { cache: "no-store" });
  if (!response.ok) return [];

  const jobs = (await response.json()) as TransformJobApi[];
  return jobs.map((job) => ({
    id: job.id,
    kind: "transform",
    status: job.status,
    filename: job.autoImport?.tableName || `Transform ${job.config?.uploadId || job.id.slice(0, 8)}`,
    importError: job.importError || (job.latestEvent?.error ? String(job.latestEvent.error) : null),
  }));
}

async function getImportJobs(): Promise<WatchedJob[]> {
  const jobs = await getJobs();
  return jobs.map((job: Partial<WatchedJob> & { id: string; status: string; filename?: string }) => ({
    id: job.id,
    kind: "import",
    status: job.status,
    filename: job.filename || `Import ${job.id.slice(0, 8)}`,
    error_summary: job.error_summary || null,
  }));
}

export function BackgroundJobToasts() {
  const statuses = useRef(new Map<string, string>());
  const initialized = useRef(false);

  useEffect(() => {
    let mounted = true;
    let timeout: number | undefined;

    const poll = async () => {
      const [imports, transforms] = await Promise.all([
        getImportJobs().catch(() => []),
        getTransformJobs().catch(() => []),
      ]);

      if (!mounted) return;

      const jobs = [...imports, ...transforms];
      let hasActiveJobs = false;

      jobs.forEach((job) => {
        const key = `${job.kind}:${job.id}`;
        const previous = statuses.current.get(key);
        const isTerminal = terminalStatuses.has(job.status);
        const wasActive = previous ? activeStatuses.has(previous) : false;

        if (activeStatuses.has(job.status)) hasActiveJobs = true;
        if (initialized.current && isTerminal && (!previous || wasActive) && previous !== job.status) {
          notifyJob(job);
        }

        statuses.current.set(key, job.status);
      });

      initialized.current = true;
      timeout = window.setTimeout(poll, hasActiveJobs ? 2000 : 8000);
    };

    poll();

    return () => {
      mounted = false;
      if (timeout) window.clearTimeout(timeout);
    };
  }, []);

  return null;
}
