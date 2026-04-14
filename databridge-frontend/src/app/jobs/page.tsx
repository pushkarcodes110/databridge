"use client";

import { useEffect, useMemo, useState } from "react";
import { cancelJob, getJob, getJobs, resumeJob } from "@/lib/api";
import { AlertCircle, CheckCircle2, Clock3, Loader2, RotateCcw, Square } from "lucide-react";
import { Button } from "@/components/ui/button";

type JobSummary = {
  id: string;
  status: string;
  filename: string;
  file_format: string;
  inserted: number;
  failed: number;
  total: number;
  progress_percent: number;
  queue_position: number | null;
  can_cancel: boolean;
  can_resume: boolean;
  created_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  error_summary: string | null;
};

type JobDetail = JobSummary & {
  error_count?: number;
  errors?: Array<{
    id: string;
    row_number: number;
    row_data: Record<string, unknown>;
    error_message: string;
    created_at: string | null;
  }>;
};

function formatDate(value: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function formatStatus(status: string) {
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function statusIcon(status: string) {
  if (status === "running") return <Loader2 className="h-4 w-4 animate-spin" />;
  if (status === "complete") return <CheckCircle2 className="h-4 w-4 text-green-500" />;
  if (status === "failed" || status === "cancelled") return <AlertCircle className="h-4 w-4 text-red-500" />;
  return <Clock3 className="h-4 w-4 text-amber-500" />;
}

export default function JobsPage() {
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string>("");
  const [selectedJob, setSelectedJob] = useState<JobDetail | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isCancelling, setIsCancelling] = useState(false);
  const [isResuming, setIsResuming] = useState(false);

  const activeJobIds = useMemo(
    () => jobs.filter((job) => job.status === "pending" || job.status === "running").map((job) => job.id),
    [jobs]
  );

  useEffect(() => {
    let mounted = true;

    const loadJobs = async () => {
      const data = await getJobs();
      if (!mounted) return;
      setJobs(data);
      setSelectedJobId((current) => current || data[0]?.id || "");
      setIsLoading(false);
    };

    loadJobs().catch(() => setIsLoading(false));
    const interval = setInterval(loadJobs, activeJobIds.length > 0 ? 2000 : 5000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [activeJobIds.length]);

  useEffect(() => {
    if (!selectedJobId) {
      setSelectedJob(null);
      return;
    }

    let mounted = true;
    const loadJob = async () => {
      const data = await getJob(selectedJobId);
      if (mounted) setSelectedJob(data);
    };

    loadJob().catch(() => undefined);
    const interval = setInterval(loadJob, 2000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [selectedJobId]);

  const handleCancel = async () => {
    if (!selectedJob) return;
    try {
      setIsCancelling(true);
      const data = await cancelJob(selectedJob.id);
      setSelectedJob((current) => (current ? { ...current, ...data } : current));
      const latestJobs = await getJobs();
      setJobs(latestJobs);
    } finally {
      setIsCancelling(false);
    }
  };

  const handleResume = async () => {
    if (!selectedJob) return;
    try {
      setIsResuming(true);
      const data = await resumeJob(selectedJob.id);
      setSelectedJob((current) => (current ? { ...current, ...data } : current));
      const latestJobs = await getJobs();
      setJobs(latestJobs);
    } finally {
      setIsResuming(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Job Queue</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Watch queued and running imports in realtime, inspect failures, and stop a job if needed.
          </p>
        </div>
        <div className="rounded-xl border bg-card px-4 py-3 text-sm text-muted-foreground">
          Active jobs: <span className="font-semibold text-foreground">{activeJobIds.length}</span>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-[380px_minmax(0,1fr)]">
        <section className="space-y-3">
          {isLoading ? (
            <div className="rounded-2xl border bg-card p-6 text-sm text-muted-foreground">Loading jobs...</div>
          ) : jobs.length === 0 ? (
            <div className="rounded-2xl border bg-card p-6 text-sm text-muted-foreground">No import jobs yet.</div>
          ) : (
            jobs.map((job) => (
              <button
                key={job.id}
                type="button"
                onClick={() => setSelectedJobId(job.id)}
                className={`w-full rounded-2xl border p-4 text-left transition ${
                  selectedJobId === job.id ? "border-primary bg-primary/5" : "bg-card hover:bg-muted/20"
                }`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate font-semibold">{job.filename}</div>
                    <div className="mt-1 text-xs text-muted-foreground">{formatDate(job.created_at)}</div>
                  </div>
                  <div className="flex items-center gap-2 text-xs font-medium">
                    {statusIcon(job.status)}
                    {formatStatus(job.status)}
                  </div>
                </div>

                <div className="mt-4">
                  <div className="mb-2 flex items-center justify-between text-xs text-muted-foreground">
                    <span>{job.inserted + job.failed} / {job.total || "?"} rows</span>
                    <span>{job.progress_percent.toFixed(0)}%</span>
                  </div>
                  <div className="h-2 overflow-hidden rounded-full bg-muted">
                    <div className="h-full bg-primary transition-all duration-300" style={{ width: `${job.progress_percent}%` }} />
                  </div>
                </div>

                <div className="mt-4 flex items-center justify-between text-xs text-muted-foreground">
                  <span>Inserted {job.inserted}</span>
                  <span>Errors {job.failed}</span>
                  <span>{job.queue_position ? `Queue #${job.queue_position}` : "—"}</span>
                </div>
              </button>
            ))
          )}
        </section>

        <section className="rounded-2xl border bg-card p-6">
          {!selectedJob ? (
            <div className="text-sm text-muted-foreground">Select a job to see the full import details.</div>
          ) : (
            <div className="space-y-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    {statusIcon(selectedJob.status)}
                    <span>{formatStatus(selectedJob.status)}</span>
                  </div>
                  <h2 className="mt-2 text-2xl font-bold">{selectedJob.filename}</h2>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Job ID: <span className="font-mono text-xs">{selectedJob.id}</span>
                  </p>
                </div>

                <div className="flex flex-wrap gap-3">
                  {selectedJob.can_resume ? (
                    <Button onClick={handleResume} disabled={isResuming}>
                      <RotateCcw className="mr-2 h-4 w-4" />
                      {isResuming ? "Resuming..." : "Resume Job"}
                    </Button>
                  ) : null}
                  {selectedJob.can_cancel ? (
                    <Button variant="destructive" onClick={handleCancel} disabled={isCancelling}>
                      <Square className="mr-2 h-4 w-4" />
                      {isCancelling ? "Stopping..." : "Stop Job"}
                    </Button>
                  ) : null}
                </div>
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Realtime progress</span>
                  <span className="font-semibold">{selectedJob.progress_percent.toFixed(1)}%</span>
                </div>
                <div className="h-3 overflow-hidden rounded-full bg-muted">
                  <div
                    className="h-full bg-primary transition-all duration-300"
                    style={{ width: `${selectedJob.progress_percent}%` }}
                  />
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-4">
                <div className="rounded-xl border bg-muted/20 p-4">
                  <div className="text-2xl font-bold">{selectedJob.total || 0}</div>
                  <div className="mt-1 text-xs uppercase tracking-wide text-muted-foreground">Total Rows</div>
                </div>
                <div className="rounded-xl border border-green-500/20 bg-green-500/10 p-4">
                  <div className="text-2xl font-bold text-green-600 dark:text-green-400">{selectedJob.inserted}</div>
                  <div className="mt-1 text-xs uppercase tracking-wide text-muted-foreground">Inserted</div>
                </div>
                <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-4">
                  <div className="text-2xl font-bold text-red-600 dark:text-red-400">{selectedJob.failed}</div>
                  <div className="mt-1 text-xs uppercase tracking-wide text-muted-foreground">Errors</div>
                </div>
                <div className="rounded-xl border bg-muted/20 p-4">
                  <div className="text-2xl font-bold">{selectedJob.queue_position || 0}</div>
                  <div className="mt-1 text-xs uppercase tracking-wide text-muted-foreground">Queue Position</div>
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-3">
                <div className="rounded-xl border p-4">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Created</div>
                  <div className="mt-1 text-sm">{formatDate(selectedJob.created_at)}</div>
                </div>
                <div className="rounded-xl border p-4">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Started</div>
                  <div className="mt-1 text-sm">{formatDate(selectedJob.started_at)}</div>
                </div>
                <div className="rounded-xl border p-4">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Completed</div>
                  <div className="mt-1 text-sm">{formatDate(selectedJob.completed_at)}</div>
                </div>
              </div>

              {selectedJob.error_summary ? (
                <div className="rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-600 dark:text-red-300">
                  {selectedJob.error_summary}
                </div>
              ) : null}

              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <h3 className="text-lg font-semibold">Recent Error Rows</h3>
                  <div className="text-sm text-muted-foreground">
                    {(selectedJob.error_count || 0) > 0 ? `${selectedJob.error_count} total errors` : "No row errors recorded"}
                  </div>
                </div>

                {(selectedJob.errors || []).length === 0 ? (
                  <div className="rounded-xl border border-dashed p-6 text-sm text-muted-foreground">
                    No import row errors for this job.
                  </div>
                ) : (
                  <div className="space-y-3">
                    {selectedJob.errors?.map((error) => (
                      <div key={error.id} className="rounded-xl border p-4">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="font-medium">Row {error.row_number || "Unknown"}</div>
                          <div className="text-xs text-muted-foreground">{formatDate(error.created_at)}</div>
                        </div>
                        <div className="mt-2 text-sm text-red-600 dark:text-red-300">{error.error_message}</div>
                        <pre className="mt-3 overflow-x-auto rounded-lg bg-muted/40 p-3 text-xs">
                          {JSON.stringify(error.row_data, null, 2)}
                        </pre>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
