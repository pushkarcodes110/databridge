"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { cancelJob, getJobProgress } from "@/lib/api";
import { Loader2, CheckCircle, AlertCircle, Square } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ImportProgressProps {
  jobId: string;
}

export function ImportProgress({ jobId }: ImportProgressProps) {
  const router = useRouter();
  const [status, setStatus] = useState("pending");
  const [inserted, setInserted] = useState(0);
  const [failed, setFailed] = useState(0);
  const [total, setTotal] = useState(0);
  const [queuePosition, setQueuePosition] = useState<number | null>(null);
  const [errorSummary, setErrorSummary] = useState<string | null>(null);
  const [isCancelling, setIsCancelling] = useState(false);

  useEffect(() => {
    let interval: NodeJS.Timeout;

    const poll = async () => {
      try {
        const data = await getJobProgress(jobId);
        setStatus(data.status);
        setInserted(data.inserted || 0);
        setFailed(data.failed || 0);
        setTotal(data.total || 0);
        setQueuePosition(typeof data.queue_position === "number" ? data.queue_position : null);
        setErrorSummary(data.error_summary || null);

        if (data.status === "complete" || data.status === "failed" || data.status === "cancelled") {
          clearInterval(interval);
        }
      } catch (err) {
        console.error("Failed to poll progress", err);
      }
    };

    poll(); // initial check
    interval = setInterval(poll, 2000); // Poll every 2 seconds matching PRD

    return () => clearInterval(interval);
  }, [jobId]);

  const totalProcessed = inserted + failed;
  const progressPercent = total > 0 ? (totalProcessed / total) * 100 : 0;
  const safePercent = Math.min(100, Math.max(0, progressPercent));

  const handleCancel = async () => {
    try {
      setIsCancelling(true);
      const data = await cancelJob(jobId);
      setStatus(data.status);
      setInserted(data.inserted || 0);
      setFailed(data.failed || 0);
      setTotal(data.total || 0);
      setQueuePosition(typeof data.queue_position === "number" ? data.queue_position : null);
      setErrorSummary(data.error_summary || null);
    } finally {
      setIsCancelling(false);
    }
  };

  return (
    <div className="bg-card border rounded-2xl shadow-sm p-8 max-w-3xl mx-auto backdrop-blur-xl">
      <div className="flex flex-col items-center text-center space-y-4">
        
        {status === "running" || status === "pending" ? (
           <div className="relative">
             <Loader2 className="w-16 h-16 text-primary animate-spin" />
             <div className="absolute inset-0 flex items-center justify-center font-semibold text-sm">
               {safePercent.toFixed(0)}%
             </div>
           </div>
        ) : status === "complete" ? (
           <CheckCircle className="w-16 h-16 text-green-500" />
        ) : (
           <AlertCircle className="w-16 h-16 text-red-500" />
        )}

        <div>
          <h2 className="text-2xl font-bold capitalize">
            {status === "pending" ? "Job Queued..." : status === "running" ? "Importing Data..." : `Import ${status}`}
          </h2>
          <p className="text-muted-foreground mt-1">
            {status === "pending"
              ? `Waiting in queue${queuePosition ? ` (#${queuePosition})` : ""}.`
              : status === "running"
                ? "DataBridge is transporting your data to NocoDB in live parallel batches."
                : status === "cancelled"
                  ? "This import was stopped before all remaining batches were processed."
                  : "Review the counters below for final import results."}
          </p>
        </div>
      </div>

      <div className="mt-8">
        <div className="w-full bg-muted rounded-full h-3 overflow-hidden shadow-inner">
           <div className="bg-primary h-3 transition-all duration-500" style={{ width: `${safePercent}%` }} />
        </div>
      </div>

      <div className="grid grid-cols-3 gap-4 mt-8">
        <div className="bg-muted/30 border rounded-xl p-4 text-center">
           <div className="text-3xl font-bold">{total || "..."}</div>
           <div className="text-xs text-muted-foreground uppercase tracking-wider font-semibold mt-1">Total Rows</div>
        </div>
        <div className="bg-green-500/10 border border-green-500/20 rounded-xl p-4 text-center text-green-600 dark:text-green-400">
           <div className="text-3xl font-bold">{inserted}</div>
           <div className="text-xs uppercase tracking-wider font-semibold mt-1">Inserted</div>
        </div>
        <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 text-center text-red-600 dark:text-red-400">
           <div className="text-3xl font-bold">{failed}</div>
           <div className="text-xs uppercase tracking-wider font-semibold mt-1">Failed</div>
        </div>
      </div>

      {errorSummary ? (
        <div className="mt-6 rounded-xl border border-red-500/20 bg-red-500/10 p-4 text-sm text-red-600 dark:text-red-300">
          {errorSummary}
        </div>
      ) : null}

      <div className="mt-6 flex flex-wrap justify-center gap-3">
        {(status === "pending" || status === "running") ? (
          <Button variant="destructive" onClick={handleCancel} disabled={isCancelling}>
            <Square className="mr-2 h-4 w-4" />
            {isCancelling ? "Stopping..." : "Stop Import"}
          </Button>
        ) : null}
        <Button variant="outline" onClick={() => router.push("/jobs")}>
          Open Job Queue
        </Button>
      </div>
    </div>
  );
}
