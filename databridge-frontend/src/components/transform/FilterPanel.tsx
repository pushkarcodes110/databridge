import { useState, useMemo } from "react";
import StatsBar from "./StatsBar";
import { EmailFilterCard } from "./filters/EmailFilterCard";
import { GenderFilterCard } from "./filters/GenderFilterCard";
import { DuplicateRemoverCard } from "./filters/DuplicateRemoverCard";
import { Button } from "@/components/ui/button";
import { useTransformStore } from "@/lib/transform-store";
import { Loader2, Download, AlertCircle } from "lucide-react";
import { toast } from "sonner";

export interface FilterPanelProps {
  uploadId: string;
  mapping: { outputColumn: string; sourceColumn: string }[];
  totalRows: number;
}

export default function FilterPanel({ uploadId, mapping, totalRows }: FilterPanelProps) {
  const [emailRowsRemoved, setEmailRowsRemoved] = useState(0);
  const [genderRowsRemoved, setGenderRowsRemoved] = useState(0);
  const [duplicatesRemoved, setDuplicatesRemoved] = useState(0);
  
  const [runStatus, setRunStatus] = useState<"idle" | "running" | "complete" | "error">("idle");
  const [runProgress, setRunProgress] = useState("");
  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);

  const filters = useTransformStore((state) => state.filters);

  const emailSourceColumn = useMemo(() => {
    const match = mapping.find((m) => m.outputColumn.toLowerCase() === "email");
    return match ? match.sourceColumn : "";
  }, [mapping]);

  const nameSourceColumn = useMemo(() => {
    const match = mapping.find((m) => {
      const col = m.outputColumn.toLowerCase();
      return col === "name" || col === "full_name";
    });
    return match ? match.sourceColumn : "";
  }, [mapping]);

  const rowsRemaining = Math.max(
    0,
    totalRows - emailRowsRemoved - genderRowsRemoved - duplicatesRemoved
  );

  const handleRunTransform = async () => {
    try {
      setRunStatus("running");
      setRunProgress("Initializing transform...");
      setDownloadUrl(null);

      const response = await fetch("/api/transform/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uploadId, mapping, filters }),
      });

      if (!response.ok || !response.body) {
        throw new Error("Failed to start formatting.");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let done = false;

      while (!done) {
        const { value, done: doneReading } = await reader.read();
        done = doneReading;
        if (value) {
          const chunk = decoder.decode(value, { stream: true });
          const messages = chunk.split("\n\n").filter(Boolean);
          
          for (const msg of messages) {
            if (msg.startsWith("data: ")) {
              try {
                const data = JSON.parse(msg.slice(6));
                
                if (data.step === "error") {
                  throw new Error(data.error);
                } else if (data.step === "complete") {
                  setRunStatus("complete");
                  setDownloadUrl(data.outputFile);
                  toast.success("Transformation complete!");
                  return;
                } else {
                  setRunProgress(`Processing ${data.step}... ${data.progress}%`);
                }
              } catch (err: any) {
                if (err.message) throw err;
                console.error("Parse error", err);
              }
            }
          }
        }
      }
      
    } catch (error: any) {
      setRunStatus("error");
      toast.error(error.message || "An error occurred during transformation.");
    }
  };

  return (
    <div className="space-y-6">
      <StatsBar
        totalRows={totalRows}
        rowsRemaining={rowsRemaining}
        rowsRemoved={emailRowsRemoved + genderRowsRemoved}
        duplicatesRemoved={duplicatesRemoved}
      />

      <div className="space-y-4">
        <EmailFilterCard
          uploadId={uploadId}
          sourceColumn={emailSourceColumn}
          onStatsChange={setEmailRowsRemoved}
        />
        
        <GenderFilterCard
          uploadId={uploadId}
          nameColumn={nameSourceColumn}
          onStatsChange={setGenderRowsRemoved}
        />
        
        <DuplicateRemoverCard
          uploadId={uploadId}
          emailColumn={emailSourceColumn}
          onStatsChange={setDuplicatesRemoved}
        />
      </div>

      <div className="flex flex-col items-end pt-4 gap-4">
        {runStatus === "running" && (
          <div className="flex items-center text-primary text-sm font-medium bg-primary/10 px-4 py-2 rounded-md">
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
            {runProgress}
          </div>
        )}
        
        {runStatus === "error" && (
          <div className="flex items-center text-destructive text-sm font-medium bg-destructive/10 px-4 py-2 rounded-md">
            <AlertCircle className="w-4 h-4 mr-2" />
            Process failed. Check connection or try again.
          </div>
        )}

        {runStatus === "complete" && downloadUrl ? (
          <Button size="lg" className="w-full sm:w-auto text-lg px-8 py-6 bg-green-600 hover:bg-green-700 text-white" asChild>
            <a href={downloadUrl} download>
              <Download className="mr-2 w-5 h-5" /> Download Reformed CSV
            </a>
          </Button>
        ) : (
          <Button 
            size="lg" 
            className="w-full sm:w-auto text-lg px-8 py-6"
            disabled={!uploadId || runStatus === "running"}
            onClick={handleRunTransform}
          >
            {runStatus === "running" ? "Processing..." : "Run Transform"}
          </Button>
        )}
      </div>
    </div>
  );
}
