"use client";

import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { UploadCloud, FileType2, Loader2 } from "lucide-react";
import { uploadChunk } from "@/lib/api";
import { v4 as uuidv4 } from "uuid";
import { Progress } from "@/components/ui/progress";
import { toast } from "sonner";

interface FileUploadProps {
  onUploadComplete: (fileId: string, filename: string) => void;
}

export function FileUpload({ onUploadComplete }: FileUploadProps) {
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);

  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (!file) return;

    setUploading(true);
    setProgress(0);

    const fileId = uuidv4();
    const chunkSize = 5 * 1024 * 1024; // 5MB chunks
    const totalChunks = Math.ceil(file.size / chunkSize);

    try {
      for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
        const start = chunkIndex * chunkSize;
        const end = Math.min(start + chunkSize, file.size);
        const chunk = file.slice(start, end);

        const formData = new FormData();
        formData.append("file", chunk, file.name);
        formData.append("chunk_index", chunkIndex.toString());
        formData.append("total_chunks", totalChunks.toString());
        formData.append("file_id", fileId);

        const res = await uploadChunk(formData, (progressEvent: any) => {
           // We don't need highly granular axois progress since chunks are fast
        });

        setProgress((chunkIndex + 1) / totalChunks * 100);
      }
      
      onUploadComplete(fileId, file.name);
    } catch (error) {
      console.error("Upload failed", error);
      toast.error("Upload failed. Please try again.");
    } finally {
      setUploading(false);
    }
  }, [onUploadComplete]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      "text/csv": [".csv"],
    },
    multiple: false
  });

  return (
    <div className="w-full">
      <div 
        {...getRootProps()} 
        className={`border-2 border-dashed rounded-2xl p-12 text-center cursor-pointer transition-all duration-300 active:scale-[0.99] ${
          isDragActive ? "border-primary bg-primary/5" : "border-muted-foreground/20 hover:border-primary/50 hover:bg-muted/10"
        }`}
      >
        <input {...getInputProps()} />
        <div className="flex flex-col items-center justify-center space-y-4">
          <div className="p-4 bg-primary/10 rounded-full">
            <UploadCloud className="w-10 h-10 text-primary" />
          </div>
          <div>
            <p className="text-xl font-semibold">Drop your file here, or click to browse</p>
            <p className="text-sm text-muted-foreground mt-2">Supports CSV files up to safely limited large sizes.</p>
          </div>
        </div>
      </div>

      {uploading && (
        <div className="mt-6 bg-card border rounded-xl overflow-hidden shadow-sm p-4 flex items-center space-x-4">
           <FileType2 className="w-8 h-8 text-primary" />
           <div className="flex-1">
             <div className="flex justify-between items-center mb-1">
               <span className="text-sm font-medium">Uploading file...</span>
               <span className="text-xs font-semibold text-primary">{progress.toFixed(0)}%</span>
             </div>
             <Progress value={progress} indeterminate={progress <= 0} />
           </div>
           <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
        </div>
      )}
    </div>
  );
}
