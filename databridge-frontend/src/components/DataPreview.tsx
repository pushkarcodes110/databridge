"use client";

import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface DataPreviewProps {
  columns: string[];
  rows: any[];
  stats?: {
    total_rows: number;
    unique_rows: number;
    file_size_mb: number;
  };
}

export function DataPreview({ columns, rows, stats }: DataPreviewProps) {
  if (!columns || columns.length === 0) return null;

  return (
    <div className="bg-card border rounded-2xl overflow-hidden shadow-sm backdrop-blur-xl">
      <div className="border-b px-6 py-4 flex flex-col md:flex-row justify-between items-start md:items-center gap-3 bg-muted/20">
        <div className="flex items-center gap-3">
          <h3 className="font-semibold">Data Preview</h3>
          <span className="text-xs font-medium text-muted-foreground bg-muted px-2.5 py-1 rounded-full border">First 20 Rows</span>
        </div>
        
        {stats && (
          <div className="flex flex-wrap items-center gap-4 text-xs font-medium text-muted-foreground">
            <div className="flex items-center gap-1.5 bg-background border px-2.5 py-1 rounded-md shadow-sm">
              <span className="opacity-70">Total Rows:</span> 
              <span className="text-foreground tracking-tight">{stats.total_rows.toLocaleString()}</span>
            </div>
            {stats.unique_rows > 0 && (
              <div className="flex items-center gap-1.5 bg-background border px-2.5 py-1 rounded-md shadow-sm">
                <span className="opacity-70">Unique Rows:</span> 
                <span className="text-foreground tracking-tight">{stats.unique_rows.toLocaleString()}</span>
              </div>
            )}
            <div className="flex items-center gap-1.5 bg-background border px-2.5 py-1 rounded-md shadow-sm">
              <span className="opacity-70">File Size:</span> 
              <span className="text-foreground tracking-tight">{stats.file_size_mb} MB</span>
            </div>
          </div>
        )}
      </div>
      <div className="overflow-x-auto max-h-[400px] custom-scrollbar">
        <Table>
          <TableHeader className="bg-muted/30 sticky top-0 backdrop-blur z-10">
            <TableRow>
              <TableHead className="w-[50px] sticky left-0 bg-muted/80 backdrop-blur z-20">#</TableHead>
              {columns.map((col, idx) => (
                <TableHead key={idx} className="whitespace-nowrap font-semibold">
                  {col}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, rowIndex) => (
              <TableRow key={rowIndex}>
                <TableCell className="font-medium text-muted-foreground sticky left-0 bg-card z-10 border-r">{rowIndex + 1}</TableCell>
                {columns.map((col, colIndex) => (
                  <TableCell key={colIndex} className="whitespace-nowrap text-sm">
                    {String(row[col] ?? "")}
                  </TableCell>
                ))}
              </TableRow>
            ))}
            {rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={columns.length + 1} className="h-24 text-center text-muted-foreground">
                  No data points found.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
