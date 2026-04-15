import { createReadStream } from "fs";
import { access, stat } from "fs/promises";
import csv from "csv-parser";
import { NextResponse } from "next/server";
import { outputFilePath } from "@/lib/server/storage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type PreviewRow = Record<string, string>;

async function previewCsv(filePath: string) {
  return new Promise<{ columns: string[]; rows: PreviewRow[]; totalRows: number }>((resolve, reject) => {
    const rows: PreviewRow[] = [];
    let columns: string[] = [];
    let totalRows = 0;

    createReadStream(filePath)
      .on("error", reject)
      .pipe(csv())
      .on("headers", (headers: string[]) => {
        columns = headers;
      })
      .on("data", (row: PreviewRow) => {
        totalRows += 1;
        if (rows.length < 20) rows.push(row);
      })
      .on("error", reject)
      .on("end", () => resolve({ columns, rows, totalRows }));
  });
}

export async function GET(
  _request: Request,
  { params }: { params: { uploadId: string } }
) {
  const filePath = outputFilePath(params.uploadId);

  try {
    await access(filePath);
    const [preview, fileInfo] = await Promise.all([previewCsv(filePath), stat(filePath)]);

    return NextResponse.json({
      columns: preview.columns,
      rows: preview.rows,
      stats: {
        total_rows: preview.totalRows,
        unique_rows: 0,
        file_size_mb: Number((fileInfo.size / (1024 * 1024)).toFixed(2)),
      },
      filePath,
      filename: `databridge-transform-${params.uploadId}.csv`,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Output file not found.";
    return NextResponse.json({ error: message }, { status: 404 });
  }
}
