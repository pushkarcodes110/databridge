import { createWriteStream, createReadStream } from "fs";
import { mkdir, unlink } from "fs/promises";
import { join } from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import type { ReadableStream as NodeReadableStream } from "stream/web";
import Busboy from "busboy";
import Papa from "papaparse";
import { v4 as uuidv4 } from "uuid";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type PreviewResult = {
  headers: string[];
  preview: Record<string, string>[];
  totalRows: number;
};

function isCsvFile(filename?: string) {
  return Boolean(filename?.toLowerCase().endsWith(".csv"));
}

async function saveMultipartCsv(request: Request, uploadDir: string, filePath: string) {
  const contentType = request.headers.get("content-type");
  if (!contentType?.includes("multipart/form-data")) {
    throw new Error("Expected multipart/form-data.");
  }

  if (!request.body) {
    throw new Error("Missing upload body.");
  }

  await mkdir(uploadDir, { recursive: true });

  return new Promise<void>((resolve, reject) => {
    const busboy = Busboy({
      headers: Object.fromEntries(request.headers.entries()),
    });

    let foundFile = false;
    let writePromise: Promise<void> | null = null;
    let rejected = false;

    const fail = (error: Error) => {
      if (rejected) return;
      rejected = true;
      reject(error);
    };

    busboy.on("file", (_fieldname, file, info) => {
      if (foundFile) {
        file.resume();
        return;
      }

      foundFile = true;
      if (!isCsvFile(info.filename)) {
        file.resume();
        fail(new Error("Only CSV files are accepted."));
        return;
      }

      writePromise = pipeline(file, createWriteStream(filePath));
      writePromise.catch(fail);
    });

    busboy.on("error", fail);
    busboy.on("finish", async () => {
      if (rejected) return;
      if (!foundFile || !writePromise) {
        fail(new Error("No CSV file was uploaded."));
        return;
      }

      try {
        await writePromise;
        resolve();
      } catch (error) {
        fail(error instanceof Error ? error : new Error("Failed to save upload."));
      }
    });

    const nodeStream = Readable.fromWeb(request.body as unknown as NodeReadableStream<Uint8Array>);
    nodeStream.on("error", fail);
    nodeStream.pipe(busboy);
  });
}

async function parsePreview(filePath: string) {
  return new Promise<PreviewResult>((resolve, reject) => {
    const headers: string[] = [];
    const preview: Record<string, string>[] = [];
    let totalRows = 0;

    const parser = Papa.parse(Papa.NODE_STREAM_INPUT, {
      skipEmptyLines: true,
    });

    parser.on("data", (row: unknown) => {
      if (!Array.isArray(row)) return;

      if (headers.length === 0) {
        headers.push(...row.map((value) => String(value ?? "")));
        return;
      }

      totalRows += 1;
      if (preview.length < 5) {
        const record = headers.reduce<Record<string, string>>((acc, header, index) => {
          acc[header] = String(row[index] ?? "");
          return acc;
        }, {});
        preview.push(record);
      }
    });

    parser.on("error", (error) => {
      reject(error instanceof Error ? error : new Error("Failed to parse CSV."));
    });

    parser.on("finish", () => {
      resolve({ headers, preview, totalRows });
    });

    createReadStream(filePath)
      .on("error", reject)
      .pipe(parser);
  });
}

export async function POST(request: Request) {
  const uploadId = uuidv4();
  const uploadDir = join("/tmp", "databridge", "uploads", uploadId);
  const filePath = join(uploadDir, "input.csv");

  try {
    await saveMultipartCsv(request, uploadDir, filePath);
    const preview = await parsePreview(filePath);

    return NextResponse.json({
      uploadId,
      headers: preview.headers,
      preview: preview.preview,
      totalRows: preview.totalRows,
    });
  } catch (error) {
    await unlink(filePath).catch(() => undefined);
    const message = error instanceof Error ? error.message : "Upload failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
