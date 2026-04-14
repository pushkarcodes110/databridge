import { createReadStream } from "fs";
import { access } from "fs/promises";
import { createHash } from "crypto";
import { join } from "path";
import csv from "csv-parser";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type DuplicateAnalysis = {
  fullDuplicates: number;
  emailDuplicates: number;
  totalRows: number;
};

function rowHash(row: Record<string, unknown>, headers: string[]) {
  const joinedValues = headers.map((header) => String(row[header] ?? "")).join("\u001f");
  return createHash("md5").update(joinedValues).digest("hex");
}

async function analyzeDuplicates(filePath: string, emailColumn: string) {
  return new Promise<DuplicateAnalysis>((resolve, reject) => {
    const seenRows = new Set<string>();
    const seenEmails = new Set<string>();
    let headers: string[] = [];
    let fullDuplicates = 0;
    let emailDuplicates = 0;
    let totalRows = 0;

    createReadStream(filePath)
      .on("error", reject)
      .pipe(csv())
      .on("headers", (parsedHeaders: string[]) => {
        headers = parsedHeaders;
      })
      .on("data", (row: Record<string, unknown>) => {
        totalRows += 1;

        const hash = rowHash(row, headers);
        if (seenRows.has(hash)) {
          fullDuplicates += 1;
        } else {
          seenRows.add(hash);
        }

        const email = String(row[emailColumn] ?? "").trim().toLowerCase();
        if (!email) return;

        if (seenEmails.has(email)) {
          emailDuplicates += 1;
        } else {
          seenEmails.add(email);
        }
      })
      .on("error", reject)
      .on("end", () => {
        resolve({ fullDuplicates, emailDuplicates, totalRows });
      });
  });
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const uploadId = url.searchParams.get("uploadId");
  const emailColumn = url.searchParams.get("emailColumn");

  if (!uploadId || !emailColumn) {
    return NextResponse.json({ error: "uploadId and emailColumn are required." }, { status: 400 });
  }

  const filePath = join("/tmp", "databridge", "uploads", uploadId, "input.csv");

  try {
    await access(filePath);
    const analysis = await analyzeDuplicates(filePath, emailColumn);
    return NextResponse.json(analysis);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to analyze duplicates.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
