import { createReadStream } from "fs";
import { access } from "fs/promises";
import { join } from "path";
import csv from "csv-parser";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type EmailAnalysis = {
  total: number;
  breakdown: { domain: string; count: number; percentage: number }[];
  invalidFormat: number;
  emptyEmails: number;
  duplicateEmails: number;
};

const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function getDomain(value: string) {
  const atIndex = value.lastIndexOf("@");
  if (atIndex === -1) return "";
  return value.slice(atIndex + 1).trim().toLowerCase();
}

async function analyzeEmailColumn(filePath: string, sourceColumn: string) {
  return new Promise<EmailAnalysis>((resolve, reject) => {
    const domainCounts = new Map<string, number>();
    const seenEmails = new Set<string>();
    let total = 0;
    let invalidFormat = 0;
    let emptyEmails = 0;
    let duplicateEmails = 0;

    createReadStream(filePath)
      .on("error", reject)
      .pipe(csv())
      .on("data", (row: Record<string, unknown>) => {
        total += 1;
        const email = String(row[sourceColumn] ?? "").trim().toLowerCase();

        if (!email) {
          emptyEmails += 1;
          return;
        }

        if (!emailPattern.test(email)) {
          invalidFormat += 1;
          return;
        }

        if (seenEmails.has(email)) {
          duplicateEmails += 1;
        } else {
          seenEmails.add(email);
        }

        const domain = getDomain(email);
        if (!domain) return;
        domainCounts.set(domain, (domainCounts.get(domain) ?? 0) + 1);
      })
      .on("error", reject)
      .on("end", () => {
        const breakdown = Array.from(domainCounts.entries())
          .map(([domain, count]) => ({
            domain,
            count,
            percentage: total === 0 ? 0 : Number(((count / total) * 100).toFixed(2)),
          }))
          .sort((a, b) => b.count - a.count || a.domain.localeCompare(b.domain));

        resolve({
          total,
          breakdown,
          invalidFormat,
          emptyEmails,
          duplicateEmails,
        });
      });
  });
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const uploadId = url.searchParams.get("uploadId");
  const sourceColumn = url.searchParams.get("sourceColumn");

  if (!uploadId || !sourceColumn) {
    return NextResponse.json({ error: "uploadId and sourceColumn are required." }, { status: 400 });
  }

  const filePath = join("/tmp", "databridge", "uploads", uploadId, "input.csv");

  try {
    await access(filePath);
    const analysis = await analyzeEmailColumn(filePath, sourceColumn);
    return NextResponse.json(analysis);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to analyze email column.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
