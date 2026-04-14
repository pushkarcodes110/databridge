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
  duplicateEmails: number;
  totalEmails: number;
  domains: { domain: string; count: number; percentage: number }[];
  invalidFormat: number;
  emptyEmails: number;
  duplicates: number;
};

const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const typoDomains: Record<string, string> = {
  "gmal.com": "gmail.com",
  "gmial.com": "gmail.com",
  "gmaill.com": "gmail.com",
  "gmai.com": "gmail.com",
  "gmail.co": "gmail.com",
  "gmail.con": "gmail.com",
  "gmail.cm": "gmail.com",
  "gmail.om": "gmail.com",
  "gmail.cmo": "gmail.com",
  "gmail.comm": "gmail.com",
  "gnail.com": "gmail.com",
  "gamil.com": "gmail.com",
  "gmaul.com": "gmail.com",
  "gmaik.com": "gmail.com",
  "googlemail.con": "googlemail.com",
  "yaho.com": "yahoo.com",
  "yahooo.com": "yahoo.com",
  "yhoo.com": "yahoo.com",
  "yahoo.co": "yahoo.com",
  "yahoo.con": "yahoo.com",
  "yhaoo.com": "yahoo.com",
  "yaoo.com": "yahoo.com",
  "yaho.co": "yahoo.com",
  "ymial.com": "ymail.com",
  "hotnail.com": "hotmail.com",
  "hotmal.com": "hotmail.com",
  "hotmai.com": "hotmail.com",
  "hotmail.co": "hotmail.com",
  "hotmail.con": "hotmail.com",
  "hotmil.com": "hotmail.com",
  "hotmaill.com": "hotmail.com",
  "outloo.com": "outlook.com",
  "outlok.com": "outlook.com",
  "outlook.co": "outlook.com",
  "outlook.con": "outlook.com",
  "outllok.com": "outlook.com",
  "icloud.co": "icloud.com",
  "icloud.con": "icloud.com",
  "iclod.com": "icloud.com",
  "aol.co": "aol.com",
  "protonmal.com": "protonmail.com",
};

function getDomain(value: string) {
  const atIndex = value.lastIndexOf("@");
  if (atIndex === -1) return "";
  return value.slice(atIndex + 1).trim().toLowerCase();
}

function correctCommonTypo(email: string) {
  const atIndex = email.lastIndexOf("@");
  if (atIndex === -1) return email;

  const localPart = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1).trim().toLowerCase();
  const correctedDomain = typoDomains[domain];
  return correctedDomain ? `${localPart}@${correctedDomain}` : email;
}

async function analyzeEmailColumn(
  filePath: string,
  sourceColumn: string,
  options: { fixTypos: boolean; normalize: boolean }
) {
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
        let email = String(row[sourceColumn] ?? "").trim();
        if (options.normalize) email = email.toLowerCase();
        if (options.fixTypos) email = correctCommonTypo(email);

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
          duplicateEmails,
          totalEmails: total,
          domains: breakdown,
          invalidFormat,
          emptyEmails,
          duplicates: duplicateEmails,
        });
      });
  });
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const uploadId = url.searchParams.get("uploadId");
  const sourceColumn = url.searchParams.get("sourceColumn");
  const fixTypos = url.searchParams.get("fixTypos") !== "false";
  const normalize = url.searchParams.get("normalize") !== "false";

  if (!uploadId || !sourceColumn) {
    return NextResponse.json({ error: "uploadId and sourceColumn are required." }, { status: 400 });
  }

  const filePath = join("/tmp", "databridge", "uploads", uploadId, "input.csv");

  try {
    await access(filePath);
    const analysis = await analyzeEmailColumn(filePath, sourceColumn, { fixTypos, normalize });
    return NextResponse.json(analysis);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to analyze email column.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
