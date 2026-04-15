import { createReadStream } from "fs";
import { access } from "fs/promises";
import csv from "csv-parser";
import { NextResponse } from "next/server";
import { uploadInputPath } from "@/lib/server/storage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type GenderRequest = {
  uploadId?: string;
  nameColumn?: string;
  sampleSize?: number;
};

type GenderServiceResponse = {
  results: Array<{
    name: string;
    firstName: string;
    gender: "male" | "female" | "unknown";
    confidence: number;
  }>;
};

const genderServiceUrl = process.env.GENDER_SERVICE_URL || process.env.EMAIL_VALIDATOR_URL || "http://localhost:8001";
const genderServiceUrls = Array.from(new Set([
  genderServiceUrl,
  "http://email-validator:8001",
  "http://host.docker.internal:8001",
  "http://localhost:8001",
]));

function firstNameFromValue(value: string) {
  return value.trim().split(/[\s-]+/)[0] || "";
}

async function readNameSample(filePath: string, nameColumn: string, sampleSize: number) {
  return new Promise<{ names: string[]; rowsRead: number }>((resolve, reject) => {
    const names: string[] = [];
    let rowsRead = 0;

    const stream = createReadStream(filePath)
      .on("error", reject)
      .pipe(csv())
      .on("data", (row: Record<string, unknown>) => {
        if (rowsRead >= sampleSize) {
          stream.destroy();
          return;
        }

        rowsRead += 1;
        const firstName = firstNameFromValue(String(row[nameColumn] ?? ""));
        if (firstName) names.push(firstName);
      })
      .on("error", reject)
      .on("close", () => resolve({ names, rowsRead }))
      .on("end", () => resolve({ names, rowsRead }));
  });
}

export async function POST(request: Request) {
  try {
    const body = await request.json() as GenderRequest;
    const uploadId = body.uploadId?.trim();
    const nameColumn = body.nameColumn?.trim();
    const sampleSize = Math.max(1, Math.min(body.sampleSize ?? 200, 200));

    if (!uploadId || !nameColumn) {
      return NextResponse.json({ error: "uploadId and nameColumn are required." }, { status: 400 });
    }

    const filePath = uploadInputPath(uploadId);
    await access(filePath);

    const { names, rowsRead } = await readNameSample(filePath, nameColumn, sampleSize);
    let data: GenderServiceResponse | { error?: string } | null = null;
    let serviceError: string | undefined;

    for (const serviceUrl of genderServiceUrls) {
      try {
        const response = await fetch(`${serviceUrl}/classify-gender`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ names }),
        });

        data = await response.json() as GenderServiceResponse | { error?: string };
        if (response.ok) break;
        serviceError = "error" in data ? data.error : "Gender service failed.";
        data = null;
      } catch (error) {
        serviceError = error instanceof Error ? error.message : "Gender service failed.";
      }
    }

    if (!data) {
      return NextResponse.json(
        { error: "Gender service failed.", detail: serviceError },
        { status: 502 }
      );
    }

    const results = (data as GenderServiceResponse).results;
    const stats = results.reduce(
      (acc, result) => {
        acc[result.gender] += 1;
        return acc;
      },
      { male: 0, female: 0, unknown: 0 }
    );

    return NextResponse.json({
      male: stats.male,
      female: stats.female,
      unknown: stats.unknown,
      sampleSize: rowsRead,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to analyze gender.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
