import { createReadStream } from "fs";
import { access } from "fs/promises";
import { join } from "path";
import csv from "csv-parser";
import { NextResponse } from "next/server";

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

    const filePath = join("/tmp", "databridge", "uploads", uploadId, "input.csv");
    await access(filePath);

    const { names, rowsRead } = await readNameSample(filePath, nameColumn, sampleSize);
    const response = await fetch(`${genderServiceUrl}/classify-gender`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ names }),
    });

    const data = await response.json() as GenderServiceResponse | { error?: string };
    if (!response.ok) {
      return NextResponse.json(
        { error: "Gender service failed.", detail: "error" in data ? data.error : undefined },
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
