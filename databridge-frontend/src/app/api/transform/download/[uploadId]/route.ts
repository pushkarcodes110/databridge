import { createReadStream } from "fs";
import { access } from "fs/promises";
import { join } from "path";
import { Readable } from "stream";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(
  _request: Request,
  { params }: { params: { uploadId: string } }
) {
  const filePath = join("/tmp", "databridge", "outputs", params.uploadId, "output.csv");

  try {
    await access(filePath);
    const stream = Readable.toWeb(createReadStream(filePath));

    return new Response(stream as ReadableStream, {
      headers: {
        "Content-Type": "text/csv",
        "Content-Disposition": `attachment; filename="databridge-transform-${params.uploadId}.csv"`,
      },
    });
  } catch {
    return NextResponse.json({ error: "Output file not found." }, { status: 404 });
  }
}
