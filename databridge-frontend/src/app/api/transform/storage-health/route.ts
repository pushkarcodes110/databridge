import { readdir, writeFile, unlink, mkdir } from "fs/promises";
import { join } from "path";
import { NextResponse } from "next/server";
import { storageRoot } from "@/lib/server/storage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

async function exists(path: string) {
  try {
    await readdir(path);
    return true;
  } catch {
    return false;
  }
}

export async function GET() {
  const root = storageRoot();
  const probe = join(root, ".frontend-write-test");
  let writable = false;
  let error: string | null = null;

  try {
    await mkdir(root, { recursive: true });
    await writeFile(probe, "ok");
    await unlink(probe);
    writable = true;
  } catch (exc) {
    error = exc instanceof Error ? exc.message : "Storage write failed.";
  }

  const [uploadsExists, outputsExists, stagesExists, jobsExists] = await Promise.all([
    exists(join(root, "uploads")),
    exists(join(root, "outputs")),
    exists(join(root, "stages")),
    exists(join(root, "transform-jobs")),
  ]);

  return NextResponse.json({
    root,
    uploadsExists,
    outputsExists,
    stagesExists,
    jobsExists,
    writable,
    error,
  });
}
