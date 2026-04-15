import { readFile, readdir, rm, rmdir, stat } from "fs/promises";
import { basename, join } from "path";
import { NextResponse } from "next/server";
import { storageRoot, transformJobsDirPath } from "@/lib/server/storage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const rootDir = storageRoot();
const maxAgeHours = Math.max(1, Number(process.env.TRANSFORM_FILE_RETENTION_HOURS || "24"));
const maxAgeMs = maxAgeHours * 60 * 60 * 1000;

type TransformJobSnapshot = {
  status?: string;
  config?: { uploadId?: string };
  updatedAt?: string;
  completedAt?: string;
};

async function protectedUploadIds(cutoff: number) {
  const protectedIds = new Set<string>();
  const entries = await readdir(transformJobsDirPath()).catch(() => []);

  await Promise.all(entries.filter((entry) => entry.endsWith(".json")).map(async (entry) => {
    try {
      const job = JSON.parse(await readFile(join(transformJobsDirPath(), entry), "utf8")) as TransformJobSnapshot;
      const uploadId = job.config?.uploadId;
      if (!uploadId) return;

      const timestamp = Date.parse(job.completedAt || job.updatedAt || "");
      const isActive = job.status === "pending" || job.status === "running";
      const isRecent = Number.isFinite(timestamp) && timestamp >= cutoff;
      if (isActive || isRecent) protectedIds.add(uploadId);
    } catch {
      // Ignore malformed snapshots.
    }
  }));

  return protectedIds;
}

async function removeOlderThan(dir: string, cutoff: number, protectedIds: Set<string>) {
  let deleted = 0;
  const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);

  for (const entry of entries) {
    if (dir === rootDir && entry.name === "transform-jobs") continue;
    if (
      ["uploads", "outputs", "stages"].includes(basename(dir)) &&
      protectedIds.has(entry.name)
    ) {
      continue;
    }

    const path = join(dir, entry.name);
    const info = await stat(path).catch(() => null);
    if (!info) continue;

    if (entry.isDirectory()) {
      deleted += await removeOlderThan(path, cutoff, protectedIds);
      await rmdir(path).catch(() => undefined);
      continue;
    }

    if (info.mtimeMs < cutoff) {
      await rm(path, { force: true });
      deleted += 1;
    }
  }

  return deleted;
}

export async function POST() {
  const cutoff = Date.now() - maxAgeMs;
  const protectedIds = await protectedUploadIds(cutoff);
  const deleted = await removeOlderThan(rootDir, cutoff, protectedIds);
  return NextResponse.json({ deleted, retentionHours: maxAgeHours, protectedUploads: protectedIds.size });
}
