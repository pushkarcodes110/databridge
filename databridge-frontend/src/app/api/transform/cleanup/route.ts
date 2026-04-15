import { readdir, rm, rmdir, stat } from "fs/promises";
import { join } from "path";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const rootDir = join("/tmp", "databridge");
const maxAgeMs = 2 * 60 * 60 * 1000;

async function removeOlderThan(dir: string, cutoff: number) {
  let deleted = 0;
  const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);

  for (const entry of entries) {
    if (dir === rootDir && entry.name === "transform-jobs") continue;

    const path = join(dir, entry.name);
    const info = await stat(path).catch(() => null);
    if (!info) continue;

    if (entry.isDirectory()) {
      deleted += await removeOlderThan(path, cutoff);
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
  const deleted = await removeOlderThan(rootDir, Date.now() - maxAgeMs);
  return NextResponse.json({ deleted });
}
