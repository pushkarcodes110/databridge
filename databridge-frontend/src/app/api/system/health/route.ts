import { mkdir, readdir, unlink, writeFile } from "fs/promises";
import { join } from "path";
import { NextResponse } from "next/server";
import { storageRoot } from "@/lib/server/storage";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type HealthStatus = "ok" | "warn" | "error";

type HealthCheck = {
  name: string;
  status: HealthStatus;
  message: string;
  latencyMs?: number;
};

const backendApiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api").replace(/\/$/, "");
const backendRootUrl = backendApiUrl.replace(/\/api$/, "");
const rapidEmailValidatorUrl = (process.env.RAPID_EMAIL_VALIDATOR_URL || "http://r0s48o0gwo4g0gkggscswg80.152.53.177.111.sslip.io").replace(/\/$/, "");
const reacherUrl = (process.env.REACHER_URL || "").replace(/\/$/, "");
const reacherEnabled = parseBooleanEnv(process.env.REACHER_ENABLED, false);

function parseBooleanEnv(value: string | undefined, fallback: boolean) {
  if (!value) return fallback;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

async function timed<T>(name: string, check: () => Promise<T>, okMessage: (value: T) => string): Promise<HealthCheck> {
  const startedAt = Date.now();
  try {
    const value = await check();
    return {
      name,
      status: "ok",
      message: okMessage(value),
      latencyMs: Date.now() - startedAt,
    };
  } catch (error) {
    return {
      name,
      status: "error",
      message: error instanceof Error ? error.message : "Health check failed.",
      latencyMs: Date.now() - startedAt,
    };
  }
}

async function fetchJson(url: string, init?: RequestInit) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);

  try {
    const response = await fetch(url, { ...init, cache: "no-store", signal: controller.signal });
    const text = await response.text();
    let data: unknown = null;
    if (text) {
      if ((response.headers.get("content-type") || "").includes("application/json") || /^[\[{]/.test(text.trim())) {
        data = JSON.parse(text);
      } else if (!response.ok) {
        throw new Error(`${response.status}: ${text.slice(0, 140)}`);
      }
    }
    if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(data)}`);
    return data;
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") throw new Error("Timed out after 5000ms.");
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function exists(path: string) {
  try {
    await readdir(path);
    return true;
  } catch {
    return false;
  }
}

async function frontendStorageHealth() {
  const root = storageRoot();
  const probe = join(root, ".health-write-test");
  await mkdir(root, { recursive: true });
  await writeFile(probe, "ok");
  await unlink(probe);

  const [uploads, outputs, jobs] = await Promise.all([
    exists(join(root, "uploads")),
    exists(join(root, "outputs")),
    exists(join(root, "transform-jobs")),
  ]);

  return { uploads, outputs, jobs };
}

export async function GET() {
  const checks = await Promise.all([
    timed("Frontend storage", frontendStorageHealth, (value) => (
      `Writable. Uploads: ${value.uploads ? "ready" : "not created"}, outputs: ${value.outputs ? "ready" : "not created"}, jobs: ${value.jobs ? "ready" : "not created"}.`
    )),
    timed("Backend API", () => fetchJson(`${backendRootUrl}/health`), () => "Backend API is responding."),
    timed("Backend storage", () => fetchJson(`${backendRootUrl}/health/storage`), (value) => {
      const storage = value as { writable?: boolean; error?: string };
      if (!storage.writable) throw new Error(storage.error || "Backend storage is not writable.");
      return "Backend storage is writable.";
    }),
    timed("NocoDB", () => fetchJson(`${backendApiUrl}/nocodb/bases`), (value) => {
      const bases = Array.isArray(value) ? value.length : 0;
      return `${bases} base${bases === 1 ? "" : "s"} available.`;
    }),
    timed("Email validator", () => fetchJson(`${rapidEmailValidatorUrl}/api/validate/batch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ emails: ["healthcheck@example.com"], timeout: 5000 }),
    }), () => "Batch validation endpoint is responding."),
    reacherEnabled
      ? (reacherUrl
        ? timed("Reacher", () => fetchJson(`${reacherUrl}/`, { method: "GET" }), () => "Reacher is responding.")
        : Promise.resolve({ name: "Reacher", status: "warn" as const, message: "REACHER_URL is not configured." }))
      : Promise.resolve({ name: "Reacher", status: "warn" as const, message: "Reacher checks are disabled (REACHER_ENABLED=false)." }),
  ]);

  return NextResponse.json({ checks });
}
