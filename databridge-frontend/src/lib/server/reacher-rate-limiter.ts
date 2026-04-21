import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { dirname } from "path";

type ReacherRateLimitConfig = {
  requestsPerMinute: number;
  requestsPerDay: number;
  stateFilePath: string;
};

type ReacherRateLimitState = {
  dayKey: string;
  dayCount: number;
  nextAllowedAt: number;
};

type ReacherReservation =
  | { allowed: true; waitMs: number }
  | { allowed: false; reason: "DAILY_LIMIT_EXCEEDED" };

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function dayKeyFrom(timestampMs: number) {
  return new Date(timestampMs).toISOString().slice(0, 10);
}

async function withFileLock<T>(stateFilePath: string, handler: () => Promise<T>) {
  const lockPath = `${stateFilePath}.lock`;
  await mkdir(dirname(stateFilePath), { recursive: true });
  const maxAttempts = 100;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      await mkdir(lockPath);
      break;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "EEXIST") {
        throw error;
      }
      await sleep(Math.min(250, 10 + attempt * 5));
      if (attempt === maxAttempts - 1) {
        throw new Error("Could not acquire Reacher rate limit lock.");
      }
    }
  }

  try {
    return await handler();
  } finally {
    await rm(lockPath, { recursive: false, force: true }).catch(() => undefined);
  }
}

async function readState(stateFilePath: string, now: number): Promise<ReacherRateLimitState> {
  try {
    const text = await readFile(stateFilePath, "utf8");
    const parsed = JSON.parse(text) as Partial<ReacherRateLimitState>;
    return {
      dayKey: typeof parsed.dayKey === "string" ? parsed.dayKey : dayKeyFrom(now),
      dayCount: Number.isFinite(parsed.dayCount) ? Math.max(0, Number(parsed.dayCount)) : 0,
      nextAllowedAt: Number.isFinite(parsed.nextAllowedAt) ? Math.max(0, Number(parsed.nextAllowedAt)) : 0,
    };
  } catch {
    return {
      dayKey: dayKeyFrom(now),
      dayCount: 0,
      nextAllowedAt: 0,
    };
  }
}

async function writeState(stateFilePath: string, state: ReacherRateLimitState) {
  await writeFile(stateFilePath, JSON.stringify(state), "utf8");
}

export function createReacherRateLimiter(config: ReacherRateLimitConfig) {
  const intervalMs = Math.max(1, Math.ceil(60_000 / Math.max(config.requestsPerMinute, 1)));

  return {
    async reserve(): Promise<ReacherReservation> {
      const now = Date.now();
      return withFileLock(config.stateFilePath, async () => {
        const state = await readState(config.stateFilePath, now);
        const today = dayKeyFrom(now);
        if (state.dayKey !== today) {
          state.dayKey = today;
          state.dayCount = 0;
        }

        if (state.dayCount >= config.requestsPerDay) {
          return { allowed: false, reason: "DAILY_LIMIT_EXCEEDED" };
        }

        const scheduledAt = Math.max(now, state.nextAllowedAt);
        state.nextAllowedAt = scheduledAt + intervalMs;
        state.dayCount += 1;
        await writeState(config.stateFilePath, state);

        return { allowed: true, waitMs: Math.max(0, scheduledAt - now) };
      });
    },
  };
}
