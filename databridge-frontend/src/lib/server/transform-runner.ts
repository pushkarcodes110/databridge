import { createReadStream, createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import { createHash } from "crypto";
import { join } from "path";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import csv from "csv-parser";
import { outputDirPath, outputFilePath, stageDirPath, uploadInputPath } from "@/lib/server/storage";

export type TransformMapping = {
  outputColumn: string;
  sourceColumn: string;
};

export type TransformConfig = {
  uploadId: string;
  totalRows?: number;
  mapping: TransformMapping[];
  filters: {
    email: {
      enabled: boolean;
      config: {
        column: string;
        selectedDomains: string[];
        fixCommonTypos: boolean;
        removeInvalidFormat: boolean;
        verifyMailboxExists: boolean;
        mailboxValidator?: "rapid" | "reacher";
        normalizeLowercase: boolean;
      };
    };
    gender: {
      enabled: boolean;
      config: {
        nameColumn: string;
        mode: "male" | "female" | "all";
        addGenderColumn: boolean;
      };
    };
    deduplication: {
      enabled: boolean;
      config: {
        removeFullDuplicates: boolean;
        removeDuplicateEmails: boolean;
        emailColumn: string;
        strategy: "first" | "last";
      };
    };
  };
};

type Row = Record<string, string>;

export type RunStats = {
  inputRows: number;
  outputRows: number;
  rowsRemoved: number;
  rowsRemovedFullDupe: number;
  rowsRemovedEmailDupe: number;
  rowsRemovedInvalidEmail: number;
  emailsTypoFixed: number;
  rowsRemovedWrongGender: number;
  maleCount: number;
  femaleCount: number;
  unknownCount: number;
  skippedRows: number;
};

const serviceUrl = process.env.EMAIL_VALIDATOR_URL || "http://localhost:8001";
const serviceUrls = Array.from(new Set([
  serviceUrl,
  "http://email-validator:8001",
  "http://host.docker.internal:8001",
  "http://localhost:8001",
]));
const rapidEmailValidatorUrl = (process.env.RAPID_EMAIL_VALIDATOR_URL || "http://r0s48o0gwo4g0gkggscswg80.152.53.177.111.sslip.io").replace(/\/$/, "");
const reacherUrl = (process.env.REACHER_URL || "http://reacher:8080").replace(/\/$/, "");
const reacherCheckPath = process.env.REACHER_CHECK_PATH || "/v1/check_email";
const reacherConcurrency = Math.max(1, Math.min(Number(process.env.REACHER_CONCURRENCY || "25"), 100));
const reacherEnabled = parseBooleanEnv(process.env.REACHER_ENABLED, false);
const EMAIL_BATCH_SIZE = 50;
const GENDER_BATCH_SIZE = 500;
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

type EmailValidationResult = {
  cleaned: string | null;
  status: "valid" | "invalid" | "typo_fixed" | "undeliverable" | "unknown";
};

type MailboxValidationProvider = "rapid" | "reacher";

type MailboxValidationResult = {
  status: string;
};

function parseBooleanEnv(value: string | undefined, fallback: boolean) {
  if (!value) return fallback;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

async function readJsonResponse<T>(response: Response, label: string): Promise<T> {
  const text = await response.text();
  const contentType = response.headers.get("content-type") || "";
  let data: unknown = null;

  if (text) {
    if (contentType.includes("application/json") || /^[\[{]/.test(text.trim())) {
      try {
        data = JSON.parse(text);
      } catch {
        throw new Error(`${label} returned invalid JSON: ${text.slice(0, 160)}`);
      }
    } else if (!response.ok) {
      throw new Error(`${label} failed (${response.status}): ${text.slice(0, 160)}`);
    }
  }

  if (!response.ok) {
    const detail = data && typeof data === "object" && "error" in data ? String((data as { error?: unknown }).error) : null;
    throw new Error(detail || `${label} failed (${response.status})`);
  }

  return data as T;
}

async function fetchJsonWithTimeout<T>(url: string, init: RequestInit, timeoutMs: number, label: string) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    return await readJsonResponse<T>(response, label);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`${label} timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function csvEscape(value: unknown) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function rowHash(row: Row, headers: string[]) {
  return createHash("md5")
    .update(headers.map((header) => row[header] ?? "").join("\u001f"))
    .digest("hex");
}

function emailDomain(email: string) {
  const atIndex = email.lastIndexOf("@");
  return atIndex === -1 ? "" : email.slice(atIndex + 1).toLowerCase();
}

function correctCommonTypo(email: string) {
  const atIndex = email.lastIndexOf("@");
  if (atIndex === -1) return { email, fixed: false };

  const localPart = email.slice(0, atIndex);
  const domain = email.slice(atIndex + 1).trim().toLowerCase();
  const correctedDomain = typoDomains[domain];
  if (!correctedDomain || correctedDomain === domain) return { email, fixed: false };

  return { email: `${localPart}@${correctedDomain}`, fixed: true };
}

function localEmailValidation(email: string, config: TransformConfig): EmailValidationResult {
  let cleaned = String(email ?? "").trim();
  if (config.filters.email.config.normalizeLowercase) cleaned = cleaned.toLowerCase();

  let typoFixed = false;
  if (config.filters.email.config.fixCommonTypos) {
    const corrected = correctCommonTypo(cleaned);
    cleaned = corrected.email;
    typoFixed = corrected.fixed;
  }

  if (!emailPattern.test(cleaned)) {
    return {
      cleaned: config.filters.email.config.removeInvalidFormat ? null : cleaned,
      status: config.filters.email.config.removeInvalidFormat ? "invalid" : "unknown",
    };
  }

  return {
    cleaned,
    status: typoFixed ? "typo_fixed" : "valid",
  };
}

function mappedColumnForSource(config: TransformConfig, sourceColumn: string) {
  return config.mapping.find((item) => item.sourceColumn === sourceColumn)?.outputColumn || sourceColumn;
}

class CsvStringifyTransform extends Transform {
  private wroteHeader = false;

  constructor(private headers: string[]) {
    super({ writableObjectMode: true });
  }

  _transform(row: Row, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    if (!this.wroteHeader) {
      this.push(`${this.headers.map(csvEscape).join(",")}\n`);
      this.wroteHeader = true;
    }

    this.push(`${this.headers.map((header) => csvEscape(row[header])).join(",")}\n`);
    callback();
  }

  _flush(callback: (error?: Error | null) => void) {
    if (!this.wroteHeader) {
      this.push(`${this.headers.map(csvEscape).join(",")}\n`);
      this.wroteHeader = true;
    }
    callback();
  }
}

function makeAsyncRowTransform(handleRow: (row: Row) => Promise<Row | Row[] | null>) {
  return new Transform({
    objectMode: true,
    transform(row: Row, _encoding, callback) {
      handleRow(row)
        .then((result) => {
          if (Array.isArray(result)) {
            result.forEach((item) => this.push(item));
          } else if (result) {
            this.push(result);
          }
          callback();
        })
        .catch((error) => callback(error));
    },
  });
}

function isPlainRow(row: unknown): row is Row {
  return Boolean(row && typeof row === "object" && !Array.isArray(row));
}

function makeBatchTransform(
  batchSize: number,
  processBatch: (rows: Row[]) => Promise<Row[]>,
) {
  let batch: Row[] = [];

  async function flushBatch(stream: Transform) {
    if (batch.length === 0) return;
    const current = batch;
    batch = [];
    const rows = await processBatch(current);
    rows.forEach((row) => stream.push(row));
  }

  return new Transform({
    objectMode: true,
    transform(row: Row, _encoding, callback) {
      batch.push(row);
      if (batch.length < batchSize) {
        callback();
        return;
      }

      flushBatch(this).then(() => callback()).catch((error) => callback(error));
    },
    flush(callback) {
      flushBatch(this).then(() => callback()).catch((error) => callback(error));
    },
  });
}

async function countRows(filePath: string) {
  let totalRows = 0;
  await pipeline(
    createReadStream(filePath),
    csv(),
    makeAsyncRowTransform(async () => {
      totalRows += 1;
      return null;
    })
  );
  return totalRows;
}

async function mapColumns(config: TransformConfig, inputPath: string, outputPath: string, emit: (event: object) => void, stats: RunStats, totalRows: number) {
  let rowsProcessed = 0;
  const headers = config.mapping.map((item) => item.outputColumn);

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeAsyncRowTransform(async (row) => {
      if (!isPlainRow(row)) {
        stats.skippedRows += 1;
        console.warn("Skipping malformed CSV row during mapping.", row);
        return null;
      }

      rowsProcessed += 1;
      stats.inputRows += 1;
      if (rowsProcessed % 500 === 0 || rowsProcessed === totalRows) {
        emit({ step: "mapping", progress: Math.round((rowsProcessed / Math.max(totalRows, 1)) * 100), rowsProcessed });
      }

      return config.mapping.reduce<Row>((acc, item) => {
        acc[item.outputColumn] = row[item.sourceColumn] ?? "";
        return acc;
      }, {});
    }),
    new CsvStringifyTransform(headers),
    createWriteStream(outputPath)
  );

  emit({ step: "mapping", progress: 100, rowsProcessed });
  return headers;
}

async function buildKeyCounts(inputPath: string, keyForRow: (row: Row) => string) {
  const counts = new Map<string, number>();

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeAsyncRowTransform(async (row) => {
      const key = keyForRow(row);
      if (key) counts.set(key, (counts.get(key) ?? 0) + 1);
      return null;
    })
  );

  return counts;
}

async function dedupePass({
  inputPath,
  outputPath,
  headers,
  strategy,
  keyForRow,
  onRemove,
}: {
  inputPath: string;
  outputPath: string;
  headers: string[];
  strategy: "first" | "last";
  keyForRow: (row: Row) => string;
  onRemove: () => void;
}) {
  const seen = new Set<string>();
  const counts = strategy === "last" ? await buildKeyCounts(inputPath, keyForRow) : null;

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeAsyncRowTransform(async (row) => {
      const key = keyForRow(row);
      if (!key) return row;

      if (strategy === "last" && counts) {
        const remaining = counts.get(key) ?? 0;
        counts.set(key, Math.max(remaining - 1, 0));
        if (remaining > 1) {
          onRemove();
          return null;
        }
        return row;
      }

      if (seen.has(key)) {
        onRemove();
        return null;
      }

      seen.add(key);
      return row;
    }),
    new CsvStringifyTransform(headers),
    createWriteStream(outputPath)
  );
}

async function deduplicate(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, stats: RunStats) {
  if (!config.filters.deduplication.enabled) {
    await pipeline(createReadStream(inputPath), createWriteStream(outputPath));
    emit({ step: "deduplication", progress: 100, rowsRemovedFullDupe: 0, rowsRemovedEmailDupe: 0 });
    return;
  }

  const { removeFullDuplicates, removeDuplicateEmails, strategy } = config.filters.deduplication.config;
  const emailColumn = mappedColumnForSource(config, config.filters.deduplication.config.emailColumn);
  const afterFullPath = `${outputPath}.full.csv`;
  let currentPath = inputPath;

  if (removeFullDuplicates) {
    await dedupePass({
      inputPath,
      outputPath: afterFullPath,
      headers,
      strategy,
      keyForRow: (row) => rowHash(row, headers),
      onRemove: () => {
        stats.rowsRemovedFullDupe += 1;
      },
    });
    currentPath = afterFullPath;
    emit({
      step: "deduplication",
      progress: 50,
      rowsRemovedFullDupe: stats.rowsRemovedFullDupe,
      rowsRemovedEmailDupe: stats.rowsRemovedEmailDupe,
    });
  }

  if (removeDuplicateEmails) {
    await dedupePass({
      inputPath: currentPath,
      outputPath,
      headers,
      strategy,
      keyForRow: (row) => String(row[emailColumn] ?? "").trim().toLowerCase(),
      onRemove: () => {
        stats.rowsRemovedEmailDupe += 1;
      },
    });
  } else {
    await pipeline(createReadStream(currentPath), createWriteStream(outputPath));
  }

  emit({
    step: "deduplication",
    progress: 100,
    rowsRemovedFullDupe: stats.rowsRemovedFullDupe,
    rowsRemovedEmailDupe: stats.rowsRemovedEmailDupe,
  });
}

function normalizeMailboxStatus(status: string | undefined) {
  return String(status || "UNKNOWN").trim().toUpperCase() || "UNKNOWN";
}

function statusFromReacher(result: Record<string, unknown> | undefined) {
  const isReachable = String(result?.is_reachable || "").toLowerCase();
  const syntax = result?.syntax && typeof result.syntax === "object" ? result.syntax as Record<string, unknown> : {};
  const mx = result?.mx && typeof result.mx === "object" ? result.mx as Record<string, unknown> : {};

  if (syntax.is_valid_syntax === false) return "INVALID_SYNTAX";
  if (mx.accepts_mail === false) return "NO_MX_RECORDS";
  if (isReachable === "safe") return "VALID";
  if (isReachable === "invalid") return "INVALID";
  if (isReachable === "risky") return "RISKY";
  return "UNKNOWN";
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  handler: (item: T, index: number) => Promise<R>
) {
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await handler(items[currentIndex], currentIndex);
    }
  });

  await Promise.all(workers);
  return results;
}

async function validateMailboxWithRapid(emails: string[]): Promise<MailboxValidationResult[]> {
  type RapidResponseItem = { email?: unknown; status?: unknown; error?: unknown };
  type RapidResponse = RapidResponseItem[] | {
    results?: RapidResponseItem[];
    items?: RapidResponseItem[];
    data?: RapidResponseItem[];
  };

  const data = await fetchJsonWithTimeout<RapidResponse>(`${rapidEmailValidatorUrl}/api/validate/batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ emails, timeout: 5000 }),
  }, 8000, "Rapid Email Validator");

  const results = Array.isArray(data)
    ? data
    : Array.isArray(data?.results)
      ? data.results
      : Array.isArray(data?.items)
        ? data.items
        : Array.isArray(data?.data)
          ? data.data
          : [];
  const resultsByEmail = new Map<string, RapidResponseItem>();
  results.forEach((item) => {
    const email = typeof item?.email === "string" ? item.email.trim().toLowerCase() : "";
    if (email) resultsByEmail.set(email, item);
  });

  return emails.map((email, index) => {
    const item = resultsByEmail.get(email.trim().toLowerCase()) || results[index];
    return { status: normalizeMailboxStatus(typeof item?.status === "string" ? item.status : undefined) };
  });
}

async function validateMailboxWithReacher(emails: string[]): Promise<MailboxValidationResult[]> {
  return mapWithConcurrency(emails, reacherConcurrency, async (email) => {
    const data = await fetchJsonWithTimeout<Record<string, unknown>>(`${reacherUrl}${reacherCheckPath}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ to_email: email }),
    }, 8000, "Reacher");
    return { status: statusFromReacher(data && typeof data === "object" ? data : undefined) };
  });
}

async function validateMailboxBatch(emails: string[], provider: MailboxValidationProvider) {
  if (provider === "reacher" && !reacherEnabled) {
    throw new Error("Reacher is disabled. Set REACHER_ENABLED=true to use the Reacher mailbox validator.");
  }
  const results: MailboxValidationResult[] = emails.map((email) => (
    String(email ?? "").trim() ? { status: "UNKNOWN" } : { status: "EMPTY" }
  ));
  const pending = emails
    .map((email, index) => ({ email: String(email ?? "").trim(), index }))
    .filter((item) => item.email);

  if (pending.length === 0) return results;

  let providerResults: MailboxValidationResult[];
  try {
    providerResults = provider === "reacher"
      ? await validateMailboxWithReacher(pending.map((item) => item.email))
      : await validateMailboxWithRapid(pending.map((item) => item.email));
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    const status = message.toLowerCase().includes("timed out") ? "VALIDATION_TIMEOUT" : "VALIDATION_FAILED";
    providerResults = pending.map(() => ({ status }));
  }

  pending.forEach((item, index) => {
    results[item.index] = providerResults[index] ?? { status: "UNKNOWN" };
  });

  return results;
}

async function cleanEmails(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, stats: RunStats, totalRows: number) {
  if (!config.filters.email.enabled || !config.filters.email.config.column) {
    await pipeline(createReadStream(inputPath), createWriteStream(outputPath));
    emit({ step: "email", progress: 100, rowsRemovedInvalidEmail: 0, emailsTypoFixed: 0 });
    return headers;
  }

  const emailColumn = config.filters.email.config.column;
  const selectedDomains = new Set(config.filters.email.config.selectedDomains);
  let rowsProcessed = 0;
  emit({
    step: "email",
    validationMode: "local",
    verifyMailbox: false,
    batchSize: EMAIL_BATCH_SIZE,
  });

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeBatchTransform(EMAIL_BATCH_SIZE, async (rows) => {
      const outputRows: Row[] = [];

      rows.forEach((row) => {
        rowsProcessed += 1;
        const result = localEmailValidation(row[emailColumn] ?? "", config);

        if (result.status === "typo_fixed") {
          stats.emailsTypoFixed += 1;
        }

        const shouldRemoveEmail =
          result.status === "invalid" ||
          result.status === "undeliverable";

        if (shouldRemoveEmail && config.filters.email.config.removeInvalidFormat) {
          stats.rowsRemovedInvalidEmail += 1;
          return;
        }

        row[emailColumn] = result.cleaned ?? row[emailColumn] ?? "";

        const domain = emailDomain(String(row[emailColumn] ?? ""));
        if (selectedDomains.size > 0 && domain && !selectedDomains.has(domain)) {
          stats.rowsRemovedInvalidEmail += 1;
          return;
        }

        outputRows.push(row);
      });

      emit({
        step: "email",
        progress: Math.round((rowsProcessed / Math.max(totalRows, 1)) * 100),
        rowsProcessed,
        rowsRemovedInvalidEmail: stats.rowsRemovedInvalidEmail,
        emailsTypoFixed: stats.emailsTypoFixed,
      });

      return outputRows;
    }),
    new CsvStringifyTransform(headers),
    createWriteStream(outputPath)
  );

  emit({
    step: "email",
    progress: 100,
    rowsRemovedInvalidEmail: stats.rowsRemovedInvalidEmail,
    emailsTypoFixed: stats.emailsTypoFixed,
  });
  return headers;
}

async function enrichEmailValidity(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, totalRows: number) {
  if (!config.filters.email.enabled || !config.filters.email.config.column || !config.filters.email.config.verifyMailboxExists) {
    await pipeline(createReadStream(inputPath), createWriteStream(outputPath));
    emit({ step: "email_enrichment", progress: 100, skipped: true });
    return headers;
  }

  const emailColumn = config.filters.email.config.column;
  const provider = config.filters.email.config.mailboxValidator || "rapid";
  if (provider === "reacher" && !reacherEnabled) {
    emit({
      step: "email_enrichment",
      progress: 0,
      provider,
      column: "status",
      warning: "Reacher is disabled (REACHER_ENABLED=false). Enable it or switch mailbox validator to Rapid.",
    });
    throw new Error("Reacher is disabled. Set REACHER_ENABLED=true or switch mailbox validator to Rapid.");
  }
  const statusColumn = "status";
  const outputHeaders = headers.includes(statusColumn) ? headers : [...headers, statusColumn];
  let rowsProcessed = 0;
  let validationIssues = 0;
  let lastValidationIssueWarning = 0;

  emit({
    step: "email_enrichment",
    progress: 0,
    provider,
    column: statusColumn,
    batchSize: EMAIL_BATCH_SIZE,
  });

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeBatchTransform(EMAIL_BATCH_SIZE, async (rows) => {
      const validation = await validateMailboxBatch(rows.map((row) => row[emailColumn] ?? ""), provider);
      if (validation.length !== rows.length) {
        throw new Error("Email validator returned an incomplete result set.");
      }

      validationIssues += validation.filter((item) => (
        item.status === "VALIDATION_TIMEOUT" || item.status === "VALIDATION_FAILED"
      )).length;
      const warning = validationIssues > lastValidationIssueWarning
        ? `${validationIssues} email validations could not be completed yet.`
        : undefined;
      if (warning) lastValidationIssueWarning = validationIssues;

      rows.forEach((row, index) => {
        rowsProcessed += 1;
        row[statusColumn] = normalizeMailboxStatus(validation[index]?.status);
      });

      emit({
        step: "email_enrichment",
        progress: Math.round((rowsProcessed / Math.max(totalRows, 1)) * 100),
        rowsProcessed,
        provider,
        column: statusColumn,
        warning,
      });

      return rows;
    }),
    new CsvStringifyTransform(outputHeaders),
    createWriteStream(outputPath)
  );

  emit({
    step: "email_enrichment",
    progress: 100,
    rowsProcessed,
    provider,
    column: statusColumn,
  });

  return outputHeaders;
}

async function classifyGenderBatch(names: string[]) {
  for (const url of serviceUrls) {
    try {
      const response = await fetch(`${url}/classify-gender`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ names }),
      });

      if (!response.ok) throw new Error("Gender classification service failed.");
      return response.json() as Promise<{
        results: Array<{ gender: "male" | "female" | "unknown"; confidence: number }>;
      }>;
    } catch {
      // Try the next configured service URL.
    }
  }

  return null;
}

async function classifyGender(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, stats: RunStats, totalRows: number) {
  if (!config.filters.gender.enabled || !config.filters.gender.config.nameColumn) {
    await pipeline(createReadStream(inputPath), createWriteStream(outputPath));
    emit({ step: "gender", progress: 100, rowsRemovedWrongGender: 0 });
    return headers;
  }

  const nameColumn = mappedColumnForSource(config, config.filters.gender.config.nameColumn);
  const outputHeaders = config.filters.gender.config.addGenderColumn && !headers.includes("gender") ? [...headers, "gender"] : headers;
  let rowsProcessed = 0;
  let serviceWarningEmitted = false;

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeBatchTransform(GENDER_BATCH_SIZE, async (rows) => {
      const classification = await classifyGenderBatch(rows.map((row) => row[nameColumn] ?? ""));
      if (!classification) {
        if (!serviceWarningEmitted) {
          emit({ step: "gender", warning: "Gender service unreachable. Transform stopped to avoid writing an inaccurate CSV." });
          serviceWarningEmitted = true;
        }
        throw new Error("Gender service unreachable. Start the email-validator service or set EMAIL_VALIDATOR_URL to http://localhost:8001.");
      }
      const outputRows: Row[] = [];

      rows.forEach((row, index) => {
        rowsProcessed += 1;
        const gender = classification.results[index]?.gender ?? "unknown";
        if (gender === "male") stats.maleCount += 1;
        if (gender === "female") stats.femaleCount += 1;
        if (gender === "unknown") stats.unknownCount += 1;

        if (config.filters.gender.config.addGenderColumn) row.gender = gender;

        if (config.filters.gender.config.mode !== "all" && gender !== config.filters.gender.config.mode) {
          stats.rowsRemovedWrongGender += 1;
          return;
        }

        outputRows.push(row);
      });

      emit({
        step: "gender",
        progress: Math.round((rowsProcessed / Math.max(totalRows, 1)) * 100),
        rowsRemovedWrongGender: stats.rowsRemovedWrongGender,
        maleCount: stats.maleCount,
        femaleCount: stats.femaleCount,
        unknownCount: stats.unknownCount,
      });

      return outputRows;
    }),
    new CsvStringifyTransform(outputHeaders),
    createWriteStream(outputPath)
  );

  emit({
    step: "gender",
    progress: 100,
    rowsRemovedWrongGender: stats.rowsRemovedWrongGender,
    maleCount: stats.maleCount,
    femaleCount: stats.femaleCount,
    unknownCount: stats.unknownCount,
  });

  return outputHeaders;
}

export async function runTransform(config: TransformConfig, emit: (event: object) => void) {
  if (!config.uploadId || config.mapping.length === 0) {
    throw new Error("uploadId and mapping are required.");
  }

  const inputPath = uploadInputPath(config.uploadId);
  const outputDir = outputDirPath(config.uploadId);
  const stageDir = stageDirPath(config.uploadId);
  await mkdir(outputDir, { recursive: true });
  await mkdir(stageDir, { recursive: true });

  const mappedPath = join(stageDir, "01-mapped.csv");
  const dedupedPath = join(stageDir, "02-deduped.csv");
  const emailPath = join(stageDir, "03-email.csv");
  const emailEnrichedPath = join(stageDir, "04-email-enriched.csv");
  const genderPath = join(stageDir, "05-gender.csv");
  const outputPath = outputFilePath(config.uploadId);
  const stats: RunStats = {
    inputRows: 0,
    outputRows: 0,
    rowsRemoved: 0,
    rowsRemovedFullDupe: 0,
    rowsRemovedEmailDupe: 0,
    rowsRemovedInvalidEmail: 0,
    emailsTypoFixed: 0,
    rowsRemovedWrongGender: 0,
    maleCount: 0,
    femaleCount: 0,
    unknownCount: 0,
    skippedRows: 0,
  };

  emit({ step: "mapping", progress: 0, rowsProcessed: 0 });
  const inputRowTotal = config.totalRows || await countRows(inputPath);
  let headers = await mapColumns(config, inputPath, mappedPath, emit, stats, inputRowTotal);

  emit({ step: "deduplication", progress: 0, rowsRemovedFullDupe: 0, rowsRemovedEmailDupe: 0 });
  await deduplicate(config, mappedPath, dedupedPath, headers, emit, stats);

  emit({ step: "email", progress: 0, rowsRemovedInvalidEmail: 0, emailsTypoFixed: 0 });
  const emailInputRows = await countRows(dedupedPath);
  headers = await cleanEmails(config, dedupedPath, emailPath, headers, emit, stats, emailInputRows);

  emit({ step: "email_enrichment", progress: 0 });
  const emailEnrichmentInputRows = await countRows(emailPath);
  headers = await enrichEmailValidity(config, emailPath, emailEnrichedPath, headers, emit, emailEnrichmentInputRows);

  emit({ step: "gender", progress: 0, rowsRemovedWrongGender: 0 });
  const genderInputRows = await countRows(emailEnrichedPath);
  headers = await classifyGender(config, emailEnrichedPath, genderPath, headers, emit, stats, genderInputRows);

  emit({ step: "write", progress: 0 });
  await pipeline(createReadStream(genderPath), createWriteStream(outputPath));
  stats.outputRows = await countRows(outputPath);
  stats.rowsRemoved = Math.max(stats.inputRows - stats.outputRows, 0);

  emit({
    step: "complete",
    progress: 100,
    outputFile: `/api/transform/download/${config.uploadId}`,
    headers,
    stats,
  });
}
