import { createReadStream, createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import { createHash } from "crypto";
import { join } from "path";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import csv from "csv-parser";

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

async function validateEmailBatch(emails: string[], config: TransformConfig) {
  for (const url of serviceUrls) {
    try {
      const response = await fetch(`${url}/validate-emails`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          emails,
          options: {
            fixTypos: config.filters.email.config.fixCommonTypos,
            removeInvalid: config.filters.email.config.removeInvalidFormat,
            verifyMailbox: config.filters.email.config.verifyMailboxExists,
            normalize: config.filters.email.config.normalizeLowercase,
          },
        }),
      });

      if (!response.ok) throw new Error("Email validation service failed.");
      return response.json() as Promise<{ results: EmailValidationResult[] }>;
    } catch {
      // Try the next configured service URL.
    }
  }

  return null;
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
  const shouldVerifyMailbox = config.filters.email.config.verifyMailboxExists;
  emit({
    step: "email",
    validationMode: shouldVerifyMailbox ? "reacher" : "local",
    verifyMailbox: shouldVerifyMailbox,
    batchSize: EMAIL_BATCH_SIZE,
  });

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeBatchTransform(EMAIL_BATCH_SIZE, async (rows) => {
      const validation = shouldVerifyMailbox
        ? await validateEmailBatch(rows.map((row) => row[emailColumn] ?? ""), config)
        : null;

      if (shouldVerifyMailbox && !validation) {
        const message = "Email validation service unreachable. Transform stopped to avoid writing a local-only email CSV.";
        emit({ step: "email", warning: message });
        throw new Error(`${message} Start the email-validator/Reacher services or disable mailbox verification.`);
      }

      if (shouldVerifyMailbox && validation && validation.results.length !== rows.length) {
        const message = "Email validation service returned an incomplete result set. Transform stopped to avoid writing an inaccurate CSV.";
        emit({ step: "email", warning: message });
        throw new Error(message);
      }

      const outputRows: Row[] = [];

      rows.forEach((row, index) => {
        rowsProcessed += 1;
        const fallback = localEmailValidation(row[emailColumn] ?? "", config);
        const result = validation?.results[index] ?? fallback;

        if (result.status === "typo_fixed" || (!validation && fallback.status === "typo_fixed")) {
          stats.emailsTypoFixed += 1;
        }

        const shouldRemoveEmail =
          result.status === "invalid" ||
          result.status === "undeliverable" ||
          (shouldVerifyMailbox && result.status === "unknown");

        if (shouldRemoveEmail && (config.filters.email.config.removeInvalidFormat || shouldVerifyMailbox)) {
          stats.rowsRemovedInvalidEmail += 1;
          return;
        }

        row[emailColumn] = result.cleaned ?? fallback.cleaned ?? row[emailColumn] ?? "";

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

  const inputPath = join("/tmp", "databridge", "uploads", config.uploadId, "input.csv");
  const outputDir = join("/tmp", "databridge", "outputs", config.uploadId);
  const stageDir = join("/tmp", "databridge", "stages", config.uploadId);
  await mkdir(outputDir, { recursive: true });
  await mkdir(stageDir, { recursive: true });

  const mappedPath = join(stageDir, "01-mapped.csv");
  const dedupedPath = join(stageDir, "02-deduped.csv");
  const emailPath = join(stageDir, "03-email.csv");
  const outputPath = join(outputDir, "output.csv");
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

  emit({ step: "gender", progress: 0, rowsRemovedWrongGender: 0 });
  const genderInputRows = await countRows(emailPath);
  headers = await classifyGender(config, emailPath, outputPath, headers, emit, stats, genderInputRows);

  emit({ step: "write", progress: 0 });
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
