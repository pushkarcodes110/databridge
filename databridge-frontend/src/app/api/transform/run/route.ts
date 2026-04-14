import { createReadStream, createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import { createHash } from "crypto";
import { join } from "path";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import csv from "csv-parser";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type TransformMapping = {
  outputColumn: string;
  sourceColumn: string;
};

type TransformConfig = {
  uploadId: string;
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

type RunStats = {
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

async function mapColumns(config: TransformConfig, inputPath: string, outputPath: string, emit: (event: object) => void, stats: RunStats) {
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
      if (rowsProcessed % 500 === 0) {
        emit({ step: "mapping", progress: Math.round((rowsProcessed / Math.max(stats.inputRows, 1)) * 100), rowsProcessed });
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
  try {
    const response = await fetch(`${serviceUrl}/validate-emails`, {
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
    return response.json() as Promise<{
      results: Array<{
        cleaned: string | null;
        status: "valid" | "invalid" | "typo_fixed" | "undeliverable" | "unknown";
      }>;
    }>;
  } catch {
    return null;
  }
}

async function cleanEmails(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, stats: RunStats) {
  if (!config.filters.email.enabled || !config.filters.email.config.column) {
    await pipeline(createReadStream(inputPath), createWriteStream(outputPath));
    emit({ step: "email", progress: 100, rowsRemovedInvalidEmail: 0, emailsTypoFixed: 0 });
    return headers;
  }

  const emailColumn = config.filters.email.config.column;
  const selectedDomains = new Set(config.filters.email.config.selectedDomains);
  let rowsProcessed = 0;
  let serviceWarningEmitted = false;

  await pipeline(
    createReadStream(inputPath),
    csv(),
    makeBatchTransform(500, async (rows) => {
      const validation = await validateEmailBatch(rows.map((row) => row[emailColumn] ?? ""), config);
      if (!validation) {
        if (!serviceWarningEmitted) {
          emit({ step: "email", warning: "Email validation service unreachable. Skipping email cleaning filter." });
          serviceWarningEmitted = true;
        }
        return rows;
      }
      const outputRows: Row[] = [];

      rows.forEach((row, index) => {
        rowsProcessed += 1;
        const result = validation.results[index];

        if (result?.status === "typo_fixed") stats.emailsTypoFixed += 1;

        if ((result?.status === "invalid" || result?.status === "undeliverable") && config.filters.email.config.removeInvalidFormat) {
          stats.rowsRemovedInvalidEmail += 1;
          return;
        }

        if (result?.cleaned) row[emailColumn] = result.cleaned;

        const domain = emailDomain(String(row[emailColumn] ?? ""));
        if (selectedDomains.size > 0 && domain && !selectedDomains.has(domain)) {
          stats.rowsRemovedInvalidEmail += 1;
          return;
        }

        outputRows.push(row);
      });

      emit({
        step: "email",
        progress: rowsProcessed,
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
  try {
    const response = await fetch(`${serviceUrl}/classify-gender`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ names }),
    });

    if (!response.ok) throw new Error("Gender classification service failed.");
    return response.json() as Promise<{
      results: Array<{ gender: "male" | "female" | "unknown"; confidence: number }>;
    }>;
  } catch {
    return null;
  }
}

async function classifyGender(config: TransformConfig, inputPath: string, outputPath: string, headers: string[], emit: (event: object) => void, stats: RunStats) {
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
    makeBatchTransform(500, async (rows) => {
      const classification = await classifyGenderBatch(rows.map((row) => row[nameColumn] ?? ""));
      if (!classification) {
        if (!serviceWarningEmitted) {
          emit({ step: "gender", warning: "Gender service unreachable. Skipping gender filter." });
          serviceWarningEmitted = true;
        }
        return rows;
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
        progress: rowsProcessed,
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

async function runTransform(config: TransformConfig, emit: (event: object) => void) {
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
  let headers = await mapColumns(config, inputPath, mappedPath, emit, stats);

  emit({ step: "deduplication", progress: 0, rowsRemovedFullDupe: 0, rowsRemovedEmailDupe: 0 });
  await deduplicate(config, mappedPath, dedupedPath, headers, emit, stats);

  emit({ step: "email", progress: 0, rowsRemovedInvalidEmail: 0, emailsTypoFixed: 0 });
  headers = await cleanEmails(config, dedupedPath, emailPath, headers, emit, stats);

  emit({ step: "gender", progress: 0, rowsRemovedWrongGender: 0 });
  headers = await classifyGender(config, emailPath, outputPath, headers, emit, stats);

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

export async function POST(request: Request) {
  const config = await request.json() as TransformConfig;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      const emit = (event: object) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
      };

      runTransform(config, emit)
        .catch((error) => {
          const message = error instanceof Error ? error.message : "Transform failed.";
          emit({ step: "error", error: message });
        })
        .finally(() => controller.close());
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
