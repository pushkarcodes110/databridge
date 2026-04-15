import { randomUUID } from "crypto";
import { mkdir, unlink, writeFile } from "fs/promises";
import { join, resolve } from "path";

const defaultStorageRoot = "/tmp/databridge";

export function storageRoot() {
  return resolve(process.env.DATABRIDGE_STORAGE_DIR?.trim() || defaultStorageRoot);
}

export function storagePath(...segments: string[]) {
  return join(storageRoot(), ...segments);
}

export function assertSafeUploadId(uploadId: string) {
  if (!/^[a-f0-9-]{36}$/i.test(uploadId)) {
    throw new Error("Invalid upload id.");
  }
}

export function uploadDirPath(uploadId: string) {
  assertSafeUploadId(uploadId);
  return storagePath("uploads", uploadId);
}

export function uploadInputPath(uploadId: string) {
  return join(uploadDirPath(uploadId), "input.csv");
}

export function outputDirPath(uploadId: string) {
  assertSafeUploadId(uploadId);
  return storagePath("outputs", uploadId);
}

export function outputFilePath(uploadId: string) {
  return join(outputDirPath(uploadId), "output.csv");
}

export function stageDirPath(uploadId: string) {
  assertSafeUploadId(uploadId);
  return storagePath("stages", uploadId);
}

export function transformJobsDirPath() {
  return storagePath("transform-jobs");
}

export async function ensureWritableStorage() {
  const root = storageRoot();
  await mkdir(root, { recursive: true });

  const probePath = join(root, `.write-test-${randomUUID()}`);
  await writeFile(probePath, "ok");
  await unlink(probePath);
  return root;
}

export function storageErrorMessage(error: unknown) {
  const code = typeof error === "object" && error && "code" in error ? String(error.code) : "";
  const message = error instanceof Error ? error.message : "Storage is not writable.";

  if (code === "EACCES" || code === "EPERM") {
    return `Upload storage is not writable at ${storageRoot()}. Mount a writable shared directory there, or set DATABRIDGE_STORAGE_DIR to a writable path. Original error: ${message}`;
  }

  return message;
}

export function storageErrorStatus(error: unknown) {
  const code = typeof error === "object" && error && "code" in error ? String(error.code) : "";
  return code === "EACCES" || code === "EPERM" ? 500 : 400;
}
