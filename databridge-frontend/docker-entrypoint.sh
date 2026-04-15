#!/bin/sh
set -eu

STORAGE_DIR="${DATABRIDGE_STORAGE_DIR:-/tmp/databridge}"

mkdir -p "$STORAGE_DIR"
chown -R nextjs:nodejs "$STORAGE_DIR"

exec su-exec nextjs "$@"
