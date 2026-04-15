"use client";

import { useEffect } from "react";

export function CleanupScheduler() {
  useEffect(() => {
    const cleanup = () => {
      fetch("/api/transform/cleanup", { method: "POST" }).catch(() => undefined);
    };

    const timeout = window.setTimeout(cleanup, 5 * 60 * 1000);
    const interval = window.setInterval(cleanup, 6 * 60 * 60 * 1000);
    return () => {
      window.clearTimeout(timeout);
      window.clearInterval(interval);
    };
  }, []);

  return null;
}
