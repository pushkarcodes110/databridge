"use client";

import { useEffect } from "react";

export function CleanupScheduler() {
  useEffect(() => {
    const cleanup = () => {
      fetch("/api/transform/cleanup", { method: "POST" }).catch(() => undefined);
    };

    cleanup();
    const interval = window.setInterval(cleanup, 30 * 60 * 1000);
    return () => window.clearInterval(interval);
  }, []);

  return null;
}
