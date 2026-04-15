"use client";

import { CheckCircle2, Clock3, Database, PlugZap } from "lucide-react";
import { cn } from "@/lib/utils";

const integrations = [
  { id: "nocodb", name: "NocoDB", description: "Default workspace import", disabled: false, icon: Database },
  { id: "airtable", name: "Airtable", description: "CRM-style tables", disabled: true, icon: PlugZap },
  { id: "sheets", name: "Google Sheets", description: "Spreadsheet sync", disabled: true, icon: PlugZap },
  { id: "hubspot", name: "HubSpot", description: "Contact import", disabled: true, icon: PlugZap },
];

export function QuickImportOptions() {
  return (
    <div className="rounded-2xl border bg-card p-5 shadow-sm">
      <div className="mb-4 flex items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">Destination</h2>
          <p className="text-sm text-muted-foreground">Choose where this CSV should land.</p>
        </div>
        <span className="rounded-full bg-primary/10 px-3 py-1 text-xs font-semibold text-primary">CSV ready</span>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        {integrations.map((integration) => {
          const Icon = integration.icon;
          const selected = integration.id === "nocodb";

          return (
            <button
              key={integration.id}
              type="button"
              disabled={integration.disabled}
              aria-pressed={selected}
              className={cn(
                "flex min-h-24 items-start gap-3 rounded-lg border p-4 text-left transition-all duration-200",
                selected && "border-primary bg-primary/10 shadow-sm",
                !integration.disabled && "hover:border-primary/60 hover:bg-primary/5 active:scale-[0.98]",
                integration.disabled && "cursor-not-allowed opacity-55"
              )}
            >
              <span className={cn("rounded-lg border bg-background p-2", selected && "border-primary/40")}>
                <Icon className="size-4" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="flex items-center gap-2 font-semibold">
                  {integration.name}
                  {selected ? <CheckCircle2 className="size-4 text-primary" /> : null}
                </span>
                <span className="mt-1 block text-sm text-muted-foreground">
                  {integration.disabled ? `${integration.description} (Coming Soon)` : integration.description}
                </span>
              </span>
              {integration.disabled ? <Clock3 className="size-4 text-muted-foreground" /> : null}
            </button>
          );
        })}
      </div>
    </div>
  );
}
