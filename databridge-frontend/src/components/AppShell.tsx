"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity, ChevronLeft, ChevronRight, Database, Filter, Moon, Settings, Sun } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Quick Import", icon: Database },
  { href: "/jobs", label: "Job History", icon: Activity },
  { href: "/transform", label: "Transform", icon: Filter },
  { href: "/settings", label: "Settings", icon: Settings },
];

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [theme, setTheme] = useState<"dark" | "light">("dark");

  useEffect(() => {
    const savedTheme = window.localStorage.getItem("databridge-theme");
    const nextTheme = savedTheme === "light" ? "light" : "dark";
    setTheme(nextTheme);
    document.documentElement.classList.toggle("dark", nextTheme === "dark");
  }, []);

  const toggleTheme = () => {
    const nextTheme = theme === "dark" ? "light" : "dark";
    setTheme(nextTheme);
    window.localStorage.setItem("databridge-theme", nextTheme);
    document.documentElement.classList.toggle("dark", nextTheme === "dark");
  };

  return (
    <div className="flex min-h-screen">
      <aside
        className={cn(
          "flex shrink-0 flex-col border-r bg-card/80 backdrop-blur-xl transition-[width] duration-300 ease-out",
          collapsed ? "w-20" : "w-64"
        )}
      >
        <div className="flex h-16 items-center gap-3 border-b px-4">
          <div className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <Database className="size-5" />
          </div>
          <h1 className={cn("text-xl font-bold tracking-tight transition-opacity duration-200", collapsed && "sr-only opacity-0")}>
            DataBridge
          </h1>
        </div>

        <nav className="flex-1 space-y-2 p-3">
          {navItems.map((item) => {
            const Icon = item.icon;
            const active = pathname === item.href;

            return (
              <Link
                key={item.href}
                href={item.href}
                title={collapsed ? item.label : undefined}
                className={cn(
                  "flex items-center gap-3 rounded-lg px-3 py-3 text-sm font-medium transition-all duration-200 hover:bg-primary/10 hover:text-foreground active:scale-[0.98]",
                  active ? "bg-primary/10 text-foreground shadow-sm" : "text-muted-foreground",
                  collapsed && "justify-center px-2"
                )}
              >
                <Icon className="size-4 shrink-0" />
                <span className={cn("transition-opacity duration-200", collapsed && "sr-only opacity-0")}>{item.label}</span>
              </Link>
            );
          })}
        </nav>

        <div className="space-y-3 border-t p-3">
          <Button
            type="button"
            variant="outline"
            className={cn("h-10 w-full justify-start gap-3", collapsed && "justify-center px-0")}
            onClick={toggleTheme}
            title={collapsed ? `Switch to ${theme === "dark" ? "light" : "dark"} mode` : undefined}
          >
            {theme === "dark" ? <Sun className="size-4" /> : <Moon className="size-4" />}
            <span className={cn(collapsed && "sr-only")}>{theme === "dark" ? "Light Mode" : "Dark Mode"}</span>
          </Button>

          <Button
            type="button"
            variant="ghost"
            className={cn("h-10 w-full justify-start gap-3", collapsed && "justify-center px-0")}
            onClick={() => setCollapsed((value) => !value)}
            title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {collapsed ? <ChevronRight className="size-4" /> : <ChevronLeft className="size-4" />}
            <span className={cn(collapsed && "sr-only")}>{collapsed ? "Expand" : "Collapse"}</span>
          </Button>

          <p className={cn("px-2 pb-1 text-xs text-muted-foreground transition-opacity duration-200", collapsed && "sr-only opacity-0")}>
            Made with Love by Pushkar
          </p>
        </div>
      </aside>

      <main className="flex-1 overflow-auto bg-dot-pattern transition-colors duration-300">
        <div className="mx-auto max-w-7xl p-8">{children}</div>
      </main>
    </div>
  );
}
