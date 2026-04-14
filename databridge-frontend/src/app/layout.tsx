import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Link from "next/link";
import { Database, Settings, Activity } from "lucide-react";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "DataBridge | Import & Transform",
  description: "Large-Scale Data Import & Transform Tool",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} bg-background text-foreground antialiased min-h-screen flex`}>
        {/* Sidebar */}
        <aside className="w-64 border-r bg-card/50 flex flex-col backdrop-blur-xl">
          <div className="h-16 flex items-center px-6 border-b">
            <Database className="w-6 h-6 mr-3 text-primary" />
            <h1 className="text-xl font-bold tracking-tight">DataBridge</h1>
          </div>
          <nav className="flex-1 p-4 space-y-2">
            <Link href="/" className="flex items-center px-4 py-3 rounded-xl hover:bg-primary/10 transition-colors text-sm font-medium">
              <Database className="w-4 h-4 mr-3" /> New Import
            </Link>
            <Link href="/jobs" className="flex items-center px-4 py-3 rounded-xl hover:bg-primary/10 transition-colors text-sm font-medium">
              <Activity className="w-4 h-4 mr-3" /> Job History
            </Link>
            <Link href="/settings" className="flex items-center px-4 py-3 rounded-xl hover:bg-primary/10 transition-colors text-sm font-medium">
              <Settings className="w-4 h-4 mr-3" /> Settings
            </Link>
          </nav>
        </aside>

        {/* Main Content */}
        <main className="flex-1 overflow-auto bg-dot-pattern">
          <div className="max-w-7xl mx-auto p-8">
            {children}
          </div>
        </main>
      </body>
    </html>
  );
}
