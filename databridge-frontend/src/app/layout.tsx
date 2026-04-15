import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Toaster } from "sonner";
import { CleanupScheduler } from "@/components/transform/CleanupScheduler";
import { AppShell } from "@/components/AppShell";
import { BackgroundJobToasts } from "@/components/BackgroundJobToasts";

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
    <html lang="en" className="dark" suppressHydrationWarning>
      <head>
        <script
          dangerouslySetInnerHTML={{
            __html: `try{var t=localStorage.getItem("databridge-theme");document.documentElement.classList.toggle("dark",t!=="light")}catch(e){document.documentElement.classList.add("dark")}`,
          }}
        />
      </head>
      <body className={`${inter.className} min-h-screen bg-background text-foreground antialiased`}>
        <AppShell>
          {children}
          <CleanupScheduler />
          <BackgroundJobToasts />
          <Toaster richColors position="top-right" />
        </AppShell>
      </body>
    </html>
  );
}
