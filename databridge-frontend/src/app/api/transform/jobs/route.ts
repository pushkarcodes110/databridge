import { listTransformJobs, startTransformJob } from "@/lib/server/transform-jobs";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const limit = Number(url.searchParams.get("limit") || "100");
  return Response.json(await listTransformJobs(limit));
}

export async function POST(request: Request) {
  const body = await request.json();
  const job = startTransformJob(body.config, body.autoImport);
  return Response.json({
    id: job.id,
    status: job.status,
    latestEvent: job.latestEvent,
  });
}
