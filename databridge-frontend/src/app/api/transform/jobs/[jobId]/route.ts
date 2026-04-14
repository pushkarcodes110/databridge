import { getTransformJob } from "@/lib/server/transform-jobs";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(_request: Request, { params }: { params: { jobId: string } }) {
  const job = getTransformJob(params.jobId);
  if (!job) {
    return Response.json({ error: "Transform job not found." }, { status: 404 });
  }

  return Response.json(job);
}
