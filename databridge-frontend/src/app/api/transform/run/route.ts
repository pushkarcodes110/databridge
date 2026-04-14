import { runTransform, TransformConfig } from "@/lib/server/transform-runner";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const config = await request.json() as TransformConfig;
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      const emit = (event: object) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
      };

      runTransform(config, emit)
        .catch((error) => {
          const message = error instanceof Error ? error.message : "Transform failed.";
          emit({ step: "error", error: message });
        })
        .finally(() => controller.close());
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
