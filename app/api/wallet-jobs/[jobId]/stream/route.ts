import { getJob } from "@/lib/jobs"

export const dynamic = "force-dynamic"

export async function GET(
  _req: Request,
  ctx: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await ctx.params

  const encoder = new TextEncoder()
  let interval: NodeJS.Timeout | null = null

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const send = async () => {
        const job = await getJob(jobId)
        if (!job) {
          controller.enqueue(encoder.encode(`event: error\ndata: ${JSON.stringify({ error: "Job not found" })}\n\n`))
          controller.close()
          if (interval) clearInterval(interval)
          return
        }

        controller.enqueue(
          encoder.encode(
            `event: progress\ndata: ${JSON.stringify({
              jobId: job.id,
              status: job.status,
              progressPercent: Number(job.progress_percent),
              currentStage: job.current_stage,
              currentStageDetail: job.current_stage_detail,
              periodsTotal: job.periods_total,
              periodsCompleted: job.periods_completed,
              marketsTotal: job.markets_total,
              marketsCompleted: job.markets_completed,
              startedAt: job.started_at,
              finishedAt: job.finished_at,
              error: job.error_message
                ? { code: job.error_code, message: job.error_message }
                : null,
            })}\n\n`,
          ),
        )

        if (job.status === "completed" || job.status === "failed" || job.status === "cancelled") {
          controller.close()
          if (interval) clearInterval(interval)
        }
      }

      await send()
      interval = setInterval(() => {
        void send()
      }, 2000)
    },
    cancel() {
      if (interval) clearInterval(interval)
    },
  })

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  })
}
