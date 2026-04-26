import { NextRequest, NextResponse } from "next/server"
import { getJob } from "@/lib/jobs"

export async function GET(
  _req: NextRequest,
  ctx: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await ctx.params
  const job = await getJob(jobId)
  if (!job) {
    return NextResponse.json({ error: "Job not found" }, { status: 404 })
  }

  return NextResponse.json({
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
    heartbeatAt: job.heartbeat_at,
    error: job.error_message
      ? {
          code: job.error_code,
          message: job.error_message,
        }
      : null,
    createdAt: job.created_at,
    updatedAt: job.updated_at,
  })
}
