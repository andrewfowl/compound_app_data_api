import { NextRequest, NextResponse } from "next/server"
import { tasks } from "@trigger.dev/sdk/v3"
import { createWalletAndJob } from "@/lib/jobs"
import type { walletIndexJob } from "@/trigger/wallet-index-job"

export async function POST(req: NextRequest) {
  const body = await req.json()
  const userId = req.headers.get("x-user-id")
  if (!userId) return NextResponse.json({ error: "Missing x-user-id header" }, { status: 401 })

  const walletAddress = String(body.walletAddress || "").trim()
  const walletStartDate = String(body.walletStartDate || "").trim()
  const reportStartDate = String(body.reportStartDate || walletStartDate).trim()
  const reportEndMonth = String(body.reportEndMonth || "").trim()
  const frequency = String(body.frequency || "monthly") as "monthly" | "quarterly" | "adhoc"
  const protocolScope = Array.isArray(body.protocolScope) && body.protocolScope.length > 0 ? body.protocolScope : ["v2", "v3"]
  const priceSourceMode = String(body.priceSourceMode || "uploaded_or_fallback")

  if (!walletAddress || !walletStartDate || !reportEndMonth) {
    return NextResponse.json({ error: "walletAddress, walletStartDate, and reportEndMonth are required" }, { status: 400 })
  }

  const job = await createWalletAndJob({ userId, address: walletAddress, walletStartDate, reportStartDate, reportEndMonth, frequency, protocolScope, priceSourceMode })
  const run = await tasks.trigger<typeof walletIndexJob>("wallet-index-job", { jobId: job.jobId }, { idempotencyKey: `wallet-job:${job.jobId}`, queue: { name: "wallet-indexing", concurrencyLimit: 2 } })
  return NextResponse.json({ ...job, backgroundProvider: "trigger.dev", triggerRunId: run.id }, { status: 201 })
}
