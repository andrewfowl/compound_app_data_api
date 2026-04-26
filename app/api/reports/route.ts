import { NextRequest, NextResponse } from "next/server"
import { listLatestReport } from "@/lib/jobs"

export async function GET(req: NextRequest) {
  const walletId = req.nextUrl.searchParams.get("walletId")
  const period = req.nextUrl.searchParams.get("period")

  if (!walletId || !period) {
    return NextResponse.json({ error: "walletId and period are required" }, { status: 400 })
  }

  const report = await listLatestReport(walletId, period)
  if (!report) {
    return NextResponse.json({ error: "Report not found" }, { status: 404 })
  }

  return NextResponse.json(report.payload_json)
}
