import { db } from "@/lib/db"
import { appendJobLog, getWalletByRequest, setJobProgress, weightedProgress, type JobStage } from "@/lib/jobs"

export type ReportEngineOutput = {
  metadata: Record<string, unknown>
  periods: Array<{
    periodLabel: string
    monthStart: unknown
    monthEnd: unknown
    normalizedEvents: Array<Record<string, unknown>>
    reconciliationRows: Array<Record<string, unknown>>
    reconciliationSummary: Array<Record<string, unknown>>
  }>
  notes?: string[]
}

export type BuilderFn = (params: {
  walletAddress: string
  walletStartDate: string
  reportEndMonth: string
  onStage: (stage: JobStage, detail: string | null, completedUnits?: number, totalUnits?: number) => Promise<void>
}) => Promise<ReportEngineOutput>

export async function runWalletJob(jobId: string, builder: BuilderFn) {
  const request = await getWalletByRequest(jobId)
  const onStage = async (stage: JobStage, detail: string | null, completedUnits = 0, totalUnits = 1) => {
    const progressPercent = weightedProgress(stage, completedUnits, totalUnits)
    await setJobProgress({ jobId, status: stage === "completed" ? "completed" : stage === "failed" ? "failed" : "running", currentStage: stage, currentStageDetail: detail, progressPercent, periodsTotal: totalUnits, periodsCompleted: completedUnits, startedAt: true })
    await appendJobLog(jobId, "info", `${stage}${detail ? `: ${detail}` : ""}`, { stage, detail, completedUnits, totalUnits, progressPercent })
  }

  try {
    const report = await builder({ walletAddress: request.address, walletStartDate: request.wallet_start_date, reportEndMonth: request.report_end_month, onStage })
    await setJobProgress({ jobId, status: "running", currentStage: "persist_results", currentStageDetail: "Saving snapshots, events, and reports", progressPercent: weightedProgress("persist_results", 0, 1) })
    await persistReport(jobId, request.wallet_id, report)
    await setJobProgress({ jobId, status: "completed", currentStage: "completed", currentStageDetail: "Finished", progressPercent: 100, finishedAt: true })
    await appendJobLog(jobId, "info", "Job completed")
    return report
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    await appendJobLog(jobId, "error", message)
    await setJobProgress({ jobId, status: "failed", currentStage: "failed", currentStageDetail: message, errorCode: "JOB_FAILED", errorMessage: message, finishedAt: true })
    throw error
  }
}

async function persistReport(jobId: string, walletId: string, report: ReportEngineOutput) {
  const client = await db.connect()
  try {
    await client.query("begin")
    for (const period of report.periods) {
      await client.query(`insert into wallet_period_snapshots (wallet_id, wallet_job_id, period_label, protocol_version, snapshot_side, payload_json)
        values ($1,$2,$3,'unified','open',$4),($1,$2,$3,'unified','close',$5)`, [walletId, jobId, period.periodLabel, JSON.stringify(period.monthStart), JSON.stringify(period.monthEnd)])
      for (const event of period.normalizedEvents) {
        await client.query(`insert into wallet_normalized_events (
          wallet_id, wallet_job_id, period_label, protocol_version, market_id, market_symbol,
          position_type, activity_type, source_action, token_symbol, token_address,
          amount_token, price_usd, amount_usd, tx_hash, block_number, block_timestamp,
          synthetic_flag, notes
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, [
          walletId, jobId, period.periodLabel, event.protocolVersion ?? null, event.marketId ?? null, event.marketSymbol ?? null,
          event.positionType ?? null, event.activityType ?? null, event.sourceAction ?? null, event.tokenSymbol ?? null, event.tokenAddress ?? null,
          event.amount ?? null, event.priceUsd ?? null, event.amountUsd ?? null, event.txHash ?? null, event.blockNumber ?? null, event.blockTimestamp ?? null,
          Boolean(event.syntheticFlag ?? false), event.notes ?? null,
        ])
      }
      for (const row of period.reconciliationRows) {
        await client.query(`insert into wallet_reconciliation_rows (
          wallet_id, wallet_job_id, period_label, protocol_version, market_id, market_symbol,
          position_family, token_symbol, row_type, tx_hash, block_timestamp, payload_json
        ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, [walletId, jobId, period.periodLabel, row.protocolVersion ?? null, row.marketId ?? null, row.marketSymbol ?? null, row.positionFamily ?? null, row.tokenSymbol ?? null, row.syntheticType ?? null, row.txHash ?? null, row.blockTimestamp ?? null, JSON.stringify(row)])
      }
      await client.query(`insert into wallet_reports (wallet_id, wallet_job_id, period_label, report_type, payload_json) values ($1,$2,$3,'monthly_reconciliation',$4)`, [walletId, jobId, period.periodLabel, JSON.stringify({ metadata: report.metadata, notes: report.notes ?? [], period })])
    }
    await client.query(`update wallet_reporting_requests set status = 'completed' where latest_job_id = $1`, [jobId])
    await client.query("commit")
  } catch (error) {
    await client.query("rollback")
    throw error
  } finally {
    client.release()
  }
}
