import { db, maybeOne, one } from "./db"

export type JobStage =
  | "queued"
  | "discover_periods"
  | "resolve_blocks"
  | "snapshots_start"
  | "snapshots_end"
  | "fetch_events_v2"
  | "fetch_events_v3"
  | "normalize_events"
  | "reconcile_periods"
  | "persist_results"
  | "completed"
  | "failed"

export const STAGE_WEIGHTS: Record<string, { start: number; end: number }> = {
  queued: { start: 0, end: 0 },
  discover_periods: { start: 1, end: 5 },
  resolve_blocks: { start: 5, end: 10 },
  snapshots_start: { start: 10, end: 20 },
  snapshots_end: { start: 20, end: 30 },
  fetch_events_v2: { start: 30, end: 55 },
  fetch_events_v3: { start: 55, end: 70 },
  normalize_events: { start: 70, end: 80 },
  reconcile_periods: { start: 80, end: 92 },
  persist_results: { start: 92, end: 99 },
  completed: { start: 100, end: 100 },
  failed: { start: 0, end: 0 },
}

export function weightedProgress(stage: keyof typeof STAGE_WEIGHTS, completedUnits = 0, totalUnits = 1): number {
  const weight = STAGE_WEIGHTS[stage]
  if (!weight) return 0
  if (stage === "completed") return 100
  if (totalUnits <= 0) return weight.start
  const stagePct = Math.max(0, Math.min(1, completedUnits / totalUnits))
  return Number((weight.start + (weight.end - weight.start) * stagePct).toFixed(2))
}

export async function createWalletAndJob(input: {
  userId: string
  chainId?: number
  address: string
  walletStartDate: string
  reportStartDate: string
  reportEndMonth: string
  frequency: "monthly" | "quarterly" | "adhoc"
  protocolScope: string[]
  priceSourceMode: string
}) {
  const client = await db.connect()
  try {
    await client.query("begin")

    const wallet = await client.query(
      `insert into wallets (user_id, chain_id, address, wallet_start_date)
       values ($1, $2, $3, $4)
       on conflict (user_id, chain_id, lower(address)) do update
       set wallet_start_date = least(wallets.wallet_start_date, excluded.wallet_start_date)
       returning id`,
      [input.userId, input.chainId ?? 1, input.address, input.walletStartDate],
    )

    const walletId = wallet.rows[0].id as string

    const request = await client.query(
      `insert into wallet_reporting_requests
         (wallet_id, report_start_date, report_end_month, frequency, protocol_scope, price_source_mode, status)
       values ($1, $2, $3, $4, $5, $6, 'queued')
       returning id`,
      [walletId, input.reportStartDate, input.reportEndMonth, input.frequency, input.protocolScope, input.priceSourceMode],
    )

    const requestId = request.rows[0].id as string

    const job = await client.query(
      `insert into wallet_jobs
         (wallet_reporting_request_id, status, progress_percent, current_stage, current_stage_detail)
       values ($1, 'queued', 0, 'queued', 'Waiting for worker')
       returning id, status, progress_percent, current_stage, current_stage_detail, created_at`,
      [requestId],
    )

    const jobRow = job.rows[0]

    await client.query(
      `update wallet_reporting_requests
       set latest_job_id = $1, status = 'queued'
       where id = $2`,
      [jobRow.id, requestId],
    )

    await client.query("commit")

    return {
      walletId,
      requestId,
      jobId: jobRow.id as string,
      status: jobRow.status as string,
      progressPercent: Number(jobRow.progress_percent),
      currentStage: jobRow.current_stage as string,
      currentStageDetail: jobRow.current_stage_detail as string,
      createdAt: jobRow.created_at as string,
    }
  } catch (error) {
    await client.query("rollback")
    throw error
  } finally {
    client.release()
  }
}

export async function getJob(jobId: string) {
  return maybeOne<{
    id: string
    status: string
    progress_percent: string
    current_stage: string | null
    current_stage_detail: string | null
    periods_total: number
    periods_completed: number
    markets_total: number
    markets_completed: number
    started_at: string | null
    finished_at: string | null
    heartbeat_at: string | null
    error_code: string | null
    error_message: string | null
    created_at: string
    updated_at: string
  }>(
    `select id, status, progress_percent, current_stage, current_stage_detail,
            periods_total, periods_completed, markets_total, markets_completed,
            started_at, finished_at, heartbeat_at, error_code, error_message,
            created_at, updated_at
     from wallet_jobs
     where id = $1`,
    [jobId],
  )
}

export async function appendJobLog(jobId: string, level: string, message: string, contextJson?: unknown) {
  await db.query(
    `insert into wallet_job_logs (wallet_job_id, level, message, context_json)
     values ($1, $2, $3, $4)`,
    [jobId, level, message, contextJson ? JSON.stringify(contextJson) : null],
  )
}

export async function setJobProgress(input: {
  jobId: string
  status?: string
  currentStage?: string
  currentStageDetail?: string | null
  progressPercent?: number
  periodsTotal?: number
  periodsCompleted?: number
  marketsTotal?: number
  marketsCompleted?: number
  startedAt?: boolean
  finishedAt?: boolean
  errorCode?: string | null
  errorMessage?: string | null
}) {
  const fields: string[] = ["heartbeat_at = now()"]
  const values: unknown[] = []

  const push = (sql: string, value: unknown) => {
    values.push(value)
    fields.push(`${sql} = $${values.length}`)
  }

  if (input.status) push("status", input.status)
  if (input.currentStage !== undefined) push("current_stage", input.currentStage)
  if (input.currentStageDetail !== undefined) push("current_stage_detail", input.currentStageDetail)
  if (input.progressPercent !== undefined) push("progress_percent", input.progressPercent)
  if (input.periodsTotal !== undefined) push("periods_total", input.periodsTotal)
  if (input.periodsCompleted !== undefined) push("periods_completed", input.periodsCompleted)
  if (input.marketsTotal !== undefined) push("markets_total", input.marketsTotal)
  if (input.marketsCompleted !== undefined) push("markets_completed", input.marketsCompleted)
  if (input.startedAt) fields.push("started_at = coalesce(started_at, now())")
  if (input.finishedAt) fields.push("finished_at = now()")
  if (input.errorCode !== undefined) push("error_code", input.errorCode)
  if (input.errorMessage !== undefined) push("error_message", input.errorMessage)

  values.push(input.jobId)
  await db.query(`update wallet_jobs set ${fields.join(", ")} where id = $${values.length}`, values)
}

export async function listLatestReport(walletId: string, periodLabel: string) {
  return maybeOne<{ payload_json: unknown }>(
    `select payload_json
     from wallet_reports
     where wallet_id = $1 and period_label = $2 and report_type = 'monthly_reconciliation'
     order by created_at desc
     limit 1`,
    [walletId, periodLabel],
  )
}

export async function getWalletByRequest(jobId: string) {
  return one<{
    wallet_id: string
    address: string
    wallet_start_date: string
    report_start_date: string
    report_end_month: string
    frequency: string
    protocol_scope: string[]
    price_source_mode: string
  }>(
    `select w.id as wallet_id, w.address, w.wallet_start_date,
            r.report_start_date, r.report_end_month, r.frequency, r.protocol_scope, r.price_source_mode
     from wallet_jobs j
     join wallet_reporting_requests r on r.id = j.wallet_reporting_request_id
     join wallets w on w.id = r.wallet_id
     where j.id = $1`,
    [jobId],
  )
}
