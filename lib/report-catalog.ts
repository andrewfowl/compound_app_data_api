import { db } from "@/lib/db"

function normalizeAddress(address: string) {
  return address.trim().toLowerCase()
}

export type WalletCatalogItem = {
  walletId: string
  address: string
  walletStartDate: string
  createdAt: string
  availablePeriods: string[]
  latestReportAt: string | null
}

export type StoredReportItem = {
  reportId: string
  walletId: string
  walletJobId: string
  address: string
  period: string
  reportType: string
  version: number
  createdAt: string
  payloadJson: unknown
}

type WalletCatalogRow = {
  walletId: string
  address: string
  walletStartDate: string
  createdAt: string
  availablePeriods: string[] | null
  latestReportAt: string | null
}

type StoredReportRow = {
  reportId: string
  walletId: string
  walletJobId: string
  address: string
  period: string
  reportType: string
  version: number
  createdAt: string
  payloadJson: unknown
}

function mapWalletCatalogRow(row: WalletCatalogRow): WalletCatalogItem {
  const availablePeriods = (row.availablePeriods ?? [])
    .filter(Boolean)
    .sort((a, b) => b.localeCompare(a))

  return {
    walletId: row.walletId,
    address: row.address,
    walletStartDate: row.walletStartDate,
    createdAt: row.createdAt,
    availablePeriods,
    latestReportAt: row.latestReportAt,
  }
}

function mapStoredReportRow(row: StoredReportRow): StoredReportItem {
  return {
    reportId: row.reportId,
    walletId: row.walletId,
    walletJobId: row.walletJobId,
    address: row.address,
    period: row.period,
    reportType: row.reportType,
    version: row.version,
    createdAt: row.createdAt,
    payloadJson: row.payloadJson,
  }
}

export async function listWalletCatalogForUser(
  userId: string
): Promise<WalletCatalogItem[]> {
  const result = await db.query<WalletCatalogRow>(
    `
    select
      w.id as "walletId",
      w.address,
      w.wallet_start_date::text as "walletStartDate",
      w.created_at::text as "createdAt",
      array_remove(array_agg(distinct wr.period_label), null) as "availablePeriods",
      max(wr.created_at)::text as "latestReportAt"
    from wallets w
    left join wallet_reports wr
      on wr.wallet_id = w.id
     and wr.report_type = 'monthly_reconciliation'
    where w.user_id = $1
    group by w.id, w.address, w.wallet_start_date, w.created_at
    order by max(wr.created_at) desc nulls last, w.created_at desc
    `,
    [userId]
  )

  return result.rows.map(mapWalletCatalogRow)
}

export async function getWalletCatalogForAddress(
  userId: string,
  address: string
): Promise<WalletCatalogItem | null> {
  const result = await db.query<WalletCatalogRow>(
    `
    select
      w.id as "walletId",
      w.address,
      w.wallet_start_date::text as "walletStartDate",
      w.created_at::text as "createdAt",
      array_remove(array_agg(distinct wr.period_label), null) as "availablePeriods",
      max(wr.created_at)::text as "latestReportAt"
    from wallets w
    left join wallet_reports wr
      on wr.wallet_id = w.id
     and wr.report_type = 'monthly_reconciliation'
    where w.user_id = $1
      and lower(w.address) = lower($2)
    group by w.id, w.address, w.wallet_start_date, w.created_at
    limit 1
    `,
    [userId, normalizeAddress(address)]
  )

  const row = result.rows[0]
  return row ? mapWalletCatalogRow(row) : null
}

export async function getLatestReportByAddressAndPeriod(
  userId: string,
  address: string,
  period: string
): Promise<StoredReportItem | null> {
  const result = await db.query<StoredReportRow>(
    `
    select
      wr.id::text as "reportId",
      wr.wallet_id as "walletId",
      wr.wallet_job_id as "walletJobId",
      w.address,
      wr.period_label as "period",
      wr.report_type as "reportType",
      wr.version,
      wr.created_at::text as "createdAt",
      wr.payload_json as "payloadJson"
    from wallet_reports wr
    join wallets w
      on w.id = wr.wallet_id
    where w.user_id = $1
      and lower(w.address) = lower($2)
      and wr.period_label = $3
      and wr.report_type = 'monthly_reconciliation'
    order by wr.created_at desc
    limit 1
    `,
    [userId, normalizeAddress(address), period]
  )

  const row = result.rows[0]
  return row ? mapStoredReportRow(row) : null
}

export async function listLatestReportsForAddress(
  userId: string,
  address: string
): Promise<StoredReportItem[]> {
  const result = await db.query<StoredReportRow>(
    `
    select distinct on (wr.period_label)
      wr.id::text as "reportId",
      wr.wallet_id as "walletId",
      wr.wallet_job_id as "walletJobId",
      w.address,
      wr.period_label as "period",
      wr.report_type as "reportType",
      wr.version,
      wr.created_at::text as "createdAt",
      wr.payload_json as "payloadJson"
    from wallet_reports wr
    join wallets w
      on w.id = wr.wallet_id
    where w.user_id = $1
      and lower(w.address) = lower($2)
      and wr.report_type = 'monthly_reconciliation'
    order by wr.period_label desc, wr.created_at desc
    `,
    [userId, normalizeAddress(address)]
  )

  return result.rows.map(mapStoredReportRow)
}