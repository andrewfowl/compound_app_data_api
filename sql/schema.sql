create extension if not exists pgcrypto;

create type wallet_job_status as enum (
  'queued',
  'running',
  'completed',
  'failed',
  'cancelled'
);

create table if not exists wallets (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  chain_id integer not null default 1,
  address text not null,
  wallet_start_date date not null,
  created_at timestamptz not null default now(),
  unique (user_id, chain_id, lower(address))
);

create table if not exists wallet_reporting_requests (
  id uuid primary key default gen_random_uuid(),
  wallet_id uuid not null references wallets(id) on delete cascade,
  report_start_date date not null,
  report_end_month text not null,
  frequency text not null check (frequency in ('monthly','quarterly','adhoc')),
  protocol_scope text[] not null default array['v2','v3'],
  price_source_mode text not null default 'uploaded_or_fallback',
  status text not null default 'queued',
  latest_job_id uuid,
  created_at timestamptz not null default now()
);

create table if not exists wallet_jobs (
  id uuid primary key default gen_random_uuid(),
  wallet_reporting_request_id uuid not null references wallet_reporting_requests(id) on delete cascade,
  status wallet_job_status not null default 'queued',
  progress_percent numeric(5,2) not null default 0,
  current_stage text,
  current_stage_detail text,
  periods_total integer not null default 0,
  periods_completed integer not null default 0,
  markets_total integer not null default 0,
  markets_completed integer not null default 0,
  started_at timestamptz,
  finished_at timestamptz,
  heartbeat_at timestamptz,
  error_code text,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists wallet_jobs_status_created_at_idx on wallet_jobs(status, created_at);

create table if not exists wallet_job_logs (
  id bigserial primary key,
  wallet_job_id uuid not null references wallet_jobs(id) on delete cascade,
  level text not null,
  message text not null,
  context_json jsonb,
  created_at timestamptz not null default now()
);

create table if not exists wallet_period_snapshots (
  id bigserial primary key,
  wallet_id uuid not null references wallets(id) on delete cascade,
  wallet_job_id uuid not null references wallet_jobs(id) on delete cascade,
  period_label text not null,
  protocol_version text not null,
  snapshot_side text not null check (snapshot_side in ('open','close')),
  payload_json jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists wallet_period_snapshots_lookup_idx
  on wallet_period_snapshots(wallet_id, period_label, protocol_version, snapshot_side);

create table if not exists wallet_normalized_events (
  id bigserial primary key,
  wallet_id uuid not null references wallets(id) on delete cascade,
  wallet_job_id uuid not null references wallet_jobs(id) on delete cascade,
  period_label text not null,
  protocol_version text not null,
  market_id text not null,
  market_symbol text,
  position_type text not null,
  activity_type text not null,
  source_action text,
  token_symbol text,
  token_address text,
  amount_token numeric,
  price_usd numeric,
  amount_usd numeric,
  tx_hash text,
  block_number bigint,
  block_timestamp timestamptz,
  synthetic_flag boolean not null default false,
  notes text,
  created_at timestamptz not null default now()
);

create index if not exists wallet_normalized_events_lookup_idx
  on wallet_normalized_events(wallet_id, period_label, protocol_version, market_id, position_type);

create table if not exists wallet_reconciliation_rows (
  id bigserial primary key,
  wallet_id uuid not null references wallets(id) on delete cascade,
  wallet_job_id uuid not null references wallet_jobs(id) on delete cascade,
  period_label text not null,
  protocol_version text not null,
  market_id text,
  market_symbol text,
  position_family text not null,
  token_symbol text,
  row_type text,
  tx_hash text,
  block_timestamp timestamptz,
  payload_json jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists wallet_reconciliation_rows_lookup_idx
  on wallet_reconciliation_rows(wallet_id, period_label, protocol_version, position_family);

create table if not exists wallet_reports (
  id bigserial primary key,
  wallet_id uuid not null references wallets(id) on delete cascade,
  wallet_job_id uuid not null references wallet_jobs(id) on delete cascade,
  period_label text not null,
  report_type text not null,
  payload_json jsonb not null,
  version integer not null default 1,
  created_at timestamptz not null default now()
);

create index if not exists wallet_reports_lookup_idx
  on wallet_reports(wallet_id, period_label, report_type, created_at desc);

create or replace function set_updated_at_wallet_jobs()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists wallet_jobs_set_updated_at on wallet_jobs;
create trigger wallet_jobs_set_updated_at
before update on wallet_jobs
for each row execute function set_updated_at_wallet_jobs();
