# Compound indexing backend API

Standalone Next.js API app with:
- Trigger.dev background jobs
- Postgres persistence
- Chainstack + optional Alchemy price fallback
- Compound v2 and v3 monthly indexing endpoints for a frontend app

## Frontend-facing endpoints

- `POST /api/wallet-jobs`
- `GET /api/wallet-jobs/[jobId]`
- `GET /api/wallet-jobs/[jobId]/stream`
- `GET /api/reports?walletId=<walletId>&period=<YYYY-MM>`
- `GET /api/health`

## Notes

- `COMPOUND_V2_MARKETS_FILE` is optional. If absent, the backend discovers Compound v2 markets onchain from Comptroller.
- `COMPOUND_V3_MARKETS_FILE` is bundled and recommended in production.
- The worker builder is based on the latest user-supplied monthly builder script.
- The backend is deployment-agnostic and can run on Railway or Vercel. Trigger.dev Cloud is used for background execution.

## Local setup

1. Copy `.env.example` to `.env.local`
2. Create the database schema:

```bash
npm run db:init
```

3. Start Trigger.dev dev:

```bash
npm run trigger:dev
```

4. Start Next.js:

```bash
npm run dev
```
