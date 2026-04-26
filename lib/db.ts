import { Pool, type QueryResultRow } from "pg"

if (!process.env.DATABASE_URL) {
  throw new Error("Missing DATABASE_URL")
}

export const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  ssl: process.env.DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false },
})

export async function maybeOne<T extends QueryResultRow>(text: string, params: unknown[] = []): Promise<T | null> {
  const result = await db.query<T>(text, params)
  return result.rows[0] ?? null
}

export async function one<T extends QueryResultRow>(text: string, params: unknown[] = []): Promise<T> {
  const result = await db.query<T>(text, params)
  const row = result.rows[0]
  if (!row) throw new Error("Expected one row but got none")
  return row
}
