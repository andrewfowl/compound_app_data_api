import { NextRequest, NextResponse } from "next/server";
import { tasks } from "@trigger.dev/sdk";
import { createWalletAndJob } from "@/lib/jobs";
import { walletIndexJob } from "@/trigger/wallet-index-job";

export async function POST(req: NextRequest) {
  const body = await req.json();
  const userId = req.headers.get("x-user-id");

  if (!userId) {
    return NextResponse.json(
      { error: "Missing x-user-id header" },
      { status: 401 },
    );
  }

  const walletAddress = String(body.walletAddress || "").trim();
  function normalizeDateInput(value: unknown) {
    const raw = String(value ?? "").trim();
    return /^\d{4}-\d{2}$/.test(raw) ? `${raw}-01` : raw;
  }

  const walletStartDate = normalizeDateInput(body.walletStartDate);
  const reportStartDate = normalizeDateInput(
    body.reportStartDate || walletStartDate,
  );
  const reportEndMonth = String(body.reportEndMonth ?? "").trim();

  const frequency = String(body.frequency || "monthly") as
    | "monthly"
    | "quarterly"
    | "adhoc";
  const protocolScope =
    Array.isArray(body.protocolScope) && body.protocolScope.length > 0
      ? body.protocolScope
      : ["v2", "v3"];
  const priceSourceMode = String(
    body.priceSourceMode || "uploaded_or_fallback",
  );

  if (!walletAddress || !walletStartDate || !reportEndMonth) {
    return NextResponse.json(
      {
        error:
          "walletAddress, walletStartDate, and reportEndMonth are required",
      },
      { status: 400 },
    );
  }

  const job = await createWalletAndJob({
    userId,
    address: walletAddress,
    walletStartDate,
    reportStartDate,
    reportEndMonth,
    frequency,
    protocolScope,
    priceSourceMode,
  });

  const run = await tasks.trigger<typeof walletIndexJob>(
    "wallet-index-job",
    { jobId: job.jobId },
    {
      idempotencyKey: `wallet-job:${job.jobId}`,
      queue: "wallet-indexing",
      concurrencyKey: userId,
    },
  );

  return NextResponse.json(
    {
      ...job,
      backgroundProvider: "trigger.dev",
      triggerRunId: run.id,
    },
    { status: 201 },
  );
}
