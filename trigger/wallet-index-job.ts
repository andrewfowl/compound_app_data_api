import { task } from "@trigger.dev/sdk";
import { setJobProgress } from "@/lib/jobs";
import { runWalletJob } from "@/lib/report-engine";
import { buildCompoundReconciliationReport } from "@/lib/build-report";

export const walletIndexJob = task({
  id: "wallet-index-job",
  queue: {
    concurrencyLimit: 2,
  },
  maxDuration: 3600,
  run: async (payload: { jobId: string }) => {
    await setJobProgress({
      jobId: payload.jobId,
      status: "running",
      currentStage: "discover_periods",
      currentStageDetail: "Trigger.dev worker started",
      progressPercent: 1,
      startedAt: true,
    });

    await runWalletJob(payload.jobId, buildCompoundReconciliationReport);

    return { jobId: payload.jobId, ok: true };
  },
});