import { NextRequest, NextResponse } from "next/server"
import {
  getLatestReportByAddressAndPeriod,
  getWalletCatalogForAddress,
  listLatestReportsForAddress,
} from "@/lib/report-catalog"

function isMonth(value: string) {
  return /^\d{4}-\d{2}$/.test(value)
}

export async function GET(req: NextRequest) {
  try {
    const userId = req.headers.get("x-user-id")

    if (!userId) {
      return NextResponse.json(
        { error: "Missing x-user-id header" },
        { status: 401 }
      )
    }

    const address = String(req.nextUrl.searchParams.get("address") || "").trim()
    const period = String(req.nextUrl.searchParams.get("period") || "").trim()

    if (!address) {
      return NextResponse.json(
        { error: "address is required" },
        { status: 400 }
      )
    }

    if (period && !isMonth(period)) {
      return NextResponse.json(
        { error: "period must be YYYY-MM" },
        { status: 400 }
      )
    }

    const wallet = await getWalletCatalogForAddress(userId, address)

    if (!wallet) {
      return NextResponse.json(
        { error: "Wallet not found" },
        { status: 404 }
      )
    }

    if (period) {
      const report = await getLatestReportByAddressAndPeriod(
        userId,
        address,
        period
      )

      if (!report) {
        return NextResponse.json(
          {
            wallet,
            error: "Report not found for requested period",
          },
          { status: 404 }
        )
      }

      return NextResponse.json({
        wallet,
        report,
        payload_json: report.payloadJson,
      })
    }

    const reports = await listLatestReportsForAddress(userId, address)

    return NextResponse.json({
      wallet,
      reports,
      count: reports.length,
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to fetch wallet reports"

    return NextResponse.json(
      { error: message },
      { status: 500 }
    )
  }
}