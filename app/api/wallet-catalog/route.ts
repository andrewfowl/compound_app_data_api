import { NextRequest, NextResponse } from "next/server"
import {
  getWalletCatalogForAddress,
  listWalletCatalogForUser,
} from "@/lib/report-catalog"

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

    if (address) {
      const wallet = await getWalletCatalogForAddress(userId, address)

      if (!wallet) {
        return NextResponse.json(
          { error: "Wallet not found" },
          { status: 404 }
        )
      }

      return NextResponse.json(wallet)
    }

    const items = await listWalletCatalogForUser(userId)

    return NextResponse.json({
      items,
      count: items.length,
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Failed to fetch wallet catalog"

    return NextResponse.json(
      { error: message },
      { status: 500 }
    )
  }
}