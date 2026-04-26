import fs from "node:fs"
import path from "node:path"
import { Interface, JsonRpcProvider, formatUnits, getAddress, id } from "ethers"
import type { ReportEngineOutput } from "./report-engine"
import type { JobStage } from "./jobs"

type StageCallback = (
  stage: JobStage,
  detail: string | null,
  completedUnits?: number,
  totalUnits?: number,
) => Promise<void>

const CHAINSTACK_RPC_URL = process.env.CHAINSTACK_RPC_URL
const ALCHEMY_PRICES_API_KEY = process.env.ALCHEMY_PRICES_API_KEY || ""
const COMPOUND_V2_COMPTROLLER_ADDRESS =
  process.env.COMPOUND_V2_COMPTROLLER_ADDRESS || "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B"
const V2_MARKETS_FILE =
  process.env.COMPOUND_V2_MARKETS_FILE || path.resolve("./data/compound-v2-markets.ethereum.json")
const V3_MARKETS_FILE =
  process.env.COMPOUND_V3_MARKETS_FILE || path.resolve("./data/compound-v3-markets.ethereum.json")
const REPORT_PRICES_FILE = process.env.REPORT_PRICES_FILE || ""

const LOG_PAGE_SIZE = Number(process.env.CHAINSTACK_LOG_PAGE_SIZE || 200)
const MAX_RPC_CONCURRENCY = Number(process.env.REPORT_RPC_CONCURRENCY || 8)
const MAX_PRICE_CONCURRENCY = Number(process.env.REPORT_PRICE_CONCURRENCY || 3)

if (!CHAINSTACK_RPC_URL) throw new Error("Missing CHAINSTACK_RPC_URL")

const comptrollerIface = new Interface([
  "function getAssetsIn(address account) view returns (address[])",
  "function markets(address cTokenAddress) view returns (bool,uint256,bool)",
  "function getAccountLiquidity(address account) view returns (uint256,uint256,uint256)",
  "function oracle() view returns (address)",
  "function getAllMarkets() view returns (address[])"
])

const cTokenIface = new Interface([
  "function symbol() view returns (string)",
  "function underlying() view returns (address)",
  "function balanceOf(address owner) view returns (uint256)",
  "function exchangeRateStored() view returns (uint256)",
  "function borrowBalanceStored(address account) view returns (uint256)",
  "function decimals() view returns (uint8)"
])

const erc20Iface = new Interface([
  "function decimals() view returns (uint8)",
  "function symbol() view returns (string)"
])

const v2OracleIface = new Interface([
  "function getUnderlyingPrice(address cToken) external view returns (uint256)"
])

const cTokenLiquidationAuxIface = new Interface([
  "event Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens)"
])

const v2EventIface = new Interface([
  "event Mint(address minter, uint256 mintAmount, uint256 mintTokens)",
  "event Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens)",
  "event Borrow(address borrower, uint256 borrowAmount, uint256 accountBorrows, uint256 totalBorrows)",
  "event RepayBorrow(address payer, address borrower, uint256 repayAmount, uint256 accountBorrows, uint256 totalBorrows)",
  "event LiquidateBorrow(address liquidator, address borrower, uint256 repayAmount, address cTokenCollateral, uint256 seizeTokens)"
])

const V2_EVENT_TOPICS = [
  id("Mint(address,uint256,uint256)"),
  id("Redeem(address,uint256,uint256)"),
  id("Borrow(address,uint256,uint256,uint256)"),
  id("RepayBorrow(address,address,uint256,uint256,uint256)"),
  id("LiquidateBorrow(address,address,uint256,address,uint256)")
]

const cometIface = new Interface([
  "function baseToken() view returns (address)",
  "function baseTokenPriceFeed() view returns (address)",
  "function balanceOf(address account) view returns (uint256)",
  "function borrowBalanceOf(address account) view returns (uint256)",
  "function collateralBalanceOf(address account, address asset) view returns (uint128)",
  "function isBorrowCollateralized(address account) view returns (bool)",
  "function isLiquidatable(address account) view returns (bool)",
  "function getPrice(address priceFeed) view returns (uint128)",
  "function getAssetInfo(uint8 i) view returns ((uint8 offset,address asset,address priceFeed,uint64 scale,uint64 borrowCollateralFactor,uint64 liquidateCollateralFactor,uint64 liquidationFactor,uint128 supplyCap))",
  "function numAssets() view returns (uint8)",
  "function factorScale() view returns (uint64)",
  "function priceScale() view returns (uint64)"
])

const v3EventIface = new Interface([
  "event Supply(address indexed from, address indexed dst, uint amount)",
  "event Withdraw(address indexed src, address indexed to, uint amount)",
  "event SupplyCollateral(address indexed from, address indexed dst, address indexed asset, uint amount)",
  "event WithdrawCollateral(address indexed src, address indexed to, address indexed asset, uint amount)",
  "event AbsorbDebt(address indexed absorber, address indexed borrower, uint basePaidOut, uint usdValue)",
  "event AbsorbCollateral(address indexed absorber, address indexed borrower, address indexed asset, uint collateralAbsorbed, uint usdValue)"
])

const V3_EVENT_TOPICS = [
  id("Supply(address,address,uint256)"),
  id("Withdraw(address,address,uint256)"),
  id("SupplyCollateral(address,address,address,uint256)"),
  id("WithdrawCollateral(address,address,address,uint256)"),
  id("AbsorbDebt(address,address,uint256,uint256)"),
  id("AbsorbCollateral(address,address,address,uint256,uint256)")
]

const STABLE_SYMBOLS = new Set(["USDC", "USDT", "DAI", "TUSD", "USDP", "USDS", "SAI"])

function createLimiter(maxConcurrent = 8) {
  let active = 0
  const queue: Array<{ fn: () => Promise<unknown> | unknown; resolve: (value: unknown) => void; reject: (reason: unknown) => void }> = []

  const runNext = () => {
    if (active >= maxConcurrent || queue.length === 0) return
    const { fn, resolve, reject } = queue.shift()!
    active += 1
    Promise.resolve()
      .then(fn)
      .then(resolve, reject)
      .finally(() => {
        active -= 1
        runNext()
      })
  }

  return function limit<T>(fn: () => Promise<T> | T): Promise<T> {
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve: resolve as (value: unknown) => void, reject })
      runNext()
    })
  }
}

const rpcLimit = createLimiter(MAX_RPC_CONCURRENCY)
const priceLimit = createLimiter(MAX_PRICE_CONCURRENCY)

function round(value: number | null | undefined, digits = 8) {
  if (value == null || !Number.isFinite(value)) return 0
  return Number(value.toFixed(digits))
}

function toNumber(value: bigint | string | number, decimals: number) {
  return Number(formatUnits(value, decimals))
}

function formatBigPrice(value: bigint, decimals: number) {
  return Number(formatUnits(value, decimals))
}

function parseWalletStartDate(value: string) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw new Error("walletStartDate must be YYYY-MM-DD")
  const [y, m, d] = value.split("-").map(Number)
  return new Date(Date.UTC(y, m - 1, d, 0, 0, 0))
}

function parseMonth(value: string) {
  if (!/^\d{4}-\d{2}$/.test(value)) throw new Error("reportEndMonth must be YYYY-MM")
  const [y, m] = value.split("-").map(Number)
  return new Date(Date.UTC(y, m - 1, 1, 0, 0, 0))
}

function monthLabel(date: Date) {
  return date.toISOString().slice(0, 7)
}

function endOfMonth(date: Date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1, 0, 0, 0))
}

function buildMonthlyPeriods(walletStartDate: Date, reportEndMonth: string) {
  const periods: Array<{ label: string; start: Date; endExclusive: Date }> = []
  let cursor = new Date(Date.UTC(walletStartDate.getUTCFullYear(), walletStartDate.getUTCMonth(), 1, 0, 0, 0))
  const endMonthStart = parseMonth(reportEndMonth)
  const walletStartMonth = new Date(Date.UTC(walletStartDate.getUTCFullYear(), walletStartDate.getUTCMonth(), 1, 0, 0, 0))

  while (cursor <= endMonthStart) {
    periods.push({
      label: monthLabel(cursor),
      start: cursor.getTime() === walletStartMonth.getTime() ? new Date(walletStartDate) : new Date(cursor),
      endExclusive: endOfMonth(cursor)
    })
    cursor = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 1, 0, 0, 0))
  }

  return periods
}

function loadJsonArray(filePath: string, required = false) {
  if (!fs.existsSync(filePath)) {
    if (required) throw new Error(`Missing file: ${filePath}`)
    return []
  }
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"))
  if (!Array.isArray(raw)) {
    if (required) throw new Error(`Invalid array JSON file: ${filePath}`)
    return []
  }
  return raw
}

async function rpcRead(provider: JsonRpcProvider, to: string, iface: Interface, method: string, args: unknown[] = [], blockTag: number | string = "latest") {
  const data = iface.encodeFunctionData(method, args)
  const raw = await rpcLimit(() => provider.call({ to, data, blockTag }))
  const decoded = iface.decodeFunctionResult(method, raw)
  return decoded.length === 1 ? decoded[0] : decoded
}

async function safeRpcRead(provider: JsonRpcProvider, to: string, iface: Interface, method: string, args: unknown[] = [], blockTag: number | string = "latest", fallback: unknown = null) {
  try {
    return await rpcRead(provider, to, iface, method, args, blockTag)
  } catch {
    return fallback
  }
}

function csvSplit(line: string) {
  const out: string[] = []
  let current = ""
  let inQuotes = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    const next = line[i + 1]
    if (ch === '"') {
      if (inQuotes && next === '"') {
        current += '"'
        i++
      } else {
        inQuotes = !inQuotes
      }
      continue
    }
    if (ch === "," && !inQuotes) {
      out.push(current.trim())
      current = ""
      continue
    }
    current += ch
  }
  out.push(current.trim())
  return out
}

function loadUploadedPrices(pathValue: string) {
  const rows: Array<{ date: string; token_address: string; token_symbol: string; price_usd: number }> = []
  if (!pathValue || !fs.existsSync(pathValue)) return rows
  const text = fs.readFileSync(pathValue, "utf8")
  if (!text.trim()) return rows
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0)
  if (lines.length < 2) return rows
  const header = csvSplit(lines[0]).map((v) => v.trim())

  for (let i = 1; i < lines.length; i++) {
    const values = csvSplit(lines[i])
    if (values.length !== header.length) continue
    const row = Object.fromEntries(header.map((col, idx) => [col, values[idx] ?? ""]))
    const date = String(row.date || "").trim()
    const tokenAddress = String(row.token_address || "").trim()
    const tokenSymbol = String(row.token_symbol || "").trim()
    const price = Number(String(row.price_usd || "").trim())
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue
    if (!Number.isFinite(price) || price < 0) continue
    if (!tokenAddress && !tokenSymbol) continue
    rows.push({ date, token_address: tokenAddress, token_symbol: tokenSymbol, price_usd: price })
  }
  return rows
}

const uploadedPrices = loadUploadedPrices(REPORT_PRICES_FILE)
const priceCache = new Map<string, number | null>()
const blockCache = new Map<string, Awaited<ReturnType<JsonRpcProvider["getBlock"]>>>()
const exchangeRateCache = new Map<string, bigint>()

function getUploadedPrice(tokenAddress: string | null | undefined, tokenSymbol: string | null | undefined, day: string) {
  const addr = (tokenAddress || "").toLowerCase()
  const sym = (tokenSymbol || "").toUpperCase()
  const byAddress = uploadedPrices.find((row) => row.date === day && row.token_address.toLowerCase() === addr && addr)
  if (byAddress) return byAddress.price_usd
  const bySymbol = uploadedPrices.find((row) => row.date === day && row.token_symbol.toUpperCase() === sym && sym)
  if (bySymbol) return bySymbol.price_usd
  return null
}

async function fetchAlchemyHistoricalPrice(tokenAddress: string | null | undefined, tokenSymbol: string | null | undefined, day: string) {
  const cacheKey = `${tokenAddress || tokenSymbol || "unknown"}|${day}`
  if (priceCache.has(cacheKey)) return priceCache.get(cacheKey) ?? null
  if (STABLE_SYMBOLS.has(String(tokenSymbol || "").toUpperCase())) {
    priceCache.set(cacheKey, 1)
    return 1
  }
  if (!ALCHEMY_PRICES_API_KEY) {
    priceCache.set(cacheKey, null)
    return null
  }
  const body = tokenAddress
    ? { network: "eth-mainnet", address: tokenAddress, startTime: `${day}T00:00:00Z`, endTime: `${day}T23:59:59Z`, interval: "1d", withMarketData: true }
    : { symbol: tokenSymbol, startTime: `${day}T00:00:00Z`, endTime: `${day}T23:59:59Z`, interval: "1d", withMarketData: true }
  try {
    const res = await priceLimit(() => fetch(`https://api.g.alchemy.com/prices/v1/${ALCHEMY_PRICES_API_KEY}/tokens/historical`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }))
    if (!res.ok) {
      priceCache.set(cacheKey, null)
      return null
    }
    const json = await res.json() as any
    const candidates = [json?.data?.[0]?.value, json?.data?.[0]?.price, json?.prices?.[0]?.value, json?.prices?.[0]?.price, json?.result?.data?.[0]?.value].filter((v) => v != null)
    const resolved = candidates.length > 0 ? Number(candidates[0]) : null
    priceCache.set(cacheKey, resolved)
    return resolved
  } catch {
    priceCache.set(cacheKey, null)
    return null
  }
}

async function resolvePriceFields(tokenAddress: string | null | undefined, tokenSymbol: string | null | undefined, day: string) {
  const uploaded = getUploadedPrice(tokenAddress, tokenSymbol, day)
  const alchemy = uploaded == null ? await fetchAlchemyHistoricalPrice(tokenAddress, tokenSymbol, day) : null
  return {
    priceUsd: uploaded ?? alchemy ?? null,
    priceSource: uploaded != null ? "uploaded" : alchemy != null ? "alchemy" : "unresolved"
  }
}

async function getBlockCached(provider: JsonRpcProvider, blockNumberOrTag: number | string) {
  const key = String(blockNumberOrTag)
  if (blockCache.has(key)) return blockCache.get(key)!
  const block = await rpcLimit(() => provider.getBlock(blockNumberOrTag))
  blockCache.set(key, block)
  return block
}

async function findBlockByTimestamp(provider: JsonRpcProvider, timestampSec: number) {
  const latest = await getBlockCached(provider, "latest")
  if (!latest) throw new Error("Failed to fetch latest block")
  let low = 0
  let high = latest.number
  let answer = latest.number
  while (low <= high) {
    const mid = Math.floor((low + high) / 2)
    const block = await getBlockCached(provider, mid)
    if (!block) {
      high = mid - 1
      continue
    }
    if (block.timestamp >= timestampSec) {
      answer = mid
      high = mid - 1
    } else {
      low = mid + 1
    }
  }
  return answer
}

function isRangeLimitError(error: unknown) {
  const text = error instanceof Error ? error.message : String(error)
  return /range limit|10 block range|block range limit exceeded|expanded block range/i.test(text)
}

async function fetchLogsRangeAdaptive(provider: JsonRpcProvider, address: string, topics: string[], start: number, end: number): Promise<any[]> {
  try {
    return await rpcLimit(() => provider.getLogs({ address, fromBlock: start, toBlock: end, topics: [topics] }))
  } catch (error) {
    if (isRangeLimitError(error) && start < end) {
      const mid = Math.floor((start + end) / 2)
      const left = await fetchLogsRangeAdaptive(provider, address, topics, start, mid)
      const right = await fetchLogsRangeAdaptive(provider, address, topics, mid + 1, end)
      return [...left, ...right]
    }
    return []
  }
}

async function fetchLogsPaged(provider: JsonRpcProvider, address: string, topics: string[], fromBlock: number, toBlock: number) {
  const allLogs: any[] = []
  for (let start = fromBlock; start <= toBlock; start += LOG_PAGE_SIZE) {
    const end = Math.min(start + LOG_PAGE_SIZE - 1, toBlock)
    const logs = await fetchLogsRangeAdaptive(provider, address, topics, start, end)
    allLogs.push(...logs)
  }
  return allLogs
}

function parseAssetInfo(info: any) {
  return {
    offset: Number(info.offset ?? info[0]),
    asset: getAddress(String(info.asset ?? info[1])),
    priceFeed: getAddress(String(info.priceFeed ?? info[2])),
    scale: BigInt(info.scale ?? info[3]),
    borrowCollateralFactor: BigInt(info.borrowCollateralFactor ?? info[4]),
    liquidateCollateralFactor: BigInt(info.liquidateCollateralFactor ?? info[5]),
    liquidationFactor: BigInt(info.liquidationFactor ?? info[6]),
    supplyCap: BigInt(info.supplyCap ?? info[7])
  }
}

async function getExchangeRateStoredCached(provider: JsonRpcProvider, cTokenAddress: string, blockNumber: number) {
  const key = `${cTokenAddress.toLowerCase()}:${blockNumber}`
  if (exchangeRateCache.has(key)) return exchangeRateCache.get(key)!
  const value = await safeRpcRead(provider, cTokenAddress, cTokenIface, "exchangeRateStored", [], blockNumber, 0n)
  const out = BigInt(value as bigint)
  exchangeRateCache.set(key, out)
  return out
}

function toDateString(timestampSec: number) {
  return new Date(timestampSec * 1000).toISOString().slice(0, 10)
}

async function discoverV2Markets(provider: JsonRpcProvider) {
  const fromFile = loadJsonArray(V2_MARKETS_FILE, false).map((row: any) => ({
    protocolVersion: "v2",
    marketId: String(row.marketId || row.cTokenSymbol),
    cTokenAddress: getAddress(String(row.cTokenAddress)),
    cTokenSymbol: String(row.cTokenSymbol),
    underlyingAddress: row.underlyingAddress ? getAddress(String(row.underlyingAddress)) : null,
    underlyingSymbol: String(row.underlyingSymbol),
    decimals: Number(row.decimals),
    cTokenDecimals: Number(row.cTokenDecimals || 8)
  }))
  if (fromFile.length > 0) return fromFile

  const cTokens = await rpcRead(provider, COMPOUND_V2_COMPTROLLER_ADDRESS, comptrollerIface, "getAllMarkets", [], "latest") as string[]
  const markets = [] as any[]
  for (const cToken of cTokens) {
    const cTokenAddress = getAddress(String(cToken))
    const [cTokenSymbol, underlyingMaybe, cTokenDecimals] = await Promise.all([
      safeRpcRead(provider, cTokenAddress, cTokenIface, "symbol", [], "latest", "cUNKNOWN"),
      safeRpcRead(provider, cTokenAddress, cTokenIface, "underlying", [], "latest", null),
      safeRpcRead(provider, cTokenAddress, cTokenIface, "decimals", [], "latest", 8)
    ])
    if (!underlyingMaybe) {
      markets.push({ protocolVersion: "v2", marketId: String(cTokenSymbol), cTokenAddress, cTokenSymbol: String(cTokenSymbol), underlyingAddress: null, underlyingSymbol: "ETH", decimals: 18, cTokenDecimals: Number(cTokenDecimals) })
      continue
    }
    const underlyingAddress = getAddress(String(underlyingMaybe))
    const [underlyingSymbol, decimals] = await Promise.all([
      safeRpcRead(provider, underlyingAddress, erc20Iface, "symbol", [], "latest", "UNKNOWN"),
      safeRpcRead(provider, underlyingAddress, erc20Iface, "decimals", [], "latest", 18)
    ])
    markets.push({ protocolVersion: "v2", marketId: String(cTokenSymbol), cTokenAddress, cTokenSymbol: String(cTokenSymbol), underlyingAddress, underlyingSymbol: String(underlyingSymbol), decimals: Number(decimals), cTokenDecimals: Number(cTokenDecimals) })
  }
  return markets
}

function loadV3Markets() {
  return loadJsonArray(V3_MARKETS_FILE, true).map((row: any) => ({
    protocolVersion: "v3",
    marketId: String(row.marketId),
    cometAddress: getAddress(String(row.cometAddress)),
    symbol: String(row.symbol),
    baseTokenAddress: getAddress(String(row.baseTokenAddress)),
    baseTokenSymbol: String(row.baseTokenSymbol)
  }))
}
async function inferV2LiquidationUnderlyingFromReceipt(
  provider: JsonRpcProvider,
  txHash: string,
  collateralCTokenAddress: string,
  liquidator: string,
  seizeTokensRaw: bigint,
  collateralDecimals: number,
  fallbackExchangeRateRaw: bigint | null = null
) {
  const receipt = await provider.getTransactionReceipt(txHash);
  let impliedUnderlyingRaw: bigint | null = null;

  if (!receipt) {
    if (fallbackExchangeRateRaw != null) {
      const fallbackRaw =
        (BigInt(seizeTokensRaw) * BigInt(fallbackExchangeRateRaw)) / 10n ** 18n;

      return {
        method: "exchange_rate_stored",
        underlyingAmount: round(toNumber(fallbackRaw, collateralDecimals), 10),
      };
    }

    return {
      method: "unresolved",
      underlyingAmount: 0,
    };
  }

  for (const log of receipt.logs) {
    if (log.address.toLowerCase() !== collateralCTokenAddress.toLowerCase()) continue;

    try {
      const parsed = cTokenLiquidationAuxIface.parseLog(log);
      if (!parsed || parsed.name !== "Redeem") continue;

      const redeemer = getAddress(String(parsed.args.redeemer));
      if (redeemer.toLowerCase() !== liquidator.toLowerCase()) continue;

      const redeemAmountRaw = BigInt(parsed.args.redeemAmount);
      const redeemTokensRaw = BigInt(parsed.args.redeemTokens);

      if (redeemTokensRaw === 0n) continue;

      impliedUnderlyingRaw =
        (BigInt(seizeTokensRaw) * redeemAmountRaw) / redeemTokensRaw;
      break;
    } catch {}
  }

  if (impliedUnderlyingRaw != null) {
    return {
      method: "same_tx_redeem",
      underlyingAmount: round(toNumber(impliedUnderlyingRaw, collateralDecimals), 10),
    };
  }

  if (fallbackExchangeRateRaw != null) {
    const fallbackRaw =
      (BigInt(seizeTokensRaw) * BigInt(fallbackExchangeRateRaw)) / 10n ** 18n;

    return {
      method: "exchange_rate_stored",
      underlyingAmount: round(toNumber(fallbackRaw, collateralDecimals), 10),
    };
  }

  return {
    method: "unresolved",
    underlyingAmount: 0,
  };
}

async function buildV2SnapshotAtBlock(provider: JsonRpcProvider, address: string, blockTag: number, markets: any[]) {
  const [assetsIn, accountLiquidityResult, oracleAddress] = await Promise.all([
    rpcRead(provider, COMPOUND_V2_COMPTROLLER_ADDRESS, comptrollerIface, "getAssetsIn", [address], blockTag),
    rpcRead(provider, COMPOUND_V2_COMPTROLLER_ADDRESS, comptrollerIface, "getAccountLiquidity", [address], blockTag),
    rpcRead(provider, COMPOUND_V2_COMPTROLLER_ADDRESS, comptrollerIface, "oracle", [], blockTag)
  ]) as [string[], [bigint, bigint, bigint], string]

  const enteredSet = new Set((assetsIn || []).map((x) => String(x).toLowerCase()))
  const [, liquidityRaw, shortfallRaw] = accountLiquidityResult
  const positions: any[] = []

  for (const market of markets) {
    const [cTokenBalanceRaw, exchangeRateRaw, borrowBalanceRaw, marketTuple, priceRaw] = await Promise.all([
      safeRpcRead(provider, market.cTokenAddress, cTokenIface, "balanceOf", [address], blockTag, 0n),
      safeRpcRead(provider, market.cTokenAddress, cTokenIface, "exchangeRateStored", [], blockTag, 0n),
      safeRpcRead(provider, market.cTokenAddress, cTokenIface, "borrowBalanceStored", [address], blockTag, 0n),
      safeRpcRead(provider, COMPOUND_V2_COMPTROLLER_ADDRESS, comptrollerIface, "markets", [market.cTokenAddress], blockTag, [false, 0n, false]),
      safeRpcRead(provider, oracleAddress, v2OracleIface, "getUnderlyingPrice", [market.cTokenAddress], blockTag, 0n)
    ])

    const supplyUnderlyingRaw = (BigInt(cTokenBalanceRaw as bigint) * BigInt(exchangeRateRaw as bigint)) / 10n ** 18n
    const supplyBalance = round(toNumber(supplyUnderlyingRaw, market.decimals), 10)
    const borrowBalance = round(toNumber(BigInt(borrowBalanceRaw as bigint), market.decimals), 10)
    const collateralFactor = Number(formatUnits(BigInt((marketTuple as any)?.[1] ?? 0n), 18))
    const enteredAsCollateral = enteredSet.has(market.cTokenAddress.toLowerCase())
    const priceUsd = round(formatBigPrice(BigInt(priceRaw as bigint), 36 - market.decimals), 10)
    const supplyUsd = round(supplyBalance * priceUsd, 8)
    const borrowUsd = round(borrowBalance * priceUsd, 8)

    if (supplyBalance !== 0 || borrowBalance !== 0 || enteredAsCollateral) {
      positions.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, enteredAsCollateral, collateralFactor: round(collateralFactor, 8), supplyBalance, supplyUsd, borrowBalance, borrowUsd, priceUsd })
    }
  }

  return {
    protocolVersion: "v2",
    blockTag,
    positions,
    summary: {
      totalCollateralUsd: round(positions.reduce((sum, x) => sum + x.supplyUsd, 0), 8),
      totalBorrowUsd: round(positions.reduce((sum, x) => sum + x.borrowUsd, 0), 8),
      totalBaseSupplyUsd: 0,
      additionalBorrowCapacityUsd: round(Number(formatUnits(BigInt(liquidityRaw), 18)), 8),
      liquidationBufferUsd: round(Number(formatUnits(BigInt(liquidityRaw), 18)), 8),
      shortfallUsd: round(Number(formatUnits(BigInt(shortfallRaw), 18)), 8),
      isLiquidatable: Number(shortfallRaw) > 0
    }
  }
}

async function buildV3SnapshotAtBlock(provider: JsonRpcProvider, address: string, blockTag: number, markets: any[]) {
  const marketsOut: any[] = []
  for (const market of markets) {
    const [baseTokenAddress, baseTokenPriceFeed, baseBalanceRaw, borrowBalanceRaw, isBorrowCollateralized, isLiquidatable, numAssets, factorScale, priceScale, baseDecimalsRaw] = await Promise.all([
      safeRpcRead(provider, market.cometAddress, cometIface, "baseToken", [], blockTag, market.baseTokenAddress),
      safeRpcRead(provider, market.cometAddress, cometIface, "baseTokenPriceFeed", [], blockTag, null),
      safeRpcRead(provider, market.cometAddress, cometIface, "balanceOf", [address], blockTag, 0n),
      safeRpcRead(provider, market.cometAddress, cometIface, "borrowBalanceOf", [address], blockTag, 0n),
      safeRpcRead(provider, market.cometAddress, cometIface, "isBorrowCollateralized", [address], blockTag, true),
      safeRpcRead(provider, market.cometAddress, cometIface, "isLiquidatable", [address], blockTag, false),
      safeRpcRead(provider, market.cometAddress, cometIface, "numAssets", [], blockTag, 0),
      safeRpcRead(provider, market.cometAddress, cometIface, "factorScale", [], blockTag, 1000000000000000000n),
      safeRpcRead(provider, market.cometAddress, cometIface, "priceScale", [], blockTag, 100000000n),
      safeRpcRead(provider, market.baseTokenAddress, erc20Iface, "decimals", [], blockTag, 6)
    ])

    const baseDecimals = Number(baseDecimalsRaw)
    const baseSupplyBalance = round(toNumber(BigInt(baseBalanceRaw as bigint), baseDecimals), 10)
    const borrowBalance = round(toNumber(BigInt(borrowBalanceRaw as bigint), baseDecimals), 10)
    let basePriceUsd = 0
    if (baseTokenPriceFeed) {
      const basePriceRaw = await safeRpcRead(provider, market.cometAddress, cometIface, "getPrice", [baseTokenPriceFeed], blockTag, 0n)
      basePriceUsd = round(formatBigPrice(BigInt(basePriceRaw as bigint), Number(String(priceScale).length - 1)), 10)
    }

    const collateralPositions: any[] = []
    let borrowCapacityUsd = 0
    let liquidationCapacityUsd = 0

    for (let i = 0; i < Number(numAssets); i++) {
      const infoRaw = await safeRpcRead(provider, market.cometAddress, cometIface, "getAssetInfo", [i], blockTag, null)
      if (!infoRaw) continue
      const info = parseAssetInfo(infoRaw)
      const assetDecimals = String(info.scale).length - 1
      const [balanceRaw, assetSymbol, priceRaw] = await Promise.all([
        safeRpcRead(provider, market.cometAddress, cometIface, "collateralBalanceOf", [address, info.asset], blockTag, 0n),
        safeRpcRead(provider, info.asset, erc20Iface, "symbol", [], blockTag, "UNKNOWN"),
        safeRpcRead(provider, market.cometAddress, cometIface, "getPrice", [info.priceFeed], blockTag, 0n)
      ])
      const collateralBalance = round(toNumber(BigInt(balanceRaw as bigint), assetDecimals), 10)
      const priceUsd = round(formatBigPrice(BigInt(priceRaw as bigint), Number(String(priceScale).length - 1)), 10)
      const collateralUsd = round(collateralBalance * priceUsd, 8)
      const borrowCollateralFactor = round(Number(formatUnits(info.borrowCollateralFactor, Number(String(factorScale).length - 1))), 8)
      const liquidateCollateralFactor = round(Number(formatUnits(info.liquidateCollateralFactor, Number(String(factorScale).length - 1))), 8)
      borrowCapacityUsd += collateralUsd * borrowCollateralFactor
      liquidationCapacityUsd += collateralUsd * liquidateCollateralFactor
      if (collateralBalance !== 0) {
        collateralPositions.push({ assetSymbol, assetAddress: info.asset, collateralBalance, collateralUsd, priceUsd, borrowCollateralFactor, liquidateCollateralFactor })
      }
    }

    const borrowUsd = round(borrowBalance * basePriceUsd, 8)
    const baseSupplyUsd = round(baseSupplyBalance * basePriceUsd, 8)
    const additionalBorrowCapacityUsd = round(Math.max(0, borrowCapacityUsd - borrowUsd), 8)
    const liquidationBufferUsd = round(Math.max(0, liquidationCapacityUsd - borrowUsd), 8)

    if (baseSupplyBalance !== 0 || borrowBalance !== 0 || collateralPositions.length > 0 || additionalBorrowCapacityUsd !== 0 || liquidationBufferUsd !== 0) {
      marketsOut.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, cometAddress: market.cometAddress, baseTokenSymbol: market.baseTokenSymbol, baseTokenAddress: getAddress(String(baseTokenAddress)), baseSupplyBalance, baseSupplyUsd, borrowBalance, borrowUsd, basePriceUsd, additionalBorrowCapacityUsd, liquidationBufferUsd, isBorrowCollateralized: Boolean(isBorrowCollateralized), isLiquidatable: Boolean(isLiquidatable), collateralPositions })
    }
  }

  return {
    protocolVersion: "v3",
    blockTag,
    markets: marketsOut,
    summary: {
      totalCollateralUsd: round(marketsOut.flatMap((m) => m.collateralPositions).reduce((sum, x) => sum + x.collateralUsd, 0), 8),
      totalBorrowUsd: round(marketsOut.reduce((sum, x) => sum + x.borrowUsd, 0), 8),
      totalBaseSupplyUsd: round(marketsOut.reduce((sum, x) => sum + x.baseSupplyUsd, 0), 8),
      additionalBorrowCapacityUsd: round(marketsOut.reduce((sum, x) => sum + x.additionalBorrowCapacityUsd, 0), 8),
      liquidationBufferUsd: round(marketsOut.reduce((sum, x) => sum + x.liquidationBufferUsd, 0), 8),
      anyLiquidatable: marketsOut.some((x) => x.isLiquidatable)
    }
  }
}

function buildUnifiedSummary(v2Snapshot: any, v3Snapshot: any) {
  return {
    totalCollateralUsd: round(v2Snapshot.summary.totalCollateralUsd + v3Snapshot.summary.totalCollateralUsd, 8),
    totalBorrowUsd: round(v2Snapshot.summary.totalBorrowUsd + v3Snapshot.summary.totalBorrowUsd, 8),
    totalBaseSupplyUsd: round(v2Snapshot.summary.totalBaseSupplyUsd + v3Snapshot.summary.totalBaseSupplyUsd, 8),
    additionalBorrowCapacityUsd: round(v2Snapshot.summary.additionalBorrowCapacityUsd + v3Snapshot.summary.additionalBorrowCapacityUsd, 8),
    liquidationBufferUsd: round(v2Snapshot.summary.liquidationBufferUsd + v3Snapshot.summary.liquidationBufferUsd, 8),
    anyLiquidatable: Boolean(v2Snapshot.summary.isLiquidatable || v3Snapshot.summary.anyLiquidatable)
  }
}

function normalizeAmountUsd(amount: number, priceUsd: number | null) {
  if (priceUsd == null) return null
  return round(amount * priceUsd, 8)
}

function v2RepayKey(txHash: string, marketId: string, amount: number) {
  return `${txHash}|${marketId}|${amount.toFixed(10)}`
}

async function collectV2NormalizedEvents(provider: JsonRpcProvider, walletLower: string, fromBlock: number, toBlock: number, markets: any[]) {
  const rawEvents: any[] = []
  const marketByCToken = new Map(markets.map((m) => [m.cTokenAddress.toLowerCase(), m]))
  for (const market of markets) {
    const logs = await fetchLogsPaged(provider, market.cTokenAddress, V2_EVENT_TOPICS, fromBlock, toBlock)
    for (const log of logs) {
      let parsed: any
      try {
        parsed = v2EventIface.parseLog(log)
      } catch {
        continue
      }
      let matches = false
      if (parsed.name === "Mint") matches = String(parsed.args.minter).toLowerCase() === walletLower
      if (parsed.name === "Redeem") matches = String(parsed.args.redeemer).toLowerCase() === walletLower
      if (parsed.name === "Borrow") matches = String(parsed.args.borrower).toLowerCase() === walletLower
      if (parsed.name === "RepayBorrow") matches = String(parsed.args.borrower).toLowerCase() === walletLower
      if (parsed.name === "LiquidateBorrow") matches = String(parsed.args.borrower).toLowerCase() === walletLower
      if (!matches) continue
      const block = await getBlockCached(provider, log.blockNumber)
      rawEvents.push({ protocolVersion: "v2", txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString(), day: toDateString(block!.timestamp), sourceAction: parsed.name, market, parsed })
    }
  }

  rawEvents.sort((a, b) => a.blockNumber - b.blockNumber || a.txHash.localeCompare(b.txHash))

  const actualRepays = new Set<string>()
  for (const e of rawEvents) {
    if (e.sourceAction === "RepayBorrow") {
      const amount = round(toNumber(BigInt(e.parsed.args.repayAmount), e.market.decimals), 10)
      actualRepays.add(v2RepayKey(e.txHash, e.market.marketId, amount))
    }
  }

  const out: any[] = []
  for (const e of rawEvents) {
    const market = e.market
    const price = await resolvePriceFields(market.underlyingAddress, market.underlyingSymbol, e.day)
    if (e.sourceAction === "Mint") {
      const amount = round(toNumber(BigInt(e.parsed.args.mintAmount), market.decimals), 10)
      out.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, positionType: "collateral", activityType: "deposit", sourceAction: "Mint", tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp })
      continue
    }
    if (e.sourceAction === "Redeem") {
      const amount = round(toNumber(BigInt(e.parsed.args.redeemAmount), market.decimals), 10)
      out.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, positionType: "collateral", activityType: "redemption", sourceAction: "Redeem", tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp })
      continue
    }
    if (e.sourceAction === "Borrow") {
      const amount = round(toNumber(BigInt(e.parsed.args.borrowAmount), market.decimals), 10)
      out.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, positionType: "debt", activityType: "borrowing", sourceAction: "Borrow", tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp })
      continue
    }
    if (e.sourceAction === "RepayBorrow") {
      const amount = round(toNumber(BigInt(e.parsed.args.repayAmount), market.decimals), 10)
      out.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, positionType: "debt", activityType: "repayment", sourceAction: "RepayBorrow", tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp })
      continue
    }
    if (e.sourceAction === "LiquidateBorrow") {
      const repayAmount = round(toNumber(BigInt(e.parsed.args.repayAmount), market.decimals), 10)
      if (!actualRepays.has(v2RepayKey(e.txHash, market.marketId, repayAmount))) {
        out.push({ protocolVersion: "v2", marketId: market.marketId, marketSymbol: market.cTokenSymbol, positionType: "debt", activityType: "repayment", sourceAction: "LiquidateBorrow", tokenSymbol: market.underlyingSymbol, tokenAddress: market.underlyingAddress, amount: repayAmount, amountUsd: normalizeAmountUsd(repayAmount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp, notes: "synthetic debt leg from LiquidateBorrow" })
      }
      const collateralCTokenAddress = getAddress(String(e.parsed.args.cTokenCollateral))
      const collateralMarket = marketByCToken.get(collateralCTokenAddress.toLowerCase())
      if (!collateralMarket) continue
      const liquidator = getAddress(String(e.parsed.args.liquidator))
      const seizeTokensRaw = BigInt(e.parsed.args.seizeTokens)
      const fallbackExchangeRate = await getExchangeRateStoredCached(provider, collateralMarket.cTokenAddress, e.blockNumber)
      const inferredCollateral = await inferV2LiquidationUnderlyingFromReceipt(provider, e.txHash, collateralMarket.cTokenAddress, liquidator, seizeTokensRaw, collateralMarket.decimals, fallbackExchangeRate)
      const collateralPrice = await resolvePriceFields(collateralMarket.underlyingAddress, collateralMarket.underlyingSymbol, e.day)
      out.push({ protocolVersion: "v2", marketId: collateralMarket.marketId, marketSymbol: collateralMarket.cTokenSymbol, positionType: "collateral", activityType: "liquidation", sourceAction: "LiquidateBorrow", tokenSymbol: collateralMarket.underlyingSymbol, tokenAddress: collateralMarket.underlyingAddress, amount: inferredCollateral.underlyingAmount, amountUsd: normalizeAmountUsd(inferredCollateral.underlyingAmount, collateralPrice.priceUsd), priceUsd: collateralPrice.priceUsd, priceSource: collateralPrice.priceSource, txHash: e.txHash, blockNumber: e.blockNumber, blockTimestamp: e.blockTimestamp, notes: `collateral leg valued via ${inferredCollateral.method}` })
    }
  }
  return out
}

async function collectV3NormalizedEvents(provider: JsonRpcProvider, walletLower: string, fromBlock: number, toBlock: number, markets: any[]) {
  const out: any[] = []
  for (const market of markets) {
    const logs = await fetchLogsPaged(provider, market.cometAddress, V3_EVENT_TOPICS, fromBlock, toBlock)
    const [baseDecimalsRaw, numAssets] = await Promise.all([
      safeRpcRead(provider, market.baseTokenAddress, erc20Iface, "decimals", [], "latest", 6),
      safeRpcRead(provider, market.cometAddress, cometIface, "numAssets", [], "latest", 0)
    ])
    const baseDecimals = Number(baseDecimalsRaw)
    const assetInfoByAddress = new Map<string, { address: string; symbol: string; decimals: number }>()
    for (let i = 0; i < Number(numAssets); i++) {
      const infoRaw = await safeRpcRead(provider, market.cometAddress, cometIface, "getAssetInfo", [i], "latest", null)
      if (!infoRaw) continue
      const info = parseAssetInfo(infoRaw)
      const symbol = await safeRpcRead(provider, info.asset, erc20Iface, "symbol", [], "latest", "UNKNOWN")
      assetInfoByAddress.set(info.asset.toLowerCase(), { address: info.asset, symbol: String(symbol), decimals: String(info.scale).length - 1 })
    }

    for (const log of logs) {
      let parsed: any
      try { parsed = v3EventIface.parseLog(log) } catch { continue }
      let include = false
      if (parsed.name === "Supply") include = String(parsed.args.dst).toLowerCase() === walletLower
      if (parsed.name === "Withdraw") include = String(parsed.args.src).toLowerCase() === walletLower
      if (parsed.name === "SupplyCollateral") include = String(parsed.args.dst).toLowerCase() === walletLower
      if (parsed.name === "WithdrawCollateral") include = String(parsed.args.src).toLowerCase() === walletLower
      if (parsed.name === "AbsorbDebt") include = String(parsed.args.borrower).toLowerCase() === walletLower
      if (parsed.name === "AbsorbCollateral") include = String(parsed.args.borrower).toLowerCase() === walletLower
      if (!include) continue
      const block = await getBlockCached(provider, log.blockNumber)
      const day = toDateString(block!.timestamp)
      if (parsed.name === "Supply") {
        const amount = round(toNumber(BigInt(parsed.args.amount), baseDecimals), 10)
        const price = await resolvePriceFields(market.baseTokenAddress, market.baseTokenSymbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "base", activityType: "base_in", sourceAction: "Supply", tokenSymbol: market.baseTokenSymbol, tokenAddress: market.baseTokenAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString(), notes: "v3 base_in can mean debt repayment or base supply" })
        continue
      }
      if (parsed.name === "Withdraw") {
        const amount = round(toNumber(BigInt(parsed.args.amount), baseDecimals), 10)
        const price = await resolvePriceFields(market.baseTokenAddress, market.baseTokenSymbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "base", activityType: "base_out", sourceAction: "Withdraw", tokenSymbol: market.baseTokenSymbol, tokenAddress: market.baseTokenAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString(), notes: "v3 base_out can mean borrowing or base withdrawal" })
        continue
      }
      if (parsed.name === "SupplyCollateral") {
        const assetAddress = getAddress(String(parsed.args.asset))
        const assetInfo = assetInfoByAddress.get(assetAddress.toLowerCase())
        if (!assetInfo) continue
        const amount = round(toNumber(BigInt(parsed.args.amount), assetInfo.decimals), 10)
        const price = await resolvePriceFields(assetInfo.address, assetInfo.symbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "collateral", activityType: "deposit", sourceAction: "SupplyCollateral", tokenSymbol: assetInfo.symbol, tokenAddress: assetInfo.address, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString() })
        continue
      }
      if (parsed.name === "WithdrawCollateral") {
        const assetAddress = getAddress(String(parsed.args.asset))
        const assetInfo = assetInfoByAddress.get(assetAddress.toLowerCase())
        if (!assetInfo) continue
        const amount = round(toNumber(BigInt(parsed.args.amount), assetInfo.decimals), 10)
        const price = await resolvePriceFields(assetInfo.address, assetInfo.symbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "collateral", activityType: "redemption", sourceAction: "WithdrawCollateral", tokenSymbol: assetInfo.symbol, tokenAddress: assetInfo.address, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString() })
        continue
      }
      if (parsed.name === "AbsorbDebt") {
        const amount = round(toNumber(BigInt(parsed.args.basePaidOut), baseDecimals), 10)
        const price = await resolvePriceFields(market.baseTokenAddress, market.baseTokenSymbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "debt", activityType: "liquidation", sourceAction: "AbsorbDebt", tokenSymbol: market.baseTokenSymbol, tokenAddress: market.baseTokenAddress, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString() })
        continue
      }
      if (parsed.name === "AbsorbCollateral") {
        const assetAddress = getAddress(String(parsed.args.asset))
        const assetInfo = assetInfoByAddress.get(assetAddress.toLowerCase())
        if (!assetInfo) continue
        const amount = round(toNumber(BigInt(parsed.args.collateralAbsorbed), assetInfo.decimals), 10)
        const price = await resolvePriceFields(assetInfo.address, assetInfo.symbol, day)
        out.push({ protocolVersion: "v3", marketId: market.marketId, marketSymbol: market.symbol, positionType: "collateral", activityType: "liquidation", sourceAction: "AbsorbCollateral", tokenSymbol: assetInfo.symbol, tokenAddress: assetInfo.address, amount, amountUsd: normalizeAmountUsd(amount, price.priceUsd), priceUsd: price.priceUsd, priceSource: price.priceSource, txHash: log.transactionHash, blockNumber: Number(log.blockNumber), blockTimestamp: new Date(block!.timestamp * 1000).toISOString() })
      }
    }
  }

  out.sort((a, b) => a.blockNumber - b.blockNumber || a.txHash.localeCompare(b.txHash) || a.positionType.localeCompare(b.positionType))
  return out
}

function summarizeV2Monthly(v2Start: any, v2End: any, normalizedEvents: any[]) {
  const groups = new Map<string, any>()
  const ensure = (marketId: string, marketSymbol: string, tokenSymbol: string) => {
    const key = `${marketId}|${tokenSymbol}`
    if (!groups.has(key)) groups.set(key, { protocolVersion: "v2", marketId, marketSymbol, tokenSymbol, beginningCollateral: 0, endingCollateral: 0, beginningBorrow: 0, endingBorrow: 0, deposits: 0, redemptions: 0, borrowings: 0, repayments: 0, liquidations: 0 })
    return groups.get(key)
  }
  for (const row of v2Start.positions) {
    const g = ensure(row.marketId, row.marketSymbol, row.tokenSymbol)
    g.beginningCollateral = row.supplyBalance
    g.beginningBorrow = row.borrowBalance
  }
  for (const row of v2End.positions) {
    const g = ensure(row.marketId, row.marketSymbol, row.tokenSymbol)
    g.endingCollateral = row.supplyBalance
    g.endingBorrow = row.borrowBalance
  }
  for (const e of normalizedEvents.filter((x) => x.protocolVersion === "v2")) {
    const g = ensure(e.marketId, e.marketSymbol, e.tokenSymbol)
    if (e.positionType === "collateral" && e.activityType === "deposit") g.deposits += e.amount
    if (e.positionType === "collateral" && e.activityType === "redemption") g.redemptions += e.amount
    if (e.positionType === "collateral" && e.activityType === "liquidation") g.liquidations += e.amount
    if (e.positionType === "debt" && e.activityType === "borrowing") g.borrowings += e.amount
    if (e.positionType === "debt" && e.activityType === "repayment") g.repayments += e.amount
  }
  return [...groups.values()].sort((a, b) => a.marketSymbol.localeCompare(b.marketSymbol) || a.tokenSymbol.localeCompare(b.tokenSymbol))
}

function summarizeV3Monthly(v3Start: any, v3End: any, normalizedEvents: any[]) {
  const groups = new Map<string, any>()
  const ensure = (marketId: string, marketSymbol: string, tokenSymbol: string) => {
    const key = `${marketId}|${tokenSymbol}`
    if (!groups.has(key)) groups.set(key, { protocolVersion: "v3", marketId, marketSymbol, tokenSymbol, beginningCollateral: 0, endingCollateral: 0, beginningBorrow: 0, endingBorrow: 0, beginningBaseSupply: 0, endingBaseSupply: 0, collateralDeposits: 0, collateralRedemptions: 0, collateralLiquidations: 0, baseIn: 0, baseOut: 0, debtLiquidations: 0 })
    return groups.get(key)
  }
  for (const market of v3Start.markets) {
    const baseGroup = ensure(market.marketId, market.marketSymbol, market.baseTokenSymbol)
    baseGroup.beginningBorrow = market.borrowBalance
    baseGroup.beginningBaseSupply = market.baseSupplyBalance
    for (const cp of market.collateralPositions) ensure(market.marketId, market.marketSymbol, cp.assetSymbol).beginningCollateral = cp.collateralBalance
  }
  for (const market of v3End.markets) {
    const baseGroup = ensure(market.marketId, market.marketSymbol, market.baseTokenSymbol)
    baseGroup.endingBorrow = market.borrowBalance
    baseGroup.endingBaseSupply = market.baseSupplyBalance
    for (const cp of market.collateralPositions) ensure(market.marketId, market.marketSymbol, cp.assetSymbol).endingCollateral = cp.collateralBalance
  }
  for (const e of normalizedEvents.filter((x) => x.protocolVersion === "v3")) {
    const g = ensure(e.marketId, e.marketSymbol, e.tokenSymbol)
    if (e.positionType === "collateral" && e.activityType === "deposit") g.collateralDeposits += e.amount
    if (e.positionType === "collateral" && e.activityType === "redemption") g.collateralRedemptions += e.amount
    if (e.positionType === "collateral" && e.activityType === "liquidation") g.collateralLiquidations += e.amount
    if (e.positionType === "base" && e.activityType === "base_in") g.baseIn += e.amount
    if (e.positionType === "base" && e.activityType === "base_out") g.baseOut += e.amount
    if (e.positionType === "debt" && e.activityType === "liquidation") g.debtLiquidations += e.amount
  }
  return [...groups.values()].sort((a, b) => a.marketSymbol.localeCompare(b.marketSymbol) || a.tokenSymbol.localeCompare(b.tokenSymbol))
}

function buildUnifiedEventTotals(events: any[]) {
  return {
    collateralDepositsUsd: round(events.filter((e) => e.positionType === "collateral" && e.activityType === "deposit").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    collateralRedemptionsUsd: round(events.filter((e) => e.positionType === "collateral" && e.activityType === "redemption").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    collateralLiquidationsUsd: round(events.filter((e) => e.positionType === "collateral" && e.activityType === "liquidation").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    debtBorrowingsUsd: round(events.filter((e) => e.positionType === "debt" && e.activityType === "borrowing").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    debtRepaymentsUsd: round(events.filter((e) => e.positionType === "debt" && e.activityType === "repayment").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    debtLiquidationsUsd: round(events.filter((e) => e.positionType === "debt" && e.activityType === "liquidation").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    baseInUsd: round(events.filter((e) => e.positionType === "base" && e.activityType === "base_in").reduce((s, x) => s + (x.amountUsd || 0), 0), 8),
    baseOutUsd: round(events.filter((e) => e.positionType === "base" && e.activityType === "base_out").reduce((s, x) => s + (x.amountUsd || 0), 0), 8)
  }
}

export async function buildCompoundReconciliationReport(params: { walletAddress: string; walletStartDate: string; reportEndMonth: string; onStage: StageCallback }): Promise<ReportEngineOutput> {
  const provider = new JsonRpcProvider(CHAINSTACK_RPC_URL)
  const wallet = getAddress(params.walletAddress)
  const walletLower = wallet.toLowerCase()
  const walletStartDate = parseWalletStartDate(params.walletStartDate)
  const periods = buildMonthlyPeriods(walletStartDate, params.reportEndMonth)

  await params.onStage("discover_periods", `Discovered ${periods.length} monthly periods`, periods.length, periods.length)

  const v2Markets = await discoverV2Markets(provider)
  const v3Markets = loadV3Markets()

  const results: ReportEngineOutput["periods"] = []
  const totalPeriods = periods.length

  for (let i = 0; i < periods.length; i++) {
    const period = periods[i]
    const startBlock = await findBlockByTimestamp(provider, Math.floor(period.start.getTime() / 1000))
    const endBlock = await findBlockByTimestamp(provider, Math.floor(period.endExclusive.getTime() / 1000) - 1)
    await params.onStage("resolve_blocks", `${period.label} blocks ${startBlock}-${endBlock}`, i + 1, totalPeriods)

    const [v2MonthStart, v3MonthStart] = await Promise.all([
      buildV2SnapshotAtBlock(provider, wallet, startBlock, v2Markets),
      buildV3SnapshotAtBlock(provider, wallet, startBlock, v3Markets)
    ])
    await params.onStage("snapshots_start", `${period.label} start snapshots`, i + 1, totalPeriods)

    const [v2MonthEnd, v3MonthEnd] = await Promise.all([
      buildV2SnapshotAtBlock(provider, wallet, endBlock, v2Markets),
      buildV3SnapshotAtBlock(provider, wallet, endBlock, v3Markets)
    ])
    await params.onStage("snapshots_end", `${period.label} end snapshots`, i + 1, totalPeriods)

    const v2Events = await collectV2NormalizedEvents(provider, walletLower, startBlock, endBlock, v2Markets)
    await params.onStage("fetch_events_v2", `${period.label} v2 events`, i + 1, totalPeriods)

    const v3Events = await collectV3NormalizedEvents(provider, walletLower, startBlock, endBlock, v3Markets)
    await params.onStage("fetch_events_v3", `${period.label} v3 events`, i + 1, totalPeriods)

    const normalizedEvents = [...v2Events, ...v3Events].sort((a, b) => a.blockNumber - b.blockNumber || a.txHash.localeCompare(b.txHash) || a.positionType.localeCompare(b.positionType))
    await params.onStage("normalize_events", `${period.label} normalized ${normalizedEvents.length} events`, i + 1, totalPeriods)

    const monthStart = { v2: v2MonthStart, v3: v3MonthStart, unifiedSummary: buildUnifiedSummary(v2MonthStart, v3MonthStart) }
    const monthEnd = { v2: v2MonthEnd, v3: v3MonthEnd, unifiedSummary: buildUnifiedSummary(v2MonthEnd, v3MonthEnd) }
    const monthlyRollforward = {
      v2: summarizeV2Monthly(v2MonthStart, v2MonthEnd, normalizedEvents),
      v3: summarizeV3Monthly(v3MonthStart, v3MonthEnd, normalizedEvents),
      unifiedEventTotalsUsd: buildUnifiedEventTotals(normalizedEvents)
    }

    results.push({
      periodLabel: period.label,
      monthStart,
      monthEnd,
      normalizedEvents,
      reconciliationRows: normalizedEvents,
      reconciliationSummary: [...monthlyRollforward.v2, ...monthlyRollforward.v3, { protocolVersion: "unified", marketId: null, marketSymbol: "all", tokenSymbol: "USD", ...monthlyRollforward.unifiedEventTotalsUsd }]
    })
    await params.onStage("reconcile_periods", `${period.label} summarized`, i + 1, totalPeriods)
  }

  return {
    metadata: {
      wallet,
      walletStartDate: params.walletStartDate,
      reportEndMonth: params.reportEndMonth,
      generatedAt: new Date().toISOString(),
      v2MarketsScanned: v2Markets.map((m) => m.cTokenSymbol),
      v3MarketsScanned: v3Markets.map((m) => m.symbol),
      pricesFile: REPORT_PRICES_FILE || null
    },
    periods: results,
    notes: [
      "This backend app is based on the latest monthly builder logic supplied by the user.",
      "COMPOUND_V2_MARKETS_FILE is optional in production because v2 markets can be discovered onchain.",
      "COMPOUND_V3_MARKETS_FILE remains recommended for production unless you add a deployment registry source."
    ]
  }
}
