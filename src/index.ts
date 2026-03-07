import { createClient } from "@clickhouse/client";
import {
  type Block,
  HypersyncClient,
  JoinMode,
  type Log,
  type Query,
  type QueryResponse,
} from "@envio-dev/hypersync-client";
import { Cron } from "croner";
import pino from "pino";
import { createPublicClient, extractChain, http } from "viem";
import * as viemChains from "viem/chains";
import { CHAIN_BY_ID } from "./chains";
import env from "./env";
import { ensureSchema } from "./schema";

const LOG_FLUSH_BATCH_SIZE = 250_000;
const BLOCK_FLUSH_BATCH_SIZE = 50_000;
const FLUSH_INTERVAL_MS = 10_000;

// Seconds to wait before re-checking the chain tip after catching up.
const POLL_INTERVAL_SECS = 10;

// ── Row types ───────────────────────────────────────────────────────────────

interface LogRow {
  chain_id: number;
  block_number: number;
  block_hash: Buffer; // FixedString(32) — raw 32 bytes
  timestamp: number;
  transaction_hash: Buffer; // FixedString(32) — raw 32 bytes
  transaction_index: number;
  log_index: number;
  address: Buffer; // FixedString(20) — raw 20 bytes
  data: Buffer; // String — raw ABI bytes
  topic0: Buffer; // FixedString(32) — raw 32 bytes
  topic1: Buffer | null; // Nullable(FixedString(32))
  topic2: Buffer | null;
  topic3: Buffer | null;
  removed: number;
}

interface BlockRow {
  chain_id: number;
  number: number;
  hash: Buffer; // FixedString(32)
  parent_hash: Buffer; // FixedString(32)
  nonce: bigint; // UInt64
  sha3_uncles: Buffer; // FixedString(32)
  logs_bloom: Buffer; // FixedString(256)
  transactions_root: Buffer; // FixedString(32)
  state_root: Buffer; // FixedString(32)
  receipts_root: Buffer; // FixedString(32)
  miner: Buffer; // FixedString(20)
  difficulty: bigint; // UInt64
  total_difficulty: Buffer; // String — hex representation
  extra_data: Buffer; // String — raw bytes
  size: bigint; // UInt64
  gas_limit: bigint; // UInt64
  gas_used: bigint; // UInt64
  timestamp: number; // UInt32
  base_fee_per_gas: bigint | null; // Nullable(UInt64)
  blob_gas_used: bigint | null; // Nullable(UInt64)
  excess_blob_gas: bigint | null; // Nullable(UInt64)
  parent_beacon_block_root: Buffer | null; // Nullable(FixedString(32))
  withdrawals_root: Buffer | null; // Nullable(FixedString(32))
  withdrawals: Buffer; // String — JSON
  uncles: Buffer; // String — JSON
  mix_hash: Buffer; // FixedString(32)
  l1_block_number: number | null; // Nullable(UInt64)
  send_count: Buffer | null; // Nullable(String)
  send_root: Buffer | null; // Nullable(FixedString(32))
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function buildTimestampMap(blocks: Block[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const block of blocks) {
    if (block.number !== undefined && block.timestamp !== undefined) {
      map.set(block.number, block.timestamp);
    }
  }
  return map;
}

// Convert a 0x-prefixed hex string to a fixed-length Buffer of `len` bytes.
// Returns a zero-filled buffer for missing/empty values.
function hexBuf(hex: string | null | undefined, len: number): Buffer {
  if (!hex || hex.length < 3) return Buffer.alloc(len);
  return Buffer.from(hex.slice(2), "hex");
}

function logToRow(
  log: Log,
  chainId: number,
  timestamps: Map<number, number>,
): LogRow {
  return {
    chain_id: chainId,
    block_number: log.blockNumber ?? 0,
    block_hash: hexBuf(log.blockHash, 32),
    timestamp: timestamps.get(log.blockNumber ?? 0) ?? 0,
    transaction_hash: hexBuf(log.transactionHash, 32),
    transaction_index: log.transactionIndex ?? 0,
    log_index: log.logIndex ?? 0,
    address: hexBuf(log.address, 20),
    data: hexBuf(log.data, 0),
    topic0: hexBuf(log.topics[0], 32),
    topic1: log.topics[1] ? hexBuf(log.topics[1], 32) : null,
    topic2: log.topics[2] ? hexBuf(log.topics[2], 32) : null,
    topic3: log.topics[3] ? hexBuf(log.topics[3], 32) : null,
    removed: log.removed ? 1 : 0,
  };
}

function blockToRow(block: Block, chainId: number): BlockRow {
  const totalDiffHex = block.totalDifficulty
    ? `0x${block.totalDifficulty.toString(16)}`
    : "0x0";
  return {
    chain_id: chainId,
    number: block.number ?? 0,
    hash: hexBuf(block.hash, 32),
    parent_hash: hexBuf(block.parentHash, 32),
    nonce: block.nonce ?? 0n,
    sha3_uncles: hexBuf(block.sha3Uncles, 32),
    logs_bloom: hexBuf(block.logsBloom, 256),
    transactions_root: hexBuf(block.transactionsRoot, 32),
    state_root: hexBuf(block.stateRoot, 32),
    receipts_root: hexBuf(block.receiptsRoot, 32),
    miner: hexBuf(block.miner, 20),
    difficulty: block.difficulty ?? 0n,
    total_difficulty: Buffer.from(totalDiffHex, "utf8"),
    extra_data: hexBuf(block.extraData, 0),
    size: block.size ?? 0n,
    gas_limit: block.gasLimit ?? 0n,
    gas_used: block.gasUsed ?? 0n,
    timestamp: block.timestamp ?? 0,
    base_fee_per_gas: block.baseFeePerGas ?? null,
    blob_gas_used: block.blobGasUsed ?? null,
    excess_blob_gas: block.excessBlobGas ?? null,
    parent_beacon_block_root: block.parentBeaconBlockRoot
      ? hexBuf(block.parentBeaconBlockRoot, 32)
      : null,
    withdrawals_root: block.withdrawalsRoot
      ? hexBuf(block.withdrawalsRoot, 32)
      : null,
    withdrawals: Buffer.from(JSON.stringify(block.withdrawals ?? []), "utf8"),
    uncles: Buffer.from(JSON.stringify(block.uncles ?? []), "utf8"),
    mix_hash: hexBuf(block.mixHash, 32),
    l1_block_number: block.l1BlockNumber ?? null,
    send_count: block.sendCount ? Buffer.from(block.sendCount, "utf8") : null,
    send_root: block.sendRoot ? hexBuf(block.sendRoot, 32) : null,
  };
}

// Returns the number of bytes needed to encode n as LEB128.
function varUIntSize(n: number): number {
  if (n === 0) return 1;
  let size = 0;
  let v = n;
  while (v > 0) {
    v >>>= 7;
    size++;
  }
  return size;
}

// Writes n as LEB128 into buf at offset and returns the new offset.
function writeVarUInt(buf: Buffer, n: number, offset: number): number {
  while (n > 0x7f) {
    buf[offset++] = (n & 0x7f) | 0x80;
    n >>>= 7;
  }
  buf[offset++] = n;
  return offset;
}

// Serialise a batch of rows into a single RowBinary buffer.
// Pre-computes the exact size to avoid O(rows × 20) small Buffer allocations
// and the subsequent Buffer.concat() over millions of entries.
function serializeBatch(rows: LogRow[]): Buffer {
  // First pass: compute total byte count.
  let size = 0;
  for (const row of rows) {
    size += 4; // chain_id UInt32
    size += 8; // block_number UInt64
    size += 32; // block_hash FixedString(32)
    size += 4; // timestamp UInt32
    size += 32; // transaction_hash FixedString(32)
    size += 4; // transaction_index UInt32
    size += 4; // log_index UInt32
    size += 20; // address FixedString(20)
    size += varUIntSize(row.data.length) + row.data.length; // data String
    size += 32; // topic0 FixedString(32)
    size += 1 + (row.topic1 !== null ? 32 : 0); // topic1 Nullable(FixedString(32))
    size += 1 + (row.topic2 !== null ? 32 : 0); // topic2
    size += 1 + (row.topic3 !== null ? 32 : 0); // topic3
    size += 1; // removed UInt8
  }

  const buf = Buffer.allocUnsafe(size);
  let off = 0;

  for (const row of rows) {
    buf.writeUInt32LE(row.chain_id, off);
    off += 4;
    // Write UInt64 as two LE UInt32s. Block numbers fit in UInt32 for the
    // foreseeable future (Ethereum is at ~22M; max UInt32 is ~4.3B).
    buf.writeUInt32LE(row.block_number, off);
    off += 4;
    buf.writeUInt32LE(0, off);
    off += 4;
    row.block_hash.copy(buf, off);
    off += 32;
    buf.writeUInt32LE(row.timestamp, off);
    off += 4;
    row.transaction_hash.copy(buf, off);
    off += 32;
    buf.writeUInt32LE(row.transaction_index, off);
    off += 4;
    buf.writeUInt32LE(row.log_index, off);
    off += 4;
    row.address.copy(buf, off);
    off += 20;
    off = writeVarUInt(buf, row.data.length, off);
    row.data.copy(buf, off);
    off += row.data.length;
    row.topic0.copy(buf, off);
    off += 32;
    for (const topic of [row.topic1, row.topic2, row.topic3] as const) {
      if (topic === null) {
        buf[off++] = 1; // null flag
      } else {
        buf[off++] = 0; // non-null flag
        topic.copy(buf, off);
        off += 32;
      }
    }
    buf[off++] = row.removed;
  }

  return buf;
}

// Write a bigint as UInt64 LE using two UInt32 writes for compatibility.
function writeUInt64LE(buf: Buffer, value: bigint, offset: number): number {
  buf.writeUInt32LE(Number(value & 0xffffffffn), offset);
  buf.writeUInt32LE(Number((value >> 32n) & 0xffffffffn), offset + 4);
  return offset + 8;
}

function serializeBlockBatch(rows: BlockRow[]): Buffer {
  // First pass: compute total byte count.
  let size = 0;
  for (const row of rows) {
    size += 4; // chain_id UInt32
    size += 8; // number UInt64
    size += 32; // hash FixedString(32)
    size += 32; // parent_hash FixedString(32)
    size += 8; // nonce UInt64
    size += 32; // sha3_uncles FixedString(32)
    size += 256; // logs_bloom FixedString(256)
    size += 32; // transactions_root FixedString(32)
    size += 32; // state_root FixedString(32)
    size += 32; // receipts_root FixedString(32)
    size += 20; // miner FixedString(20)
    size += 8; // difficulty UInt64
    size +=
      varUIntSize(row.total_difficulty.length) + row.total_difficulty.length; // total_difficulty String
    size += varUIntSize(row.extra_data.length) + row.extra_data.length; // extra_data String
    size += 8; // size UInt64
    size += 8; // gas_limit UInt64
    size += 8; // gas_used UInt64
    size += 4; // timestamp UInt32
    size += 1 + (row.base_fee_per_gas !== null ? 8 : 0); // Nullable(UInt64)
    size += 1 + (row.blob_gas_used !== null ? 8 : 0);
    size += 1 + (row.excess_blob_gas !== null ? 8 : 0);
    size += 1 + (row.parent_beacon_block_root !== null ? 32 : 0); // Nullable(FixedString(32))
    size += 1 + (row.withdrawals_root !== null ? 32 : 0);
    size += varUIntSize(row.withdrawals.length) + row.withdrawals.length; // withdrawals String
    size += varUIntSize(row.uncles.length) + row.uncles.length; // uncles String
    size += 32; // mix_hash FixedString(32)
    size += 1 + (row.l1_block_number !== null ? 8 : 0); // Nullable(UInt64)
    size +=
      1 +
      (row.send_count !== null
        ? varUIntSize(row.send_count.length) + row.send_count.length
        : 0); // Nullable(String)
    size += 1 + (row.send_root !== null ? 32 : 0); // Nullable(FixedString(32))
  }

  const buf = Buffer.allocUnsafe(size);
  let off = 0;

  for (const row of rows) {
    buf.writeUInt32LE(row.chain_id, off);
    off += 4;
    // number UInt64
    buf.writeUInt32LE(row.number, off);
    off += 4;
    buf.writeUInt32LE(0, off);
    off += 4;
    row.hash.copy(buf, off);
    off += 32;
    row.parent_hash.copy(buf, off);
    off += 32;
    off = writeUInt64LE(buf, row.nonce, off);
    row.sha3_uncles.copy(buf, off);
    off += 32;
    row.logs_bloom.copy(buf, off);
    off += 256;
    row.transactions_root.copy(buf, off);
    off += 32;
    row.state_root.copy(buf, off);
    off += 32;
    row.receipts_root.copy(buf, off);
    off += 32;
    row.miner.copy(buf, off);
    off += 20;
    off = writeUInt64LE(buf, row.difficulty, off);
    off = writeVarUInt(buf, row.total_difficulty.length, off);
    row.total_difficulty.copy(buf, off);
    off += row.total_difficulty.length;
    off = writeVarUInt(buf, row.extra_data.length, off);
    row.extra_data.copy(buf, off);
    off += row.extra_data.length;
    off = writeUInt64LE(buf, row.size, off);
    off = writeUInt64LE(buf, row.gas_limit, off);
    off = writeUInt64LE(buf, row.gas_used, off);
    buf.writeUInt32LE(row.timestamp, off);
    off += 4;
    // Nullable UInt64 fields
    for (const val of [
      row.base_fee_per_gas,
      row.blob_gas_used,
      row.excess_blob_gas,
    ] as const) {
      if (val === null) {
        buf[off++] = 1;
      } else {
        buf[off++] = 0;
        off = writeUInt64LE(buf, val, off);
      }
    }
    // Nullable FixedString(32) fields
    for (const val of [
      row.parent_beacon_block_root,
      row.withdrawals_root,
    ] as const) {
      if (val === null) {
        buf[off++] = 1;
      } else {
        buf[off++] = 0;
        val.copy(buf, off);
        off += 32;
      }
    }
    // withdrawals String (JSON)
    off = writeVarUInt(buf, row.withdrawals.length, off);
    row.withdrawals.copy(buf, off);
    off += row.withdrawals.length;
    // uncles String (JSON)
    off = writeVarUInt(buf, row.uncles.length, off);
    row.uncles.copy(buf, off);
    off += row.uncles.length;
    // mix_hash FixedString(32)
    row.mix_hash.copy(buf, off);
    off += 32;
    // l1_block_number Nullable(UInt64)
    if (row.l1_block_number === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      buf.writeUInt32LE(row.l1_block_number, off);
      off += 4;
      buf.writeUInt32LE(0, off);
      off += 4;
    }
    // send_count Nullable(String)
    if (row.send_count === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      off = writeVarUInt(buf, row.send_count.length, off);
      row.send_count.copy(buf, off);
      off += row.send_count.length;
    }
    // send_root Nullable(FixedString(32))
    if (row.send_root === null) {
      buf[off++] = 1;
    } else {
      buf[off++] = 0;
      row.send_root.copy(buf, off);
      off += 32;
    }
  }

  return buf;
}

async function flushBlockBatch(
  batch: BlockRow[],
  log: pino.Logger,
): Promise<void> {
  if (batch.length === 0) return;
  const data = serializeBlockBatch(batch);
  const url = `${env.CLICKHOUSE_URL}/?query=${encodeURIComponent(`INSERT INTO ${env.CLICKHOUSE_DB}.blocks FORMAT RowBinary`)}`;
  const credentials = btoa(
    `${env.CLICKHOUSE_USERNAME}:${env.CLICKHOUSE_PASSWORD}`,
  );
  const res = await fetch(url, {
    method: "POST",
    body: new Uint8Array(data),
    headers: { Authorization: `Basic ${credentials}` },
  });
  if (!res.ok) {
    const body = await res.text();
    log.error({ status: res.status, body }, "ClickHouse block insert failed");
    throw new Error(`ClickHouse block insert failed [${res.status}]: ${body}`);
  }
}

const REORG_SAFETY_FALLBACK = 64;

async function getFinalizedBlock(
  hypersync: HypersyncClient,
  log: pino.Logger,
): Promise<number> {
  try {
    const allChains = Object.values(viemChains);
    const chain = allChains.find((c) => c.id === env.CHAIN_ID);
    const client = createPublicClient({
      chain,
      transport: http(env.RPC_URL),
    });
    const block = await client.getBlock({ blockTag: "finalized" });
    return Number(block.number);
  } catch (err) {
    const tip = await hypersync.getHeight();
    const fallback = tip - REORG_SAFETY_FALLBACK;
    log.warn(
      { err, tip, fallback },
      "finalized block query failed, falling back to tip - safety margin",
    );
    return fallback;
  }
}

async function getChainState(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
): Promise<{ startBlock: number; totalLogs: number }> {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block, count() AS total_logs FROM ${env.CLICKHOUSE_DB}.logs WHERE chain_id = ${chainId}`,
    format: "JSONEachRow",
  });
  const rows = await result.json<{ max_block: string; total_logs: string }>();
  // Re-include the last indexed block in case the process crashed mid-block.
  // ReplacingMergeTree deduplicates any overlapping rows on merge.
  return {
    startBlock: Number(rows[0]?.max_block ?? 0),
    totalLogs: Number(rows[0]?.total_logs ?? 0),
  };
}

async function flushLogBatch(batch: LogRow[], log: pino.Logger): Promise<void> {
  if (batch.length === 0) return;
  const data = serializeBatch(batch);
  // @clickhouse/client doesn't expose RowBinary as an insert format, so we
  // use the HTTP interface directly. RowBinary is ~2× faster than JSON and
  // lets us store hashes/addresses as true binary rather than hex strings.
  const url = `${env.CLICKHOUSE_URL}/?query=${encodeURIComponent(`INSERT INTO ${env.CLICKHOUSE_DB}.logs FORMAT RowBinary`)}`;
  const credentials = btoa(
    `${env.CLICKHOUSE_USERNAME}:${env.CLICKHOUSE_PASSWORD}`,
  );
  const res = await fetch(url, {
    method: "POST",
    body: new Uint8Array(data),
    headers: { Authorization: `Basic ${credentials}` },
  });
  if (!res.ok) {
    const body = await res.text();
    log.error({ status: res.status, body }, "ClickHouse insert failed");
    throw new Error(`ClickHouse insert failed [${res.status}]: ${body}`);
  }
}

class Flusher<T> {
  private batch: T[] = [];
  private lastFlushAt = Date.now();
  private flushPromise: Promise<void> | null = null;
  private flushError: Error | null = null;

  constructor(
    private readonly log: pino.Logger,
    private readonly doFlush: (batch: T[], log: pino.Logger) => Promise<void>,
    private readonly label: string,
    private readonly batchSize: number,
    public totalRows: number = 0,
  ) {}

  async enqueue(rows: T[]) {
    if (this.flushError) throw this.flushError;
    if (rows.length === 0) return;

    this.batch.push(...rows);
    this.totalRows += rows.length;

    const now = Date.now();
    if (
      this.batch.length >= this.batchSize ||
      now - this.lastFlushAt >= FLUSH_INTERVAL_MS
    ) {
      await this.flush();
    }
  }

  private async flush() {
    if (this.batch.length === 0) return;

    if (this.flushPromise) {
      await this.flushPromise;
      if (this.flushError) throw this.flushError;
    }

    const rowsToFlush = this.batch;
    this.batch = [];
    this.lastFlushAt = Date.now();

    const count = rowsToFlush.length;
    this.flushPromise = this.doFlush(rowsToFlush, this.log)
      .then(() => {
        this.log.info(
          { count, total: this.totalRows, kind: this.label },
          "flushed batch",
        );
        this.flushPromise = null;
      })
      .catch((err) => {
        this.flushError = err;
        this.flushPromise = null;
      });
  }

  async waitDrain() {
    if (this.batch.length > 0) {
      await this.flush();
    }
    if (this.flushPromise) {
      await this.flushPromise;
    }
    if (this.flushError) throw this.flushError;
  }
}

type RunStreamConfig = {
  hypersync: HypersyncClient;
  chainId: number;
  fromBlock: number;
  toBlock: number;
  log: pino.Logger;
  logFlusher: Flusher<LogRow>;
  blockFlusher: Flusher<BlockRow>;
};

async function runStream({
  hypersync,
  chainId,
  fromBlock,
  toBlock,
  log,
  logFlusher,
  blockFlusher,
}: RunStreamConfig): Promise<{ nextBlock: number; totalLogs: number }> {
  const query = {
    fromBlock,
    toBlock,
    includeAllBlocks: true,
    logs: [{ include: {} }],
    fieldSelection: {
      log: [
        "Removed",
        "LogIndex",
        "TransactionIndex",
        "TransactionHash",
        "BlockHash",
        "BlockNumber",
        "Address",
        "Data",
        "Topic0",
        "Topic1",
        "Topic2",
        "Topic3",
      ],
      block: [
        "Number",
        "Hash",
        "ParentHash",
        "Nonce",
        "Sha3Uncles",
        "LogsBloom",
        "TransactionsRoot",
        "StateRoot",
        "ReceiptsRoot",
        "Miner",
        "Difficulty",
        "TotalDifficulty",
        "ExtraData",
        "Size",
        "GasLimit",
        "GasUsed",
        "Timestamp",
        "Uncles",
        "BaseFeePerGas",
        "BlobGasUsed",
        "ExcessBlobGas",
        "ParentBeaconBlockRoot",
        "WithdrawalsRoot",
        "Withdrawals",
        "L1BlockNumber",
        "SendCount",
        "SendRoot",
        "MixHash",
      ],
    },
    joinMode: JoinMode.Default,
  } satisfies Query;

  const receiver = await hypersync.stream(query, {
    concurrency: 20,
  });

  let totalLogs = 0;
  let lastBlock = fromBlock;
  const totalBlocks = toBlock - fromBlock;
  let lastProgressLog = Date.now();

  try {
    while (true) {
      const res: QueryResponse | null = await receiver.recv();

      if (res === null) {
        break;
      }

      lastBlock = res.nextBlock;

      const now = Date.now();
      if (now - lastProgressLog >= 10_000) {
        const blocksProcessed = lastBlock - fromBlock;
        const pct =
          totalBlocks > 0
            ? ((blocksProcessed / totalBlocks) * 100).toFixed(1)
            : "100.0";
        log.info(
          { nextBlock: lastBlock, toBlock, pct: `${pct}%`, totalLogs },
          "progress",
        );
        lastProgressLog = now;
      }

      const timestamps = buildTimestampMap(res.data.blocks);

      const logBatch = res.data.logs.map((l) =>
        logToRow(l, chainId, timestamps),
      );
      totalLogs += logBatch.length;
      await logFlusher.enqueue(logBatch);

      const blockBatch = res.data.blocks.map((b) => blockToRow(b, chainId));
      await blockFlusher.enqueue(blockBatch);
    }
  } finally {
    await receiver.close();
  }

  log.info(
    { streamTotalLogs: totalLogs, nextBlock: lastBlock },
    "stream finished",
  );

  return { nextBlock: lastBlock, totalLogs };
}

try {
  const chain = CHAIN_BY_ID.get(env.CHAIN_ID);
  if (!chain)
    throw new Error(`Chain ${env.CHAIN_ID} not found in chains config`);

  const log = pino({ level: env.LOG_LEVEL }).child({ chainId: env.CHAIN_ID });

  const clickhouse = createClient({
    url: env.CLICKHOUSE_URL,
    username: env.CLICKHOUSE_USERNAME,
    password: env.CLICKHOUSE_PASSWORD,
    database: env.CLICKHOUSE_DB,
    clickhouse_settings: {
      async_insert: 1,
      wait_for_async_insert: 0,
    },
  });

  await ensureSchema();

  const hypersync = new HypersyncClient({
    url: chain.hypersyncUrl,
    apiToken: env.HYPERSYNC_API_KEY,
  });

  let { startBlock, totalLogs } = await getChainState(clickhouse, env.CHAIN_ID);
  log.info({ startBlock, totalLogs }, "connected, resuming ingestion");

  const syncJob = new Cron(
    `*/${POLL_INTERVAL_SECS} * * * * *`,
    async () => {
      try {
        const safeBlock = await getFinalizedBlock(hypersync, log);

        if (safeBlock <= startBlock) {
          log.info(
            { safeBlock, startBlock },
            "at finalized tip, waiting for next cron tick",
          );
          return;
        }

        log.info(
          {
            fromBlock: startBlock,
            toBlock: safeBlock,
          },
          "started streaming",
        );

        const logFlusher = new Flusher<LogRow>(
          log,
          flushLogBatch,
          "logs",
          LOG_FLUSH_BATCH_SIZE,
          totalLogs,
        );
        const blockFlusher = new Flusher<BlockRow>(
          log,
          flushBlockBatch,
          "blocks",
          BLOCK_FLUSH_BATCH_SIZE,
        );
        const res = await runStream({
          hypersync,
          chainId: env.CHAIN_ID,
          fromBlock: startBlock,
          toBlock: safeBlock,
          log,
          logFlusher,
          blockFlusher,
        });
        await logFlusher.waitDrain();
        await blockFlusher.waitDrain();

        await Promise.all([
          clickhouse.command({
            query: `OPTIMIZE TABLE ${env.CLICKHOUSE_DB}.logs FINAL`,
          }),
          clickhouse.command({
            query: `OPTIMIZE TABLE ${env.CLICKHOUSE_DB}.blocks FINAL`,
          }),
        ]);

        startBlock = res.nextBlock;
        totalLogs = logFlusher.totalRows;

        log.info(
          {
            fromBlock: startBlock,
            toBlock: safeBlock,
            logsSynced: res.totalLogs,
          },
          "finished streaming",
        );
      } catch (err) {
        log.error(err, "error during sync iteration");
      }
    },
    { protect: true },
  );

  syncJob.trigger();
} catch (err) {
  pino().error(err, "fatal error");
  process.exit(1);
}
