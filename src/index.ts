import { createClient } from "@clickhouse/client";
import {
  type Block,
  HypersyncClient,
  JoinMode,
  type Log,
  type QueryResponse,
} from "@envio-dev/hypersync-client";
import pino from "pino";
import { CHAIN_BY_ID } from "./chains";
import env from "./env";
import { ensureSchema } from "./schema";

const FLUSH_BATCH_SIZE = 75_000;
const FLUSH_INTERVAL_MS = 5_000;
const MAX_CONCURRENT_FLUSHES = 8;

// Seconds to wait before re-checking the chain tip after catching up.
const POLL_INTERVAL_SECS = 10;

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

async function getChainState(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
): Promise<{ startBlock: number; totalLogs: number }> {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block, count() AS total_logs FROM ethereum.logs WHERE chain_id = ${chainId}`,
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

async function flushBatch(batch: LogRow[], log: pino.Logger): Promise<void> {
  if (batch.length === 0) return;
  const data = serializeBatch(batch);
  // @clickhouse/client doesn't expose RowBinary as an insert format, so we
  // use the HTTP interface directly. RowBinary is ~2× faster than JSON and
  // lets us store hashes/addresses as true binary rather than hex strings.
  const url = `${env.CLICKHOUSE_URL}/?query=${encodeURIComponent(`INSERT INTO ${env.CLICKHOUSE_DB}.logs FORMAT RowBinary`)}`;
  const credentials = btoa(`${env.CLICKHOUSE_USERNAME}:${env.CLICKHOUSE_PASSWORD}`);
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

async function runStream(
  hypersync: HypersyncClient,
  chainId: number,
  fromBlock: number,
  toBlock: number,
  initialTotalLogs: number,
  log: pino.Logger,
): Promise<{ nextBlock: number; totalLogs: number }> {
  const query = {
    fromBlock,
    toBlock,
    // Empty LogFilter matches every log on the chain.
    logs: [{}],
    fieldSelection: {
      log: [
        "Removed" as const,
        "LogIndex" as const,
        "TransactionIndex" as const,
        "TransactionHash" as const,
        "BlockHash" as const,
        "BlockNumber" as const,
        "Address" as const,
        "Data" as const,
        "Topic0" as const,
        "Topic1" as const,
        "Topic2" as const,
        "Topic3" as const,
      ],
      block: ["Number" as const, "Timestamp" as const],
    },
    joinMode: JoinMode.Default,
  };

  const receiver = await hypersync.stream(query, {
    concurrency: 20,
  });

  let batch: LogRow[] = [];
  let totalLogs = initialTotalLogs;
  let lastBlock = fromBlock;
  let lastFlushAt = Date.now();

  // Flush pipeline: up to MAX_CONCURRENT_FLUSHES ClickHouse POSTs run in
  // parallel so recv() is never blocked waiting for a single insert to finish.
  // A semaphore provides backpressure when ClickHouse falls behind HyperSync.
  let activeFlushes = 0;
  let flushDone: (() => void) | null = null;

  const waitForSlot = (): Promise<void> =>
    new Promise((resolve) => {
      if (activeFlushes < MAX_CONCURRENT_FLUSHES) {
        resolve();
      } else {
        flushDone = resolve;
      }
    });

  const scheduleFlush = (rows: LogRow[], nextBlock: number) => {
    if (rows.length === 0) return;
    activeFlushes++;
    flushBatch(rows, log).then(() => {
      totalLogs += rows.length;
      activeFlushes--;
      log.info({ totalLogs, nextBlock }, "flushed batch");
      if (flushDone) {
        const cb = flushDone;
        flushDone = null;
        cb();
      }
    });
  };

  const drainFlushes = (): Promise<void> =>
    new Promise((resolve) => {
      if (activeFlushes === 0) { resolve(); return; }
      const check = () => {
        if (activeFlushes === 0) { resolve(); return; }
        flushDone = check;
      };
      flushDone = check;
    });

  try {
    while (true) {
      const res: QueryResponse | null = await receiver.recv();

      if (res === null) {
        // Stream exhausted – we have reached the chain tip.
        await waitForSlot();
        scheduleFlush(batch, lastBlock);
        batch = [];
        break;
      }

      lastBlock = res.nextBlock;

      const timestamps = buildTimestampMap(res.data.blocks);
      for (const log of res.data.logs) {
        batch.push(logToRow(log, chainId, timestamps));
        if (batch.length >= FLUSH_BATCH_SIZE) {
          await waitForSlot();
          scheduleFlush(batch, lastBlock);
          batch = [];
          lastFlushAt = Date.now();
        }
      }

      // Time-based flush for slow periods near chain tip.
      if (batch.length > 0 && Date.now() - lastFlushAt >= FLUSH_INTERVAL_MS) {
        await waitForSlot();
        scheduleFlush(batch, lastBlock);
        batch = [];
        lastFlushAt = Date.now();
      }
    }
  } finally {
    // Close the stream before draining the flush queue.
    await receiver.close();
  }

  // Drain any remaining in-flight flushes.
  await drainFlushes();

  log.info({ totalLogs, nextBlock: lastBlock }, "stream finished");

  return { nextBlock: lastBlock, totalLogs };
}

async function main(): Promise<void> {
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

  await ensureSchema(clickhouse);

  const hypersync = new HypersyncClient({
    url: chain.hypersyncUrl,
    apiToken: env.HYPERSYNC_API_KEY,
  });

  let { startBlock, totalLogs } = await getChainState(clickhouse, env.CHAIN_ID);
  log.info({ startBlock, totalLogs }, "connected, resuming ingestion");

  // Continuous loop: stream up to the reorg-safe tip, then poll for new blocks.
  while (true) {
    const tip = await hypersync.getHeight();
    const safeBlock = Math.max(startBlock, tip - chain.reorgSafetyBlocks);

    if (safeBlock <= startBlock) {
      log.info(
        { tip, safeBlock, reorgSafetyBlocks: chain.reorgSafetyBlocks },
        "at reorg-safe tip, polling",
      );
      await Bun.sleep(POLL_INTERVAL_SECS * 1000);
      continue;
    }

    log.info(
      {
        fromBlock: startBlock,
        toBlock: safeBlock,
        tip,
        reorgSafetyBlocks: chain.reorgSafetyBlocks,
      },
      "streaming",
    );
    ({ nextBlock: startBlock, totalLogs } = await runStream(
      hypersync,
      env.CHAIN_ID,
      startBlock,
      safeBlock,
      totalLogs,
      log,
    ));
    log.info("caught up to safe tip, polling");
    await Bun.sleep(POLL_INTERVAL_SECS * 1000);
  }
}

main().catch((err) => {
  pino().error(err, "fatal error");
  process.exit(1);
});
