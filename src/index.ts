import { createClient } from "@clickhouse/client";
import {
  type Block,
  HypersyncClient,
  JoinMode,
  type Log,
  type QueryResponse,
} from "@envio-dev/hypersync-client";

const HYPERSYNC_URL = "https://eth.hypersync.xyz";
const HYPERSYNC_API_KEY = "b5c5baee-7507-451c-bcfb-f0d1e790a5ab";
const CHAIN_ID = 1; // Ethereum mainnet

const CLICKHOUSE_URL = "http://localhost:8123";
const CLICKHOUSE_DB = "ethereum";

const FLUSH_BATCH_SIZE = 500_000;
const FLUSH_INTERVAL_MS = 30_000;

// Seconds to wait before re-checking the chain tip after catching up.
const POLL_INTERVAL_SECS = 10;

interface LogRow {
  chain_id: number;
  block_number: number;
  block_hash: string;
  timestamp: number;
  transaction_hash: string;
  transaction_index: number;
  log_index: number;
  address: string;
  data: string;
  topic0: string;
  topic1: string | null;
  topic2: string | null;
  topic3: string | null;
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

function logToRow(
  log: Log,
  chainId: number,
  timestamps: Map<number, number>,
): LogRow {
  return {
    chain_id: chainId,
    block_number: log.blockNumber ?? 0,
    block_hash: log.blockHash ?? "",
    timestamp: timestamps.get(log.blockNumber ?? 0) ?? 0,
    transaction_hash: log.transactionHash ?? "",
    transaction_index: log.transactionIndex ?? 0,
    log_index: log.logIndex ?? 0,
    address: log.address ?? "",
    data: log.data ?? "0x",
    topic0: log.topics[0] ?? "",
    topic1: log.topics[1] ?? null,
    topic2: log.topics[2] ?? null,
    topic3: log.topics[3] ?? null,
    removed: log.removed ? 1 : 0,
  };
}

async function getStartBlock(
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
): Promise<number> {
  const result = await clickhouse.query({
    query: `SELECT max(block_number) AS max_block FROM ethereum.logs WHERE chain_id = ${chainId}`,
    format: "JSONEachRow",
  });
  const rows = await result.json<{ max_block: string }>();
  const maxBlock = Number(rows[0]?.max_block ?? 0);
  // Re-include the last indexed block in case the process crashed mid-block.
  // ReplacingMergeTree deduplicates any overlapping rows on merge.
  return maxBlock;
}

async function flushBatch(
  clickhouse: ReturnType<typeof createClient>,
  batch: LogRow[],
): Promise<void> {
  if (batch.length === 0) return;
  await clickhouse.insert({
    table: "ethereum.logs",
    values: batch,
    format: "JSONEachRow",
  });
}

async function runStream(
  hypersync: HypersyncClient,
  clickhouse: ReturnType<typeof createClient>,
  chainId: number,
  fromBlock: number,
): Promise<number> {
  const query = {
    fromBlock,
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

  const receiver = await hypersync.stream(query, {});

  let batch: LogRow[] = [];
  let totalLogs = 0;
  let lastBlock = fromBlock;
  let lastFlushAt = Date.now();

  const flush = async () => {
    await flushBatch(clickhouse, batch);
    totalLogs += batch.length;
    batch = [];
    lastFlushAt = Date.now();
    console.log(
      `[${new Date().toISOString()}] Inserted ${totalLogs.toLocaleString()} logs total, next block: ${lastBlock.toLocaleString()}`,
    );
  };

  try {
    while (true) {
      const res: QueryResponse | null = await receiver.recv();

      if (res === null) {
        // Stream exhausted – we have reached the chain tip at stream start.
        await flush();
        break;
      }

      lastBlock = res.nextBlock;

      const timestamps = buildTimestampMap(res.data.blocks);
      for (const log of res.data.logs) {
        batch.push(logToRow(log, chainId, timestamps));
      }

      const elapsed = Date.now() - lastFlushAt;
      if (batch.length >= FLUSH_BATCH_SIZE || elapsed >= FLUSH_INTERVAL_MS) {
        await flush();
      }
    }
  } finally {
    // Always close the stream to release server-side resources.
    await receiver.close();
  }

  console.log(
    `[${new Date().toISOString()}] Stream finished. Inserted ${totalLogs.toLocaleString()} logs this run, next block: ${lastBlock.toLocaleString()}`,
  );

  return lastBlock;
}

async function main(): Promise<void> {
  const clickhouse = createClient({
    url: CLICKHOUSE_URL,
    username: "default",
    password: "",
    database: CLICKHOUSE_DB,
    clickhouse_settings: {
      async_insert: 1,
      wait_for_async_insert: 0,
    },
  });

  const hypersync = new HypersyncClient({
    url: HYPERSYNC_URL,
    apiToken: HYPERSYNC_API_KEY,
  });

  console.log("Connected. Checking last indexed block…");

  let startBlock = await getStartBlock(clickhouse, CHAIN_ID);
  console.log(`Resuming from block ${startBlock.toLocaleString()}`);

  // Continuous loop: stream until chain tip, then poll for new blocks.
  while (true) {
    startBlock = await runStream(hypersync, clickhouse, CHAIN_ID, startBlock);
    console.log(
      `Caught up to chain tip. Polling again in ${POLL_INTERVAL_SECS}s…`,
    );
    await Bun.sleep(POLL_INTERVAL_SECS * 1000);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
