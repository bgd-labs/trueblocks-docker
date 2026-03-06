import { createClient } from "@clickhouse/client";
import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { logger } from "elysia-logger";
import { rateLimit } from "elysia-rate-limit";
import { tokenSet } from "./auth";

const MAX_BLOCK_RANGE = 100_000;

const CLICKHOUSE_URL = "http://localhost:8123";

const CHAIN_NAMES: Record<number, string> = {
  1: "Ethereum",
  10: "OP Mainnet",
  56: "BNB Smart Chain",
  100: "Gnosis",
  137: "Polygon",
  146: "Sonic",
  4326: "MegaETH",
  5000: "Mantle",
  8453: "Base",
  9745: "Plasma",
  42161: "Arbitrum One",
  42220: "Celo",
  43114: "Avalanche",
  57073: "Ink",
  59144: "Linea Mainnet",
  84532: "Base Sepolia",
  534352: "Scroll",
};

const clickhouse = createClient({
  url: CLICKHOUSE_URL,
  username: "default",
  password: "",
  database: "ethereum",
});

const Log = t.Object({
  address: t.String({
    description: "Address of the contract that emitted the log",
  }),
  blockHash: t.String({ description: "Hash of the block containing this log" }),
  blockNumber: t.Number({ description: "Block number containing this log" }),
  timestamp: t.Number({ description: "Unix timestamp of the block" }),
  data: t.String({ description: "ABI-encoded non-indexed log parameters" }),
  logIndex: t.Number({ description: "Index of this log within the block" }),
  topics: t.Array(t.String(), {
    description: "Indexed log topics; topics[0] is the event signature hash",
  }),
  transactionHash: t.String({
    description: "Hash of the transaction that emitted this log",
  }),
  transactionIndex: t.Number({
    description: "Index of the transaction within the block",
  }),
});

interface LogRow {
  block_number: string;
  block_hash: string;
  timestamp: string;
  transaction_hash: string;
  transaction_index: string;
  log_index: string;
  address: string;
  data: string;
  topic0: string;
  topic1: string | null;
  topic2: string | null;
  topic3: string | null;
}

function rowToLog(row: LogRow): typeof Log.static {
  const topics = [row.topic0, row.topic1, row.topic2, row.topic3].filter(
    (t): t is string => t !== null && t !== "",
  );
  return {
    address: row.address,
    blockHash: row.block_hash,
    blockNumber: Number(row.block_number),
    timestamp: Number(row.timestamp),
    data: row.data,
    logIndex: Number(row.log_index),
    topics,
    transactionHash: row.transaction_hash,
    transactionIndex: Number(row.transaction_index),
  };
}

new Elysia()
  .use(logger())
  .onError(({ error, request }) => {
    console.error(`Error on ${request.method} ${request.url}:`, error);
  })
  .use(
    openapi({
      documentation: { info: { title: "BGDown API", version: "1.0.0" } },
    }),
  )
  .use(
    rateLimit({
      max: 600,
      duration: 60_000,
      generator: (req) =>
        new URL(req.url).searchParams.get("token") ??
        req.headers.get("x-forwarded-for") ??
        "",
    }),
  )
  .get("/", ({ redirect }) => redirect("/openapi"))
  .get(
    "/chains",
    async () => {
      const result = await clickhouse.query({
        query: "SELECT DISTINCT chain_id FROM ethereum.logs ORDER BY chain_id",
        format: "JSONEachRow",
      });
      const rows = await result.json<{ chain_id: string }>();
      return rows.map(({ chain_id }) => {
        const id = Number(chain_id);
        return { id, name: CHAIN_NAMES[id] ?? `chain-${id}` };
      });
    },
    {
      response: {
        200: t.Array(
          t.Object({
            id: t.Number({ description: "EIP-155 chain ID" }),
            name: t.String({ description: "Chain name" }),
          }),
        ),
      },
    },
  )
  .get(
    "/:chainId/stats",
    async ({ params }) => {
      const [countResult, partsResult] = await Promise.all([
        clickhouse.query({
          query:
            "SELECT count() AS total_logs, max(block_number) AS max_block FROM ethereum.logs WHERE chain_id = {chainId: UInt32}",
          query_params: { chainId: params.chainId },
          format: "JSONEachRow",
        }),
        clickhouse.query({
          query:
            "SELECT formatReadableSize(sum(data_compressed_bytes)) AS compressed, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.parts WHERE table = 'logs' AND active",
          format: "JSONEachRow",
        }),
      ]);

      const [counts] = await countResult.json<{
        total_logs: string;
        max_block: string;
      }>();
      const [parts] = await partsResult.json<{
        compressed: string;
        ratio: number;
      }>();

      return {
        totalLogs: Number(counts?.total_logs ?? 0),
        maxIndexedBlock: Number(counts?.max_block ?? 0),
        compressedSize: parts?.compressed ?? "0 B",
        compressionRatio: parts?.ratio ?? 0,
      };
    },
    {
      params: t.Object({ chainId: t.String() }),
      response: {
        200: t.Object({
          totalLogs: t.Number({ description: "Total number of indexed logs" }),
          maxIndexedBlock: t.Number({
            description: "Highest block number indexed",
          }),
          compressedSize: t.String({
            description: "Compressed size of the logs table on disk",
          }),
          compressionRatio: t.Number({
            description: "Uncompressed / compressed ratio",
          }),
        }),
      },
    },
  )
  .get(
    "/:chainId/logs",
    async ({ params, query, status }) => {
      let from = query.from;
      let to = query.to;

      if (to < from) [from, to] = [to, from];
      if (to - from > MAX_BLOCK_RANGE) {
        return status(400, `Block range must be at most ${MAX_BLOCK_RANGE}`);
      }

      // Build query. topic is required so we always hit the primary index
      // (chain_id, topic0, …). address is optional and further narrows the scan.
      const addressClause = query.emitter
        ? "AND address = {emitter: String}"
        : "";

      const result = await clickhouse.query({
        query: `
          SELECT
            block_number, block_hash, timestamp, transaction_hash, transaction_index,
            log_index, address, data, topic0, topic1, topic2, topic3
          FROM ethereum.logs
          WHERE
            chain_id     = {chainId: UInt32}
            AND topic0   = {topic: String}
            AND block_number >= {from: UInt32}
            AND block_number <= {to: UInt32}
            ${addressClause}
          ORDER BY block_number, log_index
        `,
        query_params: {
          chainId: params.chainId,
          topic: query.topic,
          from,
          to,
          ...(query.emitter ? { emitter: query.emitter } : {}),
        },
        format: "JSONEachRow",
      });

      const rows = await result.json<LogRow>();
      return rows.map(rowToLog);
    },
    {
      beforeHandle: ({ query, status }) => {
        if (!tokenSet.has(query.token)) return status(401, "Unauthorized");
      },
      params: t.Object({ chainId: t.String() }),
      query: t.Object({
        token: t.String({ description: "API token" }),
        from: t.Numeric({ description: "Start block number (inclusive)" }),
        to: t.Numeric({
          description:
            "End block number (inclusive); swapped with `from` if smaller",
        }),
        topic: t.String({
          description:
            "Event signature hash (topic0) to filter by — required, maps directly to the primary index",
        }),
        emitter: t.Optional(
          t.String({
            description:
              "Contract address that emitted the log; combined with topic for a full primary-key lookup",
          }),
        ),
      }),
      response: {
        200: t.Array(Log),
        400: t.String(),
        401: t.String(),
      },
    },
  )
  .listen(3000);

console.log("Listening on http://localhost:3000");
