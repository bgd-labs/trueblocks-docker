import { createClient } from "@clickhouse/client";
import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { logger } from "elysia-logger";
import { rateLimit } from "elysia-rate-limit";
import { tokenSet } from "./auth";
import { CHAIN_BY_ID } from "./chains";
import env from "./env";
import { getSafeAddresses } from "./safe-addresses";

const DEFAULT_LIMIT = 1_000;
const MAX_LIMIT = 50_000;

const clickhouse = createClient({
  url: env.CLICKHOUSE_URL,
  username: env.CLICKHOUSE_USERNAME,
  password: env.CLICKHOUSE_PASSWORD,
  database: env.CLICKHOUSE_DB,
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
  block_hash_hex: string;
  timestamp: string;
  transaction_hash_hex: string;
  transaction_index: string;
  log_index: string;
  address_hex: string;
  data_hex: string;
  topic0_hex: string;
  topic1_hex: string | null;
  topic2_hex: string | null;
  topic3_hex: string | null;
}

function encodeCursor(blockNumber: number, logIndex: number): string {
  return Buffer.from(`${blockNumber}:${logIndex}`).toString("base64url");
}

function decodeCursor(cursor: string): {
  blockNumber: number;
  logIndex: number;
} {
  const [blockNumber, logIndex] = Buffer.from(cursor, "base64url")
    .toString()
    .split(":")
    .map(Number);
  return { blockNumber: blockNumber ?? 0, logIndex: logIndex ?? 0 };
}

function rowToLog(row: LogRow): typeof Log.static {
  const topics = [
    row.topic0_hex,
    row.topic1_hex,
    row.topic2_hex,
    row.topic3_hex,
  ].filter((t): t is string => t !== null && t !== "");
  return {
    address: row.address_hex,
    blockHash: row.block_hash_hex,
    blockNumber: Number(row.block_number),
    timestamp: Number(row.timestamp),
    data: row.data_hex,
    logIndex: Number(row.log_index),
    topics,
    transactionHash: row.transaction_hash_hex,
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
        return { id, name: CHAIN_BY_ID.get(id)?.name ?? `chain-${id}` };
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
    "/:chainId/height",
    async ({ params }) => {
      const safeAddresses = await getSafeAddresses(Number(params.chainId));
      let query =
        "SELECT max(block_number) AS height FROM ethereum.logs WHERE chain_id = {chainId: UInt32}";
      if (safeAddresses.length > 0) {
        const formattedAddresses = safeAddresses
          .map((a) => `'${a.toLowerCase().replace("0x", "")}'`)
          .join(", ");
        query += ` AND lower(hex(address)) NOT IN (${formattedAddresses})`;
      }

      const result = await clickhouse.query({
        query,
        query_params: { chainId: params.chainId },
        format: "JSONEachRow",
      });
      const [row] = await result.json<{ height: string }>();
      return { height: Number(row?.height ?? 0) };
    },
    {
      params: t.Object({ chainId: t.String() }),
      response: {
        200: t.Object({
          height: t.Number({ description: "Last indexed block number" }),
        }),
      },
    },
  )
  .get(
    "/:chainId/stats",
    async ({ params }) => {
      const safeAddresses = await getSafeAddresses(Number(params.chainId));
      let countQuery =
        "SELECT count() AS total_logs, max(block_number) AS max_block FROM ethereum.logs WHERE chain_id = {chainId: UInt32}";
      if (safeAddresses.length > 0) {
        const formattedAddresses = safeAddresses
          .map((a) => `'${a.toLowerCase().replace("0x", "")}'`)
          .join(", ");
        countQuery += ` AND lower(hex(address)) NOT IN (${formattedAddresses})`;
      }

      const [countResult, partsResult] = await Promise.all([
        clickhouse.query({
          query: countQuery,
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
    async ({ params, query }) => {
      const limit = Math.min(query.limit ?? DEFAULT_LIMIT, MAX_LIMIT);

      const cursor = query.cursor ? decodeCursor(query.cursor) : null;

      // Strip 0x prefix for unhex(); toLowerCase normalises checksummed addresses.
      const topicHex = query.topic.slice(2);
      const emitterHex = query.emitter?.toLowerCase().slice(2);

      // Cursor clause omitted on the first page to avoid UInt32/Int64 coercion
      // issues with sentinel values in ClickHouse tuple comparisons.
      const cursorClause = cursor
        ? "AND (block_number, log_index) > ({cursorBlock: UInt64}, {cursorLogIndex: UInt32})"
        : "";
      const addressClause = emitterHex
        ? "AND address = unhex({emitterHex: String})"
        : "";

      // ClickHouse resolves SELECT aliases in WHERE, so aliases must not
      // shadow column names used in the WHERE clause (topic0, address).
      // We use distinct alias names (_hex suffix) to avoid the conflict.
      const result = await clickhouse.query({
        query: `
          SELECT
            block_number,
            concat('0x', lower(hex(block_hash)))        AS block_hash_hex,
            timestamp,
            concat('0x', lower(hex(transaction_hash))) AS transaction_hash_hex,
            transaction_index,
            log_index,
            concat('0x', lower(hex(address)))           AS address_hex,
            concat('0x', lower(hex(data)))              AS data_hex,
            concat('0x', lower(hex(topic0)))            AS topic0_hex,
            if(isNull(topic1), NULL, concat('0x', lower(hex(assumeNotNull(topic1))))) AS topic1_hex,
            if(isNull(topic2), NULL, concat('0x', lower(hex(assumeNotNull(topic2))))) AS topic2_hex,
            if(isNull(topic3), NULL, concat('0x', lower(hex(assumeNotNull(topic3))))) AS topic3_hex
          FROM ethereum.logs
          WHERE
            chain_id = {chainId: UInt32}
            AND topic0 = unhex({topicHex: String})
            ${cursorClause}
            ${addressClause}
          ORDER BY block_number, log_index
          LIMIT {limit: UInt32}
        `,
        query_params: {
          chainId: params.chainId,
          topicHex,
          limit,
          ...(cursor
            ? {
                cursorBlock: cursor.blockNumber,
                cursorLogIndex: cursor.logIndex,
              }
            : {}),
          ...(emitterHex ? { emitterHex } : {}),
        },
        format: "JSONEachRow",
      });

      const rows = await result.json<LogRow>();
      const logs = rows.map(rowToLog);

      const lastRow = rows.at(-1);
      const nextCursor =
        rows.length === limit && lastRow
          ? encodeCursor(
              Number(lastRow.block_number),
              Number(lastRow.log_index),
            )
          : null;

      return { logs, nextCursor };
    },
    {
      beforeHandle: ({ query, status }) => {
        if (!tokenSet.has(query.token)) return status(401, "Unauthorized");
      },
      params: t.Object({ chainId: t.String() }),
      query: t.Object({
        token: t.String({ description: "API token" }),
        topic: t.String({
          description:
            "Event signature hash (topic0) — required, maps directly to the primary index",
        }),
        emitter: t.Optional(
          t.String({
            description:
              "Contract address; combined with topic for a full primary-key lookup",
          }),
        ),
        cursor: t.Optional(
          t.String({
            description:
              "Opaque pagination cursor from the previous response's nextCursor",
          }),
        ),
        limit: t.Optional(
          t.Numeric({
            description: `Number of logs to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
          }),
        ),
      }),
      response: {
        200: t.Object({
          logs: t.Array(Log),
          nextCursor: t.Nullable(
            t.String({
              description:
                "Pass as cursor in the next request to fetch the following page; null when no more results",
            }),
          ),
        }),
        401: t.String(),
      },
    },
  )
  .listen(env.PORT);

console.log(`Listening on http://localhost:${env.PORT}`);
