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

const Block = t.Object({
  number: t.Number({ description: "Block number" }),
  hash: t.String({ description: "Block hash" }),
  parentHash: t.String({ description: "Parent block hash" }),
  nonce: t.String({ description: "Block nonce" }),
  sha3Uncles: t.String({ description: "SHA3 of uncles data" }),
  logsBloom: t.String({ description: "Bloom filter for logs" }),
  transactionsRoot: t.String({ description: "Root of transaction trie" }),
  stateRoot: t.String({ description: "Root of state trie" }),
  receiptsRoot: t.String({ description: "Root of receipts trie" }),
  miner: t.String({ description: "Address of the block miner/validator" }),
  difficulty: t.String({ description: "Block difficulty" }),
  totalDifficulty: t.String({
    description: "Total chain difficulty at this block",
  }),
  extraData: t.String({ description: "Extra data field" }),
  size: t.String({ description: "Block size in bytes" }),
  gasLimit: t.String({ description: "Gas limit" }),
  gasUsed: t.String({ description: "Gas used" }),
  timestamp: t.Number({ description: "Unix timestamp" }),
  baseFeePerGas: t.Nullable(
    t.String({ description: "Base fee per gas (post EIP-1559)" }),
  ),
  blobGasUsed: t.Nullable(
    t.String({ description: "Blob gas used (post EIP-4844)" }),
  ),
  excessBlobGas: t.Nullable(
    t.String({ description: "Excess blob gas (post EIP-4844)" }),
  ),
  parentBeaconBlockRoot: t.Nullable(
    t.String({ description: "Parent beacon block root (post Dencun)" }),
  ),
  withdrawalsRoot: t.Nullable(
    t.String({ description: "Withdrawals trie root (post Shanghai)" }),
  ),
  withdrawals: t.String({ description: "Withdrawals JSON array" }),
  uncles: t.String({ description: "Uncle hashes JSON array" }),
  mixHash: t.String({ description: "Mix hash" }),
  l1BlockNumber: t.Nullable(
    t.Number({ description: "L1 block number (L2 chains only)" }),
  ),
  sendCount: t.Nullable(t.String({ description: "Send count (Arbitrum)" })),
  sendRoot: t.Nullable(t.String({ description: "Send root (Arbitrum)" })),
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

interface BlockRow {
  number: string;
  hash_hex: string;
  parent_hash_hex: string;
  nonce: string;
  sha3_uncles_hex: string;
  logs_bloom_hex: string;
  transactions_root_hex: string;
  state_root_hex: string;
  receipts_root_hex: string;
  miner_hex: string;
  difficulty: string;
  total_difficulty: string;
  extra_data_hex: string;
  size: string;
  gas_limit: string;
  gas_used: string;
  timestamp: string;
  base_fee_per_gas: string | null;
  blob_gas_used: string | null;
  excess_blob_gas: string | null;
  parent_beacon_block_root_hex: string | null;
  withdrawals_root_hex: string | null;
  withdrawals: string;
  uncles: string;
  mix_hash_hex: string;
  l1_block_number: string | null;
  send_count: string | null;
  send_root_hex: string | null;
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

function rowToBlock(row: BlockRow): typeof Block.static {
  return {
    number: Number(row.number),
    hash: row.hash_hex,
    parentHash: row.parent_hash_hex,
    nonce: row.nonce,
    sha3Uncles: row.sha3_uncles_hex,
    logsBloom: row.logs_bloom_hex,
    transactionsRoot: row.transactions_root_hex,
    stateRoot: row.state_root_hex,
    receiptsRoot: row.receipts_root_hex,
    miner: row.miner_hex,
    difficulty: row.difficulty,
    totalDifficulty: row.total_difficulty,
    extraData: row.extra_data_hex,
    size: row.size,
    gasLimit: row.gas_limit,
    gasUsed: row.gas_used,
    timestamp: Number(row.timestamp),
    baseFeePerGas: row.base_fee_per_gas,
    blobGasUsed: row.blob_gas_used,
    excessBlobGas: row.excess_blob_gas,
    parentBeaconBlockRoot: row.parent_beacon_block_root_hex,
    withdrawalsRoot: row.withdrawals_root_hex,
    withdrawals: row.withdrawals,
    uncles: row.uncles,
    mixHash: row.mix_hash_hex,
    l1BlockNumber: row.l1_block_number ? Number(row.l1_block_number) : null,
    sendCount: row.send_count,
    sendRoot: row.send_root_hex,
  };
}

function encodeBlockCursor(blockNumber: number): string {
  return Buffer.from(`${blockNumber}`).toString("base64url");
}

function decodeBlockCursor(cursor: string): number {
  return Number(Buffer.from(cursor, "base64url").toString());
}

const BLOCK_SELECT = `
  number,
  concat('0x', lower(hex(hash)))                  AS hash_hex,
  concat('0x', lower(hex(parent_hash)))            AS parent_hash_hex,
  toString(nonce)                                  AS nonce,
  concat('0x', lower(hex(sha3_uncles)))            AS sha3_uncles_hex,
  concat('0x', lower(hex(logs_bloom)))             AS logs_bloom_hex,
  concat('0x', lower(hex(transactions_root)))      AS transactions_root_hex,
  concat('0x', lower(hex(state_root)))             AS state_root_hex,
  concat('0x', lower(hex(receipts_root)))          AS receipts_root_hex,
  concat('0x', lower(hex(miner)))                  AS miner_hex,
  toString(difficulty)                             AS difficulty,
  total_difficulty,
  concat('0x', lower(hex(extra_data)))             AS extra_data_hex,
  toString(size)                                   AS size,
  toString(gas_limit)                              AS gas_limit,
  toString(gas_used)                               AS gas_used,
  timestamp,
  if(isNull(base_fee_per_gas), NULL, toString(assumeNotNull(base_fee_per_gas))) AS base_fee_per_gas,
  if(isNull(blob_gas_used), NULL, toString(assumeNotNull(blob_gas_used)))       AS blob_gas_used,
  if(isNull(excess_blob_gas), NULL, toString(assumeNotNull(excess_blob_gas)))   AS excess_blob_gas,
  if(isNull(parent_beacon_block_root), NULL, concat('0x', lower(hex(assumeNotNull(parent_beacon_block_root))))) AS parent_beacon_block_root_hex,
  if(isNull(withdrawals_root), NULL, concat('0x', lower(hex(assumeNotNull(withdrawals_root)))))                 AS withdrawals_root_hex,
  withdrawals,
  uncles,
  concat('0x', lower(hex(mix_hash)))               AS mix_hash_hex,
  if(isNull(l1_block_number), NULL, toString(assumeNotNull(l1_block_number))) AS l1_block_number,
  send_count,
  if(isNull(send_root), NULL, concat('0x', lower(hex(assumeNotNull(send_root))))) AS send_root_hex
`;

const LOG_SELECT = `
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
`;

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
  .guard(
    {
      beforeHandle: ({ query, status }) => {
        if (!tokenSet.has(query.token)) return status(401, "Unauthorized");
      },
      query: t.Object({
        token: t.String({
          description: "API token",
          examples: ["replace-with-secure-token"],
        }),
      }),
      response: {
        401: t.String(),
      },
    },
    (app) =>
      app
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
        .get(
          "/:chainId/height",
          async ({ params }) => {
            const safeAddresses = await getSafeAddresses(
              Number(params.chainId),
            );
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
            params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
            response: {
              200: t.Object({
                height: t.Number({
                  description: "Last indexed block number",
                }),
              }),
            },
          },
        )
        .get(
          "/:chainId/stats",
          async ({ params }) => {
            const safeAddresses = await getSafeAddresses(
              Number(params.chainId),
            );

            let logCountQuery =
              "SELECT count() AS total, max(block_number) AS max_block FROM ethereum.logs WHERE chain_id = {chainId: UInt32}";
            if (safeAddresses.length > 0) {
              const formattedAddresses = safeAddresses
                .map((a) => `'${a.toLowerCase().replace("0x", "")}'`)
                .join(", ");
              logCountQuery += ` AND lower(hex(address)) NOT IN (${formattedAddresses})`;
            }

            const [
              logCountResult,
              logPartsResult,
              blockCountResult,
              blockPartsResult,
            ] = await Promise.all([
              clickhouse.query({
                query: logCountQuery,
                query_params: { chainId: params.chainId },
                format: "JSONEachRow",
              }),
              clickhouse.query({
                query:
                  "SELECT formatReadableSize(sum(data_compressed_bytes)) AS compressed, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.parts WHERE table = 'logs' AND active",
                format: "JSONEachRow",
              }),
              clickhouse.query({
                query:
                  "SELECT count() AS total, max(number) AS max_block FROM ethereum.blocks WHERE chain_id = {chainId: UInt32}",
                query_params: { chainId: params.chainId },
                format: "JSONEachRow",
              }),
              clickhouse.query({
                query:
                  "SELECT formatReadableSize(sum(data_compressed_bytes)) AS compressed, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio FROM system.parts WHERE table = 'blocks' AND active",
                format: "JSONEachRow",
              }),
            ]);

            const [logCounts] = await logCountResult.json<{
              total: string;
              max_block: string;
            }>();
            const [logParts] = await logPartsResult.json<{
              compressed: string;
              ratio: number;
            }>();
            const [blockCounts] = await blockCountResult.json<{
              total: string;
              max_block: string;
            }>();
            const [blockParts] = await blockPartsResult.json<{
              compressed: string;
              ratio: number;
            }>();

            return {
              logs: {
                total: Number(logCounts?.total ?? 0),
                maxIndexedBlock: Number(logCounts?.max_block ?? 0),
                compressedSize: logParts?.compressed ?? "0 B",
                compressionRatio: logParts?.ratio ?? 0,
              },
              blocks: {
                total: Number(blockCounts?.total ?? 0),
                maxIndexedBlock: Number(blockCounts?.max_block ?? 0),
                compressedSize: blockParts?.compressed ?? "0 B",
                compressionRatio: blockParts?.ratio ?? 0,
              },
            };
          },
          {
            params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
            response: {
              200: t.Object({
                logs: t.Object({
                  total: t.Number({
                    description: "Total number of indexed logs",
                  }),
                  maxIndexedBlock: t.Number({
                    description: "Highest block number with logs indexed",
                  }),
                  compressedSize: t.String({
                    description: "Compressed size of the logs table on disk",
                  }),
                  compressionRatio: t.Number({
                    description: "Uncompressed / compressed ratio",
                  }),
                }),
                blocks: t.Object({
                  total: t.Number({
                    description: "Total number of indexed blocks",
                  }),
                  maxIndexedBlock: t.Number({
                    description: "Highest block number indexed",
                  }),
                  compressedSize: t.String({
                    description: "Compressed size of the blocks table on disk",
                  }),
                  compressionRatio: t.Number({
                    description: "Uncompressed / compressed ratio",
                  }),
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

            const topicHex = query.topic.slice(2);
            const emitterHex = query.emitter?.toLowerCase().slice(2);

            const cursorClause = cursor
              ? "AND (block_number, log_index) > ({cursorBlock: UInt64}, {cursorLogIndex: UInt32})"
              : "";
            const addressClause = emitterHex
              ? "AND address = unhex({emitterHex: String})"
              : "";

            const result = await clickhouse.query({
              query: `
                SELECT ${LOG_SELECT}
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
            params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
            query: t.Object({
              topic: t.String({
                description:
                  "Event signature hash (topic0) — required, maps directly to the primary index",
                examples: [
                  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                ],
              }),
              emitter: t.Optional(
                t.String({
                  description:
                    "Contract address; combined with topic for a full primary-key lookup",
                  examples: ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
                }),
              ),
              cursor: t.Optional(
                t.String({
                  description:
                    "Opaque pagination cursor from the previous response's nextCursor",
                  examples: ["MTAwMDAwMDA6MA"],
                }),
              ),
              limit: t.Optional(
                t.Numeric({
                  description: `Number of logs to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
                  examples: [100],
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
            },
          },
        )
        .get(
          "/:chainId/log/:blockNumber/:logIndex",
          async ({ params, status }) => {
            const result = await clickhouse.query({
              query: `
                SELECT ${LOG_SELECT}
                FROM ethereum.logs
                WHERE chain_id = {chainId: UInt32}
                  AND block_number = {blockNumber: UInt64}
                  AND log_index = {logIndex: UInt32}
                LIMIT 1
              `,
              query_params: {
                chainId: params.chainId,
                blockNumber: params.blockNumber,
                logIndex: params.logIndex,
              },
              format: "JSONEachRow",
            });

            const rows = await result.json<LogRow>();
            if (rows.length === 0) return status(404, "Log not found");

            return rowToLog(rows[0]);
          },
          {
            params: t.Object({
              chainId: t.String({ examples: ["1"] }),
              blockNumber: t.String({ examples: ["17000000"] }),
              logIndex: t.String({ examples: ["0"] }),
            }),
            response: {
              200: Log,
              404: t.String(),
            },
          },
        )
        .get(
          "/:chainId/blocks",
          async ({ params, query }) => {
            const limit = Math.min(query.limit ?? DEFAULT_LIMIT, MAX_LIMIT);
            const cursor = query.cursor
              ? decodeBlockCursor(query.cursor)
              : null;

            const cursorClause = cursor
              ? "AND number > {cursorBlock: UInt64}"
              : "";

            const result = await clickhouse.query({
              query: `
                SELECT ${BLOCK_SELECT}
                FROM ethereum.blocks
                WHERE chain_id = {chainId: UInt32}
                  ${cursorClause}
                ORDER BY number
                LIMIT {limit: UInt32}
              `,
              query_params: {
                chainId: params.chainId,
                limit,
                ...(cursor ? { cursorBlock: cursor } : {}),
              },
              format: "JSONEachRow",
            });

            const rows = await result.json<BlockRow>();
            const blocks = rows.map(rowToBlock);

            const lastRow = rows.at(-1);
            const nextCursor =
              rows.length === limit && lastRow
                ? encodeBlockCursor(Number(lastRow.number))
                : null;

            return { blocks, nextCursor };
          },
          {
            params: t.Object({ chainId: t.String({ examples: ["1"] }) }),
            query: t.Object({
              cursor: t.Optional(
                t.String({
                  description:
                    "Opaque pagination cursor from the previous response's nextCursor",
                  examples: ["MTAwMDA"],
                }),
              ),
              limit: t.Optional(
                t.Numeric({
                  description: `Number of blocks to return (default ${DEFAULT_LIMIT}, max ${MAX_LIMIT})`,
                  examples: [100],
                }),
              ),
            }),
            response: {
              200: t.Object({
                blocks: t.Array(Block),
                nextCursor: t.Nullable(
                  t.String({
                    description:
                      "Pass as cursor in the next request to fetch the following page; null when no more results",
                  }),
                ),
              }),
            },
          },
        )
        .get(
          "/:chainId/block/:blockNumber",
          async ({ params, status }) => {
            const result = await clickhouse.query({
              query: `
                SELECT ${BLOCK_SELECT}
                FROM ethereum.blocks
                WHERE chain_id = {chainId: UInt32}
                  AND number = {blockNumber: UInt64}
                LIMIT 1
              `,
              query_params: {
                chainId: params.chainId,
                blockNumber: params.blockNumber,
              },
              format: "JSONEachRow",
            });

            const rows = await result.json<BlockRow>();
            if (rows.length === 0) return status(404, "Block not found");

            return rowToBlock(rows[0]);
          },
          {
            params: t.Object({
              chainId: t.String({ examples: ["1"] }),
              blockNumber: t.String({ examples: ["17000000"] }),
            }),
            response: {
              200: Block,
              404: t.String(),
            },
          },
        ),
  )
  .listen(env.PORT);

console.log(`Listening on http://localhost:${env.PORT}`);
