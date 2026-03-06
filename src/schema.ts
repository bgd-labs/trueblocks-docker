import type { ClickHouseClient } from "@clickhouse/client";
import env from "./env";

export async function ensureSchema(
  clickhouse: ClickHouseClient,
): Promise<void> {
  await clickhouse.command({
    query: `CREATE DATABASE IF NOT EXISTS ${env.CLICKHOUSE_DB}`,
  });
  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${env.CLICKHOUSE_DB}.logs
      (
        chain_id          UInt32,
        block_number      UInt64               CODEC(Delta, ZSTD(3)),
        block_hash        FixedString(32)      CODEC(ZSTD(6)),
        timestamp         UInt32               CODEC(DoubleDelta, ZSTD(3)),
        transaction_hash  FixedString(32)      CODEC(ZSTD(6)),
        transaction_index UInt32               CODEC(Delta, ZSTD(3)),
        log_index         UInt32               CODEC(Delta, ZSTD(3)),
        address           FixedString(20)      CODEC(ZSTD(6)),
        data              String               CODEC(ZSTD(9)),
        topic0            FixedString(32)      CODEC(ZSTD(6)),
        topic1            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic2            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic3            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        removed           UInt8
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (chain_id, topic0, address, block_number, log_index)
      SETTINGS index_granularity = 8192
    `,
  });
}
