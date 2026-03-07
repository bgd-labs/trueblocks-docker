import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

const DB = env.CLICKHOUSE_DB;

export async function up(client: ClickHouseClient): Promise<void> {
  // MODIFY ORDER BY cannot reorder existing columns — ClickHouse requires the
  // implicit primary key to remain a prefix of the new sort key. The only way
  // to reorder is: create a new table with the desired key, copy data, rename.
  //
  // New sort key: (chain_id, topic0, block_number, log_index, address)
  //   Before: address was 3rd, forcing a full in-memory sort for topic-only
  //           queries (no address filter) → ~11 s at 6.4B rows.
  //   After:  block_number/log_index before address → topic-only queries and
  //           fromBlock/toBlock range scans become primary-key range reads.
  //
  // No explicit PRIMARY KEY — ClickHouse defaults the implicit PK to the full
  // ORDER BY. This means the sparse index includes block_number, so fromBlock/
  // toBlock range scans use binary search rather than a full scan.
  //
  // Prerequisites:
  //   - Pause the ingester before running this migration.
  //   - ~210 GiB free disk: logs_new grows alongside logs until the rename.
  //   - Disk returns to ~205 GiB after DROP TABLE logs_old.
  //
  // Timing at production scale (6.4B rows / 205 GiB compressed, measured on
  // a 50M-row local sample and extrapolated):
  //   CREATE + schema:          < 1 s
  //   INSERT SELECT (copy):    ~26 min   ← dominant cost
  //   RENAME (atomic swap):     < 1 s
  //   DROP old table:           < 1 s
  //   ADD + MATERIALIZE INDEX:  < 1 min  (background mutation)

  // 1. Create new table with the target sort order.
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${DB}.logs_new
      (
        chain_id          UInt32,
        block_number      UInt64                   CODEC(Delta, ZSTD(3)),
        timestamp         UInt32                   CODEC(DoubleDelta, ZSTD(3)),
        transaction_id    UInt64                   DEFAULT toUInt64(block_number) * 100000
                                                   CODEC(Delta, ZSTD(3)),
        transaction_index UInt32                   CODEC(Delta, ZSTD(3)),
        log_index         UInt32                   CODEC(Delta, ZSTD(3)),
        address           FixedString(20)          CODEC(ZSTD(6)),
        data              String                   CODEC(ZSTD(9)),
        topic0            FixedString(32)          CODEC(ZSTD(6)),
        topic1            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic2            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        topic3            Nullable(FixedString(32)) CODEC(ZSTD(6)),
        removed           UInt8
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (chain_id, topic0, block_number, log_index, address)
      SETTINGS index_granularity = 8192
    `,
  });

  // 2. Copy all data (sorts into new key order as it writes).
  //    ~26 min at production scale. Ingester must be paused.
  await client.command({
    query: `INSERT INTO ${DB}.logs_new SELECT * FROM ${DB}.logs`,
    clickhouse_settings: { send_progress_in_http_headers: 1 },
  });

  // 3. Atomic swap: old table becomes logs_old, new table becomes logs.
  await client.command({
    query: `RENAME TABLE ${DB}.logs TO ${DB}.logs_old, ${DB}.logs_new TO ${DB}.logs`,
  });

  // 4. Drop the old table to recover disk space.
  await client.command({
    query: `DROP TABLE IF EXISTS ${DB}.logs_old`,
  });

  // 5. Add bloom filter skip index on address.
  //    Compensates for address no longer being a leading sort column.
  //    Storage overhead: negligible (< 10 MB at production scale).
  await client.command({
    query: `
      ALTER TABLE ${DB}.logs
      ADD INDEX IF NOT EXISTS idx_address address TYPE bloom_filter(0.01) GRANULARITY 1
    `,
  });

  // 6. Materialize the index for existing parts (background mutation, < 1 min).
  await client.command({
    query: `ALTER TABLE ${DB}.logs MATERIALIZE INDEX idx_address`,
  });
}
