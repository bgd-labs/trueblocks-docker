CREATE DATABASE IF NOT EXISTS ethereum;

CREATE TABLE IF NOT EXISTS ethereum.logs
(
    chain_id          UInt64,
    block_number      UInt64,
    block_hash        String          CODEC(ZSTD(3)),
    transaction_hash  String          CODEC(ZSTD(3)),
    transaction_index UInt32,
    log_index         UInt32,
    address           String          CODEC(ZSTD(3)),
    data              String          CODEC(ZSTD(3)),
    topic0            String          CODEC(ZSTD(3)),
    topic1            Nullable(String) CODEC(ZSTD(3)),
    topic2            Nullable(String) CODEC(ZSTD(3)),
    topic3            Nullable(String) CODEC(ZSTD(3)),
    removed           UInt8
) ENGINE = ReplacingMergeTree()
-- chain_id first so all scans are partitioned by chain.
-- topic0 second so WHERE chain_id = x AND topic0 = '0x...' hits the primary index directly.
-- address third so adding it to the filter narrows to a single contract without a scan.
-- block_number + log_index last to keep deduplication correct and allow range scans within an event type.
ORDER BY (chain_id, topic0, address, block_number, log_index)
SETTINGS index_granularity = 8192;
