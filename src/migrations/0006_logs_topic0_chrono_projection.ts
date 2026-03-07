import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

export async function up(client: ClickHouseClient): Promise<void> {
  await client.command({
    query: `
      ALTER TABLE ${env.CLICKHOUSE_DB}.logs
      ADD PROJECTION IF NOT EXISTS proj_topic0_chrono
      (
        SELECT *
        ORDER BY (chain_id, topic0, block_number, log_index)
      )
    `,
  });

  await client.command({
    query: `
      ALTER TABLE ${env.CLICKHOUSE_DB}.logs
      MATERIALIZE PROJECTION proj_topic0_chrono
    `,
  });
}
