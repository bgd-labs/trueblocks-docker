import type { ClickHouseClient } from "@clickhouse/client";
import env from "../env";

export async function up(client: ClickHouseClient): Promise<void> {
  await Promise.all([
    client.command({
      query: `OPTIMIZE TABLE ${env.CLICKHOUSE_DB}.logs FINAL`,
    }),
    client.command({
      query: `OPTIMIZE TABLE ${env.CLICKHOUSE_DB}.blocks FINAL`,
    }),
  ]);
}
