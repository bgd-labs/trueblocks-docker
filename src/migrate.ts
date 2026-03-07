import { createClient } from "@clickhouse/client";
import env from "./env";

// ── Types ────────────────────────────────────────────────────────────────────

interface Migration {
  name: string;
  up: (client: ReturnType<typeof createClient>) => Promise<void>;
}

// ── Migration registry ───────────────────────────────────────────────────────
//
// Add new migrations here in order. The name must be unique and should match
// the filename so it is easy to find. Numbers are purely for ordering.

const migrations: Migration[] = [
  {
    name: "0001_initial_schema",
    up: (await import("./migrations/0001_initial_schema")).up,
  },
  {
    name: "0002_delete_blocks_gt_11289889",
    up: (await import("./migrations/0002_delete_blocks_gt_11289889")).up,
  },
  {
    name: "0003_create_blocks_table",
    up: (await import("./migrations/0003_create_blocks_table")).up,
  },
  {
    name: "0004_optimize_tables",
    up: (await import("./migrations/0004_optimize_tables")).up,
  },
];

// ── Runner ───────────────────────────────────────────────────────────────────

export async function runMigrations(): Promise<void> {
  // Connect to the `default` database first so we can create our target DB
  // without hitting a chicken-and-egg problem.
  const bootstrap = createClient({
    url: env.CLICKHOUSE_URL,
    username: env.CLICKHOUSE_USERNAME,
    password: env.CLICKHOUSE_PASSWORD,
    database: "default",
  });

  try {
    // 1. Ensure the application database exists.
    await bootstrap.command({
      query: `CREATE DATABASE IF NOT EXISTS ${env.CLICKHOUSE_DB}`,
    });

    // 2. Ensure the migrations tracking table exists.
    await bootstrap.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${env.CLICKHOUSE_DB}.migrations
        (
          name        String,
          applied_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY applied_at
      `,
    });

    // 3. Fetch already-applied migration names.
    const result = await bootstrap.query({
      query: `SELECT name FROM ${env.CLICKHOUSE_DB}.migrations`,
      format: "JSONEachRow",
    });
    const applied = new Set(
      (await result.json<{ name: string }>()).map((r) => r.name),
    );

    // 4. Apply pending migrations in order.
    let ran = 0;
    for (const migration of migrations) {
      if (applied.has(migration.name)) continue;

      console.log(`[migrate] applying ${migration.name} …`);
      await migration.up(bootstrap);
      await bootstrap.insert({
        table: `${env.CLICKHOUSE_DB}.migrations`,
        values: [{ name: migration.name }],
        format: "JSONEachRow",
      });
      ran++;
      console.log(`[migrate] applied  ${migration.name}`);
    }

    if (ran === 0) {
      console.log("[migrate] all migrations already applied");
    } else {
      console.log(`[migrate] ${ran} migration(s) applied`);
    }
  } finally {
    await bootstrap.close();
  }
}
