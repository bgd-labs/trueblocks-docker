/** biome-ignore-all lint/style/noProcessEnv: this is the file re-mapping them */
import arkenv from "arkenv";

const raw = arkenv({
  CLICKHOUSE_URL: "string.url = 'http://localhost:8123'",
  CLICKHOUSE_DB: "string = 'ethereum'",
  HYPERSYNC_API_KEY: "string",
  LOG_LEVEL: "'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal' = 'info'",
  PORT: "number.port = 3000",
  CHAIN_ID: "number = 1",
});

// Normalise the ClickHouse URL: fix scheme, remap native TCP port, then extract
// any credentials/database embedded in the URL so they can be passed explicitly
// to @clickhouse/client (which warns when the URL overrides explicit config).
const parsedUrl = new URL(
  raw.CLICKHOUSE_URL
    .replace(/^clickhouse:\/\//, "http://")
    .replace(/:9000(\/|$)/, ":8123$1"),
);

const CLICKHOUSE_USERNAME = parsedUrl.username || "default";
const CLICKHOUSE_PASSWORD = parsedUrl.password || "";
// Resolution order: explicit non-empty CLICKHOUSE_DB env var → database in
// URL path → hard-coded default. This means a URL like
// clickhouse://user:pass@host:9000/default won't silently win over an explicit
// CLICKHOUSE_DB=ethereum, but an unset/empty var still falls back gracefully.
const urlDb = parsedUrl.pathname.replace(/^\//, "");
const CLICKHOUSE_DB = raw.CLICKHOUSE_DB || urlDb || "ethereum";

// Strip credentials and path from the URL before handing it to the client.
parsedUrl.username = "";
parsedUrl.password = "";
parsedUrl.pathname = "/";

export default {
  ...raw,
  CLICKHOUSE_URL: parsedUrl.toString(),
  CLICKHOUSE_DB,
  CLICKHOUSE_USERNAME,
  CLICKHOUSE_PASSWORD,
};
