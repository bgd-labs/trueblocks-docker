import { cron } from "@elysiajs/cron";
import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { rateLimit } from "elysia-rate-limit";
import { auth } from "./auth";
import { estimatedHead, refreshHeads } from "./chainHeads";

const TRUEBLOCKS_URL = process.env.TRUEBLOCKS_URL ?? "http://trueblocks:8080";
const MAX_BLOCK_RANGE = 100_000n;

const TOML_PATH = process.env.TRUEBLOCKS_CONFIG ?? "./trueBlocks.toml";

const toml = await Bun.file(TOML_PATH).text();
const config = Bun.TOML.parse(toml) as {
  chains: Record<
    string,
    {
      chain: string;
      chainId: string;
      safetyDistance: number;
      blockTimeMs: number;
    }
  >;
};

const CHAIN_CONFIG: Record<
  string,
  { name: string; safetyDistance: bigint; blockTimeMs: number }
> = Object.fromEntries(
  Object.values(config.chains).map(
    ({ chainId, chain, safetyDistance, blockTimeMs }) => [
      chainId,
      { name: chain, safetyDistance: BigInt(safetyDistance), blockTimeMs },
    ],
  ),
);

await refreshHeads(Object.values(config.chains));

const ChainId = t.Union(Object.keys(CHAIN_CONFIG).map((id) => t.Literal(id)));

const UNITS = ["B", "KB", "MB", "GB", "TB"] as const;

function humanSize(bytes: number): string {
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < UNITS.length - 1) {
    value /= 1024;
    unit++;
  }
  return `${value.toFixed(2)} ${UNITS[unit]}`;
}

function buildUrl(
  f: bigint,
  t: bigint,
  useCache: boolean,
  chain: string,
  emitters: string[] = [],
  topics: string[] = [],
) {
  const url = new URL(`${TRUEBLOCKS_URL}/blocks`);
  url.searchParams.set("blocks", `${f}-${t}`);
  url.searchParams.set("chain", chain);
  url.searchParams.set("logs", "true");
  url.searchParams.set("cache", useCache ? "true" : "false");
  for (const address of emitters) url.searchParams.append("emitter", address);
  for (const topic of topics) url.searchParams.append("topic", topic);
  return url;
}

async function fetchUrl(url: URL) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(await response.text());
  const json = (await response.json()) as {
    data: Array<{ date?: unknown } & Record<string, unknown>>;
  };
  return (json.data ?? []).map(
    ({ date: _, ...log }) => log,
  ) as unknown as Array<typeof Log.static>;
}

const Log = t.Object({
  address: t.String(),
  blockHash: t.String(),
  blockNumber: t.Number(),
  data: t.Optional(t.String()),
  logIndex: t.Number(),
  timestamp: t.Number(),
  topics: t.Array(t.String()),
  transactionHash: t.String(),
  transactionIndex: t.Number(),
});

new Elysia()
  .use(
    cron({
      name: "refreshHeads",
      pattern: "*/2 * * * *",
      run: () => refreshHeads(Object.values(config.chains)),
    }),
  )
  .use(
    openapi({
      documentation: {
        info: { title: "TrueBlocks API", version: "1.0.0" },
        components: {
          securitySchemes: {
            bearerAuth: { type: "http", scheme: "bearer" },
          },
        },
        security: [{ bearerAuth: [] }],
      },
    }),
  )
  .get("/", ({ redirect }) => redirect("/openapi"))
  .use(
    rateLimit({
      max: 60,
      duration: 60_000,
      generator: (req) =>
        req.headers.get("authorization")?.slice(7) ??
        req.headers.get("authorization") ??
        "",
    }),
  )
  .use(auth)
  .get(
    "/:chainId/stats",
    async ({ params }) => {
      // biome-ignore lint/style/noNonNullAssertion: that is how we defined the type of CHAIN_CONFIG
      const chain = CHAIN_CONFIG[params.chainId]!.name;
      const volumes = ["/cache", "/unchained"] as const;
      const result: Record<string, Record<string, string>> = {};

      for (const volume of volumes) {
        const dirs: Record<string, string> = {};
        const topGlob = new Bun.Glob("*");
        for await (const dir of topGlob.scan({
          cwd: `${volume}/${chain}`,
          onlyFiles: false,
        })) {
          let size = 0;
          const inner = new Bun.Glob("**");
          for await (const file of inner.scan({
            cwd: `${volume}/${chain}/${dir}`,
          })) {
            size += (await Bun.file(`${volume}/${chain}/${dir}/${file}`).stat())
              .size;
          }
          dirs[dir] = humanSize(size);
        }
        result[volume] = dirs;
      }

      return result;
    },
    {
      params: t.Object({ chainId: ChainId }),
      response: {
        200: t.Record(t.String(), t.Record(t.String(), t.String())),
        401: t.String(),
      },
    },
  )
  .get(
    "/:chainId/logs",
    async ({ params, query, status, set }) => {
      // biome-ignore lint/style/noNonNullAssertion: that is how we defined the type of CHAIN_CONFIG
      const cfg = CHAIN_CONFIG[params.chainId]!;

      let from = BigInt(query.from);
      let to = BigInt(query.to);

      if (to < from) {
        [from, to] = [to, from];
      }
      if (to - from > MAX_BLOCK_RANGE) {
        return status(400, `Block range must be at most ${MAX_BLOCK_RANGE}`);
      }

      const safeBlock =
        BigInt(estimatedHead(params.chainId, cfg.blockTimeMs)) -
        cfg.safetyDistance;
      const emitters = Array.isArray(query.emitter)
        ? query.emitter
        : query.emitter
          ? [query.emitter]
          : [];
      const topics = Array.isArray(query.topic)
        ? query.topic
        : query.topic
          ? [query.topic]
          : [];

      let logs: Array<typeof Log.static>;
      try {
        if (to <= safeBlock) {
          set.headers["Cache-Control"] = "public, max-age=31536000, immutable";
          logs = await fetchUrl(
            buildUrl(from, to, true, cfg.name, emitters, topics),
          );
        } else if (from > safeBlock) {
          logs = await fetchUrl(
            buildUrl(from, to, false, cfg.name, emitters, topics),
          );
        } else {
          const [safeLogs, unsafeLogs] = await Promise.all([
            fetchUrl(
              buildUrl(from, safeBlock, true, cfg.name, emitters, topics),
            ),
            fetchUrl(
              buildUrl(safeBlock + 1n, to, false, cfg.name, emitters, topics),
            ),
          ]);
          logs = [...safeLogs, ...unsafeLogs];
        }
      } catch (err) {
        return status(502, String(err));
      }

      return logs;
    },
    {
      params: t.Object({ chainId: ChainId }),
      query: t.Object({
        from: t.Numeric(),
        to: t.Numeric(),
        emitter: t.Optional(t.Union([t.Array(t.String()), t.String()])),
        topic: t.Optional(t.Union([t.Array(t.String()), t.String()])),
      }),
      response: {
        200: t.Array(Log),
        400: t.String(),
        401: t.String(),
        502: t.String(),
      },
    },
  )
  .listen(3000);

console.log("Listening on http://localhost:3000");
