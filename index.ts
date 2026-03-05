import { cron } from "@elysiajs/cron";
import { openapi } from "@elysiajs/openapi";
import { Elysia, t } from "elysia";
import { rateLimit } from "elysia-rate-limit";
import { auth } from "./auth";
import { estimatedHead, refreshHeads } from "./chainHeads";
import { trueblocks } from "./trueblocks-client";

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

const ChainId = t.Union(
  Object.keys(CHAIN_CONFIG).map((id) => t.Literal(id)),
  { description: "EIP-155 chain ID" },
);

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

const Log = t.Object({
  address: t.String({
    description: "Address of the contract that emitted the log",
  }),
  blockHash: t.String({ description: "Hash of the block containing this log" }),
  blockNumber: t.Number({ description: "Block number containing this log" }),
  data: t.Optional(
    t.String({ description: "ABI-encoded non-indexed log parameters" }),
  ),
  logIndex: t.Number({ description: "Index of this log within the block" }),
  timestamp: t.Number({ description: "Unix timestamp of the block" }),
  topics: t.Array(t.String(), {
    description: "Indexed log topics; the first is the event signature hash",
  }),
  transactionHash: t.String({
    description: "Hash of the transaction that emitted this log",
  }),
  transactionIndex: t.Number({
    description: "Index of the transaction within the block",
  }),
  safe: t.Boolean({
    description: "Whether this log is from a block considered safe from reorgs",
  }),
});

new Elysia()
  .use(
    cron({
      name: "refreshHeads",
      pattern: "*/2 * * * *",
      run: () => refreshHeads(Object.values(config.chains)),
    }),
  )
  .get("/", ({ redirect }) => redirect("/openapi"))
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

      async function fetchBlocks(f: bigint, t: bigint, safe: boolean) {
        const { data, error } = await trueblocks.GET("/blocks", {
          params: {
            query: {
              blocks: [`${f}-${t}`],
              chain: cfg.name,
              logs: true,
              cache: safe,
              emitter: emitters.length ? emitters : undefined,
              topic: topics.length ? topics : undefined,
            },
          },
        });
        if (error) throw new Error(JSON.stringify(error));
        return (
          (data?.data ?? []) as Array<
            { date?: unknown } & Record<string, unknown>
          >
        ).map(({ date: _, ...log }) => ({ ...log, safe })) as unknown as Array<
          typeof Log.static
        >;
      }

      let logs: Array<typeof Log.static>;
      try {
        if (to <= safeBlock) {
          set.headers["Cache-Control"] = "public, max-age=31536000, immutable";
          logs = await fetchBlocks(from, to, true);
        } else if (from > safeBlock) {
          logs = await fetchBlocks(from, to, false);
        } else {
          const [safeLogs, unsafeLogs] = await Promise.all([
            fetchBlocks(from, safeBlock, true),
            fetchBlocks(safeBlock + 1n, to, false),
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
        from: t.Numeric({ description: "Start block number (inclusive)" }),
        to: t.Numeric({
          description:
            "End block number (inclusive); swapped with `from` if smaller",
        }),
        emitter: t.Optional(
          t.Union([t.Array(t.String()), t.String()], {
            description: "Filter by emitting contract address(es)",
          }),
        ),
        topic: t.Optional(
          t.Union([t.Array(t.String()), t.String()], {
            description: "Filter by log topic(s)",
          }),
        ),
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
