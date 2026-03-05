import PQueue from "p-queue";

const BASE_URL = process.env.API_URL ?? "https://logs.bgdlabs.com";
const TOKEN = process.env.API_TOKEN ?? "";
const STEP = 100;

const chainId = process.argv[2];
const emitter = process.argv[3];
if (!chainId || !emitter) {
  console.error("Usage: bun run prewarm <chainId> <emitter> [startBlock]");
  process.exit(1);
}

const startArg = process.argv[4];
const START_BLOCK = startArg ? Number(startArg) : 0;

const chainsRes = await fetch(`${BASE_URL}/chains`);
if (!chainsRes.ok) {
  console.error(`Failed to fetch chains: ${chainsRes.status}`);
  process.exit(1);
}
const chains = (await chainsRes.json()) as Array<{
  chainId: string;
  safeHead: number;
}>;
const chain = chains.find((c) => c.chainId === chainId);
if (!chain) {
  console.error(`Chain ${chainId} not found`);
  process.exit(1);
}

const END_BLOCK = chain.safeHead;
console.log(
  `Prewarming chain ${chainId} emitter ${emitter}: blocks ${START_BLOCK}–${END_BLOCK} (step ${STEP})`,
);

const queue = new PQueue({ concurrency: 1 });

let completed = 0;
const total = Math.ceil((END_BLOCK - START_BLOCK) / STEP);

async function fetchRange(from: number, to: number): Promise<void> {
  const url = `${BASE_URL}/${chainId}/logs?from=${from}&to=${to}&emitter=${emitter}&token=${TOKEN}`;
  const t0 = Date.now();
  while (true) {
    const res = await fetch(url);
    if (res.status === 429 || res.status === 502) {
      console.warn(`${res.status} ${from}-${to}, retrying...`);
      await Bun.sleep(1000);
      continue;
    }
    const ms = Date.now() - t0;
    if (!res.ok) {
      console.error(`FAIL ${from}-${to} (${res.status}) ${ms}ms`);
      return;
    }
    completed++;
    console.log(`OK   ${from}-${to} ${ms}ms [${completed}/${total}]`);
    return;
  }
}

for (let i = START_BLOCK; i < END_BLOCK; i += STEP) {
  const from = i;
  const to = Math.min(i + STEP, END_BLOCK);
  await queue.add(() => fetchRange(from, to));
}

await queue.onIdle();
console.log("Done.");
