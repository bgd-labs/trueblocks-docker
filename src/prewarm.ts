import PQueue from "p-queue";

const BASE_URL = process.env.API_URL ?? "https://logs.bgdlabs.com";
const TOKEN = process.env.API_TOKEN ?? "";
const STEP = 1000;
const END_BLOCK = 24_000_000;

const chainId = process.argv[2];
if (!chainId) {
  console.error("Usage: bun run prewarm <chainId>");
  process.exit(1);
}

const queue = new PQueue({ concurrency: 1 });

async function fetchRange(from: number, to: number): Promise<void> {
  const url = `${BASE_URL}/${chainId}/logs?from=${from}&to=${to}&token=${TOKEN}`;
  while (true) {
    const res = await fetch(url);
    if (res.status === 429 || res.status === 502) {
      console.warn(`${res.status} ${from}-${to}, retrying...`);
      await Bun.sleep(1000);
      continue;
    }
    if (!res.ok) {
      console.error(`FAIL ${from}-${to} (${res.status})`);
      return;
    }
    console.log(`OK   ${from}-${to}`);
    return;
  }
}

for (let i = 0; i < END_BLOCK; i += STEP) {
  const from = i;
  const to = i + STEP;
  queue.add(() => fetchRange(from, to));
}

await queue.onIdle();
console.log("Done.");
