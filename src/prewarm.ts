import pThrottle from "p-throttle";

const BASE_URL = process.env.API_URL ?? "https://logs.bgdlabs.com";
const TOKEN = process.env.API_TOKEN ?? "";
const STEP = 1000;
const END_BLOCK = 100_000;

const chainId = process.argv[2];
if (!chainId) {
  console.error("Usage: bun run prewarm <chainId>");
  process.exit(1);
}

const throttle = pThrottle({ limit: 5, interval: 1000 });

const fetchRange = throttle(async (from: number, to: number) => {
  const url = `${BASE_URL}/${chainId}/logs?from=${from}&to=${to}&token=${TOKEN}`;
  const res = await fetch(url);
  if (!res.ok) {
    console.error(`FAIL ${from}-${to} (${res.status})`);
    return false;
  }
  console.log(`OK   ${from}-${to}`);
  return true;
});

const ranges: Array<[number, number]> = [];
for (let i = 0; i < END_BLOCK; i += STEP) {
  ranges.push([i, i + STEP]);
}

await Promise.all(ranges.map(([from, to]) => fetchRange(from, to)));
console.log("Done.");
