type HeadInfo = { height: number; fetchedAt: number };
const chainHeads = new Map<string, HeadInfo>();

async function fetchHeight(chainId: string): Promise<number> {
  const res = await fetch(`https://${chainId}.hypersync.xyz/height`);
  const json = (await res.json()) as { height: number };
  return json.height;
}

export function estimatedHead(chainId: string, blockTime: number): number {
  const head = chainHeads.get(chainId);
  if (!head) return 0;
  const elapsedSecs = (Date.now() - head.fetchedAt) / 1000;
  return head.height + Math.floor(elapsedSecs / blockTime);
}

export async function refreshHeads(
  chains: Array<{ chainId: string }>,
): Promise<void> {
  for (const { chainId } of chains) {
    try {
      const height = await fetchHeight(chainId);
      chainHeads.set(chainId, { height, fetchedAt: Date.now() });
    } catch (err) {
      console.error(`Failed to fetch head for chain ${chainId}:`, err);
    }
  }
}
