import type { Chain } from "viem";
import {
  arbitrum,
  avalanche,
  base,
  baseSepolia,
  bsc,
  celo,
  gnosis,
  ink,
  linea,
  mainnet,
  mantle,
  megaeth,
  optimism,
  plasma,
  polygon,
  scroll,
  sonic,
} from "viem/chains";

const CHAINS: readonly Chain[] = [
  mainnet,
  optimism,
  bsc,
  gnosis,
  polygon,
  sonic,
  megaeth,
  mantle,
  base,
  plasma,
  arbitrum,
  celo,
  avalanche,
  ink,
  linea,
  baseSepolia,
  scroll,
];

interface ChainConfig {
  readonly id: number;
  readonly name: string;
  readonly hypersyncUrl: string;
}

function toChainConfig(chain: Chain): ChainConfig {
  return {
    id: chain.id,
    name: chain.name,
    hypersyncUrl: `https://${chain.id}.hypersync.xyz`,
  };
}

export const CHAIN_BY_ID: ReadonlyMap<number, ChainConfig> = new Map(
  CHAINS.map((c) => [c.id, toChainConfig(c)]),
);
