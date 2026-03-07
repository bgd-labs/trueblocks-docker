import { type Chain, createPublicClient, http } from "viem";
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
import env from "./env";

const CHAINS = [
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
] satisfies Chain[];

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

export const getViem = () => {
  const chain = CHAINS.find((c) => c.id === env.CHAIN_ID);
  return createPublicClient({
    chain,
    transport: http(env.RPC_URL),
  });
};
