export interface ChainConfig {
  readonly id: number;
  readonly name: string;
  readonly hypersyncUrl: string;
  /**
   * Number of blocks to stay behind the chain tip before ingesting.
   * Chosen conservatively per chain finality mechanism:
   *
   * - Ethereum / Gnosis (PoS): 2 epochs = 64 slots (~13 min)
   * - Polygon (PoS + L1 checkpoints every ~30 min): 128 blocks
   * - BNB Smart Chain (Parlia BFT): 15 blocks (~11 s)
   * - Avalanche (Snowman probabilistic): 10 blocks (~17 s)
   * - Celo (PBFT, single-block finality): 3 blocks
   * - Sonic (DAG-BFT, sub-second finality): 10 blocks
   * - ZK rollups (Linea, Scroll): 10 blocks — proven batches are final on
   *   L1, but we wait a short buffer for batch submission lag
   * - OP Stack / Optimistic rollups (Base, OP Mainnet, Arbitrum, Mantle,
   *   Ink, Plasma, MegaETH): 50 blocks — sequencer reorgs are extremely
   */
  readonly reorgSafetyBlocks: number;
}

export const CHAINS = [
  {
    id: 1,
    name: "Ethereum",
    hypersyncUrl: "https://eth.hypersync.xyz",
    reorgSafetyBlocks: 64,
  },
  {
    id: 10,
    name: "OP Mainnet",
    hypersyncUrl: "https://optimism.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 56,
    name: "BNB Smart Chain",
    hypersyncUrl: "https://bsc.hypersync.xyz",
    reorgSafetyBlocks: 15,
  },
  {
    id: 100,
    name: "Gnosis",
    hypersyncUrl: "https://gnosis.hypersync.xyz",
    reorgSafetyBlocks: 64,
  },
  {
    id: 137,
    name: "Polygon",
    hypersyncUrl: "https://polygon.hypersync.xyz",
    reorgSafetyBlocks: 128,
  },
  {
    id: 146,
    name: "Sonic",
    hypersyncUrl: "https://sonic.hypersync.xyz",
    reorgSafetyBlocks: 10,
  },
  {
    id: 4326,
    name: "MegaETH",
    hypersyncUrl: "https://megaeth.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 5000,
    name: "Mantle",
    hypersyncUrl: "https://mantle.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 8453,
    name: "Base",
    hypersyncUrl: "https://base.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 9745,
    name: "Plasma",
    hypersyncUrl: "https://plasma.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 42161,
    name: "Arbitrum One",
    hypersyncUrl: "https://arbitrum.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 42220,
    name: "Celo",
    hypersyncUrl: "https://celo.hypersync.xyz",
    reorgSafetyBlocks: 3,
  },
  {
    id: 43114,
    name: "Avalanche",
    hypersyncUrl: "https://avalanche.hypersync.xyz",
    reorgSafetyBlocks: 10,
  },
  {
    id: 57073,
    name: "Ink",
    hypersyncUrl: "https://ink.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 59144,
    name: "Linea Mainnet",
    hypersyncUrl: "https://linea.hypersync.xyz",
    reorgSafetyBlocks: 10,
  },
  {
    id: 84532,
    name: "Base Sepolia",
    hypersyncUrl: "https://base-sepolia.hypersync.xyz",
    reorgSafetyBlocks: 50,
  },
  {
    id: 534352,
    name: "Scroll",
    hypersyncUrl: "https://scroll.hypersync.xyz",
    reorgSafetyBlocks: 10,
  },
] as const satisfies ChainConfig[];

export const CHAIN_BY_ID: ReadonlyMap<number, ChainConfig> = new Map(
  CHAINS.map((c) => [c.id, c]),
);
