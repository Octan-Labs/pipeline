import { config as dotenvConfig } from "dotenv";

export interface Config {
  readonly rpcUrl: string;
  readonly multicallContractAddress: string;
  readonly blockNumber: number;
  readonly blockDate: string;
  readonly chunkSize: number;
  readonly chHost: string;
  readonly chDatabase: string;
  readonly chUser: string;
  readonly chPassword: string;
}

export function getConfig(): Config {
  dotenvConfig();
  return {
    rpcUrl: getEnv("ETH_RPC_URL", "https://ethereum.publicnode.com"),
    multicallContractAddress: getEnv(
      "MULTICALL_CONTRACT_ADDRESS",
      "0xeefba1e63905ef1d7acba5a8513c70307c1ce441"
    ),
    blockNumber: +getEnv("BLOCK_NUMBER", ""),
    blockDate: getEnv("BLOCK_DATE", ""),
    chunkSize: +getEnv("DEFAULT_CHUNK_SIZE", "50"),
    chHost: getEnv("CLICKHOUSE_HOST", ""),
    chDatabase: getEnv("CLICKHOUSE_DATABASE", ""),
    chUser: getEnv("CLICKHOUSE_USER", "default"),
    chPassword: getEnv("CLICKHOUSE_PASSWORD", ""),
  };
}

export function getEnv(name: string, fallback?: string): string {
  const value = process.env[name];
  if (value !== undefined) {
    return value;
  }
  if (fallback !== undefined) {
    return fallback;
  }
  throw new Error(`Missing environment variable ${name}!`);
}
