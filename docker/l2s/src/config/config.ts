import { config as dotenvConfig } from "dotenv";

export interface Config {
  readonly rpcUrl: string;
  readonly multicallContractAddress: string;
  readonly calculateDate: string;
  readonly dbHost: string;
  readonly dbPort: number;
  readonly dbUser: string;
  readonly dbPassword: string;
  readonly dbDatabase: string;
}

export function getConfig(): Config {
  dotenvConfig();
  return {
    rpcUrl: getEnv("ETH_RPC_URL", "https://ethereum.publicnode.com"),
    multicallContractAddress: getEnv(
      "MULTICALL_CONTRACT_ADDRESS",
      "0xeefba1e63905ef1d7acba5a8513c70307c1ce441"
    ),
    calculateDate: getEnv("CALCULATE_DATE", "2022-01-01"),
    dbHost: getEnv("POSTGRES_HOST", "localhost"),
    dbPort: +getEnv("POSTGRES_PORT", "5432"),
    dbUser: getEnv("POSTGRES_USER", "postgres"),
    dbPassword: getEnv("POSTGRES_PASSWORD", "postgres"),
    dbDatabase: getEnv("POSTGRES_DB", "l2s"),
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
