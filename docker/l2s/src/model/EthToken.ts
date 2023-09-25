import pool from "../database/db";

interface EthToken {
  address: string;
  symbol: string;
  name: string;
  decimal: number;
  total_supply: Date;
}

interface IEthToken {
  getAll(): Promise<EthToken[]>;
  getByAddresses(addresses: string[]): Promise<EthToken[]>;
}

export class EthTokenRepo implements IEthToken {
  async getAll(): Promise<EthToken[]> {
    const query = "SELECT * FROM eth_token";
    const { rows } = await pool.query(query);
    return rows;
  }

  async getByAddresses(addresses: string[]): Promise<EthToken[]> {
    const query = "SELECT * FROM eth_token WHERE address = ANY($1)";
    const { rows } = await pool.query(query, [addresses]);
    return rows;
  }
}
