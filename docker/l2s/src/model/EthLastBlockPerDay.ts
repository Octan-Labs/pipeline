import pool from "../database/db";

interface EthLastBlockPerDay {
  block_number: number;
  date: Date;
}

interface IEthLastBlockPerDayRepo {
  getAll(): Promise<EthLastBlockPerDay[]>;
  getByDate(date: String): Promise<EthLastBlockPerDay[]>;
}

export class EthLastBlockPerDayRepo implements IEthLastBlockPerDayRepo {
  async getAll(): Promise<EthLastBlockPerDay[]> {
    const query = "SELECT * FROM eth_last_block_per_dates";
    const { rows } = await pool.query(query);
    return rows;
  }

  async getByDate(date: String): Promise<EthLastBlockPerDay[]> {
    const query = "SELECT * FROM eth_last_block_per_dates WHERE date = $1";
    const { rows } = await pool.query(query, [date]);
    return rows;
  }
}
