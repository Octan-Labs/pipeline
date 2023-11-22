import pool from "../pool";
const pgFormat = require("pg-format");

export interface L2MintedTokenValue {
  date: Date;
  project_id: string;
  token_address: string;
  amount: number;
  price: number;
}

interface IL2MintedTokenTimeseriesValueRepo {
  insert(
    l2MintedTokenValue: L2MintedTokenValue[]
  ): Promise<L2MintedTokenValue[]>;
}

export class L2MintedTokenTimeseriesValueRepo
  implements IL2MintedTokenTimeseriesValueRepo
{
  async insert(
    l2MintedTokenValues: L2MintedTokenValue[]
  ): Promise<L2MintedTokenValue[]> {
    const values = l2MintedTokenValues.map((obj) => [
      obj.date,
      obj.project_id,
      obj.token_address,
      obj.amount,
      obj.price,
    ]);
    const query = pgFormat(
      "INSERT INTO l2_minted_token_timeseries_values(date, project_id, token_address, amount, price) VALUES %L",
      values
    );
    const { rows } = await pool.query(query);
    return rows;
  }
}
