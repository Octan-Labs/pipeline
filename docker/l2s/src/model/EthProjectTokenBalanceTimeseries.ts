import pool from "../database/posgres";
const pgFormat = require("pg-format");

export default class EthProjectTokenBalanceTimeseries {
  project_id: string;
  date: Date;
  address: string;
  token_address: string;
  balance: number;
  price: number;
}

interface IEthProjectTokenBalanceTimeseriesRepo {
  insert(
    ethProjectTokenBalanceTimeseries: EthProjectTokenBalanceTimeseries[]
  ): Promise<any>;
}

export class EthProjectTokenBalanceTimeseriesRepo
  implements IEthProjectTokenBalanceTimeseriesRepo
{
  async insert(
    ethProjectTokenBalanceTimeseries: EthProjectTokenBalanceTimeseries[]
  ): Promise<any> {
    const values = ethProjectTokenBalanceTimeseries.map((obj) => [
      obj.project_id,
      obj.date,
      obj.address,
      obj.token_address,
      obj.balance,
      obj.price,
    ]);
    const query = pgFormat(
      "INSERT INTO eth_project_token_balance_timeseries(project_id, date, address, token_address, balance, price) VALUES %L RETURNING *",
      values
    );
    const { rows } = await pool.query(query);
    return rows;
  }
}
