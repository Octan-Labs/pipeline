import pool from "../pool";
const pgFormat = require("pg-format");

export interface EthProjectEscrowValue {
  project_id: string;
  date: Date;
  address: string;
  token_address: string;
  balance: number;
  price: number;
}

interface IEthProjectEscrowTimeseriesValueRepo {
  insert(
    ethProjectEscrowValue: EthProjectEscrowValue[]
  ): Promise<EthProjectEscrowValue[]>;
}

export class EthProjectEscrowTimeseriesValueRepo
  implements IEthProjectEscrowTimeseriesValueRepo
{
  async insert(ethProjectEscrowValue: EthProjectEscrowValue[]): Promise<any> {
    const values = ethProjectEscrowValue.map((obj) => [
      obj.project_id,
      obj.date,
      obj.address,
      obj.token_address,
      obj.balance,
      obj.price,
    ]);
    const query = pgFormat(
      "INSERT INTO eth_project_escrow_timeseries_values(project_id, date, address, token_address, balance, price) VALUES %L",
      values
    );
    const { rows } = await pool.query(query);
    return rows;
  }
}
