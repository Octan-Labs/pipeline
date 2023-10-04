import pool from "../database/posgres";

export interface EthProjectAddressTokenAddress {
  project_id: string;
  project_name: string;
  address: string;
  token_address: string;
}

interface IEthProjectAddressTokenAddressRepo {
  getAll(): Promise<EthProjectAddressTokenAddress[]>;
  getByProjectId(projectId: string): Promise<EthProjectAddressTokenAddress[]>;
}

export class EthProjectAddressTokenAddressRepo
  implements IEthProjectAddressTokenAddressRepo
{
  async getAll(): Promise<EthProjectAddressTokenAddress[]> {
    const query =
      "SELECT * FROM eth_project_address_token_address WHERE token_address <> $1";
    const { rows } = await pool.query(query, ["NATIVE_TOKEN"]);
    return rows;
  }

  async getEthContract(): Promise<EthProjectAddressTokenAddress[]> {
    const query =
      "SELECT * FROM eth_project_address_token_address WHERE token_address = $1";
    const { rows } = await pool.query(query, ["NATIVE_TOKEN"]);
    return rows;
  }

  async getByProjectId(
    projectId: string
  ): Promise<EthProjectAddressTokenAddress[]> {
    const query =
      "SELECT * FROM eth_project_address_token_address WHERE project_id = $1";
    const { rows } = await pool.query(query, [projectId]);
    return rows;
  }
}
