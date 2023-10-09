import pool from "../database/posgres";

export interface EthProjectEscrow {
  project_id: string;
  project_name: string;
  address: string;
  token_address: string;
}

interface IEthProjectEscrowRepo {
  getAll(): Promise<EthProjectEscrow[]>;
  getByProjectId(projectId: string): Promise<EthProjectEscrow[]>;
}

export class EthProjectEscrowRepo implements IEthProjectEscrowRepo {
  async getAll(): Promise<EthProjectEscrow[]> {
    const query = "SELECT * FROM eth_project_escrows WHERE token_address <> $1";
    const { rows } = await pool.query(query, ["NATIVE_TOKEN"]);
    return rows;
  }

  async getEthContract(): Promise<EthProjectEscrow[]> {
    const query = "SELECT * FROM eth_project_escrows WHERE token_address = $1";
    const { rows } = await pool.query(query, ["NATIVE_TOKEN"]);
    return rows;
  }

  async getByProjectId(projectId: string): Promise<EthProjectEscrow[]> {
    const query = "SELECT * FROM eth_project_escrows WHERE project_id = $1";
    const { rows } = await pool.query(query, [projectId]);
    return rows;
  }
}
