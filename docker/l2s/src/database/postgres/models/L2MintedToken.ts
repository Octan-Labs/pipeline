import pool from "../pool";

export interface L2MintedToken {
  project_id: string;
  token_address: string;
}

export interface L2MintedTokenInfo {
  project_id: string;
  token_address: string;
  rpc_url: string;
}

interface IL2MintedTokenRepo {
  getByProjectId(projectId: string): Promise<L2MintedToken[]>;
  getAll(): Promise<L2MintedTokenInfo[]>;
}

export class L2MintedTokenRepo implements IL2MintedTokenRepo {
  async getByProjectId(id: string): Promise<L2MintedToken[]> {
    const query = "SELECT * FROM l2_minted_tokens WHERE project_id = $1";
    const { rows } = await pool.query(query, [id]);
    return rows;
  }

  async getAll(): Promise<L2MintedTokenInfo[]> {
    const query = `SELECT project_id, token_address, rpc_url
                  FROM l2_minted_tokens m
                  JOIN projects p on p.id = m.project_id`;
    const { rows } = await pool.query(query);
    return rows;
  }
}
