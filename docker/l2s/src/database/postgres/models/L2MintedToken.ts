import pool from "../pool";

export interface L2MintedToken {
  project_id: string;
  token_address: string;
}

interface IL2MintedTokenRepo {
  getByProjectId(projectId: string): Promise<L2MintedToken[]>;
}

export class L2MintedTokenRepo implements IL2MintedTokenRepo {
  async getByProjectId(id: string): Promise<L2MintedToken[]> {
    const query = "SELECT * FROM l2_minted_tokens WHERE project_id = $1";
    const { rows } = await pool.query(query, [id]);
    return rows;
  }
}
