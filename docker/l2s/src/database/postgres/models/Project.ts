import pool from "../pool";

export interface Project {
  id: string;
  name: string;
  rpc_url: string;
  technology: string;
}

interface IProjectRepo {
  getAll(): Promise<Project[]>;
  getById(id: string): Promise<Project>;
}

export class ProjectRepo implements IProjectRepo {
  async getAll(): Promise<Project[]> {
    const query = "SELECT * FROM projects";
    const { rows } = await pool.query(query);
    return rows;
  }

  async getById(id: string): Promise<Project> {
    const query = "SELECT * FROM projects WHERE id = $1";
    const { rows } = await pool.query(query, [id]);
    return rows;
  }
}
