import { Pool } from "pg";
import { getConfig } from "../config";

const config = getConfig();

const pool = new Pool({
  user: config.dbUser,
  host: config.dbHost,
  database: config.dbDatabase,
  password: config.dbPassword,
  port: config.dbPort,
});

export default pool;
