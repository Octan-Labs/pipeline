import { ClickHouseClient, createClient } from "@clickhouse/client";

const CMC_ETH_ID = 1027;

export class ClickHouseService {
  private readonly client: ClickHouseClient;
  constructor(
    host: string,
    database: string,
    username: string,
    password: string
  ) {
    this.client = createClient({
      host: host,
      database: database,
      username: username,
      password: password,
    });
  }

  async getBlockTime(blockNumber: number): Promise<any> {
    const query = `SELECT number, timestamp FROM eth_block WHERE number = {number: Int64} LIMIT 1`;
    return await this.executeQuery(query, {
      number: blockNumber,
    });
  }

  async getTokenPrices(tokenAddresses: string[], date: string): Promise<any> {
    const query = `SELECT h.id as id, h.timestamp as timestamp, close as price,
    a.eth as address, t.decimals as decimals
    FROM cmc_historical h
    JOIN cmc_address a ON h.id = a.id
    JOIN cmc_eth_token t on t.address = a.eth
    WHERE a.eth IN ({addresses: Array(TINYTEXT)})
    AND toDate(h.timestamp) = toDate({date: date})
    LIMIT 1000`;
    return await this.executeQuery(query, {
      addresses: tokenAddresses,
      date: date,
    });
  }

  async getEthPrice(date: string): Promise<any> {
    const query = `SELECT id, timestamp, close as price,'NATIVE_TOKEN' as address, 18 as decimals
    FROM cmc_historical
    WHERE id = {id: Int64}
    AND toDate(timestamp) = toDate({date: date})
    LIMIT 1`;
    return this.executeQuery(query, {
      id: CMC_ETH_ID,
      date: date,
    });
  }

  async close() {
    await this.client.close();
  }

  private async executeQuery(query: string, params: Record<string, any>) {
    const rows = await this.client.query({
      query: query,
      query_params: {
        ...params,
      },
      format: "JSONEachRow",
    });

    return await rows.json();
  }
}
