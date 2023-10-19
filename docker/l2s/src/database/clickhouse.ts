import { ClickHouseClient, createClient } from "@clickhouse/client";

export type BlockTime = {
  number: number;
  timestamp: Date;
};

export type TokenPrice = {
  id: number;
  timestamp: Date;
  price: number;
  address: string;
  decimals: number;
};

export const CMC_ETH_ID = 1027;

export class WarehouseClient {
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

  async getBlockTime(blockNumber: number): Promise<BlockTime[]> {
    const blockTimeQuery = `SELECT number, timestamp FROM eth_block WHERE number = {number: Int64} LIMIT 1`;
    const blockTimeResult = await this.client.query({
      query: blockTimeQuery,
      query_params: {
        number: blockNumber,
      },
      format: "JSONEachRow",
    });
    return await blockTimeResult.json();
  }

  async getTokenPrices(
    addresses: string[],
    sDate: string
  ): Promise<TokenPrice[]> {
    const tokenPriceQuery = `SELECT h.id as id, h.timestamp as timestamp, close as price,
    a.eth as address, t.decimals as decimals
    FROM cmc_historical h
    JOIN cmc_address a ON h.id = a.id
    JOIN cmc_eth_token t on t.address = a.eth
    WHERE a.eth IN ({addresses: Array(TINYTEXT)})
    AND toDate(h.timestamp) = toDate({date: date})
    LIMIT 1000`;

    const tokenPriceResult = await this.client.query({
      query: tokenPriceQuery,
      query_params: {
        addresses: addresses,
        date: sDate,
      },
      format: "JSONEachRow",
    });
    return await tokenPriceResult.json();
  }

  async getEthPrice(sDate: string): Promise<TokenPrice[]> {
    const ethPriceQuery = `SELECT id, timestamp, close as price,'NATIVE_TOKEN' as address, 18 as decimals
    FROM cmc_historical
    WHERE id = {id: Int64}
    AND toDate(timestamp) = toDate({date: date})
    LIMIT 1`;
    const ethPriceResult = await this.client.query({
      query: ethPriceQuery,
      query_params: {
        id: CMC_ETH_ID,
        date: sDate,
      },
      format: "JSONEachRow",
    });
    return await ethPriceResult.json();
  }

  close(): Promise<void> {
    return this.client.close();
  }
}
