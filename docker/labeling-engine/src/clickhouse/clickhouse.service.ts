import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClickHouseClient, createClient } from '@clickhouse/client';

@Injectable()
export class ClickhouseService {
  private readonly client: ClickHouseClient;

  constructor(private configService: ConfigService) {
    this.client = createClient({
      host: this.configService.get<string>('CLICKHOUSE_HOST'),
      database: this.configService.get<string>('CLICKHOUSE_DATA'),
      username: this.configService.get<string>('CLICKHOUSE_USER'),
      password: this.configService.get<string>('CLICKHOUSE_PASSWORD'),
    });
  }

  async executeQuery(query: string, params: Record<string, any>) {
    const rows = await this.client.query({
      query: query,
      query_params: {
        ...params,
      },
      format: 'JSONEachRow',
    });

    return await rows.json();
  }

  async insertRecord(tableName: string, data: Record<string, any>) {
    return await this.client.insert({
      table: tableName,
      // structure should match the desired format, JSONEachRow in this example
      values: data,
      format: 'JSONEachRow',
    });
  }
}
