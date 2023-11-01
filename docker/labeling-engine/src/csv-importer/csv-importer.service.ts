import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import { ClickhouseService } from 'src/clickhouse/clickhouse.service';
import { Timer } from 'src/utils/timer';
import { CSVParser } from './pipe/simple-csv-parser';

@Injectable()
export class CsvImporterService {
  constructor(private readonly clickHouseService: ClickhouseService) {}
  private readonly logger = new Logger(CsvImporterService.name);

  async importCsvToClickHouse(
    chainName: string,
    importPath: string,
    chunkSize: number,
  ) {
    const args: any = {
      chainName: chainName,
      chunkData: null, // will be set every new chunk
    };
    const timer = new Timer();
    const csvParser = new CSVParser([
      'Address',
      'Rank',
      'Identity',
      'Block',
      'Date',
      'Contract',
      'Reputation',
      'Total Txn',
      'Total Transfer',
      'Total Receive',
      'Total Gas Spent',
      'Total Volume',
      'Degree',
      'In Degree',
      'Out Degree',
      'UAW',
    ]);
    const readable = fs.createReadStream(importPath).pipe(csvParser);
    let chunkData = [];
    for await (const record of readable) {
      chunkData.push(record);
      if (chunkData.length >= chunkSize) {
        args.chunkData = chunkData;
        const err = await this.trackImportChunk(chainName, chunkData);
        if (err != null) {
          this.logger.error(
            `Failed to import file, error: ${err}. Terminating...`,
          );
          return;
        }
        chunkData = [];
      }
    }

    if (chunkData.length > 0) {
      args.chunkData = chunkData;
      const err = await this.trackImportChunk(chainName, chunkData);
      if (err != null) {
        this.logger.error(
          `Failed to import file, error: ${err}. Terminating...`,
        );
        return;
      }
    }

    // await this.optimizeTable(`${chainName}_top_reputations`);
    // await this.syncSortingTable(chainName);
    // await this.syncProjectTable(chainName);

    this.logger.log(
      `Finish processing file ${importPath}. Time elapsed: ${timer.elapsedMinutes()} minutes`,
    );
  }

  private async trackImportChunk(
    chainName: string,
    chunkData: any[],
  ): Promise<any> {
    try {
      const timer = new Timer();
      const imported_records = await this.processChunk(chainName, chunkData);
      this.logger.log(
        `imported_records=${imported_records} chunkSize=${
          chunkData.length
        } timestamp=${new Date()} time_elapsed=${timer.elapsedSeconds()} seconds`,
      );
    } catch (error) {
      return error;
    }
  }

  async processChunk(chainName: string, chunkData: any[]): Promise<number> {
    const entities = chunkData.map((rawData) => {
      let address = rawData['Address'];
      if (
        !(chainName === 'trx' || chainName === 'xrp' || chainName === 'neo')
      ) {
        address = rawData['Address'].toLowerCase();
      }
      return {
        address: address,
        block: rawData['Block'] ? +rawData['Block'] : null,
        is_contract: rawData['Contract'] ? rawData['Contract'] === 'Yes' : null,
        date: rawData['Date'] ? rawData['Date'] : null,
        reputation_score: +rawData['Reputation'],
        degree: +rawData['Degree'],
        in_degree: +rawData['In Degree'],
        out_degree: +rawData['Out Degree'],
        rank: +rawData['Rank'],
        total_gas_spent: +rawData['Total Gas Spent'],
        total_receive: +rawData['Total Receive'],
        total_transfer: +rawData['Total Transfer'],
        total_txn: +rawData['Total Txn'],
        total_volume: rawData['Total Volume'] ? +rawData['Total Volume'] : null,
        uaw: rawData['UAW'] ? +rawData['UAW'] : null,
      };
    });
    await this.clickHouseService.insertRecord(
      `${chainName}_reputations`,
      entities,
    );
    return chunkData.length;
  }
}
