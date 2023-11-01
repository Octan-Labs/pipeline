import { Injectable } from '@nestjs/common';
import { ClickhouseService } from '../clickhouse/clickhouse.service';
import { RankingService } from '../ranking/ranking.service';
import { DataKeyEnum } from './dto/get-address-dashboard.dto';
import { ChainKeyEnum } from 'src/common/enum';

@Injectable()
export class ReputationService {
  constructor(
    private readonly clickhouseService: ClickhouseService,
    private readonly rankingService: RankingService,
  ) {}

  async getTimeSeriesData(
    addresses: string[],
    chain_key: string,
    data_key: DataKeyEnum,
  ) {
    if (!addresses || addresses.length === 0) {
      throw new Error('At least one address is required');
    }

    if (
      chain_key !== ChainKeyEnum['XRP'] ||
      chain_key !== ChainKeyEnum['Tron']
    ) {
      addresses = addresses.map((addr) => {
        return addr.toLowerCase();
      });
    }

    const chainPrefix = chain_key.toLowerCase();

    let selectSQL = '';
    switch (data_key) {
      case DataKeyEnum['TotalReputationScore']:
        selectSQL = ', reputation_score';
        break;
      case DataKeyEnum['TotalDegree']:
        selectSQL = ', degree';
        break;
      case DataKeyEnum['TotalTxn']:
        selectSQL = ', total_txn';
        break;
      case DataKeyEnum['TotalGasSpent']:
        selectSQL = ', total_gas_spent';
        break;
      case DataKeyEnum['UniqueActiveWallet']:
        selectSQL = ', uaw as unique_active_wallet';
        break;
      default:
        break;
    }

    const query = `SELECT address, reputation_score, date ${selectSQL}
    FROM ${chainPrefix}_reputations 
    WHERE address IN ({addresses: Array(TINYTEXT)})
    LIMIT 1000`;

    // execute query with parameters.
    const data = await this.clickhouseService.executeQuery(query, {
      addresses,
    });

    return {
      data: data,
      meta: {
        chain_key: chain_key,
        last_calculated_at: await this.rankingService.getLastUpdated(chain_key),
      },
    };
  }

  async getLatestReputationScore(addresses: string[], chain_key: string) {
    if (!addresses || addresses.length === 0) {
      throw new Error('At least one address is required');
    }

    if (
      chain_key !== ChainKeyEnum['XRP'] ||
      chain_key !== ChainKeyEnum['Tron']
    ) {
      addresses = addresses.map((addr) => {
        return addr.toLowerCase();
      });
    }

    const chainPrefix = chain_key.toLowerCase();

    const query = `SELECT address, reputation_score, date
    FROM ${chainPrefix}_top_reputations
    WHERE address IN ({addresses: Array(TINYTEXT)})
    LIMIT 1000`;

    // execute query with parameters.
    const data = await this.clickhouseService.executeQuery(query, {
      addresses,
    });

    return {
      data: data,
      meta: {
        chain_key: chain_key,
        last_calculated_at: await this.rankingService.getLastUpdated(chain_key),
      },
    };
  }
}
