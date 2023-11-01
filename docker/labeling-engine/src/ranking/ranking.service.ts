import { Injectable } from '@nestjs/common';
import { ClickhouseService } from '../clickhouse/clickhouse.service';
import { ChainKeyEnum } from '../common/enum';
import { ADDRESS_REPUTATION_RANKING_SORT } from './dto/address-ranking-filter-option.dto';
import {
  CategoryEnum,
  PROJECT_REPUTATION_SORT,
} from './dto/project-filter-option.dto';

@Injectable()
export class RankingService {
  constructor(private readonly clickhouseService: ClickhouseService) {}

  async getAddressReputationRanking(
    chain_key: ChainKeyEnum,
    address?: string,
    sort = ADDRESS_REPUTATION_RANKING_SORT.GRS,
    order = 'DESC',
    take = 50,
  ) {
    const chainPrefix = this.mappingChainKeyWithPrefix(chain_key);
    let orderBy = `${sort} ${order}`;
    let whereClause = '';
    let tableName = `sorting_${chainPrefix}_top_reputations`;

    const params = {};

    if (address) {
      whereClause = `WHERE r.address = {address: TINYTEXT}`;
      params['address'] = address;
      tableName = `${chainPrefix}_top_reputations`;
    }

    if (sort === ADDRESS_REPUTATION_RANKING_SORT.TYPE) {
      orderBy = `${sort} ${order}, reputation_score DESC`;
    }

    const query = `
        SELECT rank, r.address, lb.identity, r.is_contract, 
          r.reputation_score, r.total_txn, r.total_gas_spent, r.degree, r.total_volume
        FROM 
        (
          SELECT *
          FROM ${tableName} r
          ${whereClause}
          ORDER BY ${orderBy}
          LIMIT {take: SMALLINT}
        ) r
        LEFT JOIN labeling_${chainPrefix}_addresses lb on r.address = lb.address
      `;

    params['take'] = take;

    const addresses = await this.clickhouseService.executeQuery(query, params);
    return {
      data: addresses,
      meta: {
        chain_key: chain_key,
        last_calculated_at: await this.getLastUpdated(chain_key),
        total_addresses: await this.countTotalAddress(chain_key),
      },
    };
  }

  async getProjectRankingReputation(
    chain_key: ChainKeyEnum,
    ids?: string[],
    keyword?: string,
    category = 'All',
    sort?: PROJECT_REPUTATION_SORT,
    order?: string,
    page = 1,
    take = 50,
  ) {
    const chainPrefix = this.mappingChainKeyWithPrefix(chain_key);
    const offset = (page - 1) * take;

    // parameters for query
    const queryParams: Record<string, any> = {
      category,
      limit: take,
      offset,
    };

    // parameters for count query
    const countQueryParams: Record<string, any> = {
      category,
    };

    if (keyword) {
      queryParams.keyword = keyword;
      countQueryParams.keyword = keyword;
    }

    if (ids) {
      queryParams.ids = ids;
      countQueryParams.ids = ids;
    }

    const query = `SELECT 
      row_number() OVER (ORDER BY ${sort} ${order}) as rank,
      project_id,
      project_name,
      project_category,
      total_contract,
      total_grs,
      total_txn,
      total_gas,
      total_degree,
      total_tx_volume
      FROM ${chainPrefix}_projects r
      WHERE project_category = {category: TINYTEXT}
      ${
        keyword
          ? `AND project_name ILIKE CONCAT('%', {keyword: TINYTEXT}, '%')`
          : ''
      }
      ${ids ? `AND project_id IN {ids: Array(TINYTEXT)}` : ''}
      ORDER BY ${sort} ${order}
      LIMIT {limit: int}
      OFFSET {offset: int}`;

    const countQuery = `SELECT COUNT(DISTINCT p.project_id) AS total_project
      FROM ${chainPrefix}_projects p
      WHERE p.project_id IS NOT NULL
      ${
        category != CategoryEnum.ALL
          ? `AND p.project_category = {category: TINYTEXT}`
          : ''
      }
      ${
        keyword
          ? `AND p.project_name ILIKE CONCAT('%', {keyword: TINYTEXT}, '%')`
          : ''
      }
      ${ids ? `AND p.project_id IN ({ids: Array(TINYTEXT)})` : ''}`;

    // execute queries
    const [projects, countQueryResult] = await Promise.all([
      this.clickhouseService.executeQuery(query, queryParams),
      this.clickhouseService.executeQuery(countQuery, countQueryParams),
    ]);

    const totalCount = +countQueryResult[0].total_project;

    return {
      data: projects,
      meta: {
        chain_key: chain_key,
        last_calculated_at: await this.getLastUpdated(chain_key),
        page,
        take,
        totalCount,
        pageCount: Math.ceil(totalCount / take),
        hasNextPage: totalCount > offset + take,
      },
    };
  }

  async getLastUpdated(chain_key: string) {
    const chainPrefix = this.mappingChainKeyWithPrefix(chain_key);
    const query = `SELECT max(date) AS last_calculated_at FROM ${chainPrefix}_reputations`;
    const result = await this.clickhouseService.executeQuery(query, {});
    return result[0].last_calculated_at;
  }

  async countTotalAddress(chain_key: string) {
    const chainPrefix = this.mappingChainKeyWithPrefix(chain_key);
    const query = `SELECT COUNT(*) AS total_address FROM ${chainPrefix}_top_reputations`;
    const result = await this.clickhouseService.executeQuery(query, {});
    return result[0].total_address;
  }

  private mappingChainKeyWithPrefix(chainKey: string): string {
    let prefix: string;
    switch (chainKey) {
      case ChainKeyEnum['Ethereum']:
        prefix = 'eth';
        break;
      case ChainKeyEnum['BNB Smart Chain']:
        prefix = 'bnb';
        break;
      case ChainKeyEnum['Arbitrum']:
        prefix = 'arb';
        break;
      case ChainKeyEnum['Polygon']:
        prefix = 'matic';
        break;
      case ChainKeyEnum['Tron']:
        prefix = 'trx';
        break;
      case ChainKeyEnum['Ripple']:
        prefix = 'xrp';
        break;
      case ChainKeyEnum['Aurora']:
        prefix = 'aurora';
        break;
      case ChainKeyEnum['Base']:
        prefix = 'base';
        break;
      case ChainKeyEnum['Neo']:
        prefix = 'neo';
        break;
      default:
        prefix = 'eth';
        break;
    }
    return prefix;
  }
}
