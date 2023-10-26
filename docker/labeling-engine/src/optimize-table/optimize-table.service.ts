import { Injectable } from '@nestjs/common';
import { ClickhouseService } from '../clickhouse/clickhouse.service';

@Injectable()
export class OptimizeTableService {
  constructor(private readonly clickHouseService: ClickhouseService) {}

  async optimizeTable(tableName: string) {
    // Remove duplication records
    await this.clickHouseService.executeQuery(
      `OPTIMIZE TABLE ${tableName} FINAL;`,
      {},
    );
  }

  async syncSortingTable(chainName: string) {
    const topTableName = `${chainName}_top_reputations`;
    // Truncate the table
    await this.clickHouseService.executeQuery(
      `TRUNCATE TABLE sorting_${topTableName}`,
      {},
    );

    // Fields you want to sort by
    const fieldsToSort = [
      'reputation_score',
      'total_txn',
      'total_volume',
      'total_gas_spent',
      'is_contract',
      'degree',
    ];

    const limitSorting = 100;

    // Building the subquery for each field
    const subqueries = [];
    for (const field of fieldsToSort) {
      subqueries.push(
        `SELECT * FROM ${topTableName} ORDER BY ${field} ASC LIMIT ${limitSorting}`,
        `SELECT * FROM ${topTableName} ORDER BY ${field} DESC LIMIT ${limitSorting}`,
      );
    }

    // Combining the subqueries
    const unionQuery = subqueries.join(' UNION ALL ');

    // Final query
    const query = `INSERT INTO sorting_${topTableName} (address, rank, date, reputation_score, total_txn, total_volume, total_gas_spent, is_contract, degree) 
                   SELECT DISTINCT address, rank, date, reputation_score, total_txn, total_volume, total_gas_spent, is_contract, degree 
                   FROM (${unionQuery})`;

    // Execute the query
    await this.clickHouseService.executeQuery(query, {});

    await this.optimizeTable(`sorting_${chainName}_top_reputations`);
  }

  async syncProjectTable(chainName: string) {
    await this.clickHouseService.executeQuery(
      `INSERT INTO ${chainName}_projects WITH Detail AS (
          SELECT lb.project_id as project_id,
            lb.project_name as project_name,
            lb.category as project_category,
            COUNT(lb.identity) as total_contract,
            SUM(etr.reputation_score) AS total_grs,
            SUM(etr.total_txn) as total_txn,
            SUM(etr.total_gas_spent) as total_gas,
            SUM(etr.degree) as total_degree,
            SUM(etr.total_volume) as total_tx_volume
          FROM ${chainName}_top_reputations etr
            INNER JOIN labeling_${chainName}_addresses lb on etr.address = lb.address
          WHERE lb.project_id IS NOT NULL
          GROUP BY project_id,
            project_name,
            project_category
        )
      SELECT row_number() OVER (
          ORDER BY total_grs DESC
        ) as rank,
        project_id,
        project_name,
        project_category,
        total_contract,
        total_grs,
        total_txn,
        total_gas,
        total_degree,
        total_tx_volume,
        NOW() as updated_at
      FROM Detail
      UNION ALL
      SELECT row_number() OVER (
          ORDER BY total_grs DESC
        ) as rank,
        project_id,
        project_name,
        'All' as project_category,
        SUM(total_contract) as total_contract,
        SUM(total_grs) AS total_grs,
        SUM(total_txn) as total_txn,
        SUM(total_gas) as total_gas,
        SUM(total_degree) as total_degree,
        SUM(total_tx_volume) as total_tx_volume,
        NOW() as updated_at
      FROM Detail
      GROUP BY project_id,
        project_name`,
      {},
    );

    await this.optimizeTable(`${chainName}_projects`);
  }
}
