import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  APITable,
  IGetRecordsResponseData,
  IHttpResponse,
  IRecord,
} from 'apitable';
import { ClickhouseService } from 'src/clickhouse/clickhouse.service';
import { API_TABLE_DEFAULT_PAGE_SIZE } from 'src/common/const';

@Injectable()
export class LabelingSyncService {
  constructor(
    private readonly clickHouseService: ClickhouseService,
    private configService: ConfigService,
  ) {
    this.apiTable = new APITable({
      host: this.configService.get<string>('API_TABLE_HOST'),
      token: this.configService.get<string>('API_TABLE_TOKEN'),
      fieldKey: 'name',
    });
  }

  private apiTable: APITable;
  private readonly logger = new Logger(LabelingSyncService.name);

  async syncData(
    chainKey: string,
    contractDataSheetId: string,
    contractViewId: string,
    eoaDataSheetId: string,
    eoaViewId: string,
  ) {
    // sync all labeling data into one ClickHouse table (labeling_${chainKey}_addresses)
    await this.syncLabelingContractData(
      chainKey,
      contractDataSheetId,
      contractViewId,
    );
    await this.syncLabelingEoaData(chainKey, eoaDataSheetId, eoaViewId);
    this.logger.log(`Synchronization for labeling data ${chainKey} completed.`);
  }

  async syncLabelingContractData(
    chainKey: string,
    contractDataSheetId: string,
    contractViewId: string,
  ) {
    const records = await this.fetchLabelingDataFromAPITable(
      contractDataSheetId,
      contractViewId,
    );

    if (!records) return;

    // Insert the data into ClickHouse table
    const entities = records
      .filter((record) => {
        const { project_name } = record.fields;
        return (
          project_name &&
          project_name[0] !== 'Unknown' &&
          project_name[0] !== 'Unnamed'
        );
      })
      .map((record) => {
        return {
          id: record.recordId,
          address: record.fields.address,
          identity: record.fields.identity,
          category: record.fields.category,
          is_contract: true,
          created_at: new Date(record['createdAt'])
            .toISOString()
            .slice(0, 19)
            .replace('T', ' '),
          updated_at: new Date(record['updatedAt'])
            .toISOString()
            .slice(0, 19)
            .replace('T', ' '),
          explorer_tagname: record.fields.explorer_tagname,
          explorer_contractname: record.fields.explorer_contractname,
          crawled_oklink: record.fields.crawled_oklink ?? '',
          note: record.fields.note ?? '',
          safety: record.fields.safety ?? '',
          version: record.fields.version ?? '',
          project_id: record.fields.project ? record.fields.project[0] : null,
          project_name: record.fields.project_name[0],
        };
      });

    // Query to insert new records and replace existing ones with newer versions
    // Assuming the updated_at field is being used as the version for the ReplacingMergeTree engine
    await this.clickHouseService.insertRecord(
      `labeling_${chainKey}_addresses`,
      entities,
    );
  }

  async syncLabelingEoaData(
    chainKey: string,
    eoaDataSheetId: string,
    eoaViewId: string,
  ) {
    const records = await this.fetchLabelingDataFromAPITable(
      eoaDataSheetId,
      eoaViewId,
    );

    if (!records) return;

    const entities = records.map((record) => {
      return {
        id: record.recordId,
        address: record.fields.address,
        identity: record.fields.identity,
        category: '',
        is_contract: false,
        created_at: new Date(record['createdAt'])
          .toISOString()
          .slice(0, 19)
          .replace('T', ' '),
        updated_at: new Date(record['updatedAt'])
          .toISOString()
          .slice(0, 19)
          .replace('T', ' '),
        explorer_tagname: record.fields.explorer_tagname,
        explorer_contractname: '',
        crawled_oklink: record.fields.crawled_oklink ?? '',
        note: record.fields.note ?? '',
        safety: '',
        version: '',
        project_name: null,
        project_id: null,
      };
    });

    // Query to insert new records and replace existing ones with newer versions
    // Assuming the updated_at field is being used as the version for the ReplacingMergeTree engine
    await this.clickHouseService.insertRecord(
      `labeling_${chainKey}_addresses`,
      entities,
    );
  }

  private async fetchLabelingDataFromAPITable(
    dataSheetId: string,
    viewId: string,
  ): Promise<IRecord[]> {
    const pageSize = API_TABLE_DEFAULT_PAGE_SIZE;
    let allRecords = [];
    let pageNum = 1;
    let offset = 0;
    let totalRecords = 0;

    do {
      try {
        const recordsResponse = await this.apiTable
          .datasheet(dataSheetId)
          .records.query({
            viewId: viewId,
            pageSize: pageSize,
            pageNum: pageNum,
          });
        this.logger.log(
          `Fetched ${recordsResponse.data.pageSize} records data from APITable, total = ${recordsResponse.data.total}, code=${recordsResponse.code}, success=${recordsResponse.success}, message=${recordsResponse.message} `,
        );

        const records = recordsResponse.data.records;

        // If no records are returned in the response, break out of the loop
        if (records.length === 0) {
          break;
        }

        allRecords = allRecords.concat(records);

        // Update the pageNum for the next request
        pageNum += 1;
        offset = (pageNum - 1) * pageSize;

        // Update the totalRecords from the first response
        if (totalRecords === 0) {
          totalRecords = recordsResponse.data.total;
        }
      } catch (error) {
        this.logger.error(
          `Error fetching records data from APITable, error = ${error}`,
        );
        return error;
      }
    } while (offset < totalRecords);

    return allRecords;
  }
}
