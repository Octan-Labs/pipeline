import { Injectable } from '@nestjs/common';
import { Command, Option, Positional } from 'nestjs-command';
import { ChainKeyEnum } from 'src/common/enum';
import { CsvImporterService } from 'src/csv-importer/csv-importer.service';
import { S3Service } from 'src/s3/s3.service';
import { IsValidChainKey } from 'src/utils/chain-key-validation';
import { enumValues } from 'src/utils/fns';

@Injectable()
export class ImportCsvCommand {
  constructor(
    private readonly importerService: CsvImporterService,
    private readonly s3Service: S3Service,
  ) {}

  @Command({
    command: 'import:csv',
    describe: 'import data from a CSV file into a ClickHouse table',
    // autoExit: true,
  })
  async run(
    @Option({
      name: 'chainName',
      describe: 'the name of the Chain',
      type: 'string',
      required: true,
    })
    chainName: string,
    @Option({
      name: 'filePath',
      describe: 'the path to the CSV file',
      type: 'string',
      required: true,
    })
    filePath: string,

    @Option({
      name: 's3Url',
      describe: 'the S3 url to download the CSV file',
      type: 'string',
    })
    s3Url: string,

    @Option({
      name: 'chunkSize',
      describe: 'chunk size',
      type: 'number',
      default: 1000,
    })
    chunkSize: number,
  ) {
    if (!IsValidChainKey(chainName)) {
      console.error(
        `Invalid chain key: ${chainName}. Valid chain keys are: ${enumValues(
          ChainKeyEnum,
        ).join(', ')}`,
      );
      return;
    }

    if (s3Url) {
      try {
        await this.s3Service.downloadAndSaveFile(s3Url, filePath);
        console.log(
          `File downloaded successfully from ${s3Url} to ${filePath}`,
        );
      } catch (error) {
        console.error('Error:', error.message);
        process.exit(1);
      }
    }

    await this.importerService.importCsvToClickHouse(
      chainName,
      filePath,
      chunkSize,
    );
  }
}
