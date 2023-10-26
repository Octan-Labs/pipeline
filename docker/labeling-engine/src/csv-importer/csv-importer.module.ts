import { Module } from '@nestjs/common';
import { CsvImporterService } from './csv-importer.service';
import { ClickhouseModule } from 'src/clickhouse/clickhouse.module';

@Module({
  imports: [ClickhouseModule],
  providers: [CsvImporterService],
})
export class CsvImporterModule {}
