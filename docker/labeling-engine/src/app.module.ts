import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ReputationModule } from './reputation/reputation.module';
import { CsvImporterModule } from './csv-importer/csv-importer.module';
import { RankingModule } from './ranking/ranking.module';
import { ClickhouseModule } from './clickhouse/clickhouse.module';
import { ImportCsvCommand } from 'cmd/import-csv.command';
import { CsvImporterService } from './csv-importer/csv-importer.service';
import { CommandModule } from 'nestjs-command';
import { ConfigModule } from '@nestjs/config';
import { SyncLabelingCommand } from '../cmd/sync-labeling.command';
import { LabelingSyncService } from './labeling-sync/labeling-sync.service';
import { OptimizeTableCommand } from 'cmd/optimize-table.command';
import { OptimizeTableService } from './optimize-table/optimize-table.service';
import { S3Module } from './s3/s3.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      envFilePath: '.env',
    }),
    CommandModule,
    ReputationModule,
    RankingModule,
    CsvImporterModule,
    ClickhouseModule,
    S3Module,
  ],
  controllers: [AppController],
  providers: [
    AppService,
    ImportCsvCommand,
    SyncLabelingCommand,
    OptimizeTableCommand,
    CsvImporterService,
    LabelingSyncService,
    OptimizeTableService,
  ],
  exports: [CommandModule],
})
export class AppModule {}
