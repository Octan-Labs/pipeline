import { Module } from '@nestjs/common';
import { ReputationController } from './reputation.controller';
import { ReputationService } from './reputation.service';
import { ClickhouseService } from '../clickhouse/clickhouse.service';
import { ClickhouseModule } from '../clickhouse/clickhouse.module';
import { ConfigService } from '@nestjs/config';
import { RankingService } from 'src/ranking/ranking.service';

@Module({
  imports: [ClickhouseModule],
  controllers: [ReputationController],
  providers: [
    ReputationService,
    ClickhouseService,
    ConfigService,
    RankingService,
  ],
})
export class ReputationModule {}
