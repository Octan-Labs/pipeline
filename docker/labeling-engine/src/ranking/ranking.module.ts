import { Module } from '@nestjs/common';
import { RankingController } from './ranking.controller';
import { ClickhouseModule } from '../clickhouse/clickhouse.module';
import { RankingService } from './ranking.service';

@Module({
  imports: [ClickhouseModule],
  controllers: [RankingController],
  providers: [RankingService],
  exports: [RankingService],
})
export class RankingModule {}
