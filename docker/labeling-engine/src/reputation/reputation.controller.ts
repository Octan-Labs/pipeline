import { Controller, Get, Query, ValidationPipe } from '@nestjs/common';
import { ReputationService } from './reputation.service';
import { ApiTags } from '@nestjs/swagger';
import {
  GetLatestReputationScoreDto,
  GetTimeseriesDataDto,
} from './dto/get-reputation-score.dto';

@ApiTags('Reputation')
@Controller('reputation')
export class ReputationController {
  constructor(private readonly reputationService: ReputationService) {}

  @Get('timeseries')
  async getTimeSeriesData(
    @Query(new ValidationPipe({ transform: true }))
    query: GetTimeseriesDataDto,
  ) {
    const { addresses, chain_key, data_key } = query;

    return await this.reputationService.getTimeSeriesData(
      addresses,
      chain_key,
      data_key,
    );
  }

  @Get('latest')
  async getLatestReputationScore(
    @Query(new ValidationPipe({ transform: true }))
    query: GetLatestReputationScoreDto,
  ) {
    const { addresses, chain_key } = query;
    return await this.reputationService.getLatestReputationScore(
      addresses,
      chain_key,
    );
  }
}
