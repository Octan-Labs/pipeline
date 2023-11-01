import { Controller, Get, Query, ValidationPipe } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { RankingService } from './ranking.service';
import { AddressRankingFilterOptions } from './dto/address-ranking-filter-option.dto';
import { ProjectRankingFilterOptions } from './dto/project-filter-option.dto';

@ApiTags('Ranking')
@Controller('rankings')
export class RankingController {
  constructor(private readonly rankingService: RankingService) {}

  @Get('addresses')
  async getAddressReputationRanking(
    @Query(new ValidationPipe({ transform: true }))
    filterOptions: AddressRankingFilterOptions,
  ) {
    const { take, chain_key, address, sort, order } = filterOptions;
    return this.rankingService.getAddressReputationRanking(
      chain_key,
      address?.toLowerCase(),
      sort,
      order,
      take,
    );
  }

  @Get('projects')
  async getProjectReputationRanking(
    @Query(new ValidationPipe({ transform: true }))
    filterOptions: ProjectRankingFilterOptions,
  ) {
    const { page, take, chain_key, ids, keyword, category, sort, order } =
      filterOptions;
    return this.rankingService.getProjectRankingReputation(
      chain_key,
      ids,
      keyword,
      category,
      sort,
      order,
      page,
      take,
    );
  }
}
