import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import {
  IsEnum,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';
import {
  PAGINATION_OPTIONS_DEFAULT_LIMIT,
  PAGINATION_OPTIONS_DEFAULT_MAX_LIMIT,
  PAGINATION_OPTIONS_DEFAULT_MIN_LIMIT,
} from 'src/common/const';
import { ChainKeyEnum } from 'src/common/enum';
import { enumValues } from 'src/utils/fns';

export enum ADDRESS_REPUTATION_RANKING_SORT {
  GRS = 'reputation_score',
  TYPE = 'is_contract',
  TXN = 'total_txn',
  GAS = 'total_gas_spent',
  DEGREE = 'degree',
  TX_VOLUME = 'total_volume',
}

export class AddressRankingFilterOptions {
  @ApiProperty({
    enum: enumValues(ChainKeyEnum),
    example: ChainKeyEnum['Ethereum'],
    default: ChainKeyEnum['Ethereum'],
  })
  @IsEnum(ChainKeyEnum)
  chain_key: ChainKeyEnum;

  @ApiPropertyOptional({
    type: String,
    example: '0x0000000000000000000000000000000000000000',
  })
  address?: string;

  @ApiPropertyOptional({
    enum: enumValues(ADDRESS_REPUTATION_RANKING_SORT),
    example: ADDRESS_REPUTATION_RANKING_SORT.GRS,
    default: ADDRESS_REPUTATION_RANKING_SORT.GRS,
  })
  @IsIn(enumValues(ADDRESS_REPUTATION_RANKING_SORT))
  @IsOptional()
  sort?: ADDRESS_REPUTATION_RANKING_SORT;

  @ApiPropertyOptional({
    enum: ['ASC', 'DESC'],
    example: 'DESC',
  })
  @IsIn(['ASC', 'DESC'])
  @IsOptional()
  order?: string;

  @ApiPropertyOptional({
    maximum: PAGINATION_OPTIONS_DEFAULT_MAX_LIMIT,
    default: PAGINATION_OPTIONS_DEFAULT_LIMIT,
  })
  @Type(() => Number)
  @IsInt()
  @Min(PAGINATION_OPTIONS_DEFAULT_MIN_LIMIT)
  @Max(PAGINATION_OPTIONS_DEFAULT_MAX_LIMIT)
  @IsOptional()
  readonly take?: number = PAGINATION_OPTIONS_DEFAULT_LIMIT;
}
