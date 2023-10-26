import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Transform, Type } from 'class-transformer';
import { IsArray, IsEnum, IsIn, IsOptional, MaxLength } from 'class-validator';
import { KEYWORD_MAX_LENGTH } from 'src/common/const';
import { ChainKeyEnum } from 'src/common/enum';
import { PagingOptions } from 'src/common/pagging-options';
import { enumValues } from 'src/utils/fns';

export enum PROJECT_REPUTATION_SORT {
  TOTAL_GRS = 'total_grs',
  TOTAL_TXN = 'total_txn',
  TOTAL_GAS = 'total_gas',
  TOTAL_DEGREE = 'total_degree',
  TOTAL_TX_VOLUME = 'total_tx_volume',
}

export enum CategoryEnum {
  ALL = 'All',
  DEFI = 'DeFi',
  GAMEFI = 'GameFi',
  SOCIALFI = 'SocialFi',
  NFT = 'NFT',
  L2S = 'L2s',
  DAO = 'DAO',
}

export class ProjectRankingFilterOptions extends PagingOptions {
  @ApiProperty({
    enum: enumValues(ChainKeyEnum),
    example: ChainKeyEnum['Ethereum'],
    default: ChainKeyEnum['Ethereum'],
  })
  @IsEnum(ChainKeyEnum)
  chain_key: ChainKeyEnum;

  @ApiProperty({
    isArray: true,
  })
  @IsArray()
  @IsOptional()
  @Transform(({ value }) => (Array.isArray(value) ? value : [value]))
  ids: string[];

  @ApiPropertyOptional({
    enum: enumValues(CategoryEnum),
    example: CategoryEnum.ALL,
  })
  @IsIn(enumValues(CategoryEnum), {
    message: (args) =>
      `${args.property} must be one of the following values: ${enumValues(
        CategoryEnum,
      )}`,
  })
  @IsOptional()
  category?: string;

  @ApiPropertyOptional()
  @IsOptional()
  @Type(() => String)
  @MaxLength(KEYWORD_MAX_LENGTH)
  keyword?: string;

  @ApiPropertyOptional({
    enum: enumValues(PROJECT_REPUTATION_SORT),
    example: PROJECT_REPUTATION_SORT.TOTAL_GRS,
    default: PROJECT_REPUTATION_SORT.TOTAL_GRS,
  })
  @IsIn(enumValues(PROJECT_REPUTATION_SORT))
  @IsOptional()
  sort?: PROJECT_REPUTATION_SORT;

  @ApiPropertyOptional({
    enum: ['ASC', 'DESC'],
    example: 'DESC',
  })
  @IsIn(['ASC', 'DESC'])
  @IsOptional()
  order?: string;
}
