import { ApiProperty } from '@nestjs/swagger';
import { Transform, Type } from 'class-transformer';
import { IsArray, IsEnum, IsNotEmpty, IsString } from 'class-validator';
import { ChainKeyEnum } from 'src/common/enum';
import { enumValues } from 'src/utils/fns';

export enum DataKeyEnum {
  TotalTxn = 'total_txn',
  TotalGasSpent = 'total_gas_spent',
  TotalDegree = 'total_degree',
  TotalReputationScore = 'total_reputation_score',
  UniqueActiveWallet = 'unique_active_wallet',
}

export enum TimeframeType {
  DAY = 'day',
  WEEK = 'week',
  MONTH = 'month',
  YEAR = 'year',
}
export class GetTimeseriesDataDto {
  @ApiProperty({
    enum: enumValues(ChainKeyEnum),
    example: ChainKeyEnum['Ethereum'],
    default: ChainKeyEnum['Ethereum'],
  })
  @IsEnum(ChainKeyEnum)
  chain_key: ChainKeyEnum;

  @ApiProperty({
    isArray: true,
    example: '0x0000000000000000000000000000000000000000',
  })
  @IsArray()
  @IsNotEmpty()
  @Transform(({ value }) => (Array.isArray(value) ? value : [value]))
  addresses: string[];

  @ApiProperty({
    enum: enumValues(DataKeyEnum),
    example: DataKeyEnum['TotalReputationScore'],
    default: DataKeyEnum['TotalReputationScore'],
  })
  @IsEnum(DataKeyEnum)
  data_key: DataKeyEnum;
}

export class GetLatestReputationScoreDto {
  @ApiProperty({
    enum: enumValues(ChainKeyEnum),
    example: ChainKeyEnum['Ethereum'],
    default: ChainKeyEnum['Ethereum'],
  })
  @IsEnum(ChainKeyEnum)
  chain_key: ChainKeyEnum;

  @ApiProperty({
    isArray: true,
    example: '0x0000000000000000000000000000000000000000',
  })
  @IsArray()
  @IsNotEmpty()
  @Transform(({ value }) => (Array.isArray(value) ? value : [value]))
  addresses: string[];
}
