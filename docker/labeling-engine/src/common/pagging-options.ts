import { ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsInt, IsOptional, Max, Min } from 'class-validator';
import {
  PAGINATION_OPTIONS_DEFAULT_LIMIT,
  PAGINATION_OPTIONS_DEFAULT_MAX_LIMIT,
  PAGINATION_OPTIONS_DEFAULT_MIN_LIMIT,
  PAGINATION_OPTIONS_DEFAULT_PAGE,
} from './const';

export class PagingOptions {
  @ApiPropertyOptional({
    minimum: PAGINATION_OPTIONS_DEFAULT_PAGE,
    default: PAGINATION_OPTIONS_DEFAULT_PAGE,
  })
  @Type(() => Number)
  @IsInt()
  @Min(PAGINATION_OPTIONS_DEFAULT_PAGE)
  @IsOptional()
  readonly page?: number = PAGINATION_OPTIONS_DEFAULT_PAGE;

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
