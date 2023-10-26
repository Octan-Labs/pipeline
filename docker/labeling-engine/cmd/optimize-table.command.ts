import { Injectable, Logger } from '@nestjs/common';
import { Command, Option } from 'nestjs-command';
import { ChainKeyEnum } from 'src/common/enum';
import { OptimizeTableService } from 'src/optimize-table/optimize-table.service';
import { IsValidChainKey } from 'src/utils/chain-key-validation';
import { enumValues } from 'src/utils/fns';

@Injectable()
export class OptimizeTableCommand {
  constructor(private readonly optimizeTableService: OptimizeTableService) {}
  private readonly logger = new Logger(OptimizeTableService.name);
  @Command({
    command: 'optimize_table:db',
    describe:
      'optimize reputation tables and calculate project reputation ranking',
  })
  async run(
    @Option({
      name: 'chain_key',
      describe: 'the chain key used for syncing',
      type: 'string',
    })
    chainKey: string,
  ) {
    if (!IsValidChainKey(chainKey)) {
      console.error(
        `Invalid chain key: ${chainKey}. Valid chain keys are: ${enumValues(
          ChainKeyEnum,
        ).join(', ')}`,
      );
      return;
    }

    await this.optimizeTableService.optimizeTable(
      `${chainKey}_top_reputations`,
    );

    await this.optimizeTableService.syncSortingTable(chainKey);
    await this.optimizeTableService.syncProjectTable(chainKey);

    this.logger.log(
      `Finished synchronizing sorting_${chainKey}_top_reputations table, labeling_${chainKey}_projects table`,
    );
  }
}
