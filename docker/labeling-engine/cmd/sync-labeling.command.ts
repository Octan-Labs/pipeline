import { Injectable, Logger } from '@nestjs/common';
import { Command, Option } from 'nestjs-command';
import { LabelingSyncService } from 'src/labeling-sync/labeling-sync.service';
import { IsValidChainKey } from 'src/utils/chain-key-validation';
import { ChainKeyEnum } from 'src/common/enum';
import { enumValues } from 'src/utils/fns';
import { OptimizeTableService } from 'src/optimize-table/optimize-table.service';

@Injectable()
export class SyncLabelingCommand {
  constructor(
    private readonly syncLabelingService: LabelingSyncService,
    private readonly optimizeTableService: OptimizeTableService,
  ) {}
  private logger = new Logger(SyncLabelingCommand.name);

  @Command({
    command: 'sync_labeling:db',
    describe: 'sync labeling data from API Table to ClickHouse database',
  })
  async run(
    @Option({
      name: 'chain_key',
      describe: 'the chain key used for syncing',
      type: 'string',
    })
    chainKey: string,

    @Option({
      name: 'contract_datasheet_id',
      describe: '',
      type: 'string',
    })
    contractDataSheetId: string,

    @Option({
      name: 'contract_view_id',
      describe: '',
      type: 'string',
    })
    contractViewId: string,

    @Option({
      name: 'eoa_datasheet_id',
      describe: '',
      type: 'string',
    })
    eoaDataSheetId: string,

    @Option({
      name: 'eoa_view_id',
      describe: '',
      type: 'string',
    })
    eoaViewId: string,
  ) {
    if (!IsValidChainKey(chainKey)) {
      console.error(
        `Invalid chain key: ${chainKey}. Valid chain keys are: ${enumValues(
          ChainKeyEnum,
        ).join(', ')}`,
      );
      return;
    }

    // Sync the latest data
    await this.syncLabelingService.syncData(
      chainKey,
      contractDataSheetId,
      contractViewId,
      eoaDataSheetId,
      eoaViewId,
    );

    await this.optimizeTableService.optimizeTable(
      `labeling_${chainKey}_addresses`,
    );

    this.logger.log('Synchronization completed successfully!');
  }
}
