# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
from time import time

from blockchainetl.jobs.exporters.fsspec_item_exporter import FsspecItemExporter
from evmetl.cli.cli_utils import ZERO_PADDING
from evmetl.enumeration.entity_type import EntityType
from evmetl.providers.auto import get_provider_from_uri
from blockchainetl.thread_local_proxy import ThreadLocalProxy
from evmetl.streaming.eth_streamer_adapter import EthStreamerAdapter
from evmetl.mappers import block_mapper, transaction_mapper, receipt_log_mapper, contract_mapper, token_mapper, token_transfer_mapper, trace_mapper

logger = logging.getLogger('export_all')

def export_all_common(partitions, output_dir, provider_uri, max_workers, batch_size, chain, entity_types):
    for batch_start_block, batch_end_block, partition_dir in partitions:
        # # # start # # #

        start_time = time()

        padded_batch_start_block = str(batch_start_block).zfill(ZERO_PADDING)
        padded_batch_end_block = str(batch_end_block).zfill(ZERO_PADDING)
        
        block_range = '{padded_batch_start_block}-{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
        
        file_name_suffix = '{padded_batch_start_block}_{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
    
        all_file_mapping = {
            EntityType.BLOCK: '{output_dir}/blocks{partition_dir}/blocks_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.TRANSACTION: '{output_dir}/transactions{partition_dir}/transactions_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.LOG: '{output_dir}/logs{partition_dir}/logs_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.TOKEN_TRANSFER: '{output_dir}/token_transfers{partition_dir}/token_transfers_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.TRACE: '{output_dir}/traces{partition_dir}/traces_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.CONTRACT: '{output_dir}/contracts{partition_dir}/contracts_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
            EntityType.TOKEN: '{output_dir}/tokens{partition_dir}/tokens_{file_name_suffix}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
                file_name_suffix=file_name_suffix,
            ),
        }

        all_mapper_mapping = {
            EntityType.BLOCK: block_mapper.EthBlockMapper,
            EntityType.TRANSACTION: transaction_mapper.EthTransactionMapper,
            EntityType.LOG: receipt_log_mapper.EthReceiptLogMapper,
            EntityType.TOKEN_TRANSFER: token_transfer_mapper.EthTokenTransferMapper,
            EntityType.TRACE: trace_mapper.EthTraceMapper,
            EntityType.CONTRACT: contract_mapper.EthContractMapper,
            EntityType.TOKEN: token_mapper.EthTokenMapper,
        }

        filename_without_ext_mapping = {}
        mapper_mapping = {}

        for entity_type in entity_types:
            filename_without_ext_mapping[entity_type] = all_file_mapping[entity_type]
            mapper_mapping[entity_type] = all_mapper_mapping[entity_type]

        streamer_adapter = EthStreamerAdapter(
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            batch_size=batch_size,
            item_exporter=FsspecItemExporter(
                filename_without_ext_mapping=filename_without_ext_mapping,
                mapper_mapping=mapper_mapping
            ),
            max_workers=max_workers,
            entity_types=tuple(entity_types),
            calculate_offset=False,
        )

        streamer_adapter.open()
        streamer_adapter.export_all(batch_start_block, batch_end_block)
        streamer_adapter.close()
        
        # # # finish # # #
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting entity {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))
