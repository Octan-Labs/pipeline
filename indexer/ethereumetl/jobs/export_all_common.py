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


import csv
import logging
from time import time

from ethereumetl.csv_utils import set_max_field_size_limit
from blockchainetl.file_utils import smart_open, rm
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.exporters.blocks_and_transactions_item_exporter import blocks_and_transactions_item_exporter
from ethereumetl.jobs.exporters.receipts_and_logs_item_exporter import receipts_and_logs_item_exporter
from ethereumetl.jobs.exporters.token_transfers_item_exporter import token_transfers_item_exporter
from ethereumetl.jobs.exporters.contracts_item_exporter import contracts_item_exporter
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.export_contracts_job import ExportContractsJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3
from ethereumetl.enumeration.entity_type import EntityType


logger = logging.getLogger('export_all')


def is_log_filter_supported(provider_uri):
    return 'infura' not in provider_uri


def extract_csv_column_unique(input, output, column):
    set_max_field_size_limit()

    with smart_open(input, 'r') as input_file, smart_open(output, 'w') as output_file:
        reader = csv.DictReader(input_file)
        seen = set()  # set for fast O(1) amortized lookup
        for row in reader:
            if row[column] in seen:
                continue
            seen.add(row[column])
            output_file.write(row[column] + '\n')


def export_all_common(partitions, output_dir, provider_uri, max_workers, batch_size, chain, export_traces):
    for batch_start_block, batch_end_block, partition_dir in partitions:
        # # # start # # #

        start_time = time()

        padded_batch_start_block = str(batch_start_block).zfill(8)
        padded_batch_end_block = str(batch_end_block).zfill(8)
        block_range = '{padded_batch_start_block}-{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
        file_name_suffix = '{padded_batch_start_block}_{padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )

        # # # blocks_and_transactions # # #

        blocks_output_dir = '{output_dir}/blocks{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )

        transactions_output_dir = '{output_dir}/transactions{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )

        blocks_file = '{blocks_output_dir}/blocks_{file_name_suffix}.csv'.format(
            blocks_output_dir=blocks_output_dir,
            file_name_suffix=file_name_suffix,
        )
        transactions_file = '{transactions_output_dir}/transactions_{file_name_suffix}.csv'.format(
            transactions_output_dir=transactions_output_dir,
            file_name_suffix=file_name_suffix,
        )
        logger.info('Exporting blocks {block_range} to {blocks_file}'.format(
            block_range=block_range,
            blocks_file=blocks_file,
        ))
        logger.info('Exporting transactions from blocks {block_range} to {transactions_file}'.format(
            block_range=block_range,
            transactions_file=transactions_file,
        ))

        blocks_trans_in_memory_exporter = InMemoryItemExporter(item_types=[EntityType.BLOCK, EntityType.TRANSACTION])

        job = ExportBlocksJob(
            start_block=batch_start_block,
            end_block=batch_end_block,
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            max_workers=max_workers,
            item_exporter=blocks_trans_in_memory_exporter,
            export_blocks=True,
            export_transactions=True)
        
        job.run()

        blocks = blocks_trans_in_memory_exporter.get_items(EntityType.BLOCK)
        transactions = blocks_trans_in_memory_exporter.get_items(EntityType.TRANSACTION)
        blocks_trans_file_exporter = blocks_and_transactions_item_exporter(blocks_file, transactions_file)
        blocks_trans_file_exporter.open()
        blocks_trans_file_exporter.export_items(blocks + transactions)
        blocks_trans_file_exporter.close()

        # # # receipts_and_logs # # #

        receipts_output_dir = '{output_dir}/receipts{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )

        logs_output_dir = '{output_dir}/logs{partition_dir}'.format(
            output_dir=output_dir,
            partition_dir=partition_dir,
        )

        receipts_file = '{receipts_output_dir}/receipts_{file_name_suffix}.csv'.format(
            receipts_output_dir=receipts_output_dir,
            file_name_suffix=file_name_suffix,
        )

        logs_file = '{logs_output_dir}/logs_{file_name_suffix}.csv'.format(
            logs_output_dir=logs_output_dir,
            file_name_suffix=file_name_suffix,
        )

        logger.info('Exporting receipts and logs from blocks {block_range} to {receipts_file} and {logs_file}'.format(
            block_range=block_range,
            receipts_file=receipts_file,
            logs_file=logs_file,
        ))

        receipts_logs_in_memory_exporter = InMemoryItemExporter(item_types=[EntityType.RECEIPT, EntityType.LOG])

        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction['hash'] for transaction in transactions),
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
            max_workers=max_workers,
            item_exporter=receipts_logs_in_memory_exporter,
            export_receipts=True,
            export_logs=True
        )
        job.run()

        receipts = receipts_logs_in_memory_exporter.get_items(EntityType.RECEIPT)
        logs = receipts_logs_in_memory_exporter.get_items(EntityType.LOG)
        receipts_and_logs_file_exporter = receipts_and_logs_item_exporter(receipts_file, logs_file)
        receipts_and_logs_file_exporter.open()
        receipts_and_logs_file_exporter.export_items(receipts + logs)
        receipts_and_logs_file_exporter.close()


        # # # token_transfers # # #

        token_transfers_file = None
        if is_log_filter_supported(provider_uri):
            token_transfers_output_dir = '{output_dir}/token_transfers{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )

            token_transfers_file = '{token_transfers_output_dir}/token_transfers_{file_name_suffix}.csv'.format(
                token_transfers_output_dir=token_transfers_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting ERC20 transfers from blocks {block_range} to {token_transfers_file}'.format(
                block_range=block_range,
                token_transfers_file=token_transfers_file,
            ))

            job = ExtractTokenTransfersJob(
                logs_iterable=logs,
                batch_size=batch_size,
                max_workers=max_workers,
                item_exporter=token_transfers_item_exporter(token_transfers_file)
            )
            job.run()
 
        if export_traces==True:
            from ethereumetl.jobs.exporters import traces_item_exporter
            from ethereumetl.jobs.export_traces_job import ExportTracesJob

            traces_output_dir = '{output_dir}/traces{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )

            traces_file = '{traces_output_dir}/traces_{file_name_suffix}.csv'.format(
                traces_output_dir=traces_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting Parity traces from blocks {block_range} to {traces_file}'.format(
                block_range=block_range,
                traces_file=traces_file,
            ))

            in_memory_exporter = InMemoryItemExporter(item_types=['trace'])

            include_daofork_traces = False

            if chain == 'ethereum':
                include_daofork_traces = True

            job = ExportTracesJob(
                start_block=batch_start_block,
                end_block=batch_end_block,
                batch_size=batch_size,
                web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri, timeout=60))),
                item_exporter=in_memory_exporter,
                max_workers=max_workers,
                include_genesis_traces=True,
                include_daofork_traces=include_daofork_traces
            )

            job.run()
            
            traces = in_memory_exporter.get_items('trace')
            file_exporter = traces_item_exporter(traces_file)
            file_exporter.open()
            file_exporter.export_items(traces)
            file_exporter.close()

            contracts_output_dir = '{output_dir}/contracts{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )

            contracts_file = '{contracts_output_dir}/contracts_{file_name_suffix}.csv'.format(
                contracts_output_dir=contracts_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting contracts in Parity traces from blocks {block_range} to {contracts_file}'.format(
                block_range=block_range,
                contracts_file=contracts_file,
            ))

            job = ExtractContractsJob(
                traces_iterable=traces,
                batch_size=batch_size,
                max_workers=max_workers,
                item_exporter=contracts_item_exporter(contracts_file)
            )
            job.run()
            
        else:
            # # # contracts only create by user# # #
            contracts_output_dir = '{output_dir}/contracts{partition_dir}'.format(
                output_dir=output_dir,
                partition_dir=partition_dir,
            )

            contracts_file = '{contracts_output_dir}/contracts_{file_name_suffix}.csv'.format(
                contracts_output_dir=contracts_output_dir,
                file_name_suffix=file_name_suffix,
            )
            logger.info('Exporting contracts from blocks {block_range} to {contracts_file}'.format(
                block_range=block_range,
                contracts_file=contracts_file,
            ))

            job = ExportContractsJob(
                contract_addresses_iterable=(map(lambda receipt: receipt['contract_address'] ,filter(lambda receipt: receipt['contract_address'] is not None, receipts))),
                batch_size=batch_size,
                batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True)),
                item_exporter=contracts_item_exporter(contracts_file),
                max_workers=max_workers)
            
            job.run()

        ### Need to use traces
        # # # # tokens # # #
        # if token_transfers_file is not None:
        #     tokens_output_dir = '{output_dir}/tokens{partition_dir}'.format(
        #         output_dir=output_dir,
        #         partition_dir=partition_dir,
        #     )

        #     tokens_file = '{tokens_output_dir}/tokens_{file_name_suffix}.csv'.format(
        #         tokens_output_dir=tokens_output_dir,
        #         file_name_suffix=file_name_suffix,
        #     )
        #     logger.info('Exporting tokens from blocks {block_range} to {tokens_file}'.format(
        #         block_range=block_range,
        #         tokens_file=tokens_file,
        #     ))
            
        #     job = extract_tokens_job(
        #         contracts_iterable=contracts,
        #         web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
        #         max_workers=max_workers,
        #         item_exporter=tokens_item_exporter(tokens_file)
        #     )
        #     job.run()

        # # # finish # # #
        end_time = time()
        time_diff = round(end_time - start_time, 5)
        logger.info('Exporting blocks {block_range} took {time_diff} seconds'.format(
            block_range=block_range,
            time_diff=time_diff,
        ))
