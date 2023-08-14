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


import click
import re

from os import environ

from blockchainetl.logging_utils import logging_basic_config

from ethereumetl.cli.cli_utils import get_partitions, parse_entity_types
from ethereumetl.jobs.export_all_common import export_all_common
from ethereumetl.utils import check_classic_provider_uri
from ethereumetl.enumeration.entity_type import EntityType

logging_basic_config()

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-s', '--start', required=True, type=str, default=environ.get("START"), help='Start block/ISO date/Unix time')
@click.option('-e', '--end', required=True, type=str, default=environ.get("END"), help='End block/ISO date/Unix time')
@click.option('--worker-job-index', type=int, default=int(environ.get("JOB_COMPLETION_INDEX") or -1), help='Index of the worker in a scheduled job, compatible with JOB_COMPLETION_INDEX https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/')
@click.option('--first-worker-partition-index', default=int(environ.get("FIRST_WORKER_PARTITION_INDEX") or 0), show_default=True, type=int, help='1st partition index')
@click.option('--partition-to-hour', type=bool, default=bool(False if environ.get("PARTITION_TO_HOUR") == "false" else True), show_default=True, help='Break time partition from date into hour, use if memory allocation for date is too big')
@click.option('-b', '--partition-batch-size', default=environ.get("PARTITION_BATCH_SIZE", 10000), show_default=True, type=int,
              help='The number of blocks to export in partition.')
@click.option('-p', '--provider-uri', default=environ.get("PROVIDER_URI", 'https://mainnet.infura.io'), show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-o', '--output-dir', default=environ.get("OUTPUT_DIR", 'output'), show_default=True, type=str, help='Output directory, partitioned in Hive style.')
@click.option('-w', '--max-workers', default=environ.get("MAX_WORKERS", 5), show_default=True, type=int, help='The maximum number of workers.')
@click.option('-B', '--export-batch-size', default=environ.get("EXPORT_BATCH_SIZE", 100), show_default=True, type=int, help='The number of requests in JSON RPC batches.')
@click.option('-e', '--entity-types', default=environ.get("ENTITY_TYPES", ','.join(EntityType.ALL_NO_TRACE_SUPPORT)), show_default=True, type=str,
              help='The list of entity types to export.')
@click.option('-c', '--chain', default='ethereum', show_default=True, type=str, help='The chain network to connect to.')
def export_all(start, end, worker_job_index, first_worker_partition_index, partition_to_hour, partition_batch_size, provider_uri, output_dir, max_workers, export_batch_size, entity_types,
               chain='ethereum'):
    """Exports all data for a range of blocks."""
    export_all_common(get_partitions(start, end, partition_batch_size, provider_uri, worker_job_index, first_worker_partition_index, partition_to_hour),
                      output_dir, provider_uri, max_workers, export_batch_size, chain, parse_entity_types(entity_types))