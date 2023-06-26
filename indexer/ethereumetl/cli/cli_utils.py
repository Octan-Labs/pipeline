import click
import re

from datetime import datetime, timedelta
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.service.eth_service import EthService
from ethereumetl.web3_utils import build_web3

ZERO_PADDING = 10

def is_date_range(start, end):
    """Checks for YYYY-MM-DD date format."""
    return bool(re.match('^2[0-9]{3}-[0-9]{2}-[0-9]{2}$', start) and
                re.match('^2[0-9]{3}-[0-9]{2}-[0-9]{2}$', end))


def is_unix_time_range(start, end):
    """Checks for Unix timestamp format."""
    return bool(re.match("^[0-9]{10}$|^[0-9]{13}$", start) and
                re.match("^[0-9]{10}$|^[0-9]{13}$", end))



def get_indexed_partition_as_list(worker_job_index, first_worker_partition_index, partition_batch_size, provider_uri):
    provider = get_provider_from_uri(provider_uri)
    web3 = build_web3(provider)
    last_block = int(web3.eth.getBlock("latest").number)
    last_partition_index = int(last_block / partition_batch_size)
    
    if first_worker_partition_index > last_partition_index:
        raise click.BadParameter('first_worker_partition_index must in the range of [{start}, {end}]'.format(start=0, end=last_partition_index))

    for job_index in range(worker_job_index):
        batch_start_block = partition_batch_size * (first_worker_partition_index + job_index)
        batch_end_block = partition_batch_size * (first_worker_partition_index + job_index + 1) - 1

        padded_batch_start_block = str(batch_start_block).zfill(ZERO_PADDING)
        padded_batch_end_block = str(batch_end_block).zfill(ZERO_PADDING)
        partition_dir = '/start_block={padded_batch_start_block}/end_block={padded_batch_end_block}'.format(
            padded_batch_start_block=padded_batch_start_block,
            padded_batch_end_block=padded_batch_end_block,
        )
        yield batch_start_block, batch_end_block, partition_dir


def get_partitions(start, end, partition_batch_size, provider_uri):
    """Yield partitions based on input data type."""
    if is_date_range(start, end) or is_unix_time_range(start, end):
        if is_date_range(start, end):
            start_date = datetime.strptime(start, '%Y-%m-%d').date()
            end_date = datetime.strptime(end, '%Y-%m-%d').date()

        elif is_unix_time_range(start, end):
            if len(start) == 10 and len(end) == 10:
                start_date = datetime.utcfromtimestamp(int(start)).date()
                end_date = datetime.utcfromtimestamp(int(end)).date()

            elif len(start) == 13 and len(end) == 13:
                start_date = datetime.utcfromtimestamp(int(start) / 1e3).date()
                end_date = datetime.utcfromtimestamp(int(end) / 1e3).date()

        day = timedelta(days=1)

        provider = get_provider_from_uri(provider_uri)
        web3 = build_web3(provider)
        eth_service = EthService(web3)

        while start_date <= end_date:
            batch_start_block, batch_end_block = eth_service.get_block_range_for_date(start_date)
            partition_dir = '/date={start_date!s}/'.format(start_date=start_date)
            yield batch_start_block, batch_end_block, partition_dir
            start_date += day

    elif is_block_range(start, end):
        start_block = int(start)
        end_block = int(end)

        for batch_start_block in range(start_block, end_block + 1, partition_batch_size):
            batch_end_block = batch_start_block + partition_batch_size - 1
            if batch_end_block > end_block:
                batch_end_block = end_block

            padded_batch_start_block = str(batch_start_block).zfill(ZERO_PADDING)
            padded_batch_end_block = str(batch_end_block).zfill(ZERO_PADDING)
            partition_dir = '/start_block={padded_batch_start_block}/end_block={padded_batch_end_block}'.format(
                padded_batch_start_block=padded_batch_start_block,
                padded_batch_end_block=padded_batch_end_block,
            )
            yield batch_start_block, batch_end_block, partition_dir

    else:
        raise ValueError('start and end must be either block numbers or ISO dates or Unix times')
    
def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(',')]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_TRACE_SUPPORT:
            raise click.BadOptionUsage(
                '--entity-type', '{} is not an available entity type. Supply a comma separated list of types from {}'
                    .format(entity_type, ','.join(EntityType.ALL_TRACE_SUPPORT)))

    return entity_types

def is_dividend_of_ten(number):
    return (number % 10) == 0

def validate_dividend_of_ten(ctx, param, value):
    if is_dividend_of_ten(value):
        return value
    else:
        raise click.BadParameter('value need to be a dividen of ten')