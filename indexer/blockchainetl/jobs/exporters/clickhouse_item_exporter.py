import collections
import logging
from typing import List
from urllib.parse import urlparse, parse_qsl
from dataclasses import dataclass
from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter

from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.models import ColumnDef

import clickhouse_connect


@dataclass
class Table:
    column_names: List[str]
    column_types: List[str]


class ClickHouseItemExporter:

    def __init__(self, connection_url, item_type_to_table_mapping, converters=()):
        parsed = urlparse(connection_url)
        self.username = parsed.username
        self.password = parsed.password
        self.host = parsed.hostname
        self.port = parsed.port
        self.database = parsed.path[1:].split('/')[0] if parsed.path else 'default'
        self.settings = dict(parse_qsl(parsed.query))

        if self.settings.get('table_prefix') is not None:
            for item, table in item_type_to_table_mapping.items():
                item_type_to_table_mapping[item] = "{prefix}_{table}".format(prefix=self.settings.get('table_prefix'), table=table)
            del self.settings['table_prefix']
            
        self.item_type_to_table_mapping = item_type_to_table_mapping    
        self.connection = self.create_connection()
        self.tables = {}
        self.converter = CompositeItemConverter(converters)
        self.logger = logging.getLogger('ClickHouseItemExporter')

        ## for each time grab the schema to save a prefetch of the columns on each insert
        for table in self.item_type_to_table_mapping.values():
            try:
                describe_result = self.connection.query(f'DESCRIBE TABLE {self.database}.{table}')
                column_defs = [ColumnDef(**row) for row in describe_result.named_results()
                               if row['default_type'] not in ('ALIAS', 'MATERIALIZED')]
                column_names = [cd.name for cd in column_defs]
                column_types = [cd.ch_type for cd in column_defs]
                self.tables[table] = Table(column_names, column_types)
            except DatabaseError as de:
                # this may not be critical since the user may not be exporting the type and hence the table likely
                # won't exist
                self.logger.warning('Unable to read columns for table "{}". This column will not be exported.'.format(table))
                self.logger.debug(de)
                pass

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = self.group_by_item_type(items)

        for item_type, table in self.item_type_to_table_mapping.items():
            item_group = items_grouped_by_type.get(item_type)

            if item_group:
                converted_items = list(self.convert_items(item_group))
                table_data = []

                for converted_item in converted_items:
                    record = []

                    for column in self.tables[table].column_names:
                        record.append(converted_item.get(column))
                    table_data.append(record)

                self.connection.insert(table, data=table_data, column_names=self.tables[table].column_names,
                                           column_types=self.tables[table].column_types, database=self.database)
                self.logger.info('{} items inserted: {}'.format(item_type, len(table_data)))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def create_connection(self):
        return clickhouse_connect.create_client(host=self.host, port=self.port, username=self.username,
                                                password=self.password, database=self.database,
                                                settings=self.settings)

    def close(self):
        self.connection.close()

    def group_by_item_type(self, items):
        result = collections.defaultdict(list)
        for item in items:
            result[item.get('type')].append(item)

        return result