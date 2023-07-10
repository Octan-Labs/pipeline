import logging
from pyarrow.parquet import ParquetWriter
import pyarrow as pa

from blockchainetl.atomic_counter import AtomicCounter
from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.file_utils import close_silently, get_file_handle


class FsspecItemExporter(ConsoleItemExporter):
    def __init__(self, file_format='parquet', compression='snappy', filename_without_ext_mapping={}, mapper_mapping={}):
        self.file_format = file_format
        self.compression = compression
        self.filename_without_ext_mapping = filename_without_ext_mapping
        self.mapper_mapping = mapper_mapping
        self.writer_mapping = {}
        self.counter_mapping = {}
        self.logger = logging.getLogger('FsspecItemExporter')

    def _supported_format(self):
        return ['parquet', 'csv']
    
    def open(self):
        for item_type, filename in self.filename_without_ext_mapping.items():
            file = get_file_handle(filename + '.{file_format}'.format(file_format=self.file_format), binary=True)
            writer = ParquetWriter(file.path, self.mapper_mapping[item_type].schema(), filesystem=file.fs, compression=self.compression)
            self.writer_mapping[item_type] = writer
            self.counter_mapping[item_type] = AtomicCounter()

    # Assume items sorted by item['type']
    def export_items(self, items):
        items_len = len(items)
        if items_len == 0:
            return

        curr_item_list = []
        curr_item_type = items[0].get('type')
        for index, item in enumerate(items):
            if item.get('type') != curr_item_type or index == items_len - 1:                
                del item['type']
                curr_item_list.append(item)

                writer = self.writer_mapping[curr_item_type]
                table = pa.Table.from_pylist(curr_item_list).cast(self.mapper_mapping[curr_item_type].schema())
                writer.write_table(table)
                
                counter = self.counter_mapping[curr_item_type]
                counter.increment(table.num_rows)
                curr_item_type = item.get('type')
                curr_item_list = []
            
            if curr_item_type is not None and item.get('type') == curr_item_type:
                del item['type']
                curr_item_list.append(item)

            

    def export_item(self, item):
        pass

    def close(self):
        for item_type in self.filename_without_ext_mapping.keys():
            close_silently(self.writer_mapping[item_type])
            counter = self.counter_mapping[item_type]
            if counter is not None:
                count = counter.increment() - 1
                if count > 0:
                    self.logger.info('{} items exported: {} into {}'.format(item_type, count, (self.filename_without_ext_mapping[item_type] + '.' + self.file_format)))