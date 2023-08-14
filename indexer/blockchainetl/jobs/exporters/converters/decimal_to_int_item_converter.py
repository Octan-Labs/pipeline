from decimal import Decimal

from blockchainetl.jobs.exporters.converters.simple_item_converter import SimpleItemConverter


class DecimalToIntItemConverter(SimpleItemConverter):

    def convert_field(self, key, value):
        if isinstance(value, Decimal):
            return int(value)
        else:
            return value