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


from evmetl.domain.transaction import EthTransaction
from evmetl.utils import hex_to_dec, to_normalized_address
import pyarrow as pa
import decimal

class EthTransactionMapper(object):
    def json_dict_to_transaction(self, json_dict, **kwargs):
        transaction = EthTransaction()
        transaction.hash = json_dict.get('hash')
        transaction.nonce = hex_to_dec(json_dict.get('nonce'))
        transaction.block_hash = json_dict.get('blockHash')
        transaction.block_number = hex_to_dec(json_dict.get('blockNumber'))
        transaction.block_timestamp = kwargs.get('block_timestamp')
        transaction.transaction_index = hex_to_dec(json_dict.get('transactionIndex'))
        transaction.from_address = to_normalized_address(json_dict.get('from'))
        transaction.to_address = to_normalized_address(json_dict.get('to'))
        transaction.value = decimal.Decimal(hex_to_dec(json_dict.get('value')))
        transaction.gas = decimal.Decimal(hex_to_dec(json_dict.get('gas')))
        transaction.gas_price = hex_to_dec(json_dict.get('gasPrice'))
        transaction.input = json_dict.get('input')
        transaction.max_fee_per_gas = hex_to_dec(json_dict.get('maxFeePerGas'))
        transaction.max_priority_fee_per_gas = hex_to_dec(json_dict.get('maxPriorityFeePerGas'))
        transaction.transaction_type = hex_to_dec(json_dict.get('type'))
        return transaction

    def transaction_to_dict(self, transaction):
        return {
            'type': 'transaction',
            'hash': transaction.hash,
            'nonce': transaction.nonce,
            'block_hash': transaction.block_hash,
            'block_number': transaction.block_number,
            'block_timestamp': transaction.block_timestamp,
            'transaction_index': transaction.transaction_index,
            'from_address': transaction.from_address,
            'to_address': transaction.to_address,
            'value': transaction.value,
            'gas': transaction.gas,
            'gas_price': transaction.gas_price,
            'input': transaction.input,
            'max_fee_per_gas': transaction.max_fee_per_gas,
            'max_priority_fee_per_gas': transaction.max_priority_fee_per_gas,
            'transaction_type': transaction.transaction_type
        }

    @classmethod
    def schema(cls):
        return pa.schema([
            pa.field('hash', pa.string(), nullable=False),
            pa.field('nonce', pa.decimal128(precision=38, scale=0), nullable=False),
            pa.field('transaction_index', pa.decimal128(precision=38, scale=0), nullable=False),
            pa.field('from_address', pa.string(), nullable=False),
            pa.field('to_address', pa.string()),
            pa.field('value', pa.decimal128(precision=38, scale=0)),
            pa.field('gas', pa.decimal128(precision=38, scale=0)),
            pa.field('gas_price', pa.decimal128(precision=38, scale=0)),
            # pa.field('input', pa.string()),
            pa.field('block_timestamp', pa.timestamp('s', tz='UTC'), nullable=False),
            pa.field('block_number', pa.decimal128(precision=38, scale=0), nullable=False),
            pa.field('block_hash', pa.string(), nullable=False),
            pa.field('max_fee_per_gas', pa.decimal128(precision=38, scale=0)),
            pa.field('max_priority_fee_per_gas', pa.decimal128(precision=38, scale=0)),
            pa.field('transaction_type', pa.uint8()),
            pa.field('receipt_cumulative_gas_used', pa.decimal128(precision=38, scale=0)),
            pa.field('receipt_gas_used', pa.decimal128(precision=38, scale=0)),
            pa.field('receipt_contract_address', pa.string()),
            pa.field('receipt_root', pa.string()),
            pa.field('receipt_status', pa.decimal128(precision=38, scale=0)),
            pa.field('receipt_effective_gas_price', pa.decimal128(precision=38, scale=0))
        ])