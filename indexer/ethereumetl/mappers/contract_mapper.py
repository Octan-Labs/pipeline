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


from ethereumetl.domain.contract import EthContract
import pyarrow as pa

class EthContractMapper(object):

    def rpc_result_to_contract(self, contract_address, rpc_result):
        contract = EthContract()
        contract.address = contract_address
        contract.bytecode = rpc_result

        return contract

    def contract_to_dict(self, contract):
        return {
            'type': 'contract',
            'address': contract.address,
            'bytecode': contract.bytecode,
            'function_sighashes': contract.function_sighashes,
            'is_erc20': contract.is_erc20,
            'is_erc721': contract.is_erc721,
            'block_number': contract.block_number
        }

    @classmethod
    def schema(cls):
        return pa.schema([
            pa.field('address', pa.string(), nullable=False),
            pa.field('bytecode', pa.string()),
            pa.field('function_sighashes', pa.list_(pa.string())),
            pa.field('is_erc20', pa.bool_()),
            pa.field('is_erc721', pa.bool_()),
            pa.field('block_number', pa.decimal128(precision=38, scale=0), nullable=False),
            pa.field('block_timestamp', pa.timestamp('s', tz='UTC'), nullable=False),
            pa.field('block_hash', pa.string(), nullable=False),
        ])