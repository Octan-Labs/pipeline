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
import os

import pytest
from dateutil.parser import parse
from web3 import HTTPProvider, Web3

from ethereumetl.service.eth_service import EthService
from ethereumetl.service.graph_operations import OutOfBoundsError
from ethereumetl.web3_utils import build_web3
from tests.helpers import skip_if_slow_tests_disabled


@pytest.mark.parametrize("date,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled(['2015-07-30', 0, 6911]),
    skip_if_slow_tests_disabled(['2015-07-31', 6912, 13774]),
    skip_if_slow_tests_disabled(['2017-01-01', 2912407, 2918517]),
    skip_if_slow_tests_disabled(['2017-01-02', 2918518, 2924575]),
    skip_if_slow_tests_disabled(['2018-06-10', 5761663, 5767303])
])
def test_get_block_range_for_date(date, expected_start_block, expected_end_block):
    eth_service = get_new_eth_service()
    parsed_date = parse(date)
    blocks = eth_service.get_block_range_for_date(parsed_date)
    assert blocks == (expected_start_block, expected_end_block)


@pytest.mark.parametrize("date", [
    skip_if_slow_tests_disabled(['2015-07-29']),
    skip_if_slow_tests_disabled(['2030-01-01'])
])
def test_get_block_range_for_date_fail(date):
    eth_service = get_new_eth_service()
    parsed_date = parse(date)
    with pytest.raises(OutOfBoundsError):
        eth_service.get_block_range_for_date(parsed_date)


@pytest.mark.parametrize("start_timestamp,end_timestamp,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled([1438270128, 1438270128, 10, 10]),
    skip_if_slow_tests_disabled([1438270128, 1438270129, 10, 10])
])
def test_get_block_range_for_timestamps(start_timestamp, end_timestamp, expected_start_block, expected_end_block):
    eth_service = get_new_eth_service()
    blocks = eth_service.get_block_range_for_timestamps(start_timestamp, end_timestamp)
    assert blocks == (expected_start_block, expected_end_block)


@pytest.mark.parametrize("start_timestamp,end_timestamp", [
    skip_if_slow_tests_disabled([1438270129, 1438270131])
])
def test_get_block_range_for_timestamps_fail(start_timestamp, end_timestamp):
    eth_service = get_new_eth_service()
    with pytest.raises(ValueError):
        eth_service.get_block_range_for_timestamps(start_timestamp, end_timestamp)


def get_new_eth_service():
    provider_url = os.environ.get('PROVIDER_URL', 'https://mainnet.infura.io/v3/7aef3f0cd1f64408b163814b22cc643c')
    web3 = build_web3(HTTPProvider(provider_url))
    return EthService(web3)


@pytest.mark.parametrize("date,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled(['2021-05-28', 0, 1]),
    skip_if_slow_tests_disabled(['2021-05-29', 2, 49]),
    skip_if_slow_tests_disabled(['2021-09-05', 277901, 289176]),
    skip_if_slow_tests_disabled(['2021-11-28', 3390236, 3418614]),
    skip_if_slow_tests_disabled(['2022-04-30', 10861311, 10943976]),
    skip_if_slow_tests_disabled(['2022-10-20', 31129023, 31405137]),
    skip_if_slow_tests_disabled(['2023-03-08', 67740366, 68048123]),
    skip_if_slow_tests_disabled(['2023-06-10', 99542414, 99882516   ]),
    skip_if_slow_tests_disabled(['2023-08-05', 118227514, 118551389])
])
def test_get_block_range_for_date_arb(date, expected_start_block, expected_end_block):
    arb_service = get_new_arb_service()
    parsed_date = parse(date)
    blocks = arb_service.get_block_range_for_date(parsed_date)
    assert blocks == (expected_start_block, expected_end_block)


def get_new_arb_service():
    provider_url = os.environ.get('PROVIDER_URL', 'https://endpoints.omniatech.io/v1/arbitrum/one/public')
    web3 = build_web3(HTTPProvider(provider_url))
    return EthService(web3)


@pytest.mark.parametrize("date,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled(['2020-08-29', 0, 24606]),
    skip_if_slow_tests_disabled(['2020-08-30', 24607, 53406]),
    skip_if_slow_tests_disabled(['2023-08-05', 30572327, 30600947])
])
def test_get_block_range_for_date_bsc(date, expected_start_block, expected_end_block):
    bsc_service = get_new_bsc_service()
    parsed_date = parse(date)
    blocks = bsc_service.get_block_range_for_date(parsed_date)
    assert blocks == (expected_start_block, expected_end_block)


def get_new_bsc_service():
    provider_url = os.environ.get('PROVIDER_URL', 'https://bsc-dataseed.binance.org')
    web3 = build_web3(HTTPProvider(provider_url))
    return EthService(web3)


@pytest.mark.parametrize("date,expected_start_block,expected_end_block", [
    skip_if_slow_tests_disabled(['2020-05-30', 0, 13092]),
    skip_if_slow_tests_disabled(['2020-05-31', 13093, 54982]),
    skip_if_slow_tests_disabled(['2023-08-05', 45918059, 45958055])
])
def test_get_block_range_for_date_polygon(date, expected_start_block, expected_end_block):
    eth_service = get_new_polygon_service()
    parsed_date = parse(date)
    blocks = eth_service.get_block_range_for_date(parsed_date)
    assert blocks == (expected_start_block, expected_end_block)


def get_new_polygon_service():
    provider_url = os.environ.get('PROVIDER_URL', 'https://endpoints.omniatech.io/v1/matic/mainnet/public')
    web3 = build_web3(HTTPProvider(provider_url))
    return EthService(web3)