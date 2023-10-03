import { getConfig } from "./config";
import multicallABI from "./config/abi/multical.json";
import erc20ABI from "./config/abi/erc20.json";
import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import { getMultipleContractMultipleData } from "./multicall";
import { ethers } from "ethers";
import {
  EthProjectAddressTokenAddress,
  EthProjectAddressTokenAddressRepo,
} from "./model/EthProjectAddressTokenAddress";

import EthProjectTokenBalanceTimeseries, {
  EthProjectTokenBalanceTimeseriesRepo,
} from "./model/EthProjectTokenBalanceTimeseries";
import { createClient } from "@clickhouse/client";

type TokenPrice = {
  id: number;
  timestamp: Date;
  price: number;
  address: string;
  decimals: number;
};

type BlockTime = {
  number: number;
  timestamp: Date;
};

const main = async () => {
  const config = getConfig();

  // Initialize postgres repository
  const ethProjectAddressTokenAddressRepo =
    new EthProjectAddressTokenAddressRepo();
  const ethProjectTokenBalanceTimeseriesRepo =
    new EthProjectTokenBalanceTimeseriesRepo();

  // Get contracts and tokens of all projects
  const projectContracts: EthProjectAddressTokenAddress[] =
    await ethProjectAddressTokenAddressRepo.getAll();
  const tokenAddresses: string[] = projectContracts.map((p) =>
    p.token_address.toLowerCase()
  );

  // Initialize clickhouse client
  const client = createClient({
    host: config.chHost,
    database: config.chDatabase,
    username: config.chUser,
    password: config.chPassword,
  });

  // Query block time data from clickhouse
  const blockTimeQuery = `SELECT number, timestamp FROM eth_block WHERE number = ${config.blockNumber} LIMIT 1`;
  const blockTimeResult = await client.query({
    query: blockTimeQuery,
    format: "JSONEachRow",
  });
  const blockTime: BlockTime[] = await blockTimeResult.json();
  if (blockTime.length === 0) {
    throw new Error("Block not found");
  }

  // Query token addresses data from clickhouse
  const query = `SELECT h.id as id, h.timestamp as timestamp, close as price,
                  a.eth as address, t.decimals as decimals
                  FROM cmc_historical h
                  JOIN cmc_address a ON h.id = a.id
                  JOIN cmc_eth_token t on t.address = a.eth
                  WHERE a.eth IN ({addresses: Array(TINYTEXT)})
                  AND toDate(h.timestamp) =toDate({timestamp: timestamp})
                  LIMIT 1000`;

  const result = await client.query({
    query: query,
    query_params: {
      addresses: tokenAddresses,
      timestamp: blockTime[0].timestamp,
    },
    format: "JSONEachRow",
  });

  const tokenPrices: TokenPrice[] = await result.json();

  const decimalMap = tokenPrices.reduce((acc, obj) => {
    acc[obj.address] = obj.decimals;
    return acc;
  }, {});

  const priceMap = tokenPrices.reduce((acc, obj) => {
    acc[obj.address] = obj.price;
    return acc;
  }, {});

  const projectContractTokens: any[] = projectContracts.map((obj) => ({
    ...obj,
    decimal: decimalMap[obj.token_address.toLowerCase()],
    price: priceMap[obj.token_address.toLowerCase()],
  }));

  //  Initialize provider
  const rpcProvider = new StaticJsonRpcProvider(config.rpcUrl);
  const multicallContract = new Contract(
    config.multicallContractAddress,
    multicallABI,
    rpcProvider
  );
  const CONTRACT_FUNCTION = "balanceOf";
  const contracts: Contract[] = [];
  const callInputs: string[][] = [];

  projectContractTokens.forEach((ctr) => {
    contracts.push(new Contract(ctr.token_address, erc20ABI, rpcProvider));
    callInputs.push([ctr.address]);
  });

  let tx = await getMultipleContractMultipleData(
    contracts,
    multicallContract,
    CONTRACT_FUNCTION,
    config.blockNumber,
    callInputs
  );

  // format the balance, store values in database
  const balances: EthProjectTokenBalanceTimeseries[] = tx.map((balance, i) => {
    if (balance === undefined) {
      return {
        project_id: projectContracts[i].project_id,
        date: new Date(blockTime[0].timestamp),
        address: projectContracts[i].address,
        token_address: projectContracts[i].token_address,
        balance: 0,
        price: 0,
      };
    }
    const balanceFormatted = ethers.formatUnits(
      balance.toString(),
      contracts[i].decimals ?? 18
    );
    return {
      project_id: projectContracts[i].project_id,
      date: new Date(blockTime[0].timestamp),
      address: projectContracts[i].address,
      token_address: projectContracts[i].token_address,
      balance: +balanceFormatted,
      price: priceMap[projectContracts[i].token_address] ?? 0,
    };
  });

  // store value to DB
  await ethProjectTokenBalanceTimeseriesRepo.insert(balances);
};

main().catch(console.log);
