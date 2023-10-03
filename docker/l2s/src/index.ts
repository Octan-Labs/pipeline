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

type Token = {
  address: string;
  id: number;
  symbol: string;
  decimals: number;
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
  const tokenAddresses = projectContracts.map((p) =>
    p.token_address.toLowerCase()
  );

  // Initialize clickhouse client
  const client = createClient({
    host: config.chHost,
    database: config.chDatabase,
    username: config.chUser,
    password: config.chPassword,
  });

  // Query token addresses data from clickhouse
  const query = `SELECT eth as address, id, symbol, decimals 
                  FROM cmc_address 
                  JOIN eth_token et on cmc_address.eth = et.address
                  WHERE cmc_address.eth IN ({addresses: Array(TINYTEXT)})
                  UNION ALL (
                    SELECT 'native_token' as address, id, symbol, 18 as decimals
                    FROM cmc_address
                    WHERE id = 1027
                  )`;

  const result = await client.query({
    query: query,
    query_params: {
      addresses: tokenAddresses,
    },
    format: "JSONEachRow",
  });

  const tokens: Token[] = await result.json();
  // Create a map from array2 for faster lookups
  const decimalMap = tokens.reduce((acc, obj) => {
    acc[obj.address] = obj.decimals;
    return acc;
  }, {});

  // Merge the arrays based on the "address" property
  const projectContractTokens: any[] = projectContracts.map((obj) => ({
    ...obj,
    decimal: decimalMap[obj.token_address.toLowerCase()],
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
  const callInputs: any[] = [];

  projectContractTokens.forEach((obj) => {
    contracts.push(new Contract(obj.token_address, erc20ABI, rpcProvider));
    callInputs.push([obj.address]);
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
        date: new Date(), // TODO: calculate date
        address: projectContracts[i].address,
        token_address: projectContracts[i].token_address,
        balance: 0,
      };
    }
    const balanceFormatted = ethers.formatUnits(
      balance.toString(),
      contracts[i].decimals ?? 18
    );
    return {
      project_id: projectContracts[i].project_id,
      date: new Date("2023-09-04"), // TODO: calculate date
      address: projectContracts[i].address,
      token_address: projectContracts[i].token_address,
      balance: +balanceFormatted,
    };
  });

  // store value to DB
  await ethProjectTokenBalanceTimeseriesRepo.insert(balances);
};

main().catch(console.log);
