import { getConfig } from "./config";
import multicallABI from "./config/abi/multical.json";
import erc20ABI from "./config/abi/erc20.json";
import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import {
  getMultipleAddressEthBalance,
  getMultipleContractMultipleData,
} from "./multicall";
import { ethers } from "ethers";
import {
  EthProjectEscrow,
  EthProjectEscrowRepo,
} from "./model/EthProjectEscrow";

import EthProjectEscrowValue, {
  EthProjectEscrowTimeseriesValueRepo,
} from "./model/EthProjectEscrowTimeseries";
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

const CMC_ETH_ID = 1027;

const main = async () => {
  const config = getConfig();

  // Initialize postgres repository
  const ethProjectEscrowRepo = new EthProjectEscrowRepo();
  const ethProjectEscrowTimeseriesValueRepo =
    new EthProjectEscrowTimeseriesValueRepo();

  // Get contracts and tokens of all projects
  const escrowContracts: EthProjectEscrow[] =
    await ethProjectEscrowRepo.getAll();
  const ethEscrowContracts: EthProjectEscrow[] =
    await ethProjectEscrowRepo.getEthContract();
  const tokenAddresses: string[] = escrowContracts.map((p) =>
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
  const blockTimeQuery = `SELECT number, timestamp FROM eth_block WHERE number = {number: Int64} LIMIT 1`;
  const blockTimeResult = await client.query({
    query: blockTimeQuery,
    query_params: {
      number: config.blockNumber,
    },
    format: "JSONEachRow",
  });
  const blockTime: BlockTime[] = await blockTimeResult.json();

  if (blockTime.length === 0) {
    throw new Error("Block not found");
  }

  // Query token addresses data from clickhouse
  const tokenPriceQuery = `SELECT h.id as id, h.timestamp as timestamp, close as price,
                  a.eth as address, t.decimals as decimals
                  FROM cmc_historical h
                  JOIN cmc_address a ON h.id = a.id
                  JOIN cmc_eth_token t on t.address = a.eth
                  WHERE a.eth IN ({addresses: Array(TINYTEXT)})
                  AND toDate(h.timestamp) = toDate({timestamp: timestamp})
                  LIMIT 1000`;
  const tokenPriceResult = await client.query({
    query: tokenPriceQuery,
    query_params: {
      addresses: tokenAddresses,
      timestamp: config.blockDate,
    },
    format: "JSONEachRow",
  });
  const tokenPrices: TokenPrice[] = await tokenPriceResult.json();

  const ethPriceQuery = `SELECT id, timestamp, close as price,'NATIVE_TOKEN' as address, 18 as decimals
                          FROM cmc_historical
                          WHERE id = {id: Int64}
                          AND toDate(timestamp) = toDate({timestamp: timestamp})
                          LIMIT 100`;
  const ethPriceResult = await client.query({
    query: ethPriceQuery,
    query_params: {
      id: CMC_ETH_ID,
      timestamp: config.blockDate,
    },
    format: "JSONEachRow",
  });
  const ethPrices: TokenPrice[] = await ethPriceResult.json();
  // Close clickhouse client connection
  await client.close();

  const decimalMap = tokenPrices.reduce((acc, obj) => {
    acc[obj.address] = obj.decimals;
    return acc;
  }, {});

  const priceMap = tokenPrices.reduce((acc, obj) => {
    acc[obj.address] = obj.price;
    return acc;
  }, {});

  const escrowContractTokens: any[] = escrowContracts.map((obj) => ({
    ...obj,
    decimal: decimalMap[obj.token_address.toLowerCase()],
    price: priceMap[obj.token_address.toLowerCase()],
  }));

  const ethEscrowContractTokens: any[] = ethEscrowContracts.map((obj) => ({
    ...obj,
    decimal: 18,
    price: ethPrices[0].price,
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
  const ethEscrow: string[] = [];

  escrowContractTokens.forEach((ctr) => {
    contracts.push(new Contract(ctr.token_address, erc20ABI, rpcProvider));
    callInputs.push([ctr.address]);
  });
  ethEscrowContractTokens.forEach((ctr) => {
    ethEscrow.push(ctr.address);
  });

  const tx = await getMultipleContractMultipleData(
    contracts,
    multicallContract,
    CONTRACT_FUNCTION,
    config.blockNumber,
    callInputs
  );

  const ethMulticallResult = await getMultipleAddressEthBalance(
    ethEscrow,
    config.blockNumber,
    multicallContract
  );

  // format the balance, store values in database
  const escrowBalances: EthProjectEscrowValue[] = tx.map((balance, i) => {
    if (balance === undefined) {
      return {
        project_id: escrowContractTokens[i].project_id,
        date: new Date(config.blockDate),
        address: escrowContractTokens[i].address,
        token_address: escrowContractTokens[i].token_address,
        balance: 0,
        price: 0,
      };
    }
    const balanceFormatted = ethers.formatUnits(
      balance.toString(),
      contracts[i].decimals ?? 18
    );
    return {
      project_id: escrowContractTokens[i].project_id,
      date: new Date(config.blockDate),
      address: escrowContractTokens[i].address,
      token_address: escrowContractTokens[i].token_address,
      balance: +balanceFormatted,
      price: priceMap[escrowContractTokens[i].token_address] ?? 0,
    };
  });

  const ethEscrowValues: EthProjectEscrowValue[] = ethMulticallResult.map(
    (balance, i) => {
      const balanceFormatted = ethers.formatUnits(balance.toString(), "ether");
      return {
        project_id: ethEscrowContractTokens[i].project_id,
        date: new Date(config.blockDate),
        address: ethEscrowContractTokens[i].address,
        token_address: ethEscrowContractTokens[i].token_address,
        balance: +balanceFormatted,
        price: ethEscrowContractTokens[i].price ?? 0,
      };
    }
  );

  // insert value to Postgres
  const insertPromises = [
    ethProjectEscrowTimeseriesValueRepo.insert(escrowBalances),
    ethProjectEscrowTimeseriesValueRepo.insert(ethEscrowValues),
  ];
  await Promise.all(insertPromises);
};

main().catch(console.log);
