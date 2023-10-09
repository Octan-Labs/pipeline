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
import { chunkArray } from "./utils/array";

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
const DEFAULT_DECIMAL = 18;
const DEFAULT_CHUNK_SIZE = 50;

const main = async () => {
  try {
    console.log("Start calculating l2 projects escrows value");
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
    console.log("Get escrow contracts data from Postgres success");

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

    const dateString = config.blockDate;

    // Query token addresses data from clickhouse
    const tokenPriceQuery = `SELECT h.id as id, h.timestamp as timestamp, close as price,
                  a.eth as address, t.decimals as decimals
                  FROM cmc_historical h
                  JOIN cmc_address a ON h.id = a.id
                  JOIN cmc_eth_token t on t.address = a.eth
                  WHERE a.eth IN ({addresses: Array(TINYTEXT)})
                  AND toDate(h.timestamp) = toDate({date: date})
                  LIMIT 1000`;
    const tokenPriceResult = await client.query({
      query: tokenPriceQuery,
      query_params: {
        addresses: tokenAddresses,
        date: dateString,
      },
      format: "JSONEachRow",
    });
    const tokenPrices: TokenPrice[] = await tokenPriceResult.json();
    console.log(
      "Get erc20 tokens historical price data from ClickHouse success"
    );

    const ethPriceQuery = `SELECT id, timestamp, close as price,'NATIVE_TOKEN' as address, 18 as decimals
                          FROM cmc_historical
                          WHERE id = {id: Int64}
                          AND toDate(timestamp) = toDate({date: date})
                          LIMIT 1`;
    const ethPriceResult = await client.query({
      query: ethPriceQuery,
      query_params: {
        id: CMC_ETH_ID,
        date: dateString,
      },
      format: "JSONEachRow",
    });
    const ethPrices: TokenPrice[] = await ethPriceResult.json();
    console.log("Get eth historical price data from ClickHouse success");

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

    const priceTimestamp = ethPrices[0].timestamp;

    const escrowContractTokens: any[] = escrowContracts.map((obj) => ({
      ...obj,
      decimals: decimalMap[obj.token_address.toLowerCase()],
      price: priceMap[obj.token_address.toLowerCase()],
    }));

    const ethEscrowContractTokens: any[] = ethEscrowContracts.map((obj) => ({
      ...obj,
      decimals: DEFAULT_DECIMAL,
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

    console.log("Start getting tokens balance by multicall contract");
    const chunkContracts: any[][] = chunkArray(contracts, DEFAULT_CHUNK_SIZE);
    const chunkCallInputs: any[][] = chunkArray(callInputs, DEFAULT_CHUNK_SIZE);
    const tx: any[] = [];

    for (let i = 0; i < chunkContracts.length; i++) {
      const chunkTx = await getMultipleContractMultipleData(
        chunkContracts[i],
        multicallContract,
        CONTRACT_FUNCTION,
        config.blockNumber,
        chunkCallInputs[i]
      );
      tx.push(chunkTx);
    }
    const contractData = [].concat.apply([], tx);

    const ethMulticallResult = await getMultipleAddressEthBalance(
      ethEscrow,
      config.blockNumber,
      multicallContract
    );

    console.log("Get tokens balance by multicall contract success");

    //format the balance, store values in database
    const escrowBalances: EthProjectEscrowValue[] = contractData.map(
      (balance, i) => {
        if (balance === undefined) {
          return {
            project_id: escrowContractTokens[i].project_id,
            date: priceTimestamp,
            address: escrowContractTokens[i].address,
            token_address: escrowContractTokens[i].token_address,
            balance: 0,
            price: 0,
          };
        }
        const tokenDecimal = escrowContractTokens[i].decimals;
        const balanceFormatted = ethers.formatUnits(
          balance.toString(),
          tokenDecimal ? +tokenDecimal : DEFAULT_DECIMAL
        );
        return {
          project_id: escrowContractTokens[i].project_id,
          date: priceTimestamp,
          address: escrowContractTokens[i].address,
          token_address: escrowContractTokens[i].token_address,
          balance: +balanceFormatted,
          price: priceMap[escrowContractTokens[i].token_address] ?? 0,
        };
      }
    );

    const ethEscrowValues: EthProjectEscrowValue[] = ethMulticallResult.map(
      (balance, i) => {
        const balanceFormatted = ethers.formatUnits(
          balance.toString(),
          "ether"
        );
        return {
          project_id: ethEscrowContractTokens[i].project_id,
          date: priceTimestamp,
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
    console.log("Insert escrow values to Postgres success. End!");
  } catch (error) {
    // Handle the error
    console.error("Error:", error.message);
    console.log("Retrying in 30 seconds...");

    // Wait for 30 seconds before retrying
    await new Promise((resolve) => setTimeout(resolve, 30000));

    // Retry the main function
    await main();
  }
};

main();
