import { getConfig } from "./config";
import multicallABI from "./config/abi/multical.json";
import erc20ABI from "./config/abi/erc20.json";
import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import {
  getMultipleAddressEthBalance,
  getMultipleContractMultipleData,
} from "./services/multicall";
import { ethers } from "ethers";
import {
  EthProjectEscrow,
  EthProjectEscrowRepo,
  EthProjectEscrowTimeseriesValueRepo,
  EthProjectEscrowValue,
} from "./database/postgres";
import { ClickHouseService } from "./database/clickhouse";

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

const DEFAULT_DECIMAL = 18;

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

    const clickHouseService = new ClickHouseService(
      config.chHost,
      config.chDatabase,
      config.chUser,
      config.chPassword
    );

    const blockTime: BlockTime[] = await clickHouseService.getBlockTime(
      config.blockNumber
    );

    if (blockTime.length === 0) {
      throw new Error("Block not found");
    }

    const dateString = config.blockDate;
    const tokenPrices: TokenPrice[] = await clickHouseService.getTokenPrices(
      tokenAddresses,
      dateString
    );
    console.log(
      "Get erc20 tokens historical price data from ClickHouse success"
    );

    const ethPrices: TokenPrice[] = await clickHouseService.getEthPrice(
      dateString
    );
    console.log("Get eth historical price data from ClickHouse success");

    // Close clickhouse client connection
    await clickHouseService.close();

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
    const chunkContracts: any[][] = chunkArray(contracts, config.chunkSize);
    const chunkCallInputs: any[][] = chunkArray(callInputs, config.chunkSize);
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
    console.error("Error:", error.message);
    console.log("Retrying in 30 seconds...");

    // Wait for 30 seconds before retrying
    await new Promise((resolve) => setTimeout(resolve, 30000));

    // Retry the main function
    await main();
  }
};

main();
