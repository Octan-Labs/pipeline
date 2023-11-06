import { getConfig } from "./config";
import {
  EthProjectEscrow,
  EthProjectEscrowRepo,
  EthProjectEscrowTimeseriesValueRepo,
  L2MintedTokenRepo,
} from "./database/postgres";
import { ClickHouseService } from "./database/clickhouse/client";
import { BalanceCalcService } from "./services/BalanceCalcService";
import { BalanceFormatterService } from "./services/BalanceFormatterService";
import { TotalSupplyCalcService } from "./services/TotalSupplyCalcService";
import {
  L2MintedTokenTimeseriesValueRepo,
  L2MintedTokenValue,
} from "./database/postgres/models/L2MintedTokenTimeseries";
import { ethers } from "ethers";

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
const USDC_DECIMAL = 6;
const USDC_PRICE = 1;

async function calculateCanonical(
  chHost: string,
  chDatabase: string,
  chUser: string,
  chPassword: string,
  blockNumber: number,
  blockDate: string,
  rpcUrl: string,
  multicallAddr: string,
  chunkSize: number = 100
) {
  try {
    console.log("Start calculating l2 projects escrows value");

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
      chHost,
      chDatabase,
      chUser,
      chPassword
    );

    const blockTime: BlockTime[] = await clickHouseService.getBlockTime(
      blockNumber
    );
    if (blockTime.length === 0) {
      throw new Error("Block not found");
    }

    const dateString = blockDate;
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

    const { decimalMap, priceMap } = tokenPrices.reduce(
      (acc, obj) => {
        acc.decimalMap[obj.address] = obj.decimals;
        acc.priceMap[obj.address] = obj.price;
        return acc;
      },
      { decimalMap: {}, priceMap: {} }
    );

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

    const callInputs: string[][] = [];
    const ethEscrow: string[] = [];

    escrowContractTokens.forEach((ctr) => {
      callInputs.push([ctr.address]);
    });
    ethEscrowContractTokens.forEach((ctr) => {
      ethEscrow.push(ctr.address);
    });

    console.log("Start getting tokens balance by multicall contract");
    const balanceCalcService = new BalanceCalcService(rpcUrl, multicallAddr);
    const contractData = await balanceCalcService.calculateTokensBalance(
      tokenAddresses,
      callInputs,
      chunkSize,
      blockNumber
    );
    const ethMulticallResult = await balanceCalcService.calculateEthBalance(
      ethEscrow,
      blockNumber
    );
    console.log("Get tokens balance by multicall contract success");

    //format the balance, store values in database
    const balanceFormatterService = new BalanceFormatterService();
    const escrowBalances = balanceFormatterService.formatEscrowBalance(
      contractData,
      escrowContractTokens,
      priceMap,
      priceTimestamp
    );
    const ethEscrowValues = balanceFormatterService.formatEthEscrowBalance(
      ethMulticallResult,
      ethEscrowContractTokens,
      priceTimestamp
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
    await calculateCanonical(
      chHost,
      chDatabase,
      chUser,
      chPassword,
      blockNumber,
      blockDate,
      rpcUrl,
      multicallAddr,
      chunkSize
    );
  }
}

async function calculateUSDCNativeMinted(blockDate: string) {
  console.log("Start calculating l2 USDC minted token value");
  const l2MintedTokenValues: L2MintedTokenValue[] = [];
  const l2MintedTokenRepo = new L2MintedTokenRepo();
  const l2MintedTokens = await l2MintedTokenRepo.getAll();

  for (let token of l2MintedTokens) {
    const totalSupply = await new TotalSupplyCalcService(
      token.rpc_url
    ).calculateTotalSupply(token.token_address);
    const supplyFormatted = ethers.formatUnits(
      totalSupply.toString(),
      USDC_DECIMAL
    );

    const l2MintedTokenValue: L2MintedTokenValue = {
      date: new Date(blockDate),
      project_id: token.project_id,
      token_address: token.token_address,
      amount: +supplyFormatted,
      price: USDC_PRICE,
    };

    l2MintedTokenValues.push(l2MintedTokenValue);
  }

  const l2MintedTokenValueRepo = new L2MintedTokenTimeseriesValueRepo();
  await l2MintedTokenValueRepo.insert(l2MintedTokenValues);
  console.log("Insert l2 USDC minted token values to Postgres success. End!");
}

async function main() {
  try {
    const config = getConfig();
    await calculateCanonical(
      config.chHost,
      config.chDatabase,
      config.chUser,
      config.chPassword,
      config.blockNumber,
      config.blockDate,
      config.rpcUrl,
      config.multicallContractAddress,
      config.chunkSize
    );
    await calculateUSDCNativeMinted(config.blockDate);
  } catch (error) {
    console.error("Error:", error);
  }
}

main();
