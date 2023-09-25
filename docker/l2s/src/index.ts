import { getConfig } from "./config";
import multicallABI from "./config/abi/multical.json";
import erc20ABI from "./config/abi/erc20.json";
import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import { getMultipleContractMultipleData } from "./multicall";
import { ethers } from "ethers";
import { EthLastBlockPerDayRepo } from "./model/EthLastBlockPerDay";
import { EthProjectAddressTokenAddressRepo } from "./model/EthProjectAddressTokenAddress";
import { EthTokenRepo } from "./model/EthToken";
import EthProjectTokenBalanceTimeseries, {
  EthProjectTokenBalanceTimeseriesRepo,
} from "./model/EthProjectTokenBalanceTimeseries";

const main = async () => {
  const config = getConfig();

  // Initialize repository
  const ethLastBlockPerDayRepo = new EthLastBlockPerDayRepo();
  const ethProjectAddressTokenAddressRepo =
    new EthProjectAddressTokenAddressRepo();
  const ethTokenRepo = new EthTokenRepo();
  const ethProjectTokenBalanceTimeseriesRepo =
    new EthProjectTokenBalanceTimeseriesRepo();

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

  // Get last block by date, config in .env
  const blockDate = await ethLastBlockPerDayRepo.getByDate(
    config.calculateDate
  );

  // Get contracts and tokens of all projects
  const projectContracts = await ethProjectAddressTokenAddressRepo.getAll();
  const tokenAddresses = projectContracts.map((p) => p.token_address);

  const tokensHoldByContract = await ethTokenRepo.getByAddresses(
    tokenAddresses
  );

  type TokenMap = Map<string, number>;
  const tokenMap: TokenMap = tokensHoldByContract.reduce(
    (map, token) => map.set(token.address, token.decimal),
    new Map()
  );

  projectContracts.forEach((projectContract) => {
    contracts.push(
      new Contract(projectContract.token_address, erc20ABI, rpcProvider)
    );
    callInputs.push([projectContract.address]);
  });

  let tx = await getMultipleContractMultipleData(
    contracts,
    multicallContract,
    CONTRACT_FUNCTION,
    blockDate[0].block_number,
    callInputs
  );

  // remove undefined elements
  tx = tx.filter((t) => t !== undefined);

  // format the balance, store values in database
  const balances = tx.map((balance, i) => {
    const decimal = tokenMap.get(
      projectContracts[i].token_address.toLowerCase()
    );
    const balanceFormatted = ethers.formatUnits(
      balance.toString(),
      decimal ?? 18
    );
    return {
      project_id: "base",
      date: new Date(config.calculateDate),
      address: projectContracts[i].address,
      token_address: projectContracts[i].token_address,
      balance: +balanceFormatted,
    };
  });

  // store value to DB
  await ethProjectTokenBalanceTimeseriesRepo.insert(balances);
};

main().catch(console.log);
