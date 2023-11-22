import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import multicallABI from "../config/abi/multical.json";
import erc20ABI from "../config/abi/erc20.json";
import { chunkArray } from "../utils";
import { getMultipleContractMultipleData } from "./multicall";

const CONTRACT_FUNCTION = "balanceOf";

export class BalanceCalcService {
  private readonly provider: StaticJsonRpcProvider;
  private readonly multicallContract: Contract;

  constructor(rpcUrl: string, multicallContractAddress: string) {
    this.provider = new StaticJsonRpcProvider(rpcUrl);
    this.multicallContract = new Contract(
      multicallContractAddress,
      multicallABI,
      this.provider
    );
  }
  async calculateTokensBalance(
    tokenAddresses: string[],
    callInputs: string[][],
    chunkSize: number,
    blockNumber: number
  ): Promise<any> {
    const contracts: Contract[] = [];
    tokenAddresses.forEach((address) => {
      contracts.push(new Contract(address, erc20ABI, this.provider));
    });
    const chunkContracts: any[][] = chunkArray(contracts, chunkSize);
    const chunkCallInputs: any[][] = chunkArray(callInputs, chunkSize);

    const tx: any[] = [];

    for (let i = 0; i < chunkContracts.length; i++) {
      const chunkTx = await getMultipleContractMultipleData(
        chunkContracts[i],
        this.multicallContract,
        CONTRACT_FUNCTION,
        blockNumber,
        chunkCallInputs[i]
      );
      tx.push(chunkTx);
    }
    const contractData = [].concat.apply([], tx);
    return contractData;
  }

  async calculateEthBalance(addresses: string[], blockNumber: number) {
    const calls = addresses.map((address) => ({
      target: this.multicallContract.address,
      callData: this.multicallContract.interface.encodeFunctionData(
        "getEthBalance",
        [address]
      ),
    }));
    const [, returnData] = await this.multicallContract.callStatic.aggregate(
      calls,
      {
        blockTag: blockNumber,
      }
    );
    const balances = returnData.map((data) =>
      this.multicallContract.interface.decodeFunctionResult(
        "getEthBalance",
        data
      )
    );
    return balances;
  }
}
