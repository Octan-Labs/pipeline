import { StaticJsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import usdcABI from "../config/abi/usdc-abi.json";

export class TotalSupplyCalcService {
  private readonly provider: StaticJsonRpcProvider;
  constructor(rpcUrl: string) {
    this.provider = new StaticJsonRpcProvider(rpcUrl);
  }

  async calculateTotalSupply(address: string) {
    const contract = new Contract(address, usdcABI, this.provider);
    return await contract.totalSupply();
  }
}
