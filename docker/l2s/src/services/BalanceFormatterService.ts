import { EthProjectEscrowValue } from "../database/postgres";
import { ethers } from "ethers";

const DEFAULT_DECIMAL = 18;

export class BalanceFormatterService {
  formatEscrowBalance(
    contractData: any[],
    escrowContractTokens: any[],
    priceMap: {},
    priceTimestamp: Date
  ): EthProjectEscrowValue[] {
    return contractData.map((balance, i) => {
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
    });
  }

  formatEthEscrowBalance(
    ethMulticallResult: any[],
    ethEscrowContractTokens: any[],
    priceTimestamp: Date
  ) {
    return ethMulticallResult.map((balance, i) => {
      const balanceFormatted = ethers.formatUnits(balance.toString(), "ether");
      return {
        project_id: ethEscrowContractTokens[i].project_id,
        date: priceTimestamp,
        address: ethEscrowContractTokens[i].address,
        token_address: ethEscrowContractTokens[i].token_address,
        balance: +balanceFormatted,
        price: ethEscrowContractTokens[i].price ?? 0,
      };
    });
  }
}
