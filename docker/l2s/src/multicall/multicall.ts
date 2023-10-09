import { Contract } from "@ethersproject/contracts";

interface CallData {
  address: string;
  callData: string;
}

export async function getMultipleContractMultipleData(
  contracts: Contract[],
  multicallContract: Contract,
  methodName: string,
  blockNumber: number,
  callInputs: string[][]
): Promise<any[]> {
  try {
    if (contracts.length !== callInputs.length) {
      throw new Error(
        "Contracts and callInputs arrays must have the same length"
      );
    }

    const calls: CallData[] = contracts.map((contract, i) => {
      if (!contract?.interface) {
        throw new Error(`Invalid contract at index ${i}`);
      }
      const fragment = contract.interface.getFunction(methodName);
      return {
        address: contract.address,
        callData: contract.interface.encodeFunctionData(
          fragment,
          callInputs[i]
        ),
      };
    });
    const chunks = await fetchChunk(multicallContract, calls, blockNumber);
    const { results } = chunks;
    return results.map((result, i) => {
      try {
        return contracts[i]?.interface?.decodeFunctionResult(
          methodName,
          result
        );
      } catch (error) {
        return undefined;
      }
    });
  } catch (error) {
    throw error;
  }
}

/**
 * Fetches a chunk of calls, enforcing a minimum block number constraint
 * @param multicallContract multicall contract to fetch against
 * @param chunk chunk of calls to make
 * @param blockNumber block number of the result set
 * @param minBlockNumber minimum block number of the result set
 */
async function fetchChunk(
  multicallContract: Contract,
  chunk: any,
  blockNumber: number,
  minBlockNumber = undefined,
  account = undefined
) {
  let resultsBlockNumber: any, returnData: any;

  try {
    [resultsBlockNumber, returnData] =
      account && multicallContract.signer
        ? await multicallContract.aggregate(
            chunk.map((obj: Contract) => [obj.address, obj.callData])
          )
        : await multicallContract.callStatic.aggregate(
            chunk.map((obj: Contract) => [obj.address, obj.callData]),
            {
              blockTag: blockNumber, // block number
            }
          );
  } catch (error) {
    throw error;
  }
  if (minBlockNumber && resultsBlockNumber.toNumber() < minBlockNumber) {
    throw new Error("Fetched for old block number");
  }
  return { results: returnData, blockNumber: resultsBlockNumber.toNumber() };
}

export async function getMultipleAddressEthBalance(
  addresses: string[],
  blockNumber: number,
  multicallContract: Contract
): Promise<any[]> {
  // Prepare calls for the Multicall2 contract
  const calls = addresses.map((address) => ({
    target: multicallContract.address,
    callData: multicallContract.interface.encodeFunctionData("getEthBalance", [
      address,
    ]),
  }));

  // Call the Multicall contract
  const [, returnData] = await multicallContract.callStatic.aggregate(calls, {
    blockTag: blockNumber,
  });

  const balances = returnData.map((data) =>
    multicallContract.interface.decodeFunctionResult("getEthBalance", data)
  );
  return balances;
}
