import { ChainKeyEnum } from 'src/common/enum';
import { enumValues } from './fns';

const validChainKeys = enumValues(ChainKeyEnum).map((chainKey) => {
  return chainKey.toLowerCase();
});

// Validate the chain key
export function IsValidChainKey(chain_key: string): boolean {
  return validChainKeys.includes(chain_key);
}
