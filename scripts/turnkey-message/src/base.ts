import { createPublicClient, http, type Address, type Hex } from "viem";
import { base } from "viem/chains";
import { estimateL1Fee, estimateOperatorFee } from "viem/op-stack";

import { rpcUrl } from "./settings.ts";

export function createBaseClient() {
  return createPublicClient({
    chain: base,
    transport: http(rpcUrl(), { timeout: 20_000, retryCount: 3 }),
  });
}

export type BaseClient = ReturnType<typeof createBaseClient>;

export async function estimateBaseAdditionalFees(
  client: BaseClient,
  transaction: {
    account: Address;
    to: Address;
    data: Hex;
    nonce: number;
    gas: bigint;
    maxFeePerGas: bigint;
    maxPriorityFeePerGas: bigint;
  },
): Promise<{ l1DataFee: bigint; operatorFee: bigint }> {
  const [l1DataFee, operatorFee] = await Promise.all([
    estimateL1Fee(client, { ...transaction, value: 0n }),
    estimateOperatorFee(client, { ...transaction, value: 0n }),
  ]);
  return { l1DataFee, operatorFee };
}
