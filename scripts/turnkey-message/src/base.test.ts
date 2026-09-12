import { describe, expect, test } from "bun:test";
import {
  createPublicClient,
  custom,
  decodeFunctionData,
  encodeAbiParameters,
  parseAbi,
  parseTransaction,
  toFunctionSelector,
  type EIP1193RequestFn,
  type Hex,
} from "viem";
import { base } from "viem/chains";

import { estimateBaseAdditionalFees, type BaseClient } from "./base.ts";

const L1_FEE_ABI = parseAbi([
  "function getL1Fee(bytes transaction) view returns (uint256)",
]);

describe("Base additional fee estimates", () => {
  test("reads L1 data and operator fees from the Base gas oracle", async () => {
    const transactionValues: Array<Hex | undefined> = [];
    const request = (async ({
      method,
      params,
    }: {
      method: string;
      params?: readonly unknown[];
    }) => {
      if (method === "eth_estimateGas") {
        const call = params?.[0] as { value?: Hex } | undefined;
        transactionValues.push(call?.value);
        return "0x5208";
      }
      if (method === "eth_call") {
        const call = params?.[0] as { data: string } | undefined;
        if (!call) throw new Error("eth_call parameters are missing");
        const { data } = call;
        if (data.startsWith(toFunctionSelector("getL1Fee(bytes)"))) {
          const decoded = decodeFunctionData({
            abi: L1_FEE_ABI,
            data: data as Hex,
          });
          const transaction = parseTransaction(decoded.args[0] as Hex);
          transactionValues.push(
            transaction.value === undefined
              ? undefined
              : `0x${transaction.value.toString(16)}`,
          );
          return encodeAbiParameters([{ type: "uint256" }], [123n]);
        }
        if (data.startsWith(toFunctionSelector("getOperatorFee(uint256)"))) {
          return encodeAbiParameters([{ type: "uint256" }], [7n]);
        }
      }
      throw new Error(`unexpected RPC method: ${method}`);
    }) as EIP1193RequestFn;
    const client = createPublicClient({
      chain: base,
      transport: custom({ request }),
    }) as unknown as BaseClient;

    await expect(
      estimateBaseAdditionalFees(client, {
        account: "0xA9C16673F65AE808688cB18952AFE3d9658C808f",
        to: "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
        data: "0x68656c6c6f",
        nonce: 7,
        gas: 22_000n,
        maxFeePerGas: 2_000_000_000n,
        maxPriorityFeePerGas: 1_000_000n,
      }),
    ).resolves.toEqual({ l1DataFee: 123n, operatorFee: 7n });
    expect(transactionValues).toHaveLength(2);
    expect(
      transactionValues.every(
        (value) => value === undefined || value === "0x0",
      ),
    ).toBe(true);
  });
});
