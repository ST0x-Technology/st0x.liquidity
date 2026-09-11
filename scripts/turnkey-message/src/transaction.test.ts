import { describe, expect, test } from "bun:test";
import { privateKeyToAccount } from "viem/accounts";

import { createManifest, encodeMessage } from "./message.ts";
import {
  serializeManifestTransaction,
  validateSignedTransaction,
} from "./transaction.ts";

const account = privateKeyToAccount(
  "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
);
const otherAccount = privateKeyToAccount(
  "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
);

function manifest() {
  const now = Date.now();
  return createManifest(
    {
      type: "eip1559",
      chainId: 8453,
      from: account.address,
      representedInventory: "0x10e4db39275C3b128C01bA1194D45D19aE1520d9",
      to: "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
      value: "0",
      data: encodeMessage("hello"),
      nonce: "7",
      gasLimit: "22000",
      maxFeePerGas: "2000000000",
      maxPriorityFeePerGas: "1000000",
      l1DataFeeWei: "100",
      operatorFeeWei: "5",
      maxTotalFeeWei: "44000000000105",
      accessList: [],
    },
    now,
    now + 60_000,
  );
}

describe("signed transaction validation", () => {
  test("accepts the exact approved EIP-1559 transaction", async () => {
    const approved = manifest();
    const serialized = serializeManifestTransaction(approved);
    const signature = await account.signTransaction({
      ...serialized.transaction,
      type: "eip1559",
    });

    expect(await validateSignedTransaction(approved, signature)).toEqual({
      hash: expect.stringMatching(/^0x[0-9a-f]{64}$/),
      signer: account.address,
    });
  });

  test("rejects a signed field mutation", async () => {
    const approved = manifest();
    const serialized = serializeManifestTransaction(approved);
    const signature = await account.signTransaction({
      ...serialized.transaction,
      nonce: serialized.transaction.nonce + 1,
      type: "eip1559",
    });

    await expect(
      validateSignedTransaction(approved, signature),
    ).rejects.toThrow("nonce");
  });

  test("rejects a different signer", async () => {
    const approved = manifest();
    const serialized = serializeManifestTransaction(approved);
    const signature = await otherAccount.signTransaction({
      ...serialized.transaction,
      type: "eip1559",
    });
    await expect(
      validateSignedTransaction(approved, signature),
    ).rejects.toThrow("signer");
  });

  test("rejects a different recipient", async () => {
    const approved = manifest();
    const serialized = serializeManifestTransaction(approved);
    const signature = await account.signTransaction({
      ...serialized.transaction,
      to: "0x0000000000000000000000000000000000000001",
      type: "eip1559",
    });
    await expect(
      validateSignedTransaction(approved, signature),
    ).rejects.toThrow("recipient");
  });

  test("rejects different calldata", async () => {
    const approved = manifest();
    const serialized = serializeManifestTransaction(approved);
    const signature = await account.signTransaction({
      ...serialized.transaction,
      data: encodeMessage("different"),
      type: "eip1559",
    });
    await expect(
      validateSignedTransaction(approved, signature),
    ).rejects.toThrow("calldata");
  });
});
