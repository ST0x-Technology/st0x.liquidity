import {
  getAddress,
  keccak256,
  parseTransaction,
  recoverTransactionAddress,
  serializeTransaction,
  type AccessList,
  type Hex,
  type TransactionSerialized,
} from "viem";

import { type ApprovalManifest, validateManifest } from "./message.ts";

export function serializeManifestTransaction(
  manifest: ApprovalManifest,
  options: { allowExpired?: boolean } = {},
) {
  validateManifest(manifest, Date.now(), options);
  const approved = manifest.transaction;
  const transaction = {
    chainId: approved.chainId,
    to: approved.to,
    value: 0n,
    data: approved.data,
    nonce: Number(approved.nonce),
    gas: BigInt(approved.gasLimit),
    maxFeePerGas: BigInt(approved.maxFeePerGas),
    maxPriorityFeePerGas: BigInt(approved.maxPriorityFeePerGas),
    accessList: [] satisfies AccessList,
  };

  return {
    transaction,
    unsignedTransaction: serializeTransaction({
      ...transaction,
      type: "eip1559",
    }),
  };
}

function assertEqual(label: string, actual: unknown, expected: unknown): void {
  if (actual !== expected) {
    throw new Error(
      `signed transaction ${label} mismatch: received ${String(actual)}, expected ${String(expected)}`,
    );
  }
}

export async function validateSignedTransaction(
  manifest: ApprovalManifest,
  signedTransaction: Hex,
  options: { allowExpired?: boolean } = {},
): Promise<{ hash: Hex; signer: `0x${string}` }> {
  validateManifest(manifest, Date.now(), options);
  const approved = manifest.transaction;
  const parsed = parseTransaction(signedTransaction);

  assertEqual("type", parsed.type, "eip1559");
  assertEqual("chain ID", parsed.chainId, approved.chainId);
  assertEqual(
    "recipient",
    parsed.to && getAddress(parsed.to),
    getAddress(approved.to),
  );
  assertEqual("value", parsed.value ?? 0n, 0n);
  assertEqual(
    "calldata",
    (parsed.data ?? "0x").toLowerCase(),
    approved.data.toLowerCase(),
  );
  assertEqual("nonce", parsed.nonce, Number(approved.nonce));
  assertEqual("gas limit", parsed.gas, BigInt(approved.gasLimit));
  assertEqual(
    "maximum fee per gas",
    parsed.maxFeePerGas,
    BigInt(approved.maxFeePerGas),
  );
  assertEqual(
    "maximum priority fee per gas",
    parsed.maxPriorityFeePerGas,
    BigInt(approved.maxPriorityFeePerGas),
  );
  if ((parsed.accessList?.length ?? 0) !== 0) {
    throw new Error("signed transaction access list is not empty");
  }

  const signer = getAddress(
    await recoverTransactionAddress({
      serializedTransaction: signedTransaction as TransactionSerialized,
    }),
  );
  assertEqual("signer", signer, getAddress(approved.from));

  return { hash: keccak256(signedTransaction), signer };
}
