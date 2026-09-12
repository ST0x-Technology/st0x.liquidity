import {
  getAddress,
  keccak256,
  stringToHex,
  type Address,
  type Hex,
} from "viem";

export const BASE_CHAIN_ID = 8453;
export const MAX_MESSAGE_BYTES = 4096;
const DECIMAL_INTEGER = /^(?:0|[1-9][0-9]*)$/;

export interface ApprovedTransaction {
  type: "eip1559";
  chainId: typeof BASE_CHAIN_ID;
  from: Address;
  representedInventory: Address | null;
  to: Address;
  value: "0";
  data: Hex;
  nonce: string;
  gasLimit: string;
  maxFeePerGas: string;
  maxPriorityFeePerGas: string;
  l1DataFeeWei: string;
  operatorFeeWei: string;
  maxTotalFeeWei: string;
  accessList: [];
}

export interface ApprovalManifest {
  version: 1;
  createdAtMs: number;
  expiresAtMs: number;
  message: string;
  transaction: ApprovedTransaction;
  approvalHash: Hex;
}

export function encodeMessage(message: string): Hex {
  if (message.length === 0) {
    throw new Error("message cannot be empty");
  }

  const encoded = new TextEncoder().encode(message);
  const decoded = new TextDecoder("utf-8", { fatal: true }).decode(encoded);
  if (decoded !== message) {
    throw new Error("message is not valid round-trip UTF-8");
  }
  if (encoded.byteLength > MAX_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_MESSAGE_BYTES} UTF-8 bytes`);
  }

  return `0x${Buffer.from(encoded).toString("hex")}`;
}

export function decodeMessage(data: Hex): string {
  if (!/^0x(?:[0-9a-fA-F]{2})+$/.test(data)) {
    throw new Error("calldata is not a non-empty byte-aligned hex value");
  }

  const bytes = Buffer.from(data.slice(2), "hex");
  if (bytes.byteLength > MAX_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_MESSAGE_BYTES} UTF-8 bytes`);
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    throw new Error("calldata is not strict UTF-8", { cause: error });
  }
}

function canonicalPayload(
  transaction: ApprovedTransaction,
  createdAtMs: number,
  expiresAtMs: number,
  message: string,
): string {
  return JSON.stringify({
    version: 1,
    createdAtMs,
    expiresAtMs,
    message,
    transaction: {
      type: transaction.type,
      chainId: transaction.chainId,
      from: getAddress(transaction.from),
      representedInventory:
        transaction.representedInventory === null
          ? null
          : getAddress(transaction.representedInventory),
      to: getAddress(transaction.to),
      value: transaction.value,
      data: transaction.data.toLowerCase(),
      nonce: transaction.nonce,
      gasLimit: transaction.gasLimit,
      maxFeePerGas: transaction.maxFeePerGas,
      maxPriorityFeePerGas: transaction.maxPriorityFeePerGas,
      l1DataFeeWei: transaction.l1DataFeeWei,
      operatorFeeWei: transaction.operatorFeeWei,
      maxTotalFeeWei: transaction.maxTotalFeeWei,
      accessList: transaction.accessList,
    },
  });
}

export function approvalHash(
  transaction: ApprovedTransaction,
  createdAtMs: number,
  expiresAtMs: number,
  message: string,
): Hex {
  return keccak256(
    stringToHex(
      canonicalPayload(transaction, createdAtMs, expiresAtMs, message),
    ),
  );
}

export function createManifest(
  transaction: ApprovedTransaction,
  createdAtMs: number,
  expiresAtMs: number,
): ApprovalManifest {
  const message = decodeMessage(transaction.data);
  return {
    version: 1,
    createdAtMs,
    expiresAtMs,
    message,
    transaction,
    approvalHash: approvalHash(transaction, createdAtMs, expiresAtMs, message),
  };
}

export function maximumTransactionFee(
  manifest: ApprovalManifest,
  additionalFees: {
    l1DataFeeWei: bigint;
    operatorFeeWei: bigint;
  } = {
    l1DataFeeWei: BigInt(manifest.transaction.l1DataFeeWei),
    operatorFeeWei: BigInt(manifest.transaction.operatorFeeWei),
  },
): bigint {
  return (
    BigInt(manifest.transaction.gasLimit) *
      BigInt(manifest.transaction.maxFeePerGas) +
    additionalFees.l1DataFeeWei +
    additionalFees.operatorFeeWei
  );
}

function approvedInteger(label: string, raw: unknown): bigint {
  if (typeof raw !== "string" || !DECIMAL_INTEGER.test(raw)) {
    throw new Error(
      `approval manifest ${label} is not a canonical decimal integer`,
    );
  }
  return BigInt(raw);
}

export function validateManifest(
  manifest: ApprovalManifest,
  nowMs = Date.now(),
  options: { allowExpired?: boolean } = {},
): void {
  if (!manifest || typeof manifest !== "object" || !manifest.transaction) {
    throw new Error("approval manifest shape is invalid");
  }
  if (manifest.version !== 1) {
    throw new Error(
      `unsupported manifest version: ${String(manifest.version)}`,
    );
  }
  for (const field of ["createdAtMs", "expiresAtMs"] as const) {
    if (!Number.isSafeInteger(manifest[field])) {
      throw new Error(`approval manifest ${field} is not a safe integer`);
    }
  }
  if (!options.allowExpired && manifest.expiresAtMs <= nowMs) {
    throw new Error("approval manifest has expired");
  }
  if (manifest.createdAtMs >= manifest.expiresAtMs) {
    throw new Error("approval manifest expiry is invalid");
  }
  if (manifest.createdAtMs > nowMs) {
    throw new Error("approval manifest is not active yet");
  }
  if (manifest.expiresAtMs - manifest.createdAtMs > 3_600_000) {
    throw new Error("approval manifest lifetime exceeds 3600 seconds");
  }
  if (manifest.transaction.chainId !== BASE_CHAIN_ID) {
    throw new Error("approval manifest is not for Base chain ID 8453");
  }
  if (manifest.transaction.type !== "eip1559") {
    throw new Error("approval manifest is not an EIP-1559 transaction");
  }
  if (manifest.transaction.value !== "0") {
    throw new Error("approval manifest transaction value is not zero");
  }
  if (
    !Array.isArray(manifest.transaction.accessList) ||
    manifest.transaction.accessList.length !== 0
  ) {
    throw new Error("approval manifest access list is not empty");
  }
  const decoded = decodeMessage(manifest.transaction.data);
  if (decoded !== manifest.message) {
    throw new Error("approval manifest message does not match calldata");
  }
  if (new TextEncoder().encode(decoded).byteLength > MAX_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_MESSAGE_BYTES} UTF-8 bytes`);
  }

  const nonce = approvedInteger("nonce", manifest.transaction.nonce);
  const gasLimit = approvedInteger("gasLimit", manifest.transaction.gasLimit);
  const maxFeePerGas = approvedInteger(
    "maxFeePerGas",
    manifest.transaction.maxFeePerGas,
  );
  const maxPriorityFeePerGas = approvedInteger(
    "maxPriorityFeePerGas",
    manifest.transaction.maxPriorityFeePerGas,
  );
  const l1DataFeeWei = approvedInteger(
    "l1DataFeeWei",
    manifest.transaction.l1DataFeeWei,
  );
  const operatorFeeWei = approvedInteger(
    "operatorFeeWei",
    manifest.transaction.operatorFeeWei,
  );
  const maxTotalFeeWei = approvedInteger(
    "maxTotalFeeWei",
    manifest.transaction.maxTotalFeeWei,
  );
  if (nonce > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("approval manifest nonce exceeds the safe integer range");
  }

  const maximumFee = gasLimit * maxFeePerGas + l1DataFeeWei + operatorFeeWei;
  if (maximumFee > maxTotalFeeWei) {
    throw new Error("transaction exceeds the approved maximum total fee");
  }
  if (maxPriorityFeePerGas > maxFeePerGas) {
    throw new Error("priority fee exceeds the maximum fee per gas");
  }

  const expected = approvalHash(
    manifest.transaction,
    manifest.createdAtMs,
    manifest.expiresAtMs,
    manifest.message,
  );
  if (
    typeof manifest.approvalHash !== "string" ||
    expected.toLowerCase() !== manifest.approvalHash.toLowerCase()
  ) {
    throw new Error("approval hash does not match the manifest");
  }
}
