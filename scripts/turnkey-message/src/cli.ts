#!/usr/bin/env bun

import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";

import {
  getAddress,
  isHash,
  parseAbi,
  zeroHash,
  type Address,
  type Hash,
  type Hex,
} from "viem";

import {
  createBaseClient,
  estimateBaseAdditionalFees,
  type BaseClient,
} from "./base.ts";
import { createApiKeyStamper } from "./api-key-stamper.ts";
import { writeJsonAtomic } from "./durable-file.ts";
import {
  acknowledgeInboxMessages,
  BaseRpcError,
  scanInbox,
  transactionToMessage,
} from "./inbox.ts";
import {
  BASE_CHAIN_ID,
  createManifest,
  encodeMessage,
  maximumTransactionFee,
  type ApprovalManifest,
  validateManifest,
} from "./message.ts";
import { loadSettings, type Settings } from "./settings.ts";
import {
  serializeManifestTransaction,
  validateSignedTransaction,
} from "./transaction.ts";
import {
  createTurnkeyClient,
  policyPreflight,
  signOrResume,
} from "./turnkey.ts";

const INVENTORY_ABI = parseAbi([
  "function OPERATOR_ROLE() view returns (bytes32)",
  "function hasRole(bytes32 role, address account) view returns (bool)",
]);
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface Arguments {
  command: string;
  values: Map<string, string>;
  flags: Set<string>;
}

interface TurnkeyAuth {
  keyName: string;
  userId: string;
  keysFolder?: string;
}

interface ConfirmedTransaction {
  blockHash: Hash | null;
  blockNumber: bigint | null;
  from: Address;
  to: Address | null;
  value: bigint;
  input: Hex;
}

interface ConfirmedReceipt {
  blockHash: Hash;
  blockNumber: bigint;
}

export function parseArguments(argv: string[]): Arguments {
  const [command, ...rest] = argv;
  if (!command)
    throw new Error("command is required: prepare, send, inbox, or decode");
  const parsed = parseArgs({
    args: rest,
    allowPositionals: false,
    strict: true,
    options: {
      config: { type: "string" },
      recipient: { type: "string" },
      "message-file": { type: "string" },
      "max-total-fee-wei": { type: "string" },
      output: { type: "string" },
      "ttl-seconds": { type: "string" },
      manifest: { type: "string" },
      "approval-hash": { type: "string" },
      "turnkey-key-name": { type: "string" },
      "turnkey-keys-folder": { type: "string" },
      "turnkey-user-id": { type: "string" },
      checkpoint: { type: "string" },
      "from-block": { type: "string" },
      tx: { type: "string" },
      watch: { type: "boolean" },
      "allow-contract-recipient": { type: "boolean" },
    },
  });
  const values = new Map(
    Object.entries(parsed.values).flatMap(([key, value]) =>
      typeof value === "string" ? [[key, value]] : [],
    ),
  );
  const flags = new Set(
    Object.entries(parsed.values).flatMap(([key, value]) =>
      value === true ? [key] : [],
    ),
  );
  return { command, values, flags };
}

function required(arguments_: Arguments, name: string): string {
  const value = arguments_.values.get(name);
  if (!value) throw new Error(`--${name} is required`);
  return value;
}

export function requiredTransactionHash(arguments_: Arguments): Hash {
  const hash = required(arguments_, "tx");
  if (!isHash(hash)) {
    throw new Error("--tx must be a 32-byte transaction hash");
  }
  return hash;
}

export function turnkeyAuth(arguments_: Arguments): TurnkeyAuth {
  const keyName = arguments_.values.get("turnkey-key-name");
  const userId = arguments_.values.get("turnkey-user-id");
  const keysFolder = arguments_.values.get("turnkey-keys-folder");
  if (keyName === undefined || userId === undefined) {
    throw new Error(
      "--turnkey-key-name and --turnkey-user-id are required for Turnkey authentication",
    );
  }
  if (!UUID_PATTERN.test(userId)) {
    throw new Error("--turnkey-user-id must be a UUID");
  }
  return { keyName, userId, ...(keysFolder ? { keysFolder } : {}) };
}

function optionalBigInt(
  arguments_: Arguments,
  name: string,
): bigint | undefined {
  const value = arguments_.values.get(name);
  if (value === undefined) return undefined;
  if (!/^\d+$/.test(value))
    throw new Error(`--${name} must be an unsigned integer`);
  return BigInt(value);
}

async function assertEoa(
  rpc: BaseClient,
  address: Address,
  label: string,
): Promise<void> {
  const code = await rpc.getCode({ address });
  if (code && code !== "0x")
    throw new Error(`${label} ${address} has deployed code`);
}

export async function assertRecipientExecution(
  rpc: BaseClient,
  recipient: Address,
  allowContractRecipient: boolean,
): Promise<void> {
  const code = await rpc.getCode({ address: recipient });
  if (code && code !== "0x" && !allowContractRecipient) {
    throw new Error(
      `recipient ${recipient} has deployed code; review its fallback behavior and pass --allow-contract-recipient to opt in`,
    );
  }
}

export async function assertBaseChain(rpc: BaseClient): Promise<void> {
  if ((await rpc.getChainId()) !== BASE_CHAIN_ID) {
    throw new Error("RPC is not Base chain ID 8453");
  }
}

export function transactionConfirmations(
  latest: bigint,
  blockNumber: bigint | null,
): number {
  if (blockNumber === null || latest < blockNumber) return 0;
  return Number(latest - blockNumber + 1n);
}

export function assertSameTransactionInclusion(
  transaction: Pick<ConfirmedTransaction, "blockHash" | "blockNumber">,
  receipt: ConfirmedReceipt,
): void {
  if (
    transaction.blockHash === null ||
    transaction.blockNumber === null ||
    transaction.blockHash.toLowerCase() !== receipt.blockHash.toLowerCase() ||
    transaction.blockNumber !== receipt.blockNumber
  ) {
    throw new Error("transaction and receipt refer to different inclusions");
  }
}

async function assertInventoryAuthority(
  rpc: BaseClient,
  settings: Settings,
): Promise<void> {
  await assertBaseChain(rpc);
  await assertEoa(rpc, settings.signer, "configured signer");
  const inventoryCode = await rpc.getCode({ address: settings.inventory });
  if (!inventoryCode || inventoryCode === "0x") {
    throw new Error(`inventory ${settings.inventory} has no deployed code`);
  }
  const operatorRole = await rpc.readContract({
    address: settings.inventory,
    abi: INVENTORY_ABI,
    functionName: "OPERATOR_ROLE",
  });
  const [isAdmin, isOperator] = await Promise.all([
    rpc.readContract({
      address: settings.inventory,
      abi: INVENTORY_ABI,
      functionName: "hasRole",
      args: [zeroHash, settings.signer],
    }),
    rpc.readContract({
      address: settings.inventory,
      abi: INVENTORY_ABI,
      functionName: "hasRole",
      args: [operatorRole, settings.signer],
    }),
  ]);
  if (!isAdmin || !isOperator) {
    throw new Error(
      `configured signer does not hold DEFAULT_ADMIN_ROLE and OPERATOR_ROLE on ${settings.inventory}`,
    );
  }
}

export function assertCanonicalMessageTransaction(
  transaction: ConfirmedTransaction,
  receipt: ConfirmedReceipt,
  settings: { signer: Address; recipient: Address },
  data: Hex,
): void {
  const mismatches = [
    transaction.blockHash === null ||
    transaction.blockHash.toLowerCase() !== receipt.blockHash.toLowerCase()
      ? "blockHash"
      : undefined,
    transaction.blockNumber === null ||
    transaction.blockNumber !== receipt.blockNumber
      ? "blockNumber"
      : undefined,
    getAddress(transaction.from) !== settings.signer ? "from" : undefined,
    !transaction.to || getAddress(transaction.to) !== settings.recipient
      ? "to"
      : undefined,
    transaction.value !== 0n ? "value" : undefined,
    transaction.input.toLowerCase() !== data.toLowerCase()
      ? "input"
      : undefined,
  ].filter((field): field is string => field !== undefined);
  if (mismatches.length > 0) {
    throw new Error(
      `confirmed Base transaction differs from the approved message: ${mismatches.join(", ")}`,
    );
  }
}

async function prepare(arguments_: Arguments): Promise<void> {
  const messagePath = required(arguments_, "message-file");
  const messageBytes = await readFile(messagePath);
  let message: string;
  try {
    message = new TextDecoder("utf-8", { fatal: true }).decode(messageBytes);
  } catch (error) {
    throw new Error(`message file ${messagePath} is not strict UTF-8`, {
      cause: error,
    });
  }
  const data = encodeMessage(message);
  const recipient = getAddress(required(arguments_, "recipient"));
  const maximumTotalFee = optionalBigInt(arguments_, "max-total-fee-wei");
  if (maximumTotalFee === undefined) {
    throw new Error("--max-total-fee-wei is required");
  }
  const ttl = optionalBigInt(arguments_, "ttl-seconds") ?? 600n;
  if (ttl < 60n || ttl > 3_600n) {
    throw new Error("--ttl-seconds must be an integer from 60 through 3600");
  }
  const settings = await loadSettings(required(arguments_, "config"));
  const rpc = createBaseClient();
  await assertInventoryAuthority(rpc, settings);
  await assertRecipientExecution(
    rpc,
    recipient,
    arguments_.flags.has("allow-contract-recipient"),
  );
  const [nonce, fees, balance] = await Promise.all([
    rpc.getTransactionCount({ address: settings.signer, blockTag: "pending" }),
    rpc.estimateFeesPerGas(),
    rpc.getBalance({ address: settings.signer, blockTag: "pending" }),
  ]);
  if (!fees.maxFeePerGas || !fees.maxPriorityFeePerGas) {
    throw new Error("Base RPC did not return EIP-1559 fee estimates");
  }
  const gas = await rpc.estimateGas({
    account: settings.signer,
    to: recipient,
    value: 0n,
    data,
    maxFeePerGas: fees.maxFeePerGas,
    maxPriorityFeePerGas: fees.maxPriorityFeePerGas,
  });
  const additionalFees = await estimateBaseAdditionalFees(rpc, {
    account: settings.signer,
    to: recipient,
    data,
    nonce,
    gas,
    maxFeePerGas: fees.maxFeePerGas,
    maxPriorityFeePerGas: fees.maxPriorityFeePerGas,
  });
  const maximumNetworkFee =
    gas * fees.maxFeePerGas +
    additionalFees.l1DataFee +
    additionalFees.operatorFee;
  if (maximumNetworkFee > maximumTotalFee) {
    throw new Error(
      `estimated maximum fee ${maximumNetworkFee} wei exceeds operator cap ${maximumTotalFee} wei`,
    );
  }
  if (balance < maximumNetworkFee) {
    throw new Error(
      `signer balance ${balance} wei cannot cover maximum fee ${maximumNetworkFee} wei`,
    );
  }
  const ttlSeconds = Number(ttl);
  const now = Date.now();
  const manifest = createManifest(
    {
      type: "eip1559",
      chainId: BASE_CHAIN_ID,
      from: settings.signer,
      representedInventory: settings.inventory,
      to: recipient,
      value: "0",
      data,
      nonce: nonce.toString(),
      gasLimit: gas.toString(),
      maxFeePerGas: fees.maxFeePerGas.toString(),
      maxPriorityFeePerGas: fees.maxPriorityFeePerGas.toString(),
      l1DataFeeWei: additionalFees.l1DataFee.toString(),
      operatorFeeWei: additionalFees.operatorFee.toString(),
      maxTotalFeeWei: maximumTotalFee.toString(),
      accessList: [],
    },
    now,
    now + ttlSeconds * 1_000,
  );
  const output =
    arguments_.values.get("output") ?? ".tmp/turnkey-message/manifest.json";
  await writeJsonAtomic(output, manifest);
  console.log(JSON.stringify({ output, ...manifest }, null, 2));
}

function assertManifestIdentity(
  manifest: ApprovalManifest,
  settings: Settings,
): void {
  const transaction = manifest.transaction;
  if (
    getAddress(transaction.from) !== settings.signer ||
    getAddress(transaction.representedInventory) !== settings.inventory
  ) {
    throw new Error(
      "manifest identities do not match the pinned configuration",
    );
  }
}

async function send(arguments_: Arguments): Promise<void> {
  const settings = await loadSettings(required(arguments_, "config"));
  const manifestPath = required(arguments_, "manifest");
  const manifest = JSON.parse(
    await readFile(manifestPath, "utf8"),
  ) as ApprovalManifest;
  validateManifest(manifest);
  assertManifestIdentity(manifest, settings);
  if (
    required(arguments_, "approval-hash").toLowerCase() !==
    manifest.approvalHash.toLowerCase()
  ) {
    throw new Error("--approval-hash does not match the manifest");
  }
  const rpc = createBaseClient();
  await assertInventoryAuthority(rpc, settings);
  const recipient = getAddress(manifest.transaction.to);
  await assertRecipientExecution(
    rpc,
    recipient,
    arguments_.flags.has("allow-contract-recipient"),
  );
  const [nonce, balance, gas] = await Promise.all([
    rpc.getTransactionCount({ address: settings.signer, blockTag: "pending" }),
    rpc.getBalance({ address: settings.signer, blockTag: "pending" }),
    rpc.estimateGas({
      account: settings.signer,
      to: recipient,
      value: 0n,
      data: manifest.transaction.data,
      maxFeePerGas: BigInt(manifest.transaction.maxFeePerGas),
      maxPriorityFeePerGas: BigInt(manifest.transaction.maxPriorityFeePerGas),
    }),
  ]);
  if (nonce.toString() !== manifest.transaction.nonce) {
    throw new Error(
      "pending nonce changed; prepare and approve a new manifest",
    );
  }
  if (gas > BigInt(manifest.transaction.gasLimit)) {
    throw new Error("current gas estimate exceeds the approved gas limit");
  }
  const additionalFees = await estimateBaseAdditionalFees(rpc, {
    account: settings.signer,
    to: recipient,
    data: manifest.transaction.data,
    nonce,
    gas: BigInt(manifest.transaction.gasLimit),
    maxFeePerGas: BigInt(manifest.transaction.maxFeePerGas),
    maxPriorityFeePerGas: BigInt(manifest.transaction.maxPriorityFeePerGas),
  });
  const transactionFee = maximumTransactionFee(manifest, {
    l1DataFeeWei: additionalFees.l1DataFee,
    operatorFeeWei: additionalFees.operatorFee,
  });
  if (transactionFee > BigInt(manifest.transaction.maxTotalFeeWei)) {
    throw new Error("current Base fee estimate exceeds the approved fee cap");
  }
  if (balance < transactionFee) {
    throw new Error(
      "signer balance cannot cover the transaction's maximum fee",
    );
  }

  const auth = turnkeyAuth(arguments_);
  const stamper = await createApiKeyStamper({
    keyName: auth.keyName,
    ...(auth.keysFolder ? { keysFolder: auth.keysFolder } : {}),
  });
  const turnkey = createTurnkeyClient(stamper);
  const preflight = await policyPreflight(
    turnkey,
    settings.organizationId,
    auth.userId,
  );
  console.log(JSON.stringify({ turnkeyPreflight: preflight }, null, 2));
  const { unsignedTransaction } = serializeManifestTransaction(manifest);
  const signed = await signOrResume(
    turnkey,
    settings,
    unsignedTransaction,
    manifest.approvalHash,
    `${manifestPath}.activity.json`,
  );
  const verified = await validateSignedTransaction(manifest, signed);

  let broadcastHash: Hex;
  try {
    broadcastHash = await rpc.sendRawTransaction({
      serializedTransaction: signed,
    });
  } catch (error) {
    try {
      await rpc.getTransaction({ hash: verified.hash });
      broadcastHash = verified.hash;
    } catch {
      throw new Error("Base RPC rejected the signed transaction", {
        cause: error,
      });
    }
  }
  if (broadcastHash.toLowerCase() !== verified.hash.toLowerCase()) {
    throw new Error(
      "Base RPC returned a transaction hash that differs from the local hash",
    );
  }
  const receipt = await rpc.waitForTransactionReceipt({
    hash: verified.hash,
    confirmations: settings.requiredConfirmations,
    timeout: 180_000,
  });
  if (receipt.status !== "success")
    throw new Error("public message transaction reverted");
  const canonical = await rpc.getTransaction({ hash: verified.hash });
  assertCanonicalMessageTransaction(
    canonical,
    receipt,
    { signer: settings.signer, recipient },
    manifest.transaction.data,
  );
  console.log(
    JSON.stringify(
      {
        transactionHash: verified.hash,
        explorer: `https://basescan.org/tx/${verified.hash}`,
        confirmations: settings.requiredConfirmations,
        message: manifest.message,
      },
      null,
      2,
    ),
  );
}

async function inbox(arguments_: Arguments): Promise<void> {
  const settings = await loadSettings(required(arguments_, "config"));
  const rpc = createBaseClient();
  await assertBaseChain(rpc);
  const checkpointPath =
    arguments_.values.get("checkpoint") ?? ".tmp/turnkey-message/inbox.json";
  const fromBlock = optionalBigInt(arguments_, "from-block");
  const watch = arguments_.flags.has("watch");
  do {
    try {
      const result = await scanInbox(rpc, {
        recipient: settings.signer,
        trustedSender: settings.trustedSender,
        confirmations: settings.requiredConfirmations,
        checkpointPath,
        ...(fromBlock === undefined ? {} : { fromBlock }),
      });
      console.log(JSON.stringify(result, null, 2));
      await acknowledgeInboxMessages(
        checkpointPath,
        settings.signer,
        settings.trustedSender,
        result.messages.map(({ hash }) => hash),
      );
    } catch (error) {
      if (!watch || !(error instanceof BaseRpcError)) throw error;
      console.error(
        `inbox scan failed; retrying in 15 seconds: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    if (!watch) break;
    await Bun.sleep(15_000);
  } while (true);
}

async function decode(arguments_: Arguments): Promise<void> {
  const settings = await loadSettings(required(arguments_, "config"));
  const rpc = createBaseClient();
  await assertBaseChain(rpc);
  const hash = requiredTransactionHash(arguments_);
  const [transaction, receipt, latest] = await Promise.all([
    rpc.getTransaction({ hash }),
    rpc.getTransactionReceipt({ hash }),
    rpc.getBlockNumber(),
  ]);
  assertSameTransactionInclusion(transaction, receipt);
  const message = transactionToMessage(
    transaction,
    settings.signer,
    settings.trustedSender,
    receipt.status === "success",
  );
  console.log(
    JSON.stringify(
      {
        explorer: `https://basescan.org/tx/${hash}`,
        confirmations: transactionConfirmations(
          latest,
          transaction.blockNumber,
        ),
        direction: message ? "inbound" : "other",
        transaction: message ?? {
          hash,
          from: transaction.from,
          to: transaction.to,
          value: transaction.value.toString(),
          input: transaction.input,
          status: receipt.status,
        },
      },
      null,
      2,
    ),
  );
}

async function main(): Promise<void> {
  const arguments_ = parseArguments(process.argv.slice(2));
  switch (arguments_.command) {
    case "prepare":
      await prepare(arguments_);
      break;
    case "send":
      await send(arguments_);
      break;
    case "inbox":
      await inbox(arguments_);
      break;
    case "decode":
      await decode(arguments_);
      break;
    default:
      throw new Error(`unknown command: ${arguments_.command}`);
  }
}

export function formatError(error: unknown): string {
  const messages: string[] = [];
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (current instanceof Error && !seen.has(current)) {
    seen.add(current);
    messages.push(current.message);
    current = current.cause;
  }
  if (current !== undefined && !seen.has(current)) {
    messages.push(String(current));
  }
  return messages.length > 0 ? messages.join(": ") : String(error);
}

if (import.meta.main) {
  await main().catch((error: unknown) => {
    console.error(formatError(error));
    process.exitCode = 1;
  });
}
