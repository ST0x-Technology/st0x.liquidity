import { mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";

import lockfile from "proper-lockfile";
import {
  BlockNotFoundError,
  getAddress,
  HttpRequestError,
  RpcRequestError,
  SocketClosedError,
  TimeoutError,
  WebSocketRequestError,
  type Address,
  type Hash,
  type Hex,
} from "viem";

import type { BaseClient } from "./base.ts";
import { writeJsonAtomic } from "./durable-file.ts";
import { decodeMessage } from "./message.ts";

export interface InboxMessage {
  hash: Hash;
  blockNumber: string;
  blockHash: Hash;
  from: Address;
  to: Address;
  value: string;
  text?: string;
  trust: "trusted" | "untrusted";
  valid: boolean;
  warnings: string[];
}

export interface InboxCheckpoint {
  version: 1;
  recipient: Address;
  trustedSender: Address;
  nextBlock: string;
  recentBlocks: Array<{ number: string; hash: Hash }>;
}

export interface InboxHistory {
  version: 1;
  recipient: Address;
  trustedSender: Address;
  messages: InboxMessage[];
  pending: Hash[];
}

export interface InboxScanResult {
  checkpoint: InboxCheckpoint;
  messages: InboxMessage[];
  historyPath: string;
}

interface InboundTransaction {
  hash: Hash;
  blockNumber: bigint | null;
  blockHash: Hash | null;
  from: Address;
  to: Address | null;
  value: bigint;
  input: Hex;
}

interface InboxIdentity {
  recipient: Address;
  trustedSender: Address;
}

function identity(recipient: Address, trustedSender: Address): InboxIdentity {
  return {
    recipient: getAddress(recipient),
    trustedSender: getAddress(trustedSender),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isHash(value: unknown): value is Hash {
  return typeof value === "string" && /^0x[0-9a-fA-F]{64}$/.test(value);
}

function isAddress(value: unknown): value is Address {
  if (typeof value !== "string") return false;
  try {
    getAddress(value);
    return true;
  } catch {
    return false;
  }
}

function matchesIdentity(
  value: Record<string, unknown>,
  expected: InboxIdentity,
) {
  return (
    isAddress(value.recipient) &&
    getAddress(value.recipient) === expected.recipient &&
    isAddress(value.trustedSender) &&
    getAddress(value.trustedSender) === expected.trustedSender
  );
}

function hasValidIdentity(value: Record<string, unknown>): boolean {
  return isAddress(value.recipient) && isAddress(value.trustedSender);
}

function isCheckpointBlock(
  value: unknown,
): value is { number: string; hash: Hash } {
  return (
    isRecord(value) &&
    typeof value.number === "string" &&
    /^(?:0|[1-9][0-9]*)$/.test(value.number) &&
    isHash(value.hash)
  );
}

function isInboxMessage(value: unknown): value is InboxMessage {
  return (
    isRecord(value) &&
    isHash(value.hash) &&
    typeof value.blockNumber === "string" &&
    /^(?:0|[1-9][0-9]*)$/.test(value.blockNumber) &&
    isHash(value.blockHash) &&
    isAddress(value.from) &&
    isAddress(value.to) &&
    typeof value.value === "string" &&
    /^(?:0|[1-9][0-9]*)$/.test(value.value) &&
    (value.text === undefined || typeof value.text === "string") &&
    (value.trust === "trusted" || value.trust === "untrusted") &&
    typeof value.valid === "boolean" &&
    Array.isArray(value.warnings) &&
    value.warnings.every((warning) => typeof warning === "string")
  );
}

export function transactionToMessage(
  transaction: InboundTransaction,
  recipient: Address,
  trustedSender: Address,
  succeeded: boolean,
): InboxMessage | undefined {
  if (!transaction.to || getAddress(transaction.to) !== getAddress(recipient)) {
    return undefined;
  }
  if (transaction.blockNumber === null || transaction.blockHash === null) {
    throw new Error(`transaction ${transaction.hash} is not mined`);
  }

  const warnings: string[] = [];
  if (transaction.value !== 0n) warnings.push("transaction value is not zero");
  if (!succeeded) warnings.push("transaction execution failed");
  let text: string | undefined;
  try {
    text = decodeMessage(transaction.input);
  } catch (error) {
    warnings.push(error instanceof Error ? error.message : String(error));
  }

  return {
    hash: transaction.hash,
    blockNumber: transaction.blockNumber.toString(),
    blockHash: transaction.blockHash,
    from: getAddress(transaction.from),
    to: getAddress(transaction.to),
    value: transaction.value.toString(),
    ...(text === undefined ? {} : { text }),
    trust:
      getAddress(transaction.from) === getAddress(trustedSender)
        ? "trusted"
        : "untrusted",
    valid: warnings.length === 0,
    warnings,
  };
}

export function reconcileCheckpoint(
  checkpoint: InboxCheckpoint,
  canonicalHashes: ReadonlyMap<string, Hash>,
):
  | { rewound: false; checkpoint: InboxCheckpoint }
  | { rewound: true; checkpoint: InboxCheckpoint } {
  for (let index = checkpoint.recentBlocks.length - 1; index >= 0; index -= 1) {
    const block = checkpoint.recentBlocks[index];
    if (!block) continue;
    const canonical = canonicalHashes.get(block.number);
    if (canonical?.toLowerCase() !== block.hash.toLowerCase()) continue;
    if (index === checkpoint.recentBlocks.length - 1) {
      return { rewound: false, checkpoint };
    }
    const rewind = BigInt(block.number) + 1n;
    return {
      rewound: true,
      checkpoint: {
        ...checkpoint,
        nextBlock: rewind.toString(),
        recentBlocks: checkpoint.recentBlocks.slice(0, index + 1),
      },
    };
  }

  const oldest = checkpoint.recentBlocks[0];
  if (!oldest) return { rewound: false, checkpoint };
  const rewind = BigInt(oldest.number);
  return {
    rewound: true,
    checkpoint: {
      ...checkpoint,
      nextBlock: rewind.toString(),
      recentBlocks: [],
    },
  };
}

export function reconcileHistory(
  history: InboxHistory,
  rewindBlock: bigint,
): InboxHistory {
  const messages = history.messages.filter(
    ({ blockNumber }) => BigInt(blockNumber) < rewindBlock,
  );
  const retained = new Set(messages.map(({ hash }) => hash.toLowerCase()));
  return {
    ...history,
    messages,
    pending: history.pending.filter((hash) => retained.has(hash.toLowerCase())),
  };
}

export async function loadCheckpoint(
  path: string,
  recipient: Address,
  trustedSender: Address,
  fromBlock?: bigint,
): Promise<InboxCheckpoint> {
  return (
    await loadCheckpointState(
      path,
      identity(recipient, trustedSender),
      fromBlock,
    )
  ).checkpoint;
}

async function loadCheckpointState(
  path: string,
  expectedIdentity: InboxIdentity,
  fromBlock?: bigint,
): Promise<{ checkpoint: InboxCheckpoint; created: boolean }> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      if (fromBlock === undefined) {
        throw new Error(
          `no inbox checkpoint exists at ${path}; a starting block is required`,
        );
      }
      return {
        checkpoint: {
          version: 1,
          ...expectedIdentity,
          nextBlock: fromBlock.toString(),
          recentBlocks: [],
        },
        created: true,
      };
    }
    throw new Error(`checkpoint ${path} is corrupt`, { cause: error });
  }
  if (
    !isRecord(parsed) ||
    parsed.version !== 1 ||
    !hasValidIdentity(parsed) ||
    typeof parsed.nextBlock !== "string" ||
    !/^\d+$/.test(parsed.nextBlock) ||
    !Array.isArray(parsed.recentBlocks) ||
    !parsed.recentBlocks.every(isCheckpointBlock)
  ) {
    throw new Error(`checkpoint ${path} is corrupt`);
  }
  if (!matchesIdentity(parsed, expectedIdentity)) {
    throw new Error(
      `checkpoint ${path} belongs to a different recipient or trusted sender; use a different checkpoint path`,
    );
  }
  return { checkpoint: parsed as unknown as InboxCheckpoint, created: false };
}

async function loadHistory(
  path: string,
  expectedIdentity: InboxIdentity,
): Promise<InboxHistory> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { version: 1, ...expectedIdentity, messages: [], pending: [] };
    }
    throw new Error(`inbox history ${path} is corrupt`, { cause: error });
  }
  if (
    !isRecord(parsed) ||
    parsed.version !== 1 ||
    !hasValidIdentity(parsed) ||
    !Array.isArray(parsed.messages) ||
    !parsed.messages.every(isInboxMessage) ||
    !Array.isArray(parsed.pending) ||
    !parsed.pending.every(isHash)
  ) {
    throw new Error(`inbox history ${path} is corrupt`);
  }
  if (!matchesIdentity(parsed, expectedIdentity)) {
    throw new Error(
      `inbox history ${path} belongs to a different recipient or trusted sender; use a different checkpoint path`,
    );
  }
  const messageHashes = new Set(
    parsed.messages.map((message) => message.hash.toLowerCase()),
  );
  if (!parsed.pending.every((hash) => messageHashes.has(hash.toLowerCase()))) {
    throw new Error(`inbox history ${path} is corrupt`);
  }
  return parsed as unknown as InboxHistory;
}

export class BaseRpcError extends Error {}

function isRetryableRpcError(error: unknown): boolean {
  if (
    error instanceof SocketClosedError ||
    error instanceof TimeoutError ||
    error instanceof WebSocketRequestError
  ) {
    return true;
  }
  if (error instanceof HttpRequestError) {
    return (
      error.status === undefined ||
      error.status === 408 ||
      error.status === 425 ||
      error.status === 429 ||
      error.status >= 500
    );
  }
  if (error instanceof RpcRequestError) {
    return [-32_603, -32_002, -32_005].includes(error.code);
  }
  if (error instanceof Error && error.cause !== undefined) {
    return isRetryableRpcError(error.cause);
  }
  return false;
}

async function retry<T>(operation: () => Promise<T>): Promise<T> {
  let lastError: unknown;
  for (const delayMs of [0, 250, 1_000, 4_000]) {
    if (delayMs > 0) await Bun.sleep(delayMs);
    try {
      return await operation();
    } catch (error) {
      if (!isRetryableRpcError(error)) throw error;
      lastError = error;
    }
  }
  throw new BaseRpcError("Base RPC operation failed after four attempts", {
    cause: lastError,
  });
}

function pendingMessages(history: InboxHistory): InboxMessage[] {
  const pending = new Set(history.pending.map((hash) => hash.toLowerCase()));
  return history.messages.filter(({ hash }) => pending.has(hash.toLowerCase()));
}

function pruneHistory(
  history: InboxHistory,
  checkpoint: InboxCheckpoint,
): InboxHistory {
  const pending = new Set(history.pending.map((hash) => hash.toLowerCase()));
  const replayFloor = BigInt(
    checkpoint.recentBlocks[0]?.number ?? checkpoint.nextBlock,
  );
  return {
    ...history,
    messages: history.messages.filter(
      ({ hash, blockNumber }) =>
        pending.has(hash.toLowerCase()) || BigInt(blockNumber) >= replayFloor,
    ),
  };
}

export async function acknowledgeInboxMessages(
  checkpointPath: string,
  recipient: Address,
  trustedSender: Address,
  hashes: Hash[],
): Promise<void> {
  if (hashes.length === 0) return;
  const release = await lockfile.lock(checkpointPath, {
    realpath: false,
    retries: { forever: true, factor: 1.2, minTimeout: 100, maxTimeout: 1_000 },
  });
  try {
    const historyPath = `${checkpointPath}.messages.json`;
    const history = await loadHistory(
      historyPath,
      identity(recipient, trustedSender),
    );
    const acknowledged = new Set(hashes.map((hash) => hash.toLowerCase()));
    history.pending = history.pending.filter(
      (hash) => !acknowledged.has(hash.toLowerCase()),
    );
    const checkpoint = await loadCheckpointState(
      checkpointPath,
      identity(recipient, trustedSender),
    );
    await writeJsonAtomic(
      historyPath,
      pruneHistory(history, checkpoint.checkpoint),
    );
  } finally {
    await release();
  }
}

export async function scanInbox(
  client: BaseClient,
  options: {
    recipient: Address;
    trustedSender: Address;
    confirmations: number;
    checkpointPath: string;
    fromBlock?: bigint;
    chunkSize?: number;
    reorgWindow?: number;
  },
  persist: typeof writeJsonAtomic = writeJsonAtomic,
): Promise<InboxScanResult> {
  const chunkSize = options.chunkSize ?? 100;
  if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0) {
    throw new Error("inbox chunk size must be a positive safe integer");
  }
  const reorgWindow = options.reorgWindow ?? 64;
  if (!Number.isSafeInteger(reorgWindow) || reorgWindow <= 0) {
    throw new Error("inbox reorg window must be a positive safe integer");
  }
  if (
    !Number.isSafeInteger(options.confirmations) ||
    options.confirmations <= 0
  ) {
    throw new Error("inbox confirmations must be a positive safe integer");
  }
  if (options.fromBlock !== undefined && options.fromBlock < 0n) {
    throw new Error("inbox from block must be non-negative");
  }
  await mkdir(dirname(options.checkpointPath), { recursive: true });
  const release = await lockfile.lock(options.checkpointPath, {
    realpath: false,
    retries: {
      forever: true,
      factor: 1.2,
      minTimeout: 100,
      maxTimeout: 1_000,
    },
  });
  try {
    return await scanInboxLocked(
      client,
      options,
      chunkSize,
      reorgWindow,
      persist,
    );
  } finally {
    await release();
  }
}

async function scanInboxLocked(
  client: BaseClient,
  options: {
    recipient: Address;
    trustedSender: Address;
    confirmations: number;
    checkpointPath: string;
    fromBlock?: bigint;
  },
  chunkSize: number,
  reorgWindow: number,
  persist: typeof writeJsonAtomic,
): Promise<InboxScanResult> {
  const historyPath = `${options.checkpointPath}.messages.json`;
  const expectedIdentity = identity(options.recipient, options.trustedSender);
  const loadedCheckpoint = await loadCheckpointState(
    options.checkpointPath,
    expectedIdentity,
    options.fromBlock,
  );
  let checkpoint = loadedCheckpoint.checkpoint;
  let history = await loadHistory(historyPath, expectedIdentity);
  if (loadedCheckpoint.created) {
    await persist(options.checkpointPath, checkpoint);
  }

  const hashes = new Map<string, Hash>();
  for (const recent of checkpoint.recentBlocks.toReversed()) {
    const hash = await retry(async () => {
      try {
        return (await client.getBlock({ blockNumber: BigInt(recent.number) }))
          .hash;
      } catch (error) {
        if (error instanceof BlockNotFoundError) return null;
        throw error;
      }
    });
    if (!hash) continue;
    hashes.set(recent.number, hash);
    if (hash.toLowerCase() === recent.hash.toLowerCase()) break;
  }
  const reconciled = reconcileCheckpoint(checkpoint, hashes);
  if (reconciled.rewound) {
    checkpoint = reconciled.checkpoint;
    history = reconcileHistory(history, BigInt(checkpoint.nextBlock));
    await persist(historyPath, history);
    await persist(options.checkpointPath, checkpoint);
  }

  const latest = await retry(() => client.getBlockNumber());
  const confirmationDepth = BigInt(options.confirmations - 1);
  if (latest < confirmationDepth) {
    return { checkpoint, messages: pendingMessages(history), historyPath };
  }
  const lastConfirmed = latest - confirmationDepth;
  const known = new Set(history.messages.map(({ hash }) => hash.toLowerCase()));

  while (BigInt(checkpoint.nextBlock) <= lastConfirmed) {
    const first = BigInt(checkpoint.nextBlock);
    const last = [first + BigInt(chunkSize - 1), lastConfirmed].reduce(
      (a, b) => (a < b ? a : b),
    );
    for (let number = first; number <= last; number += 1n) {
      const block = await retry(() =>
        client.getBlock({ blockNumber: number, includeTransactions: true }),
      );
      if (!block.hash) throw new Error(`confirmed block ${number} has no hash`);
      checkpoint.recentBlocks.push({
        number: number.toString(),
        hash: block.hash,
      });

      for (const item of block.transactions) {
        if (typeof item === "string" || !item.to) continue;
        if (getAddress(item.to) !== getAddress(options.recipient)) continue;
        const receipt = await retry(() =>
          client.getTransactionReceipt({ hash: item.hash }),
        );
        if (
          item.blockNumber !== number ||
          item.blockHash?.toLowerCase() !== block.hash.toLowerCase() ||
          receipt.transactionHash.toLowerCase() !== item.hash.toLowerCase() ||
          receipt.blockNumber !== number ||
          receipt.blockHash.toLowerCase() !== block.hash.toLowerCase()
        ) {
          throw new BaseRpcError(
            `transaction ${item.hash} does not match scanned block ${number}`,
          );
        }
        const message = transactionToMessage(
          item as InboundTransaction,
          options.recipient,
          options.trustedSender,
          receipt.status === "success",
        );
        if (message && !known.has(message.hash.toLowerCase())) {
          history.messages.push(message);
          history.pending.push(message.hash);
          known.add(message.hash.toLowerCase());
        }
      }
    }

    checkpoint.nextBlock = (last + 1n).toString();
    checkpoint.recentBlocks = checkpoint.recentBlocks.slice(-reorgWindow);
    await persist(historyPath, history);
    await persist(options.checkpointPath, checkpoint);
    history = pruneHistory(history, checkpoint);
    known.clear();
    for (const message of history.messages) {
      known.add(message.hash.toLowerCase());
    }
    await persist(historyPath, history);
  }

  return { checkpoint, messages: pendingMessages(history), historyPath };
}
