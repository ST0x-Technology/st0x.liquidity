import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import {
  BlockNotFoundError,
  HttpRequestError,
  RpcRequestError,
  type Address,
  type Hash,
} from "viem";

import type { BaseClient } from "./base.ts";
import { writeJsonAtomic } from "./durable-file.ts";
import { encodeMessage } from "./message.ts";
import {
  acknowledgeInboxMessages,
  BaseRpcError,
  reconcileCheckpoint,
  reconcileHistory,
  scanInbox,
  transactionToMessage,
  type InboxCheckpoint,
  type InboxHistory,
} from "./inbox.ts";

const recipient: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
const trusted: Address = "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82";

describe("inbox classification", () => {
  test("keeps untrusted senders visible", () => {
    const message = transactionToMessage(
      {
        blockHash: `0x${"11".repeat(32)}`,
        blockNumber: 10n,
        from: "0x0000000000000000000000000000000000000001",
        hash: `0x${"22".repeat(32)}`,
        input: encodeMessage("hello"),
        to: recipient,
        value: 0n,
      },
      recipient,
      trusted,
      true,
    );

    expect(message?.trust).toBe("untrusted");
    expect(message?.text).toBe("hello");
  });

  test("flags value and malformed payloads", () => {
    const message = transactionToMessage(
      {
        blockHash: `0x${"11".repeat(32)}`,
        blockNumber: 10n,
        from: trusted,
        hash: `0x${"22".repeat(32)}`,
        input: "0xff",
        to: recipient,
        value: 1n,
      },
      recipient,
      trusted,
      true,
    );

    expect(message?.valid).toBe(false);
    expect(message?.warnings).toEqual([
      "transaction value is not zero",
      "calldata is not strict UTF-8",
    ]);
  });

  test("flags calldata above the message limit", () => {
    const message = transactionToMessage(
      {
        blockHash: `0x${"11".repeat(32)}`,
        blockNumber: 10n,
        from: trusted,
        hash: `0x${"22".repeat(32)}`,
        input: `0x${"61".repeat(4097)}`,
        to: recipient,
        value: 0n,
      },
      recipient,
      trusted,
      true,
    );

    expect(message?.valid).toBe(false);
    expect(message?.warnings).toContain("message exceeds 4096 UTF-8 bytes");
    expect(message?.text).toBeUndefined();
  });

  test("preserves the exact calldata decode warning", () => {
    const message = transactionToMessage(
      {
        blockHash: `0x${"11".repeat(32)}`,
        blockNumber: 10n,
        from: trusted,
        hash: `0x${"22".repeat(32)}`,
        input: "0x",
        to: recipient,
        value: 0n,
      },
      recipient,
      trusted,
      true,
    );

    expect(message?.warnings).toEqual([
      "calldata is not a non-empty byte-aligned hex value",
    ]);
  });

  test("ignores transactions to another recipient", () => {
    expect(
      transactionToMessage(
        {
          blockHash: `0x${"11".repeat(32)}`,
          blockNumber: 10n,
          from: trusted,
          hash: `0x${"22".repeat(32)}`,
          input: encodeMessage("hello"),
          to: "0x0000000000000000000000000000000000000001",
          value: 0n,
        },
        recipient,
        trusted,
        true,
      ),
    ).toBeUndefined();
  });

  test("rejects an unmined transaction", () => {
    expect(() =>
      transactionToMessage(
        {
          blockHash: null,
          blockNumber: null,
          from: trusted,
          hash: `0x${"22".repeat(32)}`,
          input: encodeMessage("hello"),
          to: recipient,
          value: 0n,
        },
        recipient,
        trusted,
        true,
      ),
    ).toThrow("not mined");
  });

  test("marks failed execution as invalid", () => {
    const message = transactionToMessage(
      {
        blockHash: `0x${"11".repeat(32)}`,
        blockNumber: 10n,
        from: trusted,
        hash: `0x${"22".repeat(32)}`,
        input: encodeMessage("hello"),
        to: recipient,
        value: 0n,
      },
      recipient,
      trusted,
      false,
    );
    expect(message?.warnings).toContain("transaction execution failed");
    expect(message?.valid).toBe(false);
  });
});

describe("checkpoint reconciliation", () => {
  const checkpoint: InboxCheckpoint = {
    version: 1,
    recipient,
    trustedSender: trusted,
    nextBlock: "13",
    recentBlocks: [
      { number: "10", hash: `0x${"10".repeat(32)}` },
      { number: "11", hash: `0x${"11".repeat(32)}` },
      { number: "12", hash: `0x${"12".repeat(32)}` },
    ],
  };
  const history: InboxHistory = {
    version: 1,
    recipient,
    trustedSender: trusted,
    messages: [
      {
        blockHash: `0x${"12".repeat(32)}`,
        blockNumber: "12",
        from: trusted,
        hash: `0x${"22".repeat(32)}`,
        text: "hello",
        to: recipient,
        trust: "trusted",
        valid: true,
        value: "0",
        warnings: [],
      },
    ],
    pending: [`0x${"22".repeat(32)}`],
  };

  test("rewinds and removes orphaned messages", () => {
    const reconciled = reconcileCheckpoint(
      checkpoint,
      new Map<string, Hash>([
        ["10", `0x${"10".repeat(32)}` as Hash],
        ["11", `0x${"aa".repeat(32)}` as Hash],
      ]),
    );

    expect(reconciled.rewound).toBe(true);
    if (!reconciled.rewound) throw new Error("expected checkpoint rewind");
    expect(reconciled.checkpoint.nextBlock).toBe("11");
    expect(reconciled.checkpoint.recentBlocks).toHaveLength(1);
    expect(
      reconcileHistory(history, BigInt(reconciled.checkpoint.nextBlock))
        .messages,
    ).toHaveLength(0);
  });

  test("keeps a checkpoint whose ancestry matches", () => {
    const reconciled = reconcileCheckpoint(
      checkpoint,
      new Map<string, Hash>(
        checkpoint.recentBlocks.map(({ number, hash }) => [number, hash]),
      ),
    );
    expect(reconciled).toEqual({ rewound: false, checkpoint });
  });

  test("treats a missing canonical hash as divergence", () => {
    const reconciled = reconcileCheckpoint(
      checkpoint,
      new Map<string, Hash>([
        ["10", `0x${"10".repeat(32)}` as Hash],
        ["11", `0x${"11".repeat(32)}` as Hash],
      ]),
    );
    expect(reconciled.rewound).toBe(true);
    if (!reconciled.rewound) throw new Error("expected checkpoint rewind");
    expect(reconciled.checkpoint.nextBlock).toBe("12");
  });

  test("compares canonical hashes without case sensitivity", () => {
    const upper = {
      ...checkpoint,
      recentBlocks: [{ number: "10", hash: `0x${"AB".repeat(32)}` as Hash }],
    };
    expect(
      reconcileCheckpoint(
        upper,
        new Map([["10", `0x${"ab".repeat(32)}` as Hash]]),
      ),
    ).toEqual({ rewound: false, checkpoint: upper });
  });

  test("stops canonical lookup at the newest matching ancestor", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-ancestor-"));
    const checkpointPath = join(directory, "checkpoint.json");
    await writeFile(checkpointPath, `${JSON.stringify(checkpoint)}\n`);
    const requested: bigint[] = [];
    const client = {
      async getBlock({ blockNumber }: { blockNumber: bigint }) {
        requested.push(blockNumber);
        return {
          hash:
            blockNumber === 11n
              ? (`0x${"11".repeat(32)}` as Hash)
              : (`0x${"aa".repeat(32)}` as Hash),
        };
      },
      async getBlockNumber() {
        return 0n;
      },
    } as unknown as BaseClient;

    const result = await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 3,
      checkpointPath,
    });

    expect(requested).toEqual([12n, 11n]);
    expect(result.checkpoint.nextBlock).toBe("12");
  });

  test("persists a rewind before an early return", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-"));
    const checkpointPath = join(directory, "checkpoint.json");
    await writeFile(checkpointPath, `${JSON.stringify(checkpoint)}\n`);
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify(history)}\n`,
    );
    const client = {
      async getBlock() {
        return { hash: `0x${"aa".repeat(32)}` as Hash };
      },
      async getBlockNumber() {
        return 0n;
      },
    } as unknown as BaseClient;

    await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 3,
      checkpointPath,
    });

    const persisted = JSON.parse(await readFile(checkpointPath, "utf8"));
    const persistedHistory = JSON.parse(
      await readFile(`${checkpointPath}.messages.json`, "utf8"),
    );
    expect(persisted.nextBlock).toBe("10");
    expect(persistedHistory.messages).toHaveLength(0);
  });

  test("rewinds when the newest retained block is unavailable", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-missing-"));
    const checkpointPath = join(directory, "checkpoint.json");
    await writeFile(checkpointPath, `${JSON.stringify(checkpoint)}\n`);
    const client = {
      async getBlock({ blockNumber }: { blockNumber: bigint }) {
        if (blockNumber === 12n) throw new BlockNotFoundError({ blockNumber });
        return {
          hash: `0x${blockNumber.toString(16).padStart(64, "0")}` as Hash,
        };
      },
      async getBlockNumber() {
        return 0n;
      },
    } as unknown as BaseClient;

    const result = await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 3,
      checkpointPath,
    });

    expect(result.checkpoint.nextBlock).toBe("10");
  });
});

describe("inbox scan boundary", () => {
  test("rejects a negative starting block", async () => {
    await expect(
      scanInbox({} as BaseClient, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath: "unused",
        fromBlock: -1n,
      }),
    ).rejects.toThrow("non-negative");
  });

  test.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid chunk size %p",
    async (chunkSize) => {
      await expect(
        scanInbox({} as BaseClient, {
          recipient,
          trustedSender: trusted,
          confirmations: 3,
          checkpointPath: "unused",
          fromBlock: 0n,
          chunkSize,
        }),
      ).rejects.toThrow("positive safe integer");
    },
  );

  test.each([0, -1, 1.5, Number.POSITIVE_INFINITY])(
    "rejects invalid reorg window %p",
    async (reorgWindow) => {
      await expect(
        scanInbox({} as BaseClient, {
          recipient,
          trustedSender: trusted,
          confirmations: 3,
          checkpointPath: "unused",
          fromBlock: 0n,
          reorgWindow,
        }),
      ).rejects.toThrow("positive safe integer");
    },
  );

  test.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid confirmation count %p",
    async (confirmations) => {
      await expect(
        scanInbox({} as BaseClient, {
          recipient,
          trustedSender: trusted,
          confirmations,
          checkpointPath: "unused",
          fromBlock: 0n,
        }),
      ).rejects.toThrow("positive safe integer");
    },
  );

  test("scans a confirmed block and suppresses a known message", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-scan-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const blockHash = `0x${"11".repeat(32)}` as Hash;
    const transactionHash = `0x${"22".repeat(32)}` as Hash;
    const client = {
      async getBlockNumber() {
        return 7n;
      },
      async getBlock() {
        return {
          hash: blockHash,
          transactions: [
            {
              blockHash,
              blockNumber: 7n,
              from: trusted,
              hash: transactionHash,
              input: encodeMessage("hello"),
              to: recipient,
              value: 0n,
            },
          ],
        };
      },
      async getTransactionReceipt() {
        return {
          status: "success",
          transactionHash,
          blockHash,
          blockNumber: 7n,
        };
      },
    } as unknown as BaseClient;
    const options = {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
      fromBlock: 7n,
      reorgWindow: 1,
    };

    const first = await scanInbox(client, options);
    expect(first.messages.map(({ hash }) => hash)).toEqual([transactionHash]);
    expect(first.checkpoint.nextBlock).toBe("8");
    expect(first.checkpoint.recentBlocks).toEqual([
      { number: "7", hash: blockHash },
    ]);
    const history = JSON.parse(
      await readFile(`${checkpointPath}.messages.json`, "utf8"),
    );
    expect(history.messages).toHaveLength(1);

    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "7", recentBlocks: [] })}\n`,
    );
    const second = await scanInbox(client, options);
    expect(second.messages.map(({ hash }) => hash)).toEqual([transactionHash]);
    await acknowledgeInboxMessages(
      checkpointPath,
      recipient,
      trusted,
      second.messages.map(({ hash }) => hash),
    );
    const third = await scanInbox(client, options);
    expect(third.messages).toHaveLength(0);
    const persistedHistory = JSON.parse(
      await readFile(`${checkpointPath}.messages.json`, "utf8"),
    );
    expect(persistedHistory.messages).toHaveLength(1);
  });

  test("prunes acknowledged messages outside the replay horizon", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-prune-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const oldHash = `0x${"33".repeat(32)}` as Hash;
    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "11", recentBlocks: [{ number: "10", hash: `0x${"10".repeat(32)}` }] })}\n`,
    );
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify({
        version: 1,
        recipient,
        trustedSender: trusted,
        messages: [
          {
            blockHash: `0x${"01".repeat(32)}`,
            blockNumber: "1",
            from: trusted,
            hash: oldHash,
            text: "old",
            to: recipient,
            trust: "trusted",
            valid: true,
            value: "0",
            warnings: [],
          },
        ],
        pending: [oldHash],
      })}\n`,
    );

    await acknowledgeInboxMessages(checkpointPath, recipient, trusted, [
      oldHash,
    ]);

    const persisted = JSON.parse(
      await readFile(`${checkpointPath}.messages.json`, "utf8"),
    );
    expect(persisted.messages).toEqual([]);
    expect(persisted.pending).toEqual([]);
  });

  test("persists a fresh checkpoint before an empty scan returns", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-initial-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const client = {
      async getBlockNumber() {
        return 10n;
      },
    } as unknown as BaseClient;

    await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 3,
      checkpointPath,
      fromBlock: 20n,
    });

    const checkpoint = JSON.parse(await readFile(checkpointPath, "utf8"));
    expect(checkpoint.nextBlock).toBe("20");
  });

  test.each([
    [
      "recipient",
      "0x0000000000000000000000000000000000000001" as Address,
      trusted,
    ],
    [
      "trusted sender",
      recipient,
      "0x0000000000000000000000000000000000000001" as Address,
    ],
  ])(
    "rejects a changed scan %s",
    async (_label, changedRecipient, changedTrusted) => {
      const directory = await mkdtemp(
        join(tmpdir(), "turnkey-inbox-identity-"),
      );
      const checkpointPath = join(directory, "checkpoint.json");
      const client = {
        async getBlockNumber() {
          return 0n;
        },
      } as unknown as BaseClient;
      await scanInbox(client, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath,
        fromBlock: 1n,
      });

      await expect(
        scanInbox(client, {
          recipient: changedRecipient,
          trustedSender: changedTrusted,
          confirmations: 1,
          checkpointPath,
        }),
      ).rejects.toThrow("use a different checkpoint path");
    },
  );

  test("rejects malformed persisted array entries", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-corrupt-"));
    const checkpointPath = join(directory, "checkpoint.json");
    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "1", recentBlocks: [{ number: "0" }] })}\n`,
    );
    await expect(
      scanInbox({} as BaseClient, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath,
      }),
    ).rejects.toThrow("checkpoint");

    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "1", recentBlocks: [] })}\n`,
    );
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, messages: [{ hash: "bad" }], pending: [] })}\n`,
    );
    await expect(
      scanInbox({} as BaseClient, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath,
      }),
    ).rejects.toThrow("history");
  });

  test("distinguishes history identity mismatches from corrupt identities", async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "turnkey-history-identity-"),
    );
    const checkpointPath = join(directory, "checkpoint.json");
    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "1", recentBlocks: [] })}\n`,
    );
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify({ version: 1, recipient: "0x0000000000000000000000000000000000000001", trustedSender: trusted, messages: [], pending: [] })}\n`,
    );
    const options = {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
    };

    await expect(scanInbox({} as BaseClient, options)).rejects.toThrow(
      "use a different checkpoint path",
    );
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify({ version: 1, recipient: "bad", trustedSender: trusted, messages: [], pending: [] })}\n`,
    );
    await expect(scanInbox({} as BaseClient, options)).rejects.toThrow(
      "is corrupt",
    );
  });

  test("retains only the configured recent block window", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-window-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const blockHash = (number: bigint) =>
      `0x${number.toString(16).padStart(64, "0")}` as Hash;
    const transactionHash = (number: bigint) =>
      `0x${(number + 100n).toString(16).padStart(64, "0")}` as Hash;
    const client = {
      async getBlockNumber() {
        return 5n;
      },
      async getBlock({ blockNumber }: { blockNumber: bigint }) {
        return {
          hash: blockHash(blockNumber),
          transactions: [
            {
              blockHash: blockHash(blockNumber),
              blockNumber,
              from: trusted,
              hash: transactionHash(blockNumber),
              input: encodeMessage(`message ${blockNumber}`),
              to: recipient,
              value: 0n,
            },
          ],
        };
      },
      async getTransactionReceipt({ hash }: { hash: Hash }) {
        const number = BigInt(hash);
        const blockNumber = number - 100n;
        return {
          status: "success",
          transactionHash: hash,
          blockHash: blockHash(blockNumber),
          blockNumber,
        };
      },
    } as unknown as BaseClient;

    const result = await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
      fromBlock: 1n,
      reorgWindow: 3,
    });

    expect(result.checkpoint.nextBlock).toBe("6");
    expect(result.checkpoint.recentBlocks.map(({ number }) => number)).toEqual([
      "3",
      "4",
      "5",
    ]);
    expect(result.messages.map(({ hash }) => hash)).toEqual(
      [1n, 2n, 3n, 4n, 5n].map(transactionHash),
    );
  });

  test("keeps an earlier chunk pending when a later inclusion is inconsistent", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-delivery-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const hashes = [
      `0x${"11".repeat(32)}` as Hash,
      `0x${"22".repeat(32)}` as Hash,
    ];
    const transactionHashes = [
      `0x${"33".repeat(32)}` as Hash,
      `0x${"44".repeat(32)}` as Hash,
    ];
    let inconsistent = true;
    const client = {
      async getBlockNumber() {
        return 2n;
      },
      async getBlock({ blockNumber }: { blockNumber: bigint }) {
        const index = Number(blockNumber - 1n);
        return {
          hash: hashes[index],
          transactions: [
            {
              blockHash: hashes[index],
              blockNumber,
              from: trusted,
              hash: transactionHashes[index],
              input: encodeMessage(`message ${blockNumber}`),
              to: recipient,
              value: 0n,
            },
          ],
        };
      },
      async getTransactionReceipt({ hash }: { hash: Hash }) {
        const index = transactionHashes.indexOf(hash);
        return {
          status: "success",
          transactionHash: hash,
          blockHash: inconsistent && index === 1 ? hashes[0] : hashes[index],
          blockNumber: BigInt(index + 1),
        };
      },
    } as unknown as BaseClient;
    const options = {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
      fromBlock: 1n,
      chunkSize: 1,
    };

    await expect(scanInbox(client, options)).rejects.toBeInstanceOf(
      BaseRpcError,
    );
    const checkpoint = JSON.parse(await readFile(checkpointPath, "utf8"));
    expect(checkpoint.nextBlock).toBe("2");

    inconsistent = false;
    const retried = await scanInbox(client, options);
    expect(retried.messages.map(({ hash }) => hash)).toEqual(transactionHashes);
  });

  test("does not redeliver an acknowledged message after a checkpoint-write crash", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-crash-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const transactionHash = `0x${"77".repeat(32)}` as Hash;
    const blockHash = (number: bigint) =>
      `0x${number.toString(16).padStart(64, "0")}` as Hash;
    await writeFile(
      checkpointPath,
      `${JSON.stringify({ version: 1, recipient, trustedSender: trusted, nextBlock: "1", recentBlocks: [] })}\n`,
    );
    await writeFile(
      `${checkpointPath}.messages.json`,
      `${JSON.stringify({
        version: 1,
        recipient,
        trustedSender: trusted,
        messages: [
          {
            blockHash: blockHash(1n),
            blockNumber: "1",
            from: trusted,
            hash: transactionHash,
            text: "acknowledged",
            to: recipient,
            trust: "trusted",
            valid: true,
            value: "0",
            warnings: [],
          },
        ],
        pending: [],
      })}\n`,
    );
    const client = {
      async getBlockNumber() {
        return 100n;
      },
      async getBlock({ blockNumber }: { blockNumber: bigint }) {
        return {
          hash: blockHash(blockNumber),
          transactions:
            blockNumber === 1n
              ? [
                  {
                    blockHash: blockHash(1n),
                    blockNumber: 1n,
                    from: trusted,
                    hash: transactionHash,
                    input: encodeMessage("acknowledged"),
                    to: recipient,
                    value: 0n,
                  },
                ]
              : [],
        };
      },
      async getTransactionReceipt() {
        return {
          status: "success",
          transactionHash,
          blockHash: blockHash(1n),
          blockNumber: 1n,
        };
      },
    } as unknown as BaseClient;
    const options = {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
      chunkSize: 100,
      reorgWindow: 64,
    };
    await expect(
      scanInbox(client, options, async (path, value) => {
        if (path === checkpointPath) {
          throw new Error("simulated checkpoint-write crash");
        }
        await writeJsonAtomic(path, value);
      }),
    ).rejects.toThrow("simulated checkpoint-write crash");
    const recovered = await scanInbox(client, options);

    expect(recovered.messages).toEqual([]);
  });

  test("recovers when a block read succeeds on the third attempt", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-retry-"));
    const checkpointPath = join(directory, "checkpoint.json");
    const blockHash = `0x${"55".repeat(32)}` as Hash;
    const transactionHash = `0x${"66".repeat(32)}` as Hash;
    let blockCalls = 0;
    const client = {
      async getBlockNumber() {
        return 7n;
      },
      async getBlock() {
        blockCalls += 1;
        if (blockCalls < 3) {
          throw new HttpRequestError({
            status: 503,
            url: "https://mainnet.base.org",
          });
        }
        return {
          hash: blockHash,
          transactions: [
            {
              blockHash,
              blockNumber: 7n,
              from: trusted,
              hash: transactionHash,
              input: encodeMessage("recovered"),
              to: recipient,
              value: 0n,
            },
          ],
        };
      },
      async getTransactionReceipt() {
        return {
          status: "success",
          transactionHash,
          blockHash,
          blockNumber: 7n,
        };
      },
    } as unknown as BaseClient;

    const result = await scanInbox(client, {
      recipient,
      trustedSender: trusted,
      confirmations: 1,
      checkpointPath,
      fromBlock: 7n,
    });

    expect(blockCalls).toBe(3);
    expect(result.messages.map(({ text }) => text)).toEqual(["recovered"]);
  });

  test("raises BaseRpcError after four failed latest-block reads", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-exhaust-"));
    let calls = 0;
    const client = {
      async getBlockNumber() {
        calls += 1;
        throw new HttpRequestError({
          status: 503,
          url: "https://mainnet.base.org",
        });
      },
    } as unknown as BaseClient;

    await expect(
      scanInbox(client, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath: join(directory, "checkpoint.json"),
        fromBlock: 0n,
      }),
    ).rejects.toBeInstanceOf(BaseRpcError);
    expect(calls).toBe(4);
  }, 10_000);

  test("does not retry a permanent RPC error", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-permanent-"));
    const error = new RpcRequestError({
      body: { method: "eth_blockNumber" },
      error: { code: -32_602, message: "invalid params" },
      url: "https://mainnet.base.org",
    });
    let calls = 0;
    const client = {
      async getBlockNumber() {
        calls += 1;
        throw error;
      },
    } as unknown as BaseClient;

    await expect(
      scanInbox(client, {
        recipient,
        trustedSender: trusted,
        confirmations: 1,
        checkpointPath: join(directory, "checkpoint.json"),
        fromBlock: 0n,
      }),
    ).rejects.toBe(error);
    expect(calls).toBe(1);
  });

  test("serializes scans sharing one checkpoint", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-inbox-lock-"));
    const checkpointPath = join(directory, "checkpoint.json");
    let active = 0;
    let maximumActive = 0;
    const client = {
      async getBlockNumber() {
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        await Bun.sleep(25);
        active -= 1;
        return 0n;
      },
    } as unknown as BaseClient;
    const options = {
      recipient,
      trustedSender: trusted,
      confirmations: 2,
      checkpointPath,
      fromBlock: 0n,
    };

    await Promise.all([scanInbox(client, options), scanInbox(client, options)]);

    expect(maximumActive).toBe(1);
  });
});
