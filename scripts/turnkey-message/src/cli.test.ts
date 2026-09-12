import { mkdtemp, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import type { Address, Hash, Hex } from "viem";

import type { BaseClient } from "./base.ts";
import {
  assertBaseChain,
  assertCanonicalMessageTransaction,
  assertRecipientExecution,
  assertSameTransactionInclusion,
  formatError,
  parseArguments,
  requiredTransactionHash,
  turnkeyAuth,
  transactionConfirmations,
} from "./cli.ts";

function thrownCode(action: () => unknown): string | undefined {
  try {
    action();
  } catch (error) {
    return (error as NodeJS.ErrnoException).code;
  }
  return undefined;
}

describe("CLI arguments", () => {
  test("formats the complete error cause chain", () => {
    expect(
      formatError(
        new Error("send failed", {
          cause: new Error("RPC rejected", { cause: "timeout" }),
        }),
      ),
    ).toBe("send failed: RPC rejected: timeout");
    expect(formatError("plain failure")).toBe("plain failure");
  });

  test("rejects a missing string option value", () => {
    expect(
      thrownCode(() => parseArguments(["inbox", "--from-block", "--watch"])),
    ).toBe("ERR_PARSE_ARGS_INVALID_OPTION_VALUE");
  });

  test("rejects unknown options", () => {
    expect(thrownCode(() => parseArguments(["inbox", "--watc"]))).toBe(
      "ERR_PARSE_ARGS_UNKNOWN_OPTION",
    );
  });

  test("keeps string values and boolean flags distinct", () => {
    const parsed = parseArguments([
      "inbox",
      "--from-block",
      "42",
      "--recipient",
      "0x0000000000000000000000000000000000000001",
      "--watch",
    ]);
    expect(parsed.values.get("from-block")).toBe("42");
    expect(parsed.values.get("recipient")).toBe(
      "0x0000000000000000000000000000000000000001",
    );
    expect(parsed.flags.has("watch")).toBe(true);
  });

  test("requires a 32-byte transaction hash", () => {
    expect(() => requiredTransactionHash(parseArguments(["decode"]))).toThrow(
      "--tx is required",
    );
    expect(() =>
      requiredTransactionHash(parseArguments(["decode", "--tx", "0x1234"])),
    ).toThrow("--tx must be a 32-byte transaction hash");
    expect(
      requiredTransactionHash(
        parseArguments(["decode", "--tx", `0x${"12".repeat(32)}`]),
      ),
    ).toBe(`0x${"12".repeat(32)}`);
  });

  test("requires a complete Turnkey identity", () => {
    expect(() => turnkeyAuth(parseArguments(["send"]))).toThrow(
      "required for Turnkey authentication",
    );
    expect(() =>
      turnkeyAuth(parseArguments(["send", "--turnkey-key-name", "juan"])),
    ).toThrow("required for Turnkey authentication");
    expect(() =>
      turnkeyAuth(
        parseArguments(["send", "--turnkey-keys-folder", "/tmp/keys"]),
      ),
    ).toThrow("required for Turnkey authentication");
  });

  test("selects a pinned Turnkey identity", () => {
    const userId = "46b69738-7427-4227-9cbe-797c8d4d1fdb";
    expect(
      turnkeyAuth(
        parseArguments([
          "send",
          "--turnkey-key-name",
          "juan",
          "--turnkey-user-id",
          userId,
        ]),
      ),
    ).toEqual({ keyName: "juan", userId });
    expect(
      turnkeyAuth(
        parseArguments([
          "send",
          "--turnkey-key-name",
          "juan",
          "--turnkey-user-id",
          userId,
          "--turnkey-keys-folder",
          "/tmp/keys",
        ]),
      ),
    ).toEqual({ keyName: "juan", userId, keysFolder: "/tmp/keys" });
    expect(() =>
      turnkeyAuth(
        parseArguments([
          "send",
          "--turnkey-key-name",
          "juan",
          "--turnkey-user-id",
          "not-a-uuid",
        ]),
      ),
    ).toThrow("must be a UUID");
  });

  test("rejects an invalid UTF-8 message before preparing a manifest", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-cli-"));
    const messagePath = join(directory, "message.txt");
    const outputPath = join(directory, "manifest.json");
    await writeFile(messagePath, Uint8Array.from([0xff]));
    const child = Bun.spawn(
      [
        process.execPath,
        "run",
        join(import.meta.dir, "cli.ts"),
        "prepare",
        "--config",
        join(directory, "missing.toml"),
        "--message-file",
        messagePath,
        "--max-total-fee-wei",
        "1",
        "--output",
        outputPath,
      ],
      { stderr: "pipe", stdout: "pipe" },
    );

    expect(await child.exited).not.toBe(0);
    expect(await new Response(child.stderr).text()).toContain(
      "is not strict UTF-8",
    );
    expect(await readdir(directory)).toEqual(["message.txt"]);
  });
});

describe("Base transaction checks", () => {
  const signer: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
  const recipient: Address = "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82";
  const blockHash = `0x${"11".repeat(32)}` as Hash;
  const data = "0x68656c6c6f" as Hex;
  const transaction = {
    blockHash,
    blockNumber: 10n,
    from: signer,
    to: recipient,
    value: 0n,
    input: data,
  };

  test("rejects a non-Base RPC", async () => {
    const rpc = {
      async getChainId() {
        return 1;
      },
    } as BaseClient;
    await expect(assertBaseChain(rpc)).rejects.toThrow("chain ID 8453");
    const baseRpc = {
      async getChainId() {
        return 8453;
      },
    } as BaseClient;
    await expect(assertBaseChain(baseRpc)).resolves.toBeUndefined();
  });

  test("requires an explicit opt-in for a contract recipient", async () => {
    const contractRpc = {
      async getCode() {
        return "0x6000" as Hex;
      },
    } as unknown as BaseClient;
    await expect(
      assertRecipientExecution(contractRpc, recipient, false),
    ).rejects.toThrow("--allow-contract-recipient");
    await expect(
      assertRecipientExecution(contractRpc, recipient, true),
    ).resolves.toBeUndefined();

    const eoaRpc = {
      async getCode() {
        return undefined;
      },
    } as unknown as BaseClient;
    await expect(
      assertRecipientExecution(eoaRpc, recipient, false),
    ).resolves.toBeUndefined();
  });

  test("does not report negative confirmations across RPC backend lag", () => {
    expect(transactionConfirmations(9n, 10n)).toBe(0);
    expect(transactionConfirmations(10n, 10n)).toBe(1);
    expect(transactionConfirmations(12n, 10n)).toBe(3);
    expect(transactionConfirmations(10n, null)).toBe(0);
  });

  test("rejects transaction and receipt data from different inclusions", () => {
    expect(() =>
      assertSameTransactionInclusion(transaction, {
        blockHash: `0x${"22".repeat(32)}` as Hash,
        blockNumber: 10n,
      }),
    ).toThrow("different inclusions");
    expect(() =>
      assertSameTransactionInclusion(
        { ...transaction, blockNumber: null },
        { blockHash, blockNumber: 10n },
      ),
    ).toThrow("different inclusions");
    expect(() =>
      assertSameTransactionInclusion(
        { ...transaction, blockHash: null },
        { blockHash, blockNumber: 10n },
      ),
    ).toThrow("different inclusions");
    expect(() =>
      assertSameTransactionInclusion(
        { ...transaction, blockNumber: 11n },
        { blockHash, blockNumber: 10n },
      ),
    ).toThrow("different inclusions");
    expect(() =>
      assertSameTransactionInclusion(transaction, {
        blockHash,
        blockNumber: 10n,
      }),
    ).not.toThrow();
  });

  test("binds a mined transaction to the receipt block", () => {
    expect(() =>
      assertCanonicalMessageTransaction(
        transaction,
        { blockHash, blockNumber: 10n },
        { signer, recipient },
        data,
      ),
    ).not.toThrow();
    expect(() =>
      assertCanonicalMessageTransaction(
        { ...transaction, blockHash: `0x${"22".repeat(32)}` as Hash },
        { blockHash, blockNumber: 10n },
        { signer, recipient },
        data,
      ),
    ).toThrow("differs from the approved message");
    expect(() =>
      assertCanonicalMessageTransaction(
        { ...transaction, blockNumber: null },
        { blockHash, blockNumber: 10n },
        { signer, recipient },
        data,
      ),
    ).toThrow("differs from the approved message");
    for (const [changed, field] of [
      [
        {
          ...transaction,
          from: "0x0000000000000000000000000000000000000001" as Address,
        },
        "from",
      ],
      [
        {
          ...transaction,
          to: "0x0000000000000000000000000000000000000001" as Address,
        },
        "to",
      ],
      [{ ...transaction, value: 1n }, "value"],
      [{ ...transaction, input: "0x00" as Hex }, "input"],
    ] as const) {
      expect(() =>
        assertCanonicalMessageTransaction(
          changed,
          { blockHash, blockNumber: 10n },
          { signer, recipient },
          data,
        ),
      ).toThrow(`approved message: ${field}`);
    }
  });
});
