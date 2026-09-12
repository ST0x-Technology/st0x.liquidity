import { mkdtemp, readFile, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import type { Address, Hash, Hex } from "viem";

import type { BaseClient } from "./base.ts";
import {
  assertBaseChain,
  assertCanonicalMessageTransaction,
  assertFundedWallet,
  assertManifestIdentity,
  assertRecipientExecution,
  assertSameTransactionInclusion,
  assertTurnkeyWalletAuthority,
  broadcastAndConfirm,
  formatError,
  inboxWallet,
  parseArguments,
  readCanonicalInclusion,
  requiredTransactionHash,
  turnkeyWalletSelection,
  turnkeyAuth,
  transactionConfirmations,
  validateSendManifest,
} from "./cli.ts";
import { createManifest, encodeMessage } from "./message.ts";
import { SEARCHER_ADDRESS, type Settings } from "./settings.ts";

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

  test("redacts the configured Base RPC URL from errors", () => {
    const prior = process.env.BASE_RPC_URL;
    process.env.BASE_RPC_URL = "https://rpc.example/secret-key";
    try {
      expect(
        formatError(new Error("failed at https://rpc.example/secret-key")),
      ).toBe("failed at [REDACTED_BASE_RPC_URL]");
    } finally {
      if (prior === undefined) delete process.env.BASE_RPC_URL;
      else process.env.BASE_RPC_URL = prior;
    }
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

  test("selects the Turnkey wallet and requires an explicit test mode", () => {
    const wallet: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
    expect(
      turnkeyWalletSelection(
        parseArguments(["prepare", "--turnkey-wallet", wallet]),
      ),
    ).toEqual({ wallet, representsInventory: true });
    expect(
      turnkeyWalletSelection(
        parseArguments([
          "prepare",
          "--turnkey-wallet",
          wallet,
          "--test-wallet",
        ]),
      ),
    ).toEqual({ wallet, representsInventory: false });
    expect(() => turnkeyWalletSelection(parseArguments(["prepare"]))).toThrow(
      "--turnkey-wallet is required",
    );
  });

  test("selects the inbox recipient wallet", () => {
    const wallet: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
    expect(
      inboxWallet(parseArguments(["inbox", "--inbox-wallet", wallet])),
    ).toBe(wallet);
    expect(() => inboxWallet(parseArguments(["inbox"]))).toThrow(
      "--inbox-wallet is required",
    );
    expect(inboxWallet(parseArguments(["decode"]), wallet)).toBe(wallet);
  });

  test("rejects an unfunded selected Turnkey wallet", () => {
    const wallet: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
    expect(() => assertFundedWallet(0n, wallet)).toThrow("has no Base ETH");
    expect(() => assertFundedWallet(1n, wallet)).not.toThrow();
  });

  test("permits an expired manifest only for its matching resumable activity", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-send-"));
    const statePath = join(directory, "activity.json");
    const wallet: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
    const manifest = createManifest(
      {
        type: "eip1559",
        chainId: 8453,
        from: wallet,
        representedInventory: null,
        to: "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
        value: "0",
        data: encodeMessage("hello"),
        nonce: "0",
        gasLimit: "22000",
        maxFeePerGas: "2",
        maxPriorityFeePerGas: "1",
        l1DataFeeWei: "0",
        operatorFeeWei: "0",
        maxTotalFeeWei: "44000",
        accessList: [],
      },
      1_000,
      1_600,
    );

    await expect(
      validateSendManifest(manifest, statePath, 1_601),
    ).rejects.toThrow("expired");
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash: manifest.approvalHash, phase: "submitting" })}\n`,
    );
    await expect(
      validateSendManifest(manifest, statePath, 1_601),
    ).rejects.toThrow("expired");
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash: `0x${"22".repeat(32)}`, phase: "submitted", activityId: "wrong" })}\n`,
    );
    await expect(
      validateSendManifest(manifest, statePath, 1_601),
    ).rejects.toThrow("expired");
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash: manifest.approvalHash, phase: "submitted", activityId: "activity-id" })}\n`,
    );
    await expect(
      validateSendManifest(manifest, statePath, 1_601),
    ).resolves.toBeTrue();
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash: manifest.approvalHash, phase: "broadcast", activityId: "activity-id", transactionHash: `0x${"33".repeat(32)}` })}\n`,
    );
    await expect(
      validateSendManifest(manifest, statePath, 1_601),
    ).resolves.toBeTrue();
  });

  test("binds the selected wallet and representation mode to the manifest", () => {
    const wallet: Address = "0xA9C16673F65AE808688cB18952AFE3d9658C808f";
    const inventory: Address = "0x10e4db39275C3b128C01bA1194D45D19aE1520d9";
    const settings: Settings = {
      chainId: 8453,
      signer: "0x0000000000000000000000000000000000000001",
      organizationId: "org-test",
      inventory,
      trustedSender: SEARCHER_ADDRESS,
      requiredConfirmations: 3,
    };
    const transaction = {
      type: "eip1559" as const,
      chainId: 8453 as const,
      from: wallet,
      representedInventory: null,
      to: SEARCHER_ADDRESS,
      value: "0" as const,
      data: encodeMessage("test"),
      nonce: "0",
      gasLimit: "22000",
      maxFeePerGas: "1",
      maxPriorityFeePerGas: "1",
      l1DataFeeWei: "0",
      operatorFeeWei: "0",
      maxTotalFeeWei: "22000",
      accessList: [] as [],
    };
    const testManifest = createManifest(transaction, 1_000, 1_600);

    expect(() =>
      assertManifestIdentity(testManifest, settings, {
        wallet,
        representsInventory: false,
      }),
    ).not.toThrow();
    expect(() =>
      assertManifestIdentity(testManifest, settings, {
        wallet,
        representsInventory: true,
      }),
    ).toThrow("representation mode");
    const inventoryManifest = createManifest(
      { ...transaction, representedInventory: inventory },
      1_000,
      1_600,
    );
    expect(() =>
      assertManifestIdentity(inventoryManifest, settings, {
        wallet,
        representsInventory: true,
      }),
    ).not.toThrow();
    expect(() =>
      assertManifestIdentity(inventoryManifest, settings, {
        wallet: "0x0000000000000000000000000000000000000002",
        representsInventory: true,
      }),
    ).toThrow("selected Turnkey wallet");
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
        "--turnkey-wallet",
        "0xA9C16673F65AE808688cB18952AFE3d9658C808f",
        "--recipient",
        "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
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

  test("rejects a prepare TTL below the supported range", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-cli-ttl-"));
    const messagePath = join(directory, "message.txt");
    const outputPath = join(directory, "manifest.json");
    await writeFile(messagePath, "hello");
    const child = Bun.spawn(
      [
        process.execPath,
        "run",
        join(import.meta.dir, "cli.ts"),
        "prepare",
        "--config",
        join(directory, "missing.toml"),
        "--turnkey-wallet",
        "0xA9C16673F65AE808688cB18952AFE3d9658C808f",
        "--recipient",
        "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
        "--message-file",
        messagePath,
        "--max-total-fee-wei",
        "1",
        "--ttl-seconds",
        "5",
        "--output",
        outputPath,
      ],
      { stderr: "pipe", stdout: "pipe" },
    );

    expect(await child.exited).not.toBe(0);
    expect(await new Response(child.stderr).text()).toContain(
      "--ttl-seconds must be an integer from 60 through 3600",
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

  test("permits a test wallet without claiming inventory authority", async () => {
    const checkedAddresses: Address[] = [];
    const rpc = {
      async getChainId() {
        return 8453;
      },
      async getCode({ address }: { address: Address }) {
        checkedAddresses.push(address);
        return undefined;
      },
      async readContract() {
        throw new Error("must not inspect inventory roles in test-wallet mode");
      },
    } as unknown as BaseClient;
    const settings: Settings = {
      chainId: 8453,
      signer: "0x0000000000000000000000000000000000000001",
      organizationId: "org-test",
      inventory: "0x10e4db39275C3b128C01bA1194D45D19aE1520d9",
      trustedSender: SEARCHER_ADDRESS,
      requiredConfirmations: 3,
    };

    await expect(
      assertTurnkeyWalletAuthority(rpc, settings, {
        wallet: signer,
        representsInventory: false,
      }),
    ).resolves.toBeUndefined();
    expect(checkedAddresses).toEqual([signer]);
    await expect(
      assertTurnkeyWalletAuthority(rpc, settings, {
        wallet: settings.signer,
        representsInventory: false,
      }),
    ).rejects.toThrow("must differ from the configured inventory wallet");
  });

  test("checks inventory roles for the selected Turnkey wallet", async () => {
    const inventory: Address = "0x10e4db39275C3b128C01bA1194D45D19aE1520d9";
    const operatorRole = `0x${"22".repeat(32)}` as Hex;
    const roleAccounts: Address[] = [];
    const rpc = {
      async getChainId() {
        return 8453;
      },
      async getCode({ address }: { address: Address }) {
        return address === inventory ? ("0x6000" as Hex) : undefined;
      },
      async readContract(parameters: {
        functionName: string;
        args?: readonly [Hex, Address];
      }) {
        if (parameters.functionName === "OPERATOR_ROLE") return operatorRole;
        if (parameters.args) roleAccounts.push(parameters.args[1]);
        return true;
      },
    } as unknown as BaseClient;
    const settings: Settings = {
      chainId: 8453,
      signer: "0x0000000000000000000000000000000000000001",
      organizationId: "org-test",
      inventory,
      trustedSender: SEARCHER_ADDRESS,
      requiredConfirmations: 3,
    };

    await expect(
      assertTurnkeyWalletAuthority(rpc, settings, {
        wallet: signer,
        representsInventory: true,
      }),
    ).resolves.toBeUndefined();
    expect(roleAccounts).toEqual([signer, signer]);
  });

  test.each([
    ["deployed inventory", false, true, true, "has no deployed code"],
    ["admin role", true, false, true, "DEFAULT_ADMIN_ROLE"],
    ["operator role", true, true, false, "OPERATOR_ROLE"],
  ] as const)(
    "rejects a production wallet without %s",
    async (_label, inventoryDeployed, isAdmin, isOperator, expected) => {
      const inventory: Address = "0x10e4db39275C3b128C01bA1194D45D19aE1520d9";
      const operatorRole = `0x${"22".repeat(32)}` as Hex;
      const rpc = {
        async getChainId() {
          return 8453;
        },
        async getCode({ address }: { address: Address }) {
          if (address === inventory) {
            return inventoryDeployed ? ("0x6000" as Hex) : undefined;
          }
          return undefined;
        },
        async readContract(parameters: {
          functionName: string;
          args?: readonly [Hex, Address];
        }) {
          if (parameters.functionName === "OPERATOR_ROLE") return operatorRole;
          return parameters.args?.[0] === operatorRole ? isOperator : isAdmin;
        },
      } as unknown as BaseClient;
      const settings: Settings = {
        chainId: 8453,
        signer,
        organizationId: "org-test",
        inventory,
        trustedSender: SEARCHER_ADDRESS,
        requiredConfirmations: 3,
      };

      await expect(
        assertTurnkeyWalletAuthority(rpc, settings, {
          wallet: signer,
          representsInventory: true,
        }),
      ).rejects.toThrow(expected);
    },
  );

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

  test("retries a transaction lookup from an inconsistent RPC backend", async () => {
    let attempts = 0;
    const canonical = await readCanonicalInclusion(
      async () => transaction,
      async () => {
        attempts += 1;
        return {
          blockHash:
            attempts === 1 ? (`0x${"22".repeat(32)}` as Hash) : blockHash,
          blockNumber: 10n,
        };
      },
      [0, 0],
    );

    expect(canonical.transaction).toEqual(transaction);
    expect(attempts).toBe(2);
  });

  test("retries a rejected canonical inclusion lookup", async () => {
    let attempts = 0;
    const canonical = await readCanonicalInclusion(
      async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("transient transport failure");
        return transaction;
      },
      async () => ({ blockHash, blockNumber: 10n }),
      [0, 0],
    );

    expect(canonical.transaction).toEqual(transaction);
    expect(attempts).toBe(2);
  });

  test("resumes confirmation without rebroadcasting after an indeterminate result", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-send-"));
    const statePath = join(directory, "activity.json");
    const approvalHash = `0x${"11".repeat(32)}` as Hex;
    const transactionHash = `0x${"22".repeat(32)}` as Hash;
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash, phase: "submitted", activityId: "activity-id" })}\n`,
    );
    let broadcasts = 0;
    let confirmations = 0;
    const run = () =>
      broadcastAndConfirm(
        statePath,
        approvalHash,
        transactionHash,
        async () => {
          broadcasts += 1;
          return transactionHash;
        },
        async (hash) => {
          confirmations += 1;
          if (confirmations === 1) throw new Error("receipt timeout");
          return hash;
        },
      );

    await expect(run()).rejects.toThrow("confirmation is indeterminate");
    expect(JSON.parse(await readFile(statePath, "utf8"))).toEqual({
      version: 1,
      approvalHash,
      phase: "broadcast",
      activityId: "activity-id",
      transactionHash,
    });
    await expect(run()).resolves.toBe(transactionHash);
    expect(broadcasts).toBe(1);
    expect(confirmations).toBe(2);
    await expect(
      broadcastAndConfirm(
        statePath,
        approvalHash,
        `0x${"44".repeat(32)}`,
        async () => {
          broadcasts += 1;
          return transactionHash;
        },
        async (hash) => hash,
      ),
    ).rejects.toThrow("different approved message");
    expect(broadcasts).toBe(1);
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
