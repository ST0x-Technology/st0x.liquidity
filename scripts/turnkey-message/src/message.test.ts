import { describe, expect, test } from "bun:test";

import {
  approvalHash,
  type ApprovedTransaction,
  type ApprovalManifest,
  createManifest,
  decodeMessage,
  encodeMessage,
  MAX_MESSAGE_BYTES,
  maximumTransactionFee,
  validateManifest,
} from "./message.ts";

const transaction = {
  type: "eip1559" as const,
  chainId: 8453 as const,
  from: "0xA9C16673F65AE808688cB18952AFE3d9658C808f" as const,
  representedInventory: "0x10e4db39275C3b128C01bA1194D45D19aE1520d9" as const,
  to: "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82" as const,
  value: "0" as const,
  data: encodeMessage("hello"),
  nonce: "7",
  gasLimit: "22000",
  maxFeePerGas: "2000000000",
  maxPriorityFeePerGas: "1000000",
  l1DataFeeWei: "100",
  operatorFeeWei: "5",
  maxTotalFeeWei: "44000000000105",
  accessList: [] as [],
};

describe("message encoding", () => {
  test("round trips strict UTF-8", () => {
    expect(decodeMessage(encodeMessage("hello 👋"))).toBe("hello 👋");
  });

  test("rejects empty and unpaired-surrogate messages", () => {
    expect(() => encodeMessage("")).toThrow("empty");
    expect(() => encodeMessage("\ud800")).toThrow("UTF-8");
  });

  test("rejects messages above the byte limit", () => {
    expect(() => encodeMessage("a".repeat(4097))).toThrow("4096");
  });

  test("accepts a message exactly at the byte limit", () => {
    expect(encodeMessage("a".repeat(MAX_MESSAGE_BYTES))).toHaveLength(
      2 + MAX_MESSAGE_BYTES * 2,
    );
  });

  test("rejects malformed UTF-8 calldata", () => {
    expect(() => decodeMessage("0xff")).toThrow("UTF-8");
  });

  test("rejects decoded calldata above the byte limit", () => {
    expect(() =>
      decodeMessage(`0x${"61".repeat(MAX_MESSAGE_BYTES + 1)}`),
    ).toThrow("4096");
  });
});

describe("approval manifest", () => {
  test("binds a test wallet without claiming the managed inventory", () => {
    const testTransaction: ApprovedTransaction = {
      ...transaction,
      representedInventory: null,
    };
    const manifest = createManifest(testTransaction, 1_000, 1_600);

    expect(manifest.transaction.representedInventory).toBeNull();
    expect(() => validateManifest(manifest, 1_100)).not.toThrow();
    expect(() =>
      validateManifest(
        {
          ...manifest,
          transaction: {
            ...manifest.transaction,
            representedInventory: transaction.representedInventory,
          },
        },
        1_100,
      ),
    ).toThrow("approval hash");
  });

  test.each([
    ["nonce", "8"],
    ["gasLimit", "21999"],
    ["maxFeePerGas", "1999999999"],
    ["maxPriorityFeePerGas", "999999"],
    ["l1DataFeeWei", "99"],
    ["operatorFeeWei", "4"],
    ["maxTotalFeeWei", "44000000000106"],
    ["to", "0x0000000000000000000000000000000000000001"],
    ["representedInventory", "0x0000000000000000000000000000000000000002"],
    ["from", "0x0000000000000000000000000000000000000003"],
  ] as const)("binds the transaction %s", (field, value) => {
    const manifest = createManifest(transaction, 1_000, 1_600);
    const mutated = {
      ...manifest,
      transaction: { ...manifest.transaction, [field]: value },
    };

    expect(() => validateManifest(mutated, 1_100)).toThrow("approval hash");
  });

  test("rejects expired manifests and fee bounds", () => {
    const manifest = createManifest(transaction, 1_000, 1_600);
    expect(() => validateManifest(manifest, 1_601)).toThrow("expired");

    const excessive = createManifest(
      { ...transaction, maxTotalFeeWei: "44000000000104" },
      1_000,
      1_600,
    );
    expect(() => validateManifest(excessive, 1_100)).toThrow(
      "maximum total fee",
    );
  });

  test("calculates the transaction fee independently from the operator cap", () => {
    const manifest = createManifest(
      { ...transaction, maxTotalFeeWei: "99999999999999" },
      1_000,
      1_600,
    );
    expect(maximumTransactionFee(manifest)).toBe(44_000_000_000_105n);
  });

  test.each([
    ["priority fee", { maxPriorityFeePerGas: "2000000001" }, "priority fee"],
    ["chain", { chainId: 1 }, "chain ID"],
    ["type", { type: "legacy" }, "EIP-1559"],
    ["value", { value: "1" }, "value"],
    ["access list", { accessList: [{}] }, "access list"],
    [
      "message",
      { data: encodeMessage("different") },
      "does not match calldata",
    ],
    ["hex nonce", { nonce: "0x10" }, "canonical decimal"],
    ["exponent nonce", { nonce: "1e3" }, "canonical decimal"],
    ["unsafe nonce", { nonce: "9007199254740992" }, "safe integer"],
  ])("rejects an invalid %s", (_label, changes, expected) => {
    const createdAt = 1_000;
    const expiresAt = 1_600;
    const mutatedTransaction = { ...transaction, ...changes };
    const manifest = {
      version: 1,
      createdAtMs: createdAt,
      expiresAtMs: expiresAt,
      message: "hello",
      transaction: mutatedTransaction,
      approvalHash: approvalHash(
        mutatedTransaction as typeof transaction,
        createdAt,
        expiresAt,
        "hello",
      ),
    } as unknown as ApprovalManifest;
    expect(() => validateManifest(manifest, 1_000)).toThrow(expected);
  });

  test("rejects an invalid version and expiry order", () => {
    const manifest = createManifest(transaction, 1_000, 1_600);
    expect(() =>
      validateManifest(
        { ...manifest, version: 2 } as unknown as ApprovalManifest,
        1_000,
      ),
    ).toThrow("version");

    const invalidExpiry = createManifest(transaction, 1_000, 1_000);
    expect(() => validateManifest(invalidExpiry, 999)).toThrow("expiry");
  });

  test("rejects a future or overlong approval window", () => {
    const future = createManifest(transaction, 2_000, 2_600);
    expect(() => validateManifest(future, 1_999)).toThrow("not active");

    const overlong = createManifest(transaction, 1_000, 3_601_001);
    expect(() => validateManifest(overlong, 1_000)).toThrow("3600 seconds");
  });

  test("rejects a missing approval hash with a domain error", () => {
    const manifest = createManifest(transaction, 1_000, 1_600);
    expect(() =>
      validateManifest(
        { ...manifest, approvalHash: undefined } as unknown as ApprovalManifest,
        1_000,
      ),
    ).toThrow("approval hash does not match");
  });

  test.each([undefined, Number.NaN, Number.POSITIVE_INFINITY, 1.5])(
    "rejects invalid manifest timestamps: %p",
    (timestamp) => {
      const manifest = createManifest(transaction, 1_000, 1_600);
      expect(() =>
        validateManifest(
          {
            ...manifest,
            expiresAtMs: timestamp,
          } as unknown as ApprovalManifest,
          1_000,
        ),
      ).toThrow("expiresAtMs");
    },
  );
});
