import { createECDH } from "node:crypto";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";

import { createApiKeyStamper, defaultKeysFolder } from "./api-key-stamper.ts";

async function writeKeys(folder: string, privateKey: string) {
  const rawPrivateKey = privateKey.replace(":p256", "");
  const key = createECDH("prime256v1");
  key.setPrivateKey(Buffer.from(rawPrivateKey, "hex"));
  await writeFile(join(folder, "incident.private"), privateKey, {
    mode: 0o600,
  });
  await writeFile(
    join(folder, "incident.public"),
    key.getPublicKey("hex", "compressed"),
  );
}

describe("Turnkey API-key stamper", () => {
  test("loads exact P-256 keys from the Turnkey CLI layout", async () => {
    const folder = await mkdtemp(join(tmpdir(), "turnkey-api-key-"));
    await writeKeys(folder, `${"11".repeat(32)}:p256`);

    const stamper = await createApiKeyStamper({
      keyName: "incident",
      keysFolder: folder,
    });

    expect(stamper.apiPrivateKey).toBe("11".repeat(32));
    expect(stamper.apiPublicKey).toBe(
      "020217e617f0b6443928278f96999e69a23a4f2c152bdf6d6cdf66e5b80282d4ed",
    );
  });

  test("loads a valid odd-parity compressed public key", async () => {
    const folder = await mkdtemp(join(tmpdir(), "turnkey-api-key-"));
    await writeKeys(folder, "22".repeat(32));

    const stamper = await createApiKeyStamper({
      keyName: "incident",
      keysFolder: folder,
    });
    expect(stamper.apiPublicKey.startsWith("03")).toBe(true);
  });

  test("uses the platform-specific default key folder", () => {
    expect(defaultKeysFolder("darwin", "/Users/operator")).toBe(
      "/Users/operator/Library/Application Support/turnkey/keys",
    );
    expect(defaultKeysFolder("linux", "/home/operator")).toBe(
      "/home/operator/.config/turnkey/keys",
    );
  });

  test("rejects a private key readable by other users", async () => {
    const folder = await mkdtemp(join(tmpdir(), "turnkey-api-key-"));
    await writeKeys(folder, "11".repeat(32));
    await chmod(join(folder, "incident.private"), 0o644);

    await expect(
      createApiKeyStamper({ keyName: "incident", keysFolder: folder }),
    ).rejects.toThrow("group/world");
  });

  test("rejects malformed keys and key-name traversal", async () => {
    const folder = await mkdtemp(join(tmpdir(), "turnkey-api-key-"));
    await writeKeys(folder, `${"11".repeat(32)}\n`);

    await expect(
      createApiKeyStamper({ keyName: "incident", keysFolder: folder }),
    ).rejects.toThrow("exact P-256");
    await expect(
      createApiKeyStamper({ keyName: "../incident", keysFolder: folder }),
    ).rejects.toThrow("unsupported characters");
  });

  test("rejects a public key from a different private key", async () => {
    const folder = await mkdtemp(join(tmpdir(), "turnkey-api-key-"));
    await writeKeys(folder, "11".repeat(32));
    const other = createECDH("prime256v1");
    other.setPrivateKey(Buffer.from("22".repeat(32), "hex"));
    await writeFile(
      join(folder, "incident.public"),
      other.getPublicKey("hex", "compressed"),
    );

    await expect(
      createApiKeyStamper({ keyName: "incident", keysFolder: folder }),
    ).rejects.toThrow("does not match");
  });
});
