import { createECDH } from "node:crypto";
import { stat, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

import { ApiKeyStamper } from "@turnkey/api-key-stamper";

const PRIVATE_KEY_PATTERN = /^(?<key>[0-9a-fA-F]{64})(?::p256)?$/;
const PUBLIC_KEY_PATTERN = /^(02|03)[0-9a-fA-F]{64}$/;
const KEY_NAME_PATTERN = /^[a-zA-Z0-9._-]+$/;

export function defaultKeysFolder(
  platform = process.platform,
  home = homedir(),
): string {
  return platform === "darwin"
    ? join(home, "Library", "Application Support", "turnkey", "keys")
    : join(home, ".config", "turnkey", "keys");
}

async function exactKey(path: string, pattern: RegExp, label: string) {
  const value = await readFile(path, "utf8");
  if (!pattern.test(value)) {
    throw new Error(`${label} must contain one exact P-256 hex key`);
  }
  return value.match(pattern)?.groups?.key ?? value;
}

function derivedPublicKey(privateKey: string): string {
  try {
    const key = createECDH("prime256v1");
    key.setPrivateKey(Buffer.from(privateKey, "hex"));
    return key.getPublicKey("hex", "compressed");
  } catch (error) {
    throw new Error("Turnkey private API key is not a valid P-256 scalar", {
      cause: error,
    });
  }
}

export async function createApiKeyStamper(options: {
  keyName: string;
  keysFolder?: string;
}): Promise<ApiKeyStamper> {
  if (!KEY_NAME_PATTERN.test(options.keyName)) {
    throw new Error("Turnkey key name contains unsupported characters");
  }
  const folder = resolve(options.keysFolder ?? defaultKeysFolder());
  const privatePath = join(folder, `${options.keyName}.private`);
  const publicPath = join(folder, `${options.keyName}.public`);
  let privateMetadata;
  try {
    privateMetadata = await stat(privatePath);
  } catch (error) {
    throw new Error(`cannot inspect Turnkey private API key ${privatePath}`, {
      cause: error,
    });
  }
  if ((privateMetadata.mode & 0o077) !== 0) {
    throw new Error(
      "Turnkey private API key must not be group/world accessible",
    );
  }
  const [apiPrivateKey, apiPublicKey] = await Promise.all([
    exactKey(privatePath, PRIVATE_KEY_PATTERN, "Turnkey private API key"),
    exactKey(publicPath, PUBLIC_KEY_PATTERN, "Turnkey public API key"),
  ]);
  if (
    derivedPublicKey(apiPrivateKey).toLowerCase() !== apiPublicKey.toLowerCase()
  ) {
    throw new Error("Turnkey public API key does not match the private key");
  }
  return new ApiKeyStamper({ apiPrivateKey, apiPublicKey });
}
