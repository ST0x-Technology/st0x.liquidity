import { readFile } from "node:fs/promises";

import { parse } from "smol-toml";
import { getAddress, type Address } from "viem";

import { BASE_CHAIN_ID } from "./message.ts";

export const SEARCHER_ADDRESS = getAddress(
  "0x2352a1FcA90182509dCa9c12B2CAd582a38E8b82",
);

interface RawSettings {
  wallet?: {
    kind?: unknown;
    address?: unknown;
    organization_id?: unknown;
  };
  chains?: {
    base?: {
      required_confirmations?: unknown;
      trading?: { inventory?: unknown; inventory_mode?: unknown };
    };
  };
}

export interface Settings {
  chainId: typeof BASE_CHAIN_ID;
  signer: Address;
  organizationId: string;
  inventory: Address;
  trustedSender: Address;
  requiredConfirmations: number;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`config field ${name} is required`);
  }
  return value;
}

export async function loadSettings(configPath: string): Promise<Settings> {
  const raw = parse(await readFile(configPath, "utf8")) as RawSettings;
  if (raw.wallet?.kind !== "turnkey") {
    throw new Error("configured wallet kind must be turnkey");
  }
  if (raw.chains?.base?.trading?.inventory_mode !== "managed") {
    throw new Error("Base inventory mode must be managed");
  }
  const confirmations = raw.chains.base.required_confirmations;
  if (!Number.isSafeInteger(confirmations) || Number(confirmations) < 1) {
    throw new Error("Base required_confirmations must be a positive integer");
  }

  return {
    chainId: BASE_CHAIN_ID,
    signer: getAddress(requiredString(raw.wallet.address, "wallet.address")),
    organizationId: requiredString(
      raw.wallet.organization_id,
      "wallet.organization_id",
    ),
    inventory: getAddress(
      requiredString(
        raw.chains.base.trading?.inventory,
        "chains.base.trading.inventory",
      ),
    ),
    trustedSender: SEARCHER_ADDRESS,
    requiredConfirmations: Number(confirmations),
  };
}

export function rpcUrl(): string {
  const value = process.env.BASE_RPC_URL;
  if (!value) {
    throw new Error("BASE_RPC_URL is required");
  }
  return value;
}
