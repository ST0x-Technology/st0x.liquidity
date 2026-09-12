import { readFile } from "node:fs/promises";

import { TurnkeyClient, type TActivity } from "@turnkey/http";
import type { Hex } from "viem";

import { writeJsonAtomic } from "./durable-file.ts";
import type { Settings } from "./settings.ts";

interface TurnkeyApi {
  getWhoami(input: { organizationId: string }): Promise<{
    organizationId: string;
    userId: string;
  }>;
  getOrganization(input: { organizationId: string }): Promise<{
    organizationData: {
      organizationId?: string;
      rootQuorum?: { threshold: number; userIds: string[] };
    };
  }>;
  getActivity(input: {
    organizationId: string;
    activityId: string;
  }): Promise<{ activity: ActivityShape }>;
  signTransaction(input: {
    type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2";
    timestampMs: string;
    organizationId: string;
    parameters: {
      signWith: string;
      unsignedTransaction: string;
      type: "TRANSACTION_TYPE_ETHEREUM";
    };
  }): Promise<{ activity: ActivityShape }>;
}

interface TurnkeyStamper {
  stamp(payload: string): Promise<{
    stampHeaderName: string;
    stampHeaderValue: string;
  }>;
}

interface SubmittingActivityState {
  version: 1;
  approvalHash: Hex;
  phase: "submitting";
}

interface SubmittedActivityState {
  version: 1;
  approvalHash: Hex;
  phase: "submitted";
  activityId: string;
}

type ActivityState = SubmittingActivityState | SubmittedActivityState;

type ActivityShape = Pick<
  TActivity,
  "id" | "organizationId" | "status" | "type" | "result"
>;

export class PendingTurnkeyActivityError extends Error {
  constructor(
    readonly activityId: string,
    readonly status: string,
  ) {
    super(
      `Turnkey activity ${activityId} is ${status}; rerun send with the same manifest after approval`,
    );
  }
}

async function readActivityState(
  path: string,
): Promise<ActivityState | undefined> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`cannot read Turnkey activity state at ${path}`, {
      cause: error,
    });
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`Turnkey activity state at ${path} is corrupt`, {
      cause: error,
    });
  }
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("version" in parsed) ||
    parsed.version !== 1 ||
    !("approvalHash" in parsed) ||
    typeof parsed.approvalHash !== "string" ||
    !("phase" in parsed) ||
    (parsed.phase !== "submitting" && parsed.phase !== "submitted") ||
    (parsed.phase === "submitted" &&
      (!("activityId" in parsed) || typeof parsed.activityId !== "string"))
  ) {
    throw new Error(`Turnkey activity state at ${path} is corrupt`);
  }
  return parsed as ActivityState;
}

function completedSignedTransaction(activity: ActivityShape): Hex {
  if (activity.status !== "ACTIVITY_STATUS_COMPLETED") {
    if (
      activity.status === "ACTIVITY_STATUS_PENDING" ||
      activity.status === "ACTIVITY_STATUS_CONSENSUS_NEEDED" ||
      activity.status === "ACTIVITY_STATUS_CREATED" ||
      activity.status === "ACTIVITY_STATUS_AUTHENTICATORS_NEEDED"
    ) {
      throw new PendingTurnkeyActivityError(activity.id, activity.status);
    }
    throw new Error(
      `Turnkey activity ${activity.id} ended as ${activity.status}`,
    );
  }
  const signed = activity.result?.signTransactionResult?.signedTransaction;
  if (!signed || !/^(?:0x)?[0-9a-fA-F]+$/.test(signed)) {
    throw new Error("completed Turnkey activity omitted a signed transaction");
  }
  return (signed.startsWith("0x") ? signed : `0x${signed}`) as Hex;
}

function asActivity(value: unknown): ActivityShape {
  if (
    typeof value !== "object" ||
    value === null ||
    !("id" in value) ||
    typeof value.id !== "string" ||
    !("organizationId" in value) ||
    typeof value.organizationId !== "string" ||
    !("status" in value) ||
    typeof value.status !== "string" ||
    !("type" in value) ||
    typeof value.type !== "string" ||
    !("result" in value) ||
    typeof value.result !== "object" ||
    value.result === null
  ) {
    throw new Error("Turnkey returned an activity with an unexpected shape");
  }
  return value as ActivityShape;
}

function assertActivityIdentity(
  activity: ActivityShape,
  organizationId: string,
  expectedActivityId?: string,
): void {
  if (
    activity.organizationId !== organizationId ||
    activity.type !== "ACTIVITY_TYPE_SIGN_TRANSACTION_V2" ||
    (expectedActivityId !== undefined && activity.id !== expectedActivityId)
  ) {
    throw new Error(
      "Turnkey returned an activity with unexpected identity or type",
    );
  }
}

export function createTurnkeyClient(stamper: TurnkeyStamper): TurnkeyClient {
  return new TurnkeyClient({ baseUrl: "https://api.turnkey.com" }, stamper);
}

export async function policyPreflight(
  client: Pick<TurnkeyApi, "getWhoami" | "getOrganization">,
  organizationId: string,
  expectedUserId: string,
): Promise<{
  userId: string;
  isRootUser: boolean;
  rootQuorumThreshold: number;
}> {
  const [identity, organizationResponse] = await Promise.all([
    client.getWhoami({ organizationId }),
    client.getOrganization({ organizationId }),
  ]);
  if (identity.organizationId !== organizationId) {
    throw new Error("Turnkey API user belongs to a different organization");
  }
  if (identity.userId !== expectedUserId) {
    throw new Error("Turnkey API credential belongs to an unexpected user");
  }
  const organization = organizationResponse.organizationData;
  if (organization.organizationId !== organizationId) {
    throw new Error("Turnkey returned a different organization");
  }
  const rootQuorum = organization.rootQuorum;
  if (!rootQuorum || rootQuorum.threshold < 2) {
    throw new Error("Turnkey root quorum does not require a second root user");
  }
  const isRootUser = rootQuorum.userIds.includes(identity.userId);
  if (!isRootUser) {
    throw new Error("Turnkey API user is not a root quorum member");
  }
  return {
    userId: identity.userId,
    isRootUser,
    rootQuorumThreshold: rootQuorum.threshold,
  };
}

export async function signOrResume(
  client: Pick<TurnkeyApi, "getActivity" | "signTransaction">,
  settings: Settings,
  unsignedTransaction: Hex,
  approvalHash: Hex,
  statePath: string,
): Promise<Hex> {
  const prior = await readActivityState(statePath);
  let activity: ActivityShape;
  if (prior) {
    if (prior.approvalHash.toLowerCase() !== approvalHash.toLowerCase()) {
      throw new Error(
        "saved Turnkey activity belongs to a different approval manifest",
      );
    }
    if (prior.phase === "submitting") {
      throw new Error(
        "Turnkey submission outcome is unknown; inspect Turnkey activities before removing the state file",
      );
    }
    const response = await client.getActivity({
      organizationId: settings.organizationId,
      activityId: prior.activityId,
    });
    activity = asActivity(response.activity);
    assertActivityIdentity(activity, settings.organizationId, prior.activityId);
  } else {
    await writeJsonAtomic(statePath, {
      version: 1,
      approvalHash,
      phase: "submitting",
    } satisfies SubmittingActivityState);
    const response = await client.signTransaction({
      type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
      timestampMs: Date.now().toString(),
      organizationId: settings.organizationId,
      parameters: {
        signWith: settings.signer,
        unsignedTransaction: unsignedTransaction.slice(2),
        type: "TRANSACTION_TYPE_ETHEREUM",
      },
    });
    activity = asActivity(response.activity);
    assertActivityIdentity(activity, settings.organizationId);
    await writeJsonAtomic(statePath, {
      version: 1,
      approvalHash,
      phase: "submitted",
      activityId: activity.id,
    } satisfies SubmittedActivityState);
  }

  return completedSignedTransaction(activity);
}
