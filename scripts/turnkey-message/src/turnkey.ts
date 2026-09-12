import { mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";

import { TurnkeyClient, type TActivity } from "@turnkey/http";
import lockfile from "proper-lockfile";
import { isHash, type Address, type Hex } from "viem";

import { writeJsonAtomic } from "./durable-file.ts";

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

interface BroadcastActivityState {
  version: 1;
  approvalHash: Hex;
  phase: "broadcast";
  activityId: string;
  transactionHash: Hex;
}

type ActivityState =
  SubmittingActivityState | SubmittedActivityState | BroadcastActivityState;

type ActivityShape = Pick<
  TActivity,
  "id" | "organizationId" | "status" | "type"
> & {
  result?: TActivity["result"] | null;
  votes?: TActivity["votes"];
};

interface TurnkeySigningIdentity {
  organizationId: string;
  wallet: Address;
}

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
    (parsed.phase !== "submitting" &&
      parsed.phase !== "submitted" &&
      parsed.phase !== "broadcast") ||
    ((parsed.phase === "submitted" || parsed.phase === "broadcast") &&
      (!("activityId" in parsed) || typeof parsed.activityId !== "string")) ||
    (parsed.phase === "broadcast" &&
      (!("transactionHash" in parsed) ||
        typeof parsed.transactionHash !== "string" ||
        !isHash(parsed.transactionHash)))
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
  if (!activity.votes) {
    throw new Error("completed Turnkey activity omitted approval votes");
  }
  const approvedUsers = new Set(
    activity.votes
      .filter(({ selection }) => selection === "VOTE_SELECTION_APPROVED")
      .map(({ userId }) => userId),
  );
  if (approvedUsers.size < 2) {
    throw new Error(
      "completed Turnkey activity does not contain two distinct approvals",
    );
  }
  const signed = activity.result?.signTransactionResult?.signedTransaction;
  if (!signed || !/^(?:0x)?(?:[0-9a-fA-F]{2})+$/.test(signed)) {
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
    ("result" in value &&
      value.result !== undefined &&
      value.result !== null &&
      typeof value.result !== "object") ||
    ("votes" in value &&
      value.votes !== undefined &&
      (!Array.isArray(value.votes) ||
        !value.votes.every(
          (vote) =>
            typeof vote === "object" &&
            vote !== null &&
            "userId" in vote &&
            typeof vote.userId === "string" &&
            "selection" in vote &&
            typeof vote.selection === "string",
        )))
  ) {
    throw new Error("Turnkey returned an activity with an unexpected shape");
  }
  return value as ActivityShape;
}

export async function canResumeSubmittedActivity(
  statePath: string,
  approvalHash: Hex,
): Promise<boolean> {
  const state = await readActivityState(statePath);
  return (
    state !== undefined &&
    state.phase !== "submitting" &&
    state.approvalHash.toLowerCase() === approvalHash.toLowerCase()
  );
}

export async function savedBroadcastTransactionHash(
  statePath: string,
  approvalHash: Hex,
): Promise<Hex | undefined> {
  const state = await readActivityState(statePath);
  if (state?.phase !== "broadcast") return undefined;
  if (state.approvalHash.toLowerCase() !== approvalHash.toLowerCase()) {
    throw new Error(
      "saved Base transaction belongs to a different approval manifest",
    );
  }
  return state.transactionHash;
}

async function withActivityStateLock<T>(
  statePath: string,
  operation: () => Promise<T>,
): Promise<T> {
  await mkdir(dirname(statePath), { recursive: true });
  const release = await lockfile.lock(statePath, {
    realpath: false,
    retries: { forever: true, factor: 1.2, minTimeout: 100, maxTimeout: 1_000 },
  });
  try {
    return await operation();
  } finally {
    await release();
  }
}

export async function broadcastOrResume(
  statePath: string,
  approvalHash: Hex,
  expectedTransactionHash: Hex,
  operation: () => Promise<Hex>,
): Promise<Hex> {
  return withActivityStateLock(statePath, async () => {
    const state = await readActivityState(statePath);
    if (
      state === undefined ||
      state.approvalHash.toLowerCase() !== approvalHash.toLowerCase() ||
      state.phase === "submitting"
    ) {
      throw new Error(
        "Turnkey activity state does not match the approved transaction",
      );
    }
    if (state.phase === "broadcast") {
      if (
        state.transactionHash.toLowerCase() !==
        expectedTransactionHash.toLowerCase()
      ) {
        throw new Error(
          "saved Base transaction belongs to a different approved message",
        );
      }
      return state.transactionHash;
    }

    await writeJsonAtomic(statePath, {
      version: 1,
      approvalHash,
      phase: "broadcast",
      activityId: state.activityId,
      transactionHash: expectedTransactionHash,
    } satisfies BroadcastActivityState);
    let transactionHash: Hex;
    try {
      transactionHash = await operation();
    } catch (error) {
      throw new Error(
        `Base transaction ${expectedTransactionHash} submission is indeterminate; inspect it before preparing another manifest`,
        { cause: error },
      );
    }
    if (
      transactionHash.toLowerCase() !== expectedTransactionHash.toLowerCase()
    ) {
      throw new Error(
        "Base RPC returned a transaction hash that differs from the local hash",
      );
    }
    return transactionHash;
  });
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
  signingIdentity: TurnkeySigningIdentity,
  unsignedTransaction: Hex,
  approvalHash: Hex,
  statePath: string,
): Promise<Hex> {
  return withActivityStateLock(statePath, () =>
    signOrResumeLocked(
      client,
      signingIdentity,
      unsignedTransaction,
      approvalHash,
      statePath,
    ),
  );
}

async function signOrResumeLocked(
  client: Pick<TurnkeyApi, "getActivity" | "signTransaction">,
  signingIdentity: TurnkeySigningIdentity,
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
      organizationId: signingIdentity.organizationId,
      activityId: prior.activityId,
    });
    activity = asActivity(response.activity);
    assertActivityIdentity(
      activity,
      signingIdentity.organizationId,
      prior.activityId,
    );
  } else {
    await writeJsonAtomic(statePath, {
      version: 1,
      approvalHash,
      phase: "submitting",
    } satisfies SubmittingActivityState);
    const response = await client.signTransaction({
      type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
      timestampMs: Date.now().toString(),
      organizationId: signingIdentity.organizationId,
      parameters: {
        signWith: signingIdentity.wallet,
        unsignedTransaction: unsignedTransaction.slice(2),
        type: "TRANSACTION_TYPE_ETHEREUM",
      },
    });
    activity = asActivity(response.activity);
    assertActivityIdentity(activity, signingIdentity.organizationId);
    await writeJsonAtomic(statePath, {
      version: 1,
      approvalHash,
      phase: "submitted",
      activityId: activity.id,
    } satisfies SubmittedActivityState);
  }

  if (
    activity.status === "ACTIVITY_STATUS_COMPLETED" &&
    activity.votes === undefined
  ) {
    const expectedActivityId = activity.id;
    const response = await client.getActivity({
      organizationId: signingIdentity.organizationId,
      activityId: expectedActivityId,
    });
    activity = asActivity(response.activity);
    assertActivityIdentity(
      activity,
      signingIdentity.organizationId,
      expectedActivityId,
    );
  }

  return completedSignedTransaction(activity);
}
