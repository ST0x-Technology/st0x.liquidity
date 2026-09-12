import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";
import type { TActivity } from "@turnkey/http";
import type { Hex } from "viem";

import { SEARCHER_ADDRESS, type Settings } from "./settings.ts";
import {
  PendingTurnkeyActivityError,
  policyPreflight,
  signOrResume,
} from "./turnkey.ts";

const settings: Settings = {
  chainId: 8453,
  signer: "0xA9C16673F65AE808688cB18952AFE3d9658C808f",
  organizationId: "org-test",
  inventory: "0x10e4db39275C3b128C01bA1194D45D19aE1520d9",
  trustedSender: SEARCHER_ADDRESS,
  requiredConfirmations: 3,
};

function activity(status: TActivity["status"], result?: TActivity["result"]) {
  return {
    id: "activity-id",
    organizationId: "org-test",
    status,
    type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2" as const,
    result: result ?? {},
  };
}

describe("Turnkey activity resumption", () => {
  test("persists a pending activity and resumes it without resubmitting", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    let submissions = 0;
    let queries = 0;
    const client = {
      async signTransaction() {
        submissions += 1;
        return { activity: activity("ACTIVITY_STATUS_CONSENSUS_NEEDED") };
      },
      async getActivity() {
        queries += 1;
        return {
          activity: activity("ACTIVITY_STATUS_COMPLETED", {
            signTransactionResult: { signedTransaction: "02" },
          }),
        };
      },
    };
    const approvalHash = `0x${"11".repeat(32)}` as Hex;

    await expect(
      signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).rejects.toBeInstanceOf(PendingTurnkeyActivityError);
    expect(JSON.parse(await readFile(statePath, "utf8"))).toEqual({
      version: 1,
      approvalHash,
      phase: "submitted",
      activityId: "activity-id",
    });

    expect(
      await signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).toBe("0x02");
    expect(submissions).toBe(1);
    expect(queries).toBe(1);
  });

  test("refuses to reuse an activity for a changed approval", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    const firstHash = `0x${"11".repeat(32)}` as Hex;
    const client = {
      async signTransaction() {
        return { activity: activity("ACTIVITY_STATUS_PENDING") };
      },
      async getActivity() {
        throw new Error("must not query");
      },
    };
    await expect(
      signOrResume(client, settings, "0x02", firstHash, statePath),
    ).rejects.toBeInstanceOf(PendingTurnkeyActivityError);

    await expect(
      signOrResume(client, settings, "0x02", `0x${"22".repeat(32)}`, statePath),
    ).rejects.toThrow("different approval manifest");
  });

  test("fails closed after an ambiguous submission", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    let submissions = 0;
    const client = {
      async signTransaction() {
        submissions += 1;
        throw new Error("connection lost");
      },
      async getActivity() {
        throw new Error("must not query without an activity ID");
      },
    };
    const approvalHash = `0x${"11".repeat(32)}` as Hex;

    await expect(
      signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).rejects.toThrow("connection lost");
    await expect(
      signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).rejects.toThrow("outcome is unknown");
    expect(submissions).toBe(1);
  });

  test("rejects corrupt persisted state", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    await writeFile(statePath, "{}\n");
    const client = {
      async signTransaction() {
        throw new Error("must not submit");
      },
      async getActivity() {
        throw new Error("must not query");
      },
    };
    await expect(
      signOrResume(client, settings, "0x02", `0x${"11".repeat(32)}`, statePath),
    ).rejects.toThrow("is corrupt");
  });

  test("rejects a resumed activity with a different ID", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    const approvalHash = `0x${"11".repeat(32)}` as Hex;
    await writeFile(
      statePath,
      `${JSON.stringify({ version: 1, approvalHash, phase: "submitted", activityId: "expected-id" })}\n`,
    );
    const client = {
      async signTransaction() {
        throw new Error("must not submit");
      },
      async getActivity() {
        return {
          activity: activity("ACTIVITY_STATUS_COMPLETED", {
            signTransactionResult: { signedTransaction: "02" },
          }),
        };
      },
    };

    await expect(
      signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).rejects.toThrow("unexpected identity or type");
  });

  test("does not persist submitted state for a malformed activity", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const statePath = join(directory, "activity.json");
    const approvalHash = `0x${"11".repeat(32)}` as Hex;
    const client = {
      async signTransaction() {
        return {
          activity: {
            organizationId: "org-test",
            status: "ACTIVITY_STATUS_PENDING",
            type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
            result: {},
          } as unknown as TActivity,
        };
      },
      async getActivity() {
        throw new Error("must not query");
      },
    };

    await expect(
      signOrResume(client, settings, "0x02", approvalHash, statePath),
    ).rejects.toThrow("unexpected shape");
    expect(JSON.parse(await readFile(statePath, "utf8"))).toEqual({
      version: 1,
      approvalHash,
      phase: "submitting",
    });
  });

  test.each([
    ["organization", { organizationId: "other-org" }],
    ["type", { type: "ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2" }],
  ])("rejects an unexpected activity %s", async (_label, changes) => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const client = {
      async signTransaction() {
        return {
          activity: {
            ...activity("ACTIVITY_STATUS_COMPLETED", {
              signTransactionResult: { signedTransaction: "02" },
            }),
            ...(changes as Partial<TActivity>),
          },
        };
      },
      async getActivity() {
        throw new Error("must not query");
      },
    };
    await expect(
      signOrResume(
        client,
        settings,
        "0x02",
        `0x${"11".repeat(32)}`,
        join(directory, "activity.json"),
      ),
    ).rejects.toThrow("unexpected identity or type");
  });

  test("rejects a terminal failed activity", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-message-"));
    const client = {
      async signTransaction() {
        return { activity: activity("ACTIVITY_STATUS_FAILED") };
      },
      async getActivity() {
        throw new Error("must not query");
      },
    };
    await expect(
      signOrResume(
        client,
        settings,
        "0x02",
        `0x${"11".repeat(32)}`,
        join(directory, "activity.json"),
      ),
    ).rejects.toThrow("ended as ACTIVITY_STATUS_FAILED");
  });
});

describe("Turnkey policy preflight", () => {
  const expectedUser = "46b69738-7427-4227-9cbe-797c8d4d1fdb";
  const client = {
    async getWhoami() {
      return { organizationId: "org-test", userId: expectedUser };
    },
    async getOrganization() {
      return {
        organizationData: {
          organizationId: "org-test",
          rootQuorum: { threshold: 2, userIds: [expectedUser, "reviewer"] },
        },
      };
    },
  };

  test("accepts the explicitly expected API user", async () => {
    expect(await policyPreflight(client, "org-test", expectedUser)).toEqual({
      userId: expectedUser,
      isRootUser: true,
      rootQuorumThreshold: 2,
    });
  });

  test("rejects an API key belonging to another user", async () => {
    await expect(
      policyPreflight(client, "org-test", "other-user"),
    ).rejects.toThrow("unexpected user");
  });

  test("rejects an API user outside the root quorum", async () => {
    const clientWithoutRootUser = {
      ...client,
      async getOrganization() {
        return {
          organizationData: {
            organizationId: "org-test",
            rootQuorum: { threshold: 2, userIds: ["reviewer", "other"] },
          },
        };
      },
    };
    await expect(
      policyPreflight(clientWithoutRootUser, "org-test", expectedUser),
    ).rejects.toThrow("not a root quorum member");
  });

  test("rejects mismatched Turnkey organizations", async () => {
    await expect(
      policyPreflight(
        {
          ...client,
          async getWhoami() {
            return { organizationId: "other-org", userId: expectedUser };
          },
        },
        "org-test",
        expectedUser,
      ),
    ).rejects.toThrow("different organization");
    await expect(
      policyPreflight(
        {
          ...client,
          async getOrganization() {
            return {
              organizationData: {
                organizationId: "other-org",
                rootQuorum: { threshold: 2, userIds: [expectedUser] },
              },
            };
          },
        },
        "org-test",
        expectedUser,
      ),
    ).rejects.toThrow("different organization");
  });

  test.each([
    undefined,
    { threshold: 0, userIds: [expectedUser] },
    { threshold: 1, userIds: [expectedUser] },
  ])("rejects an invalid root quorum: %p", async (rootQuorum) => {
    await expect(
      policyPreflight(
        {
          ...client,
          async getOrganization() {
            return {
              organizationData: {
                organizationId: "org-test",
                ...(rootQuorum === undefined
                  ? {}
                  : {
                      rootQuorum: {
                        threshold: rootQuorum.threshold,
                        userIds: [...rootQuorum.userIds],
                      },
                    }),
              },
            };
          },
        },
        "org-test",
        expectedUser,
      ),
    ).rejects.toThrow("does not require a second root user");
  });
});
