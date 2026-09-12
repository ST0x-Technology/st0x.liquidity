import { mkdir, mkdtemp, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, test } from "bun:test";

import { writeJsonAtomic } from "./durable-file.ts";

describe("atomic JSON writes", () => {
  test("allows concurrent attempts without temporary-name collisions", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-atomic-"));
    const path = join(directory, "state.json");

    await Promise.all(
      Array.from({ length: 20 }, (_, value) =>
        writeJsonAtomic(path, { value }),
      ),
    );

    expect(await readdir(directory)).toEqual(["state.json"]);
  });

  test("removes the temporary file when rename fails", async () => {
    const directory = await mkdtemp(join(tmpdir(), "turnkey-atomic-"));
    const targetDirectory = join(directory, "state.json");
    await mkdir(targetDirectory);
    await writeFile(join(targetDirectory, "child"), "child");

    await expect(
      writeJsonAtomic(targetDirectory, { value: 1 }),
    ).rejects.toThrow();
    expect(
      (await readdir(directory)).filter((name) => name.endsWith(".tmp")),
    ).toEqual([]);
  });
});
