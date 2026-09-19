// @vitest-environment node

import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import viteConfig from "../vite.config";

it("loads API_PROXY_TARGET from a local Vite env file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "limnopulse-vite-"));
  const originalDirectory = process.cwd();
  await writeFile(
    join(directory, ".env.local"),
    "API_PROXY_TARGET=http://proxy.example.test:9000\n",
  );
  process.chdir(directory);
  try {
    const config = await (
      viteConfig as unknown as (env: { command: "serve"; mode: string }) => {
        server?: {
          proxy?: Record<string, { target?: string }>;
        };
      }
    )({ command: "serve", mode: "development" });
    expect(config.server?.proxy?.["/v1"]?.target).toBe(
      "http://proxy.example.test:9000",
    );
  } finally {
    process.chdir(originalDirectory);
    await rm(directory, { recursive: true, force: true });
  }
});
