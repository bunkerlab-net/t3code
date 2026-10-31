import { assert, describe, it } from "@effect/vitest";
import { buildOmpRpcLaunch, resolveOmpLaunchArgs } from "./launch.ts";

describe("OMP native launch", () => {
  it("prevents protocol, session and approval overrides, including equals and positional forms", () => {
    for (const value of [
      "--mode acp",
      "--mode=rpc",
      "--yolo",
      "--auto-approve",
      "--approval-mode=yolo",
      "--no-ui",
      "--session /other",
      "--continue",
      "--cwd=/other",
      "--goal 'run forever'",
      "--plan-yolo",
      "--resume",
      "-- prompt",
      "@instructions.txt",
      "prompt",
      "--profile",
    ])
      assert.isFalse(resolveOmpLaunchArgs(value).ok, value);
    assert.deepEqual(
      resolveOmpLaunchArgs(
        '--profile "work account" --model=anthropic/claude --thinking high --no-lsp',
      ),
      {
        ok: true,
        args: [
          "--profile",
          "work account",
          "--model",
          "anthropic/claude",
          "--thinking",
          "high",
          "--no-lsp",
        ],
      },
    );
  });

  it("maps modes natively and preserves tool UI while keeping background helpers tool-free", () => {
    for (const [runtimeMode, native] of [
      ["approval-required", "always-ask"],
      ["auto", "always-ask"],
      ["auto-accept-edits", "write"],
      ["full-access", "yolo"],
    ] as const) {
      const launch = buildOmpRpcLaunch({
        launchArgs: [],
        environment: {},
        runtimeMode,
        sessionPath: "/sessions/thread.jsonl",
      });
      assert.deepEqual(launch.args, [
        "--mode",
        "rpc-ui",
        "--approval-mode",
        native,
        "--no-title",
        "--no-pty",
        "--session",
        "/sessions/thread.jsonl",
      ]);
    }
    const resolved = resolveOmpLaunchArgs(
      "--extension demo.ts --hook hook.ts --tools bash --profile work",
    );
    assert.isTrue(resolved.ok);
    if (!resolved.ok) return;
    const environment = { PATH: "/usr/bin", OMP_PROFILE: "work" };
    const launch = buildOmpRpcLaunch({
      launchArgs: resolved.args,
      environment,
      ephemeral: true,
      disableTools: true,
      disableExtensions: true,
    });
    assert.deepEqual(launch.args, [
      "--profile",
      "work",
      "--mode",
      "rpc-ui",
      "--approval-mode",
      "always-ask",
      "--no-title",
      "--no-pty",
      "--no-session",
      "--no-tools",
      "--no-extensions",
    ]);
    assert.deepEqual(launch.env, environment);
    assert.notStrictEqual(launch.env, environment);
  });
});
