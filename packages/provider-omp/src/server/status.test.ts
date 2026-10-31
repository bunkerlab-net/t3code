import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { OmpSettings } from "../settings.ts";
import { checkOmpProviderStatus, parseOmpCatalog } from "./status.ts";

function versionSpawner(version: string, commands: string[][]) {
  return ChildProcessSpawner.make((command) => {
    assert.isTrue(ChildProcess.isStandardCommand(command));
    if (ChildProcess.isStandardCommand(command)) commands.push([...command.args]);
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(900_000_001),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.succeed(new TextEncoder().encode(`${version}\n`)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });
}

const settings = Schema.decodeSync(OmpSettings)({ enabled: true });

describe("Oh My Pi status", () => {
  it.effect("checks only the version in background and leaves authentication unknown", () =>
    Effect.gen(function* () {
      const commands: string[][] = [];
      const status = yield* checkOmpProviderStatus(settings, {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          versionSpawner("18.14.1", commands),
        ),
        Effect.provide(NodeServices.layer),
      );
      assert.deepEqual(commands, [["--version"]]);
      assert.equal(status.status, "ready");
      assert.equal(status.auth.status, "unknown");
      assert.equal(status.supportsTextGeneration, false);
      assert.equal(status.supportsConversationRollback, false);
      assert.deepEqual(status.supportedRuntimeModes, [
        "approval-required",
        "auto-accept-edits",
        "full-access",
      ]);
      const empty = yield* checkOmpProviderStatus(
        settings,
        {},
        { models: [], slashCommands: [] },
      ).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          versionSpawner("18.14.1", commands),
        ),
        Effect.provide(NodeServices.layer),
      );
      assert.equal(empty.auth.status, "unauthenticated");
      assert.equal(empty.status, "warning");
    }),
  );

  it.effect("does not execute disabled providers and rejects unsupported protocol versions", () =>
    Effect.gen(function* () {
      const commands: string[][] = [];
      const spawner = versionSpawner("1.0.0", commands);
      const disabled = yield* checkOmpProviderStatus({ ...settings, enabled: false }, {}).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(NodeServices.layer),
      );
      assert.isFalse(disabled.enabled);
      assert.deepEqual(commands, []);
      const old = yield* checkOmpProviderStatus(settings, {}).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(NodeServices.layer),
      );
      assert.equal(old.status, "error");
      assert.include(old.message ?? "", "18.14.1");
    }),
  );

  it("validates native inventories and preserves slash command arguments", () => {
    const result = parseOmpCatalog(
      {
        models: [
          { provider: "anthropic", id: "claude", name: "Claude" },
          { provider: "anthropic", id: "claude", name: "Duplicate" },
        ],
      },
      {
        commands: [
          { name: "compact", source: "builtin", input: { hint: "Instructions" } },
          { name: "project-skill", source: "skill" },
        ],
      },
    );
    assert.equal(result.models.length, 1);
    assert.equal(result.models[0]?.slug, "anthropic/claude");
    assert.deepEqual(result.slashCommands, [{ name: "compact", input: { hint: "Instructions" } }]);
    assert.throws(() =>
      parseOmpCatalog({ models: [{ id: "missing-provider" }] }, { commands: [] }),
    );
    assert.throws(() => parseOmpCatalog({ models: [] }, { commands: "invalid" }));
  });
});
