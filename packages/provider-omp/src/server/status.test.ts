import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { OmpSettings } from "../settings.ts";
import { checkOmpProviderStatus, parseOmpCatalog } from "./status.ts";

function versionSpawner(
  version: string,
  commands: string[][],
  inventory: () =>
    | ReadonlyArray<{ provider: string; id: string; name?: string }>
    | undefined
    | null = () => [],
  onRequest: Effect.Effect<unknown> = Effect.void,
) {
  return ChildProcessSpawner.make((command) => {
    assert.isTrue(ChildProcess.isStandardCommand(command));
    if (ChildProcess.isStandardCommand(command)) commands.push([...command.args]);
    if (ChildProcess.isStandardCommand(command) && !command.args.includes("--version")) {
      return Effect.gen(function* () {
        const stdout = yield* Queue.unbounded<Uint8Array>();
        const exit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
        const emit = (frame: unknown) =>
          Queue.offer(stdout, new TextEncoder().encode(`${JSON.stringify(frame)}\n`));
        yield* emit({
          type: "ready",
          protocolVersion: 1,
          supportedProtocolVersions: [1, 2],
          maxFrameBytes: 1048576,
          maxReassembledFrameBytes: 67108864,
        });
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(900_000_002),
          exitCode: Deferred.await(exit),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((bytes: Uint8Array) => {
            const frame = decodeFrame(new TextDecoder().decode(bytes));
            const models = inventory();
            return onRequest.pipe(
              Effect.andThen(
                models === null
                  ? Effect.void
                  : emit({
                      type: "response",
                      id: frame.id,
                      command: frame.type,
                      success: models !== undefined,
                      ...(models === undefined
                        ? { error: "private provider diagnostic" }
                        : {
                            data:
                              frame.type === "negotiate_protocol"
                                ? { protocolVersion: 2 }
                                : frame.type === "get_available_models"
                                  ? { models }
                                  : { commands: [] },
                          }),
                    }),
              ),
            );
          }),
          stdout: Stream.fromQueue(stdout),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      });
    }
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
const decodeFrame = Schema.decodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

describe("Oh My Pi status", () => {
  it.effect(
    "discovers models automatically and refreshes the native inventory on health checks",
    () =>
      Effect.gen(function* () {
        const commands: string[][] = [];
        let models: ReadonlyArray<{ provider: string; id: string; name?: string }> | undefined = [
          { provider: "anthropic", id: "claude", name: "Claude" },
          { provider: "openai", id: "gpt", name: "GPT" },
        ];
        const probe = checkOmpProviderStatus(settings, {}).pipe(
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            versionSpawner("18.14.1", commands, () => models),
          ),
          Effect.provide(NodeServices.layer),
        );
        const status = yield* probe;
        assert.deepEqual(commands[0], ["--version"]);
        assert.include(commands[1] ?? [], "rpc-ui");
        assert.include(commands[1] ?? [], "--no-session");
        assert.equal(status.status, "ready");
        assert.equal(status.auth.status, "authenticated");
        assert.deepEqual(
          status.models.map((model) => model.slug),
          ["default", "anthropic/claude", "openai/gpt"],
        );
        assert.equal(status.supportsTextGeneration, false);
        assert.equal(status.supportsConversationRollback, false);
        assert.deepEqual(status.supportedRuntimeModes, [
          "approval-required",
          "auto-accept-edits",
          "full-access",
        ]);
        models = [];
        const empty = yield* probe;
        assert.equal(empty.auth.status, "unauthenticated");
        assert.equal(empty.status, "warning");
        assert.deepEqual(
          empty.models.map((model) => model.slug),
          ["default"],
        );
        models = undefined;
        const failed = yield* probe;
        assert.equal(failed.auth.status, "unknown");
        assert.equal(failed.status, "warning");
        assert.include(failed.message ?? "", "Sign in");
        assert.notInclude(failed.message ?? "", "private provider diagnostic");
      }),
  );

  it.effect("bounds stalled discovery and reports an actionable warning", () =>
    Effect.gen(function* () {
      const requested = yield* Deferred.make<void>();
      const checking = yield* checkOmpProviderStatus(settings, {}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          versionSpawner("18.14.1", [], () => null, Deferred.succeed(requested, undefined)),
        ),
        Effect.forkScoped,
      );
      yield* Deferred.await(requested);
      yield* TestClock.adjust(10_000);
      const status = yield* Fiber.join(checking);
      assert.equal(status.installed, true);
      assert.equal(status.version, "18.14.1");
      assert.equal(status.status, "warning");
      assert.equal(status.auth.status, "unknown");
      assert.include(status.message ?? "", "select a default model");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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
