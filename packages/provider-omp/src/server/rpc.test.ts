import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { makeOmpRpcConnection, type OmpRpcRecord } from "./rpc.ts";

const decode = Schema.decodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const makeFixture = Effect.fn("OmpRpcTest.makeFixture")(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const commands = yield* Queue.unbounded<OmpRpcRecord>();
  const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const emit = (record: OmpRpcRecord) =>
    Queue.offer(stdout, new TextEncoder().encode(`${JSON.stringify(record)}\n`));
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(2_000_000_000),
        exitCode: Deferred.await(exited),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((bytes: Uint8Array) =>
          Queue.offer(commands, decode(new TextDecoder().decode(bytes))),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  const connection = yield* makeOmpRpcConnection({
    command: "omp",
    args: ["--mode", "rpc-ui"],
    cwd: undefined,
    env: {},
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
  return { connection, commands, emit, stdout, exited };
});

describe("OMP RPC transport", () => {
  it.effect(
    "negotiates lossless protocol after ready and correlates explicit prompt IDs without consuming events",
    () =>
      Effect.gen(function* () {
        const { connection, commands, emit } = yield* makeFixture();
        const negotiating = yield* connection.ready.pipe(Effect.forkScoped);
        yield* emit({
          type: "ready",
          protocolVersion: 1,
          supportedProtocolVersions: [1, 2],
          maxFrameBytes: 1048576,
          maxReassembledFrameBytes: 67108864,
        });
        const negotiate = yield* Queue.take(commands);
        assert.equal(negotiate.type, "negotiate_protocol");
        assert.equal(negotiate.protocolVersion, 2);
        yield* emit({
          type: "extension_ui_request",
          id: "startup",
          method: "select",
          options: ["Continue", "Cancel"],
        });
        assert.equal((yield* Queue.take(connection.events)).id, "startup");
        yield* connection.send({ type: "extension_ui_response", id: "startup", value: "Cancel" });
        assert.equal((yield* Queue.take(commands)).type, "extension_ui_response");
        yield* emit({
          type: "response",
          id: negotiate.id,
          command: "negotiate_protocol",
          success: true,
          data: { protocolVersion: 2 },
        });
        yield* Fiber.join(negotiating);
        const pending = yield* connection
          .request({ type: "prompt", id: "turn-ticket", message: "hello" })
          .pipe(Effect.forkScoped);
        assert.equal((yield* Queue.take(commands)).id, "turn-ticket");
        yield* emit({ type: "response", id: "turn-ticket", command: "prompt", success: true });
        yield* Fiber.join(pending);
        yield* emit({
          type: "prompt_result",
          id: "turn-ticket",
          status: "completed",
          sessionSettled: true,
        });
        assert.equal((yield* Queue.take(connection.events)).id, "turn-ticket");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "fails pending requests on process exit and does not expose native error payloads",
    () =>
      Effect.gen(function* () {
        const { connection, commands, emit, stdout, exited } = yield* makeFixture();
        const failed = yield* connection
          .request({ type: "get_state" })
          .pipe(Effect.result, Effect.forkScoped);
        const command = yield* Queue.take(commands);
        yield* emit({
          type: "response",
          id: command.id,
          command: "get_state",
          success: false,
          error: "secret API key",
        });
        const failure = yield* Fiber.join(failed);
        assert.isTrue(Result.isFailure(failure));
        if (Result.isFailure(failure)) assert.notInclude(failure.failure.message, "secret API key");
        const timed = yield* connection
          .request({ type: "get_state", id: "retryable" }, 10)
          .pipe(Effect.result, Effect.forkScoped);
        yield* Queue.take(commands);
        yield* TestClock.adjust(10);
        const timedResult = yield* Fiber.join(timed);
        assert.isTrue(Result.isFailure(timedResult));
        if (Result.isFailure(timedResult))
          assert.equal(timedResult.failure._tag, "OmpRpcTimeoutError");
        const pending = yield* connection
          .request({ type: "get_state", id: "retryable" })
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Queue.take(commands);
        yield* Deferred.succeed(exited, ChildProcessSpawner.ExitCode(7));
        yield* Queue.end(stdout);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(pending)));
        assert.equal(yield* connection.exited, 7);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
