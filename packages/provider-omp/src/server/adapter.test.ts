import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { makeOmpAdapterV2 } from "./adapter.ts";
import type { OmpRpcRecord } from "./rpc.ts";
import nativeFixture from "./fixtures/native-cli-18.14.1.json" with { type: "json" };

const layerTest = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer().pipe(Layer.provide(NodeServices.layer)),
);
const INSTANCE = ProviderInstanceId.make("omp");
const THREAD = ThreadId.make("omp-thread");
const SESSION = ProviderSessionId.make("omp-session");
const FILE = "/fake/.omp/agent/sessions/conversation.jsonl";
const selection = { instanceId: INSTANCE, model: "default" };
const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "approval-required",
  interactionMode: "default",
  cwd: null,
});
const decode = Schema.decodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);

const setup = Effect.fn("OmpAdapterTest.setup")(function* (startupDialog = false) {
  const stdout = yield* Queue.unbounded<Uint8Array>();
  const commands = yield* Queue.unbounded<OmpRpcRecord>();
  const exit = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  let sessionFile = FILE;
  let settled = true;
  let startupAnswered = !startupDialog;
  let negotiation: OmpRpcRecord | undefined;
  const emit = (record: OmpRpcRecord) =>
    Queue.offer(stdout, new TextEncoder().encode(`${JSON.stringify(record)}\n`));
  const respond = (command: OmpRpcRecord, data: unknown = {}) =>
    emit({ type: "response", id: command.id, command: command.type, success: true, data });
  const spawner = ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      yield* emit({
        type: "ready",
        protocolVersion: 1,
        supportedProtocolVersions: [1, 2],
        maxFrameBytes: 1048576,
        maxReassembledFrameBytes: 67108864,
      });
      if (startupDialog)
        yield* emit({
          type: "extension_ui_request",
          id: "startup",
          method: "confirm",
          title: "Extension startup",
        });
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(999_999_999),
        exitCode: Deferred.await(exit),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((bytes: Uint8Array) =>
          Effect.gen(function* () {
            const command = decode(new TextDecoder().decode(bytes));
            yield* Queue.offer(commands, command);
            if (command.type === "prompt") {
              settled = false;
              return;
            }
            if (command.type === "extension_ui_response") {
              if (command.id === "startup") {
                startupAnswered = true;
                if (negotiation !== undefined) yield* respond(negotiation, { protocolVersion: 2 });
              }
              return;
            }
            if (command.type === "negotiate_protocol" && !startupAnswered) {
              negotiation = command;
              return;
            }
            if (command.type === "abort_and_restore_queue") settled = true;
            if (command.type === "switch_session") sessionFile = String(command.sessionPath);
            const data =
              command.type === "negotiate_protocol"
                ? { protocolVersion: 2 }
                : command.type === "get_state"
                  ? {
                      sessionFile,
                      isSettled: settled,
                      model: { provider: "anthropic", id: "test", contextWindow: 200000 },
                    }
                  : command.type === "set_model"
                    ? { provider: command.provider, id: command.modelId, contextWindow: 200000 }
                    : command.type === "get_messages"
                      ? {
                          messages: [
                            { role: "user", content: "restored user" },
                            {
                              role: "assistant",
                              content: [{ type: "text", text: "restored assistant" }],
                            },
                          ],
                        }
                      : {};
            yield* respond(command, data);
          }),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );
  const adapter = yield* makeOmpAdapterV2({
    instanceId: INSTANCE,
    settings: { enabled: true, binaryPath: "omp", launchArgs: "", customModels: [] },
    environment: {},
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
  const runtime = yield* adapter.openSession({
    threadId: THREAD,
    providerSessionId: SESSION,
    modelSelection: selection,
    runtimePolicy: policy,
  });
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const observed: ProviderAdapter.ProviderAdapterV2Event[] = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) => {
      observed.push(event);
      return Queue.offer(events, event);
    }),
    Effect.forkScoped,
  );
  const takeEvent = Effect.fn("OmpAdapterTest.takeEvent")(function* (
    predicate: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean,
  ) {
    while (true) {
      const event = yield* Queue.take(events);
      if (predicate(event)) return event;
    }
  });
  const takeCommand = Effect.fn("OmpAdapterTest.takeCommand")(function* (type: string) {
    while (true) {
      const command = yield* Queue.take(commands);
      if (command.type === type) return command;
    }
  });
  const ensuring = yield* runtime
    .ensureThread({
      threadId: THREAD,
      modelSelection: selection,
      runtimePolicy: policy,
    })
    .pipe(Effect.forkScoped);
  if (startupDialog) {
    const question = yield* takeEvent(
      (event) => event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
    );
    if (question.type === "turn_item.updated" && question.turnItem.type === "approval_request") {
      yield* runtime.respondToRuntimeRequest({
        requestId: question.turnItem.requestId,
        decision: "decline",
      });
    }
  }
  const thread = yield* Fiber.join(ensuring);
  return { runtime, thread, emit, respond, takeEvent, takeCommand, observed };
});

const start = Effect.fn("OmpAdapterTest.start")(function* (
  runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
  thread: OrchestrationV2ProviderThread,
  ordinal = 1,
  text = "Hello",
  appContext: ReadonlyArray<{ readonly key: string; readonly text: string }> = [],
) {
  const at = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD,
    projectId: ProjectId.make("omp-project"),
    title: "Native OMP",
    providerInstanceId: INSTANCE,
    modelSelection: selection,
    runtimeMode: "approval-required",
    interactionMode: "default",
    createdBy: "user",
    creationSource: "web",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: thread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD },
    forkedFrom: null,
    createdAt: at,
    updatedAt: at,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* runtime.startTurn({
    appThread,
    threadId: THREAD,
    runId: RunId.make(`run-${ordinal}`),
    runOrdinal: ordinal,
    providerTurnOrdinal: ordinal,
    attemptId: RunAttemptId.make(`attempt-${ordinal}`),
    rootNodeId: NodeId.make(`node-${ordinal}`),
    providerThread: thread,
    message: {
      messageId: MessageId.make(`message-${ordinal}`),
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: selection,
    runtimePolicy: policy,
    appContext,
  });
});

describe("Oh My Pi native adapter", () => {
  it.effect("replays recorded native CLI text, read-tool, and approved-write turns", () =>
    Effect.gen(function* () {
      const fake = yield* setup();
      for (const [index, scenario] of nativeFixture.scenarios.entries()) {
        yield* start(fake.runtime, fake.thread, index + 1, scenario.prompt);
        const prompt = yield* fake.takeCommand("prompt");
        for (const record of scenario.records) {
          const frame: OmpRpcRecord = record.frame;
          if (record.direction === "stdin") {
            if (frame.type !== "extension_ui_response") continue;
            const event = yield* fake.takeEvent(
              (event) =>
                event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
            );
            assert.equal(event.type, "turn_item.updated");
            if (event.type !== "turn_item.updated" || event.turnItem.type !== "approval_request")
              return;
            assert.include(event.turnItem.prompt, "Path: proof-written.txt");
            yield* fake.runtime.respondToRuntimeRequest({
              requestId: event.turnItem.requestId,
              decision: "accept",
            });
            const response = yield* fake.takeCommand("extension_ui_response");
            assert.equal(response.value, frame.value);
            assert.equal(response.id, frame.id);
          } else {
            yield* fake.emit(frame.id === scenario.promptId ? { ...frame, id: prompt.id } : frame);
          }
        }
        const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
        assert.equal(terminal.type === "turn.terminal" && terminal.status, "completed");
        assert.isFalse(yield* fake.runtime.hasPendingBackgroundWork!);
      }
      assert.isTrue(
        fake.observed.some(
          (event) =>
            event.type === "message.updated" &&
            event.message.text === "Hello from the local deterministic fixture.",
        ),
      );
      assert.isTrue(
        fake.observed.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.toolName === "read" &&
            event.turnItem.status === "completed",
        ),
      );
      assert.isTrue(
        fake.observed.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "file_change" &&
            event.turnItem.fileName === "proof-written.txt" &&
            event.turnItem.status === "completed",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "returns the runtime before extension startup asks a question so negotiation cannot deadlock",
    () =>
      Effect.gen(function* () {
        const fake = yield* setup(true);
        assert.deepEqual(yield* fake.takeCommand("extension_ui_response"), {
          type: "extension_ui_response",
          id: "startup",
          confirmed: false,
        });
        assert.equal(fake.thread.nativeThreadRef?.nativeId, FILE);
        assert.equal(fake.runtime.providerSession.status, "ready");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "retains an acknowledged yielding prompt until session_settled and streams final messages and tools",
    () =>
      Effect.gen(function* () {
        const fake = yield* setup();
        yield* start(fake.runtime, fake.thread);
        const prompt = yield* fake.takeCommand("prompt");
        yield* fake.respond(prompt);
        yield* fake.emit({ type: "agent_start" });
        yield* fake.emit({
          type: "message_start",
          messageId: "m1",
          message: { role: "assistant", content: [] },
        });
        yield* fake.emit({
          type: "message_update",
          messageId: "m1",
          assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Hello" },
        });
        yield* fake.emit({
          type: "message_end",
          messageId: "m1",
          message: { role: "assistant", content: [{ type: "text", text: "Hello world" }] },
        });
        yield* fake.emit({
          type: "tool_execution_start",
          toolCallId: "tool-1",
          toolName: "bash",
          args: { command: "pwd" },
        });
        yield* fake.emit({
          type: "tool_execution_end",
          toolCallId: "tool-1",
          toolName: "bash",
          result: { content: [{ type: "text", text: "/workspace" }] },
          isError: false,
        });
        yield* fake.emit({
          type: "agent_end",
          messages: [],
          isTerminal: false,
          awaitingAsyncWork: true,
        });
        yield* fake.emit({
          type: "prompt_result",
          id: prompt.id,
          agentInvoked: true,
          status: "completed",
          sessionSettled: false,
        });
        yield* fake.emit({
          type: "message_start",
          messageId: "m2",
          message: { role: "assistant", content: [] },
        });
        yield* fake.emit({
          type: "message_end",
          messageId: "m2",
          message: { role: "assistant", content: [{ type: "text", text: "Async result" }] },
        });
        yield* fake.takeEvent(
          (event) => event.type === "message.updated" && event.message.text === "Async result",
        );
        assert.isFalse(fake.observed.some((event) => event.type === "turn.terminal"));
        assert.isTrue(yield* fake.runtime.hasPendingBackgroundWork!);
        assert.isTrue(
          fake.observed.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              event.turnItem.output === "/workspace" &&
              event.turnItem.status === "completed",
          ),
        );
        assert.isTrue(
          fake.observed.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "Hello world" &&
              !event.message.streaming,
          ),
        );
        yield* fake.emit({ type: "session_settled" });
        const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
        assert.isFalse(yield* fake.runtime.hasPendingBackgroundWork!);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("stops before agent_start using atomic queue withdrawal rather than plain abort", () =>
    Effect.gen(function* () {
      const fake = yield* setup();
      yield* start(fake.runtime, fake.thread);
      yield* fake.takeCommand("prompt");
      const running = yield* fake.takeEvent((event) => event.type === "provider_turn.updated");
      assert.isTrue(running.type === "provider_turn.updated");
      if (running.type !== "provider_turn.updated") return;
      yield* fake.runtime.interruptTurn({
        providerThread: fake.thread,
        providerTurnId: running.providerTurn.id,
      });
      assert.equal(
        (yield* fake.takeCommand("abort_and_restore_queue")).type,
        "abort_and_restore_queue",
      );
      const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "interrupted");
      yield* start(fake.runtime, fake.thread, 2);
      assert.equal((yield* fake.takeCommand("prompt")).id, "t3-turn:attempt-2");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "returns exact native approval choices, refuses unavailable decisions and cancels live prompts on failure",
    () =>
      Effect.gen(function* () {
        const fake = yield* setup();
        yield* start(fake.runtime, fake.thread);
        const prompt = yield* fake.takeCommand("prompt");
        yield* fake.emit({
          type: "extension_ui_request",
          id: "approve-1",
          method: "select",
          title: "Allow tool: bash\nCommand: pwd",
          options: ["Approve", "Approve bash for this session", "Deny"],
        });
        const approval = yield* fake.takeEvent(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
        );
        if (approval.type !== "turn_item.updated" || approval.turnItem.type !== "approval_request")
          return;
        assert.include(approval.turnItem.prompt ?? "", "Command: pwd");
        yield* fake.runtime.respondToRuntimeRequest({
          requestId: approval.turnItem.requestId,
          decision: "acceptForSession",
        });
        assert.deepEqual(yield* fake.takeCommand("extension_ui_response"), {
          type: "extension_ui_response",
          id: "approve-1",
          value: "Approve bash for this session",
        });
        yield* fake.emit({
          type: "extension_ui_request",
          id: "approve-2",
          method: "select",
          title: "Critical command",
          options: ["Approve", "Deny"],
        });
        const second = yield* fake.takeEvent(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "approval_request" &&
            event.turnItem.title === "Critical command",
        );
        if (second.type !== "turn_item.updated" || second.turnItem.type !== "approval_request")
          return;
        const secondRequestId = second.turnItem.requestId;
        const invalid = yield* fake.runtime
          .respondToRuntimeRequest({
            requestId: second.turnItem.requestId,
            decision: "acceptForSession",
          })
          .pipe(Effect.result);
        assert.isTrue(Result.isFailure(invalid));
        yield* fake.emit({
          type: "prompt_result",
          id: prompt.id,
          status: "error",
          agentInvoked: true,
          error: { message: "rate limited", httpStatus: 429 },
          sessionSettled: true,
        });
        const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(
          terminal.type === "turn.terminal" &&
            terminal.status === "failed" &&
            terminal.failure.class === "usage_limit",
        );
        assert.deepEqual(yield* fake.takeCommand("extension_ui_response"), {
          type: "extension_ui_response",
          id: "approve-2",
          cancelled: true,
        });
        assert.isTrue(
          fake.observed.some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === secondRequestId &&
              event.runtimeRequest.status === "cancelled",
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("settles a rejected prompt ticket before accepting the next turn", () =>
    Effect.gen(function* () {
      const fake = yield* setup();
      yield* start(fake.runtime, fake.thread);
      const prompt = yield* fake.takeCommand("prompt");
      yield* fake.emit({
        type: "response",
        id: prompt.id,
        command: "prompt",
        success: false,
        error: "Input handler rejected prompt",
      });
      yield* fake.emit({
        type: "prompt_result",
        id: prompt.id,
        status: "error",
        agentInvoked: false,
        error: { message: "Input handler rejected prompt" },
        sessionSettled: true,
      });
      const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "failed");
      assert.isFalse(yield* fake.runtime.hasPendingBackgroundWork!);
      yield* start(fake.runtime, fake.thread, 2);
      assert.equal((yield* fake.takeCommand("prompt")).id, "t3-turn:attempt-2");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("keeps slash commands intact and settles commands handled without an agent turn", () =>
    Effect.gen(function* () {
      const fake = yield* setup();
      yield* start(fake.runtime, fake.thread, 1, "/help", [
        { key: "host", text: "T3 host context" },
      ]);
      const prompt = yield* fake.takeCommand("prompt");
      assert.equal(prompt.message, "/help");
      yield* fake.emit({
        type: "extension_ui_request",
        id: "help",
        method: "notify",
        message: "Native command help",
        notifyType: "info",
      });
      yield* fake.takeEvent(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.type === "system_notice" &&
          event.turnItem.message === "Native command help",
      );
      yield* fake.respond(prompt, { agentInvoked: false });
      const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
      assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      assert.isFalse(yield* fake.runtime.hasPendingBackgroundWork!);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "releases turn ownership when an oversized prompt is rejected before writing to the provider",
    () =>
      Effect.gen(function* () {
        const fake = yield* setup();
        const rejected = yield* start(fake.runtime, fake.thread, 1, "x".repeat(1024 * 1024)).pipe(
          Effect.result,
        );
        assert.isTrue(Result.isFailure(rejected));
        assert.isFalse(yield* fake.runtime.hasPendingBackgroundWork!);
        yield* start(fake.runtime, fake.thread, 2, "A bounded retry");
        const prompt = yield* fake.takeCommand("prompt");
        assert.equal(prompt.id, "t3-turn:attempt-2");
        assert.equal(prompt.message, "A bounded retry");
        yield* fake.respond(prompt, { agentInvoked: false });
        const terminal = yield* fake.takeEvent((event) => event.type === "turn.terminal");
        assert.isTrue(terminal.type === "turn.terminal" && terminal.status === "completed");
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect(
    "resumes the persisted native session path and reads history without claiming rollback support",
    () =>
      Effect.gen(function* () {
        const fake = yield* setup();
        const resumed = yield* fake.runtime.resumeThread({
          providerThread: {
            ...fake.thread,
            nativeThreadRef: {
              driver: fake.thread.driver,
              nativeId: "/saved/history.jsonl",
              strength: "strong",
            },
          },
        });
        const command = yield* fake.takeCommand("switch_session");
        assert.equal(command.sessionPath, "/saved/history.jsonl");
        assert.equal(resumed.nativeThreadRef?.nativeId, "/saved/history.jsonl");
        const snapshot = yield* fake.runtime.readThreadSnapshot({ providerThread: resumed });
        assert.deepEqual(
          snapshot.messages.map((message) => message.text),
          ["restored user", "restored assistant"],
        );
        assert.isFalse(fake.runtime.providerSession.capabilities.threads.canRollbackThread);
      }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
