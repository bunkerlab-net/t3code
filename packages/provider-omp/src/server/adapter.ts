import {
  ProviderDriverKind,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  isProviderNativeImageAttachment,
  providerMessageTextWithAttachmentPaths,
} from "@t3tools/provider-core/server/attachmentPrompt";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import { withAgentDeviceEnvironment } from "@t3tools/provider-core/server/mcpSession";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import { OmpSettings } from "../settings.ts";
import { buildOmpRpcLaunch, resolveOmpLaunchArgs } from "./launch.ts";
import { makeOmpMcpBridge } from "./mcp.ts";
import {
  makeOmpRpcConnection,
  ompRecordField as field,
  ompRecordString as string,
  ompRecordNumber as number,
  parseOmpModelSlug,
  type OmpRpcRecord,
} from "./rpc.ts";
import { ompApprovalChoices, ompQuestions, ompUiResponse } from "./ui.ts";

const OMP_PROVIDER = ProviderDriverKind.make("omp");
const decodeOmpSettings = Schema.decodeSync(OmpSettings);

const CAPABILITIES = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: true,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

interface StreamItem {
  readonly id: string;
  readonly kind: "assistant_message" | "reasoning";
  readonly startedAt: DateTime.Utc;
  text: string;
  completed: boolean;
  flushScheduled: boolean;
}
interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly promptId: string;
  readonly ordinals: Map<string, number>;
  readonly streams: Map<string, StreamItem>;
  readonly steerIds: Set<string>;
  readonly tools: Map<
    string,
    {
      readonly args: unknown;
      readonly name: string;
      readonly startedAt: DateTime.Utc;
      item?: OrchestrationV2TurnItem;
    }
  >;
  messageId: string;
  messageOrdinal: number;
  nextOrdinal: number;
  interrupted: boolean;
  promptCompleted: boolean;
  failure: OrchestrationV2ProviderFailure | null;
}
interface PendingUi {
  readonly event: OmpRpcRecord;
  readonly request: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly item: OrchestrationV2TurnItem;
}
function nativeRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: OMP_PROVIDER, nativeId, strength };
}
function contentText(content: unknown): string {
  return typeof content === "string"
    ? content
    : Array.isArray(content)
      ? content
          .map((block) => (string(block, "type") === "text" ? (string(block, "text") ?? "") : ""))
          .join("")
      : "";
}

export interface OmpAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: OmpSettings;
  readonly environment: NodeJS.ProcessEnv;
}

export const makeOmpAdapterV2 = Effect.fn("makeOmpAdapterV2")(function* (
  options: OmpAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fs = yield* FileSystem.FileSystem;
  const host = yield* ProviderHost.ProviderHost;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const agentScope = yield* AgentScope;
  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: OMP_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: OMP_PROVIDER,
    getCapabilities: Effect.fn("OmpAdapter.getCapabilities")(function () {
      return Effect.succeed(CAPABILITIES);
    }),
    planSelectionTransition: Effect.fn("OmpAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("OmpAdapter.openSession")(
      function* (input) {
        const scope = yield* Effect.scope;
        const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
        const mcpConfig = yield* mcpSessions.read(input.threadId);
        const resolved = resolveOmpLaunchArgs(options.settings.launchArgs);
        if (!resolved.ok) return yield* protocolError(resolved.message);
        const launch = buildOmpRpcLaunch({
          launchArgs: resolved.args,
          environment: withAgentDeviceEnvironment(options.environment, mcpConfig),
          runtimeMode: input.runtimePolicy.runtimeMode,
        });
        const wrapped = yield* agentScope.wrap({
          command: options.settings.binaryPath || "omp",
          args: launch.args,
          env: launch.env,
          name: "omp",
          threadId: input.threadId,
        });
        const stopScope =
          wrapped.unit === undefined
            ? Effect.void
            : yield* Effect.cached(agentScope.stop(wrapped.unit));
        yield* Effect.addFinalizer(() => stopScope);
        const connection = yield* makeOmpRpcConnection({
          command: wrapped.command,
          args: wrapped.args,
          env: launch.env,
          cwd,
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
        yield* connection.exited.pipe(
          Effect.matchEffect({ onFailure: () => stopScope, onSuccess: () => stopScope }),
          Effect.forkIn(scope),
        );
        const now = yield* DateTime.now;
        let session: OrchestrationV2ProviderSession = {
          id: input.providerSessionId,
          driver: OMP_PROVIDER,
          providerInstanceId: options.instanceId,
          status: "starting",
          cwd,
          model: input.modelSelection.model,
          capabilities: CAPABILITIES,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        };
        const events = yield* Queue.unbounded<
          ProviderAdapter.ProviderAdapterV2Event,
          ProviderAdapter.ProviderAdapterV2Error | Cause.Done
        >();
        const permit = yield* Semaphore.make(1);
        const pending = new Map<string, PendingUi>();
        let thread: OrchestrationV2ProviderThread | null = null;
        let active: ActiveTurn | null = null;
        let closed = false;
        let settled = true;
        let baselineModel: { provider: string; modelId: string } | null = null;
        let baselineThinking: string | undefined;
        let appliedModel: string | null = null;
        let modelContextWindow: number | undefined;
        let handleMcp: (event: OmpRpcRecord) => Effect.Effect<void> = () => Effect.void;
        let mcpInstructions = "";
        let setupWarning: string | undefined;
        const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);
        const request = (record: OmpRpcRecord) =>
          connection
            .request(record)
            .pipe(Effect.mapError((cause) => protocolError("Oh My Pi RPC request failed", cause)));
        const updateSession = Effect.fn("OmpAdapter.updateSession")(function* (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = null,
        ) {
          session = { ...session, status, lastError, updatedAt: yield* DateTime.now };
          yield* emit({
            type: "provider_session.updated",
            driver: OMP_PROVIDER,
            providerSession: session,
          });
        });
        const updateThread = Effect.fn("OmpAdapter.updateThread")(function* (
          patch: Partial<OrchestrationV2ProviderThread>,
        ) {
          if (thread === null) return;
          thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
          yield* emit({
            type: "provider_thread.updated",
            driver: OMP_PROVIDER,
            providerThread: thread,
          });
        });
        const ordinal = (turn: ActiveTurn, id: string) => {
          const existing = turn.ordinals.get(id);
          if (existing !== undefined) return existing;
          const value = turn.nextOrdinal++;
          turn.ordinals.set(id, value);
          return value;
        };
        const baseItem = (
          turn: ActiveTurn,
          id: string,
          startedAt: DateTime.Utc,
          updatedAt: DateTime.Utc,
        ) => ({
          id: ids.derive.turnItemFromProviderItem({ driver: OMP_PROVIDER, nativeItemId: id }),
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          nodeId: ids.derive.nodeFromProviderItem({ driver: OMP_PROVIDER, nativeItemId: id }),
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: nativeRef(id),
          parentItemId: null,
          ordinal: ordinal(turn, id),
          startedAt,
          updatedAt,
        });
        // Streaming records share the event-pump span rather than creating spans per token.
        const emitNode = Effect.fnUntraced(function* (
          turn: ActiveTurn,
          item: OrchestrationV2TurnItem,
          kind: OrchestrationV2ExecutionNode["kind"],
        ) {
          yield* emit({
            type: "node.updated",
            driver: OMP_PROVIDER,
            node: {
              id: item.nodeId!,
              threadId: item.threadId,
              runId: item.runId,
              parentNodeId: turn.input.rootNodeId,
              rootNodeId: turn.input.rootNodeId,
              kind,
              status: item.status,
              countsForRun: false,
              providerThreadId: item.providerThreadId ?? null,
              providerTurnId: item.providerTurnId ?? null,
              nativeItemRef: item.nativeItemRef ?? null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: item.startedAt,
              completedAt: item.completedAt,
            },
          });
        });
        // Streaming records share the event-pump span rather than creating spans per token.
        const emitStream = Effect.fnUntraced(function* (turn: ActiveTurn, item: StreamItem) {
          if (!item.text) return;
          const at = yield* DateTime.now;
          const base = {
            ...baseItem(turn, item.id, item.startedAt, at),
            status: item.completed ? ("completed" as const) : ("running" as const),
            title: null,
            completedAt: item.completed ? at : null,
          };
          const messageId = ids.derive.messageFromProviderItem({
            driver: OMP_PROVIDER,
            nativeItemId: item.id,
          });
          const turnItem: OrchestrationV2TurnItem =
            item.kind === "assistant_message"
              ? {
                  ...base,
                  type: "assistant_message",
                  messageId,
                  text: item.text,
                  streaming: !item.completed,
                }
              : { ...base, type: "reasoning", text: item.text, streaming: !item.completed };
          yield* emitNode(turn, turnItem, item.kind);
          yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem });
          if (item.kind === "assistant_message")
            yield* emit({
              type: "message.updated",
              driver: OMP_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.input.threadId,
                runId: turn.input.runId,
                nodeId: base.nodeId,
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming: !item.completed,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: at,
              },
            });
        });
        // Streaming records share the event-pump span rather than creating spans per token.
        const streamItem = Effect.fnUntraced(function* (
          turn: ActiveTurn,
          kind: StreamItem["kind"],
          index: number,
        ) {
          const id = `${turn.providerTurn.id}:${turn.messageId}:c${index}`;
          let item = turn.streams.get(id);
          if (item === undefined) {
            item = {
              id,
              kind,
              text: "",
              startedAt: yield* DateTime.now,
              completed: false,
              flushScheduled: false,
            };
            turn.streams.set(id, item);
            ordinal(turn, id);
          }
          return item;
        });
        const resolveUi = Effect.fn("OmpAdapter.resolveUi")(function* (
          ui: PendingUi,
          status: "resolved" | "cancelled",
        ) {
          pending.delete(String(ui.request.id));
          const at = yield* DateTime.now;
          const itemStatus = status === "resolved" ? "completed" : "cancelled";
          yield* emit({
            type: "runtime_request.updated",
            driver: OMP_PROVIDER,
            threadId: ui.node.threadId,
            runtimeRequest: { ...ui.request, status, resolvedAt: at },
          });
          yield* emit({
            type: "node.updated",
            driver: OMP_PROVIDER,
            node: { ...ui.node, status: itemStatus, completedAt: at },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: OMP_PROVIDER,
            turnItem: { ...ui.item, status: itemStatus, completedAt: at, updatedAt: at },
          });
        });
        const cancelUi = Effect.fn("OmpAdapter.cancelUi")(function* () {
          for (const ui of pending.values()) {
            yield* connection
              .send({ type: "extension_ui_response", id: string(ui.event, "id"), cancelled: true })
              .pipe(Effect.ignore);
            yield* resolveUi(ui, "cancelled");
          }
        });
        const finish = Effect.fn("OmpAdapter.finish")(function* (broken = false) {
          if (broken) settled = true;
          const turn = active;
          if (turn === null) return;
          active = null;
          const at = yield* DateTime.now;
          const status = turn.interrupted ? "interrupted" : turn.failure ? "failed" : "completed";
          for (const item of turn.streams.values()) {
            if (item.completed) continue;
            item.completed = true;
            yield* emitStream(turn, item);
          }
          for (const tool of turn.tools.values()) {
            if (tool.item?.completedAt !== null) continue;
            const item: OrchestrationV2TurnItem = {
              ...tool.item,
              status: status === "completed" ? "cancelled" : status,
              completedAt: at,
              updatedAt: at,
            };
            yield* emitNode(turn, item, "tool_call");
            yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
          }
          yield* cancelUi();
          const finalState = broken
            ? undefined
            : yield* connection
                .request({ type: "get_state" }, 2_000)
                .pipe(Effect.orElseSucceed(() => undefined));
          const usage = field(finalState, "contextUsage");
          const usedTokens = number(usage, "tokens");
          const maxTokens = number(usage, "contextWindow");
          yield* emit({
            type: "provider_turn.updated",
            driver: OMP_PROVIDER,
            threadId: turn.input.threadId,
            providerTurn: {
              ...turn.providerTurn,
              status,
              completedAt: at,
              ...(usedTokens !== undefined &&
              usedTokens >= 0 &&
              maxTokens !== undefined &&
              maxTokens > 0
                ? {
                    tokenUsage: {
                      usedTokens: Math.trunc(usedTokens),
                      maxTokens: Math.trunc(maxTokens),
                      updatedAt: DateTime.formatIso(at),
                    },
                  }
                : {}),
            },
          });
          yield* updateThread({ status: "idle" });
          yield* updateSession(broken ? "error" : "ready", turn.failure?.message ?? null);
          if (turn.failure !== null && !turn.interrupted) {
            const failureId = `${turn.promptId}:failure`;
            const item: OrchestrationV2TurnItem = {
              ...baseItem(turn, failureId, at, at),
              type: "error",
              title: null,
              status: "failed",
              completedAt: at,
              failure: turn.failure,
            };
            yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
            yield* emit({
              type: "turn.terminal",
              driver: OMP_PROVIDER,
              providerThreadId: turn.input.providerThread.id,
              providerTurnId: turn.providerTurn.id,
              runOrdinal: turn.input.runOrdinal,
              status: "failed",
              failure: turn.failure,
              failureItemOrdinal: ordinal(turn, failureId),
              threadDisposition: broken ? "broken" : "reusable",
            });
          } else {
            yield* emit({
              type: "turn.terminal",
              driver: OMP_PROVIDER,
              providerThreadId: turn.input.providerThread.id,
              providerTurnId: turn.providerTurn.id,
              runOrdinal: turn.input.runOrdinal,
              status: turn.interrupted ? "interrupted" : "completed",
              failure: null,
              threadDisposition: broken ? "broken" : "reusable",
            });
          }
        });
        const handleUi = Effect.fn("OmpAdapter.handleUi")(function* (event: OmpRpcRecord) {
          const method = string(event, "method");
          if (method === "cancel") {
            const target = [...pending.values()].find(
              (ui) => string(ui.event, "id") === string(event, "targetId"),
            );
            if (target !== undefined) yield* resolveUi(target, "cancelled");
            return;
          }
          const nativeId = string(event, "id");
          if (
            nativeId === undefined ||
            !["select", "confirm", "input", "editor", "ask"].includes(method ?? "")
          )
            return;
          // The shared question UI cannot mask secrets; never collect one as a normal answer.
          if (event.secret === true) {
            yield* connection.send({
              type: "extension_ui_response",
              id: nativeId,
              cancelled: true,
            });
            return;
          }
          const turn = active;
          const requestId = yield* ids.allocate.runtimeRequest({
            driver: OMP_PROVIDER,
            ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
            nativeRequestId: nativeId,
          });
          const at = yield* DateTime.now;
          const nodeId = ids.derive.approvalNode({ requestId });
          const threadId = turn?.input.threadId ?? thread?.appThreadId ?? input.threadId;
          const choices = ompApprovalChoices(event);
          const approval = choices.size > 0;
          const runtimeRequest: OrchestrationV2RuntimeRequest = {
            id: requestId,
            nodeId,
            providerTurnId: turn?.providerTurn.id ?? null,
            nativeRequestRef: nativeRef(nativeId),
            kind: approval ? "permission" : "user_input",
            status: "pending",
            responseCapability: { type: "live", providerSessionId: input.providerSessionId },
            createdAt: at,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: turn?.input.runId ?? null,
            parentNodeId: turn?.input.rootNodeId ?? null,
            rootNodeId: turn?.input.rootNodeId ?? nodeId,
            kind: approval ? "approval_request" : "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId: thread?.id ?? null,
            providerTurnId: turn?.providerTurn.id ?? null,
            nativeItemRef: nativeRef(nativeId),
            runtimeRequestId: requestId,
            checkpointScopeId: null,
            startedAt: at,
            completedAt: null,
          };
          const base = {
            id: ids.derive.approvalTurnItem({ requestId }),
            threadId,
            runId: node.runId,
            nodeId,
            providerThreadId: node.providerThreadId,
            providerTurnId: node.providerTurnId,
            nativeItemRef: nativeRef(nativeId),
            parentItemId: null,
            ordinal: turn === null ? 0 : ordinal(turn, nativeId),
            status: "waiting" as const,
            title: approval
              ? string(event, "title")?.split("\n")[0] || "Oh My Pi approval"
              : (string(event, "title") ?? "Oh My Pi question"),
            startedAt: at,
            completedAt: null,
            updatedAt: at,
          };
          const item: OrchestrationV2TurnItem = approval
            ? {
                ...base,
                type: "approval_request",
                requestId,
                requestKind: "permission",
                prompt: [string(event, "title"), string(event, "message")]
                  .filter(Boolean)
                  .join("\n"),
                options: [...choices].map(([decision, label]) => ({ decision, label })),
              }
            : { ...base, type: "user_input_request", requestId, questions: ompQuestions(event) };
          pending.set(String(requestId), { event, request: runtimeRequest, node, item });
          yield* emit({
            type: "runtime_request.updated",
            driver: OMP_PROVIDER,
            threadId,
            runtimeRequest,
          });
          yield* emit({ type: "node.updated", driver: OMP_PROVIDER, node });
          yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
          yield* updateSession("waiting");
        });
        // Native event records share the stream's enclosing span.
        const handleEvent = Effect.fnUntraced(function* (event: OmpRpcRecord) {
          // Native tool-roster info is routine mount chatter, not a chat warning.
          // OMP separately informs the model; real warnings/errors still surface.
          if (event.type === "notice" && event.level === "info" && event.source === "xdev") return;
          if (
            event.type === "notice" ||
            (event.type === "extension_ui_request" && event.method === "notify")
          ) {
            const message = string(event, "message");
            if (message === undefined) return;
            const turn = active;
            if (turn === null) {
              setupWarning = message.slice(0, 4096);
              return;
            }
            const at = yield* DateTime.now;
            const id = `${turn.promptId}:notice:${turn.nextOrdinal}`;
            yield* emit({
              type: "turn_item.updated",
              driver: OMP_PROVIDER,
              turnItem: {
                ...baseItem(turn, id, at, at),
                type: "system_notice",
                title: null,
                status: "completed",
                completedAt: at,
                message: message.slice(0, 4096),
              },
            });
            if (event.source === "session-persistence" && event.level === "error") {
              turn.failure = makeProviderFailure({
                message:
                  "Oh My Pi could not save this conversation. Check its storage before continuing.",
                class: "provider_error",
              });
            }
            return;
          }
          if (event.type === "extension_ui_request") return yield* handleUi(event);
          if (event.type === "host_tool_call" || event.type === "host_tool_cancel")
            return yield* handleMcp(event);
          const turn = active;
          if (event.type === "agent_start") {
            settled = false;
            if (turn === null) {
              yield* updateSession("error", "Oh My Pi started work outside an active T3 turn.");
              yield* connection.terminate;
              return;
            }
            yield* updateSession("running");
          }
          if (turn === null) return;
          switch (event.type) {
            case "message_start":
              if (string(event.message, "role") === "assistant") {
                turn.messageOrdinal++;
                turn.messageId = string(event, "messageId") ?? `m${turn.messageOrdinal}`;
              }
              return;
            case "message_update": {
              const delta = event.assistantMessageEvent;
              const type = string(delta, "type");
              if (
                !["text_delta", "thinking_delta", "text_end", "thinking_end"].includes(type ?? "")
              )
                return;
              const item = yield* streamItem(
                turn,
                type?.startsWith("text") ? "assistant_message" : "reasoning",
                number(delta, "contentIndex") ?? 0,
              );
              if (type?.endsWith("_end")) {
                item.text = string(delta, "content") ?? string(delta, "thinking") ?? item.text;
                item.completed = true;
                yield* emitStream(turn, item);
              } else {
                item.text += string(delta, "delta") ?? "";
                if (!item.flushScheduled) {
                  item.flushScheduled = true;
                  yield* Effect.sleep(50).pipe(
                    Effect.andThen(
                      permit.withPermits(1)(
                        Effect.suspend(() => {
                          item.flushScheduled = false;
                          return active !== turn || item.completed
                            ? Effect.void
                            : emitStream(turn, item);
                        }),
                      ),
                    ),
                    Effect.forkIn(scope),
                  );
                }
              }
              return;
            }
            case "message_end": {
              if (string(event.message, "role") !== "assistant") return;
              const content = field(event.message, "content");
              if (Array.isArray(content))
                for (const [index, block] of content.entries()) {
                  const type = string(block, "type");
                  if (type !== "text" && type !== "thinking") continue;
                  const item = yield* streamItem(
                    turn,
                    type === "text" ? "assistant_message" : "reasoning",
                    index,
                  );
                  item.text = string(block, type) ?? item.text;
                  item.completed = true;
                  yield* emitStream(turn, item);
                }
              return;
            }
            case "tool_execution_start":
            case "tool_execution_update":
            case "tool_execution_end": {
              const nativeId = string(event, "toolCallId");
              if (nativeId === undefined) return;
              const id = `${turn.providerTurn.id}:tool:${nativeId}`;
              const at = yield* DateTime.now;
              const tool = turn.tools.get(id) ?? {
                args: event.args,
                name: string(event, "toolName") ?? "tool",
                startedAt: at,
              };
              const end = event.type === "tool_execution_end";
              const result = end ? event.result : event.partialResult;
              const output = contentText(field(result, "content"));
              const base = {
                ...baseItem(turn, id, tool.startedAt, at),
                title: tool.name,
                status: end
                  ? event.isError === true
                    ? ("failed" as const)
                    : ("completed" as const)
                  : ("running" as const),
                completedAt: end ? at : null,
              };
              const path = string(tool.args, "path") ?? string(tool.args, "file_path");
              const item: OrchestrationV2TurnItem =
                tool.name === "bash"
                  ? {
                      ...base,
                      type: "command_execution",
                      input: string(tool.args, "command") ?? "",
                      ...(output ? { output } : {}),
                    }
                  : (tool.name === "write" || tool.name === "edit") && path !== undefined
                    ? {
                        ...base,
                        type: "file_change",
                        fileName: path,
                        ...(tool.name === "write" && string(tool.args, "content") !== undefined
                          ? { newStr: string(tool.args, "content")! }
                          : {}),
                        ...(string(field(result, "details"), "diff") === undefined
                          ? {}
                          : { diffStr: string(field(result, "details"), "diff")! }),
                      }
                    : {
                        ...base,
                        type: "dynamic_tool",
                        ...mcpToolPresentation({ toolName: tool.name }),
                        toolName: tool.name,
                        input: tool.args ?? {},
                        ...(result === undefined ? {} : { output: result }),
                      };
              tool.item = item;
              turn.tools.set(id, tool);
              yield* emitNode(turn, item, "tool_call");
              yield* emit({ type: "turn_item.updated", driver: OMP_PROVIDER, turnItem: item });
              return;
            }
            case "response": {
              const responseId = string(event, "id");
              if (
                event.command === "steer" &&
                responseId !== undefined &&
                turn.steerIds.delete(responseId)
              ) {
                if (event.success === false) {
                  const at = yield* DateTime.now;
                  yield* emit({
                    type: "turn_item.updated",
                    driver: OMP_PROVIDER,
                    turnItem: {
                      ...baseItem(turn, `${responseId}:rejected`, at, at),
                      type: "system_notice",
                      title: "Steering was rejected",
                      status: "completed",
                      completedAt: at,
                      message: (
                        string(event, "error") ?? "Oh My Pi rejected the steering message."
                      ).slice(0, 4096),
                    },
                  });
                }
                return;
              }
              if (event.id !== turn.promptId || event.command !== "prompt") return;
              if (event.success === false) {
                turn.failure = makeProviderFailure({
                  message: string(event, "error") ?? "Oh My Pi rejected the prompt.",
                  class: "provider_error",
                });
                const state = yield* connection
                  .request({ type: "get_state" }, 2_000)
                  .pipe(Effect.orElseSucceed(() => undefined));
                if (field(state, "isSettled") === true) {
                  settled = true;
                  yield* finish();
                }
              } else if (field(event.data, "agentInvoked") === false) {
                turn.promptCompleted = true;
                settled = true;
                yield* finish();
              }
              return;
            }
            case "prompt_result": {
              if (event.id !== turn.promptId) return;
              turn.promptCompleted = true;
              if (event.status === "aborted") turn.interrupted = true;
              if (event.status === "error") {
                const status = number(event.error, "httpStatus");
                turn.failure = makeProviderFailure({
                  message: string(event.error, "message") ?? "Oh My Pi turn failed.",
                  class:
                    status === 429 || string(event.error, "code") === "cost_cap"
                      ? "usage_limit"
                      : "provider_error",
                });
              }
              settled = event.sessionSettled === true;
              if (settled) yield* finish();
              return;
            }
            case "session_settled":
              if (turn.promptCompleted) {
                settled = true;
                yield* finish();
              }
              return;
            // agent_end is a yield, not quiescence: asynchronous jobs may still wake this turn.
            default:
              return;
          }
        });
        yield* Stream.fromQueue(connection.events).pipe(
          Stream.runForEach((event) => permit.withPermits(1)(handleEvent(event))),
          Effect.catchCause((cause) =>
            permit.withPermits(1)(
              Effect.gen(function* () {
                if (closed) return;
                settled = true;
                if (active !== null) {
                  active.failure = makeProviderFailure({
                    message: "Oh My Pi disconnected before the turn finished.",
                    class: "provider_error",
                    cause,
                  });
                  yield* finish(true);
                } else yield* cancelUi();
                yield* updateSession("error", "Oh My Pi disconnected.");
              }),
            ),
          ),
          Effect.forkIn(scope),
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            closed = true;
          }),
        );
        const initialize = yield* Effect.cached(
          Effect.gen(function* () {
            yield* connection.ready;
            yield* request({ type: "set_ask_dialog", enabled: true });
            if (mcpConfig !== undefined) {
              const bridge = yield* makeOmpMcpBridge({ config: mcpConfig, connection }).pipe(
                Effect.catch((cause) => {
                  setupWarning =
                    "T3 Code tools are unavailable in this Oh My Pi session. Native tools remain available.";
                  return Effect.logWarning("Oh My Pi MCP setup failed", {
                    errorTag: cause._tag,
                  }).pipe(Effect.as(undefined));
                }),
              );
              if (bridge !== undefined) {
                handleMcp = (event) => bridge.handleEvent(event).pipe(Effect.asVoid);
                mcpInstructions = bridge.instructions;
              }
            }
            const initialState = yield* request({ type: "get_state" });
            const initialModel = field(initialState, "model");
            const initialProvider = string(initialModel, "provider");
            const initialModelId = string(initialModel, "id");
            if (initialProvider !== undefined && initialModelId !== undefined)
              baselineModel = { provider: initialProvider, modelId: initialModelId };
            baselineThinking = string(initialState, "thinkingLevel");
            modelContextWindow = number(initialModel, "contextWindow");
            yield* updateSession("ready");
          }).pipe(Effect.provideService(Scope.Scope, scope)),
        );
        const applySelection = Effect.fn("OmpAdapter.applySelection")(function* (
          selection: ModelSelection,
        ) {
          const model =
            selection.model === "default" ? baselineModel : parseOmpModelSlug(selection.model);
          if (selection.model !== "default" && model === null)
            return yield* protocolError("Oh My Pi models use provider/model IDs.");
          if (model !== null && appliedModel !== selection.model) {
            const selected = yield* request({ type: "set_model", ...model });
            modelContextWindow = number(selected, "contextWindow");
            appliedModel = selection.model;
          }
          session = { ...session, model: selection.model };
          const thinking =
            getModelSelectionStringOptionValue(selection, "thinking") ?? baselineThinking;
          if (thinking !== undefined) {
            const available = yield* request({ type: "get_available_thinking_levels" });
            const levels = field(available, "levels");
            if (Array.isArray(levels) && levels.includes(thinking))
              yield* request({ type: "set_thinking_level", level: thinking });
          }
        });
        const ensureThread = Effect.fn("OmpAdapter.ensureThread")(function* (
          threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        ) {
          if (active !== null)
            return yield* protocolError("Cannot switch an active Oh My Pi session.");
          yield* initialize;
          const existing = threadInput.existingProviderThread;
          const path = threadInput.forceNewNativeThread
            ? undefined
            : (existing?.nativeThreadRef?.nativeId ?? input.initialNativeThreadId);
          if (path !== undefined && thread?.nativeThreadRef?.nativeId !== path) {
            const selectedModel =
              threadInput.modelSelection.model === "default"
                ? null
                : parseOmpModelSlug(threadInput.modelSelection.model);
            const response = yield* request({
              type: "switch_session",
              sessionPath: path,
              ...selectedModel,
            });
            if (field(response, "cancelled") === true)
              return yield* protocolError("Oh My Pi cancelled session restoration.");
            appliedModel = null;
          } else if (
            threadInput.forceNewNativeThread ||
            (thread !== null && thread.appThreadId !== threadInput.threadId)
          ) {
            const response = yield* request({ type: "new_session" });
            if (field(response, "cancelled") === true)
              return yield* protocolError("Oh My Pi cancelled session creation.");
            appliedModel = null;
          }
          const state = yield* request({ type: "get_state" });
          modelContextWindow = number(field(state, "model"), "contextWindow");
          const nativeId = string(state, "sessionFile");
          if (nativeId === undefined)
            return yield* protocolError("Oh My Pi did not report a session file.");
          const at = yield* DateTime.now;
          thread =
            existing !== undefined
              ? {
                  ...existing,
                  providerSessionId: input.providerSessionId,
                  nativeThreadRef: nativeRef(nativeId),
                  status: "idle",
                  updatedAt: at,
                }
              : {
                  id: ids.derive.providerThread({ driver: OMP_PROVIDER, nativeThreadId: nativeId }),
                  driver: OMP_PROVIDER,
                  providerInstanceId: options.instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadInput.threadId,
                  ownerNodeId: null,
                  nativeThreadRef: nativeRef(nativeId),
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  pendingBackgroundTasks: [],
                  createdAt: at,
                  updatedAt: at,
                };
          yield* emit({
            type: "provider_thread.updated",
            driver: OMP_PROVIDER,
            providerThread: thread,
          });
          return thread;
        });
        const promptPayload = Effect.fn("OmpAdapter.promptPayload")(function* (
          text: string,
          attachments: ReadonlyArray<ChatAttachment>,
        ) {
          const message = yield* providerMessageTextWithAttachmentPaths({
            text,
            attachments,
            resolveAttachmentPath: host.resolveAttachmentPath,
            resolveExtractedTextPath: host.resolveExtractedTextPath,
          });
          const images: Array<{ type: "image"; mimeType: string; data: string }> = [];
          for (const attachment of attachments) {
            if (!isProviderNativeImageAttachment(attachment)) continue;
            const path = host.resolveAttachmentPath(attachment);
            if (path === null)
              return yield* protocolError("An Oh My Pi image attachment is unavailable.");
            images.push({
              type: "image",
              mimeType: attachment.mimeType,
              data: Buffer.from(yield* fs.readFile(path)).toString("base64"),
            });
          }
          return { message, ...(images.length ? { images } : {}) };
        });
        const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
          instanceId: options.instanceId,
          driver: OMP_PROVIDER,
          providerSessionId: input.providerSessionId,
          get providerSession() {
            return session;
          },
          events: Stream.fromQueue(events),
          hasPendingBackgroundWork: Effect.sync(() => !settled),
          backgroundTasksEndWithProcess: true,
          getModelContextWindow: (selection) =>
            selection.instanceId === options.instanceId && selection.model === appliedModel
              ? modelContextWindow
              : undefined,
          ensureThread: (value) =>
            ensureThread(value).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterEnsureThreadError({
                    driver: OMP_PROVIDER,
                    threadId: value.threadId,
                    cause,
                  }),
              ),
            ),
          resumeThread: (value) =>
            ensureThread({
              threadId: value.threadId ?? value.providerThread.appThreadId ?? input.threadId,
              existingProviderThread: value.providerThread,
              modelSelection: value.modelSelection ?? input.modelSelection,
              runtimePolicy: value.runtimePolicy ?? input.runtimePolicy,
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterResumeThreadError({
                    driver: OMP_PROVIDER,
                    providerSessionId: input.providerSessionId,
                    providerThreadId: value.providerThread.id,
                    cause,
                  }),
              ),
            ),
          startTurn: Effect.fn("OmpAdapter.startTurn")(
            function* (turnInput) {
              if (active !== null || !settled)
                return yield* protocolError("An Oh My Pi turn is already active.");
              if (
                thread === null ||
                thread.nativeThreadRef?.nativeId !==
                  turnInput.providerThread.nativeThreadRef?.nativeId
              )
                return yield* protocolError("The Oh My Pi thread is not bound to this session.");
              yield* applySelection(turnInput.modelSelection);
              const context = [
                mcpInstructions,
                ...(turnInput.appContext?.map((item) => item.text) ?? []),
              ]
                .filter(Boolean)
                .join("\n\n");
              const payload = yield* promptPayload(
                context && !turnInput.message.text.trimStart().startsWith("/")
                  ? `${context}\n\n${turnInput.message.text}`
                  : turnInput.message.text,
                turnInput.message.attachments,
              );
              const at = yield* DateTime.now;
              const promptId = `t3-turn:${turnInput.attemptId}`;
              const providerTurn: OrchestrationV2ProviderTurn = {
                id: ids.derive.providerTurn({ driver: OMP_PROVIDER, nativeTurnId: promptId }),
                providerThreadId: turnInput.providerThread.id,
                nodeId: turnInput.rootNodeId,
                runAttemptId: turnInput.attemptId,
                nativeTurnRef: nativeRef(promptId, "weak"),
                ordinal: turnInput.providerTurnOrdinal,
                status: "running",
                startedAt: at,
                completedAt: null,
              };
              yield* permit.withPermits(1)(
                Effect.gen(function* () {
                  if (active !== null || closed)
                    return yield* protocolError("Oh My Pi turn ownership changed during startup.");
                  thread = turnInput.providerThread;
                  active = {
                    input: turnInput,
                    providerTurn,
                    promptId,
                    ordinals: new Map(),
                    streams: new Map(),
                    steerIds: new Set(),
                    tools: new Map(),
                    messageId: "m0",
                    messageOrdinal: 0,
                    nextOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
                    interrupted: false,
                    promptCompleted: false,
                    failure: null,
                  };
                  settled = false;
                  yield* connection.send({ id: promptId, type: "prompt", ...payload });
                  yield* emit({
                    type: "provider_turn.updated",
                    driver: OMP_PROVIDER,
                    threadId: turnInput.threadId,
                    providerTurn,
                  });
                  yield* updateThread({
                    status: "active",
                    firstRunOrdinal: thread.firstRunOrdinal ?? turnInput.runOrdinal,
                    lastRunOrdinal: turnInput.runOrdinal,
                  });
                  yield* updateSession("running");
                  if (setupWarning !== undefined) {
                    yield* emit({
                      type: "turn_item.updated",
                      driver: OMP_PROVIDER,
                      turnItem: {
                        ...baseItem(active, `${promptId}:mcp-warning`, at, at),
                        type: "system_notice",
                        title: null,
                        status: "completed",
                        completedAt: at,
                        message: setupWarning,
                      },
                    });
                  }
                }).pipe(
                  Effect.onError(() =>
                    Effect.sync(() => {
                      if (active?.promptId === promptId) {
                        active = null;
                        settled = true;
                      }
                    }),
                  ),
                  Effect.uninterruptible,
                ),
              );
            },
            (effect, value) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver: OMP_PROVIDER,
                      threadId: value.threadId,
                      providerThreadId: value.providerThread.id,
                      runId: value.runId,
                      cause,
                    }),
                ),
              ),
          ),
          steerTurn: Effect.fn("OmpAdapter.steerTurn")(
            function* (value) {
              const turn = active;
              if (turn === null || turn.providerTurn.id !== value.providerTurnId)
                return yield* protocolError("The Oh My Pi turn is no longer active.");
              const payload = yield* promptPayload(value.message.text, value.message.attachments);
              yield* permit.withPermits(1)(
                Effect.gen(function* () {
                  if (active !== turn || turn.interrupted)
                    return yield* protocolError(
                      "The Oh My Pi turn ended while preparing steering.",
                    );
                  const id = `${turn.promptId}:steer:${value.message.messageId}`;
                  yield* connection.send({ type: "steer", id, ...payload });
                  turn.steerIds.add(id);
                }).pipe(Effect.uninterruptible),
              );
            },
            (effect, value) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterSteerRunError({
                      driver: OMP_PROVIDER,
                      providerThreadId: value.providerThread.id,
                      providerTurnId: value.providerTurnId,
                      cause,
                    }),
                ),
              ),
          ),
          interruptTurn: Effect.fn("OmpAdapter.interruptTurn")(
            function* (value) {
              const turn = active;
              if (turn === null || turn.providerTurn.id !== value.providerTurnId) return;
              turn.interrupted = true;
              if (value.requestRuntimeRestart) {
                yield* connection.terminate;
                yield* permit.withPermits(1)(finish(true));
                return;
              }
              // Plain abort requeues stranded steering and can start another run; atomically withdraw it.
              yield* request({ type: "abort_and_restore_queue" });
              const state = yield* request({ type: "get_state" });
              if (field(state, "isSettled") === true)
                yield* permit.withPermits(1)(
                  Effect.gen(function* () {
                    if (active !== turn) return;
                    settled = true;
                    yield* finish();
                  }),
                );
            },
            (effect, value) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterInterruptError({
                      driver: OMP_PROVIDER,
                      providerThreadId: value.providerThread.id,
                      providerTurnId: value.providerTurnId,
                      cause,
                    }),
                ),
              ),
          ),
          respondToRuntimeRequest: Effect.fn("OmpAdapter.respondToRuntimeRequest")(
            function* (value) {
              const ui = pending.get(String(value.requestId));
              if (ui === undefined)
                return yield* protocolError("The Oh My Pi question is no longer pending.");
              const response = ompUiResponse(ui.event, value.decision, value.answers);
              if (response === null)
                return yield* protocolError("Oh My Pi did not offer that response.");
              yield* connection.send({
                type: "extension_ui_response",
                id: string(ui.event, "id"),
                ...response,
              });
              yield* resolveUi(ui, response.cancelled === true ? "cancelled" : "resolved");
              yield* updateSession(active === null ? "ready" : "running");
            },
            (effect, value) =>
              effect.pipe(
                permit.withPermits(1),
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                      driver: OMP_PROVIDER,
                      requestId: value.requestId,
                      cause,
                    }),
                ),
              ),
          ),
          readThreadSnapshot: Effect.fn("OmpAdapter.readThreadSnapshot")(
            function* (value) {
              if (
                thread === null ||
                thread.nativeThreadRef?.nativeId !== value.providerThread.nativeThreadRef?.nativeId
              )
                return yield* protocolError("Oh My Pi snapshot requested for another session.");
              const data = yield* request({ type: "get_messages" });
              const raw = field(data, "messages");
              const snapshotThread = thread;
              const messages = (Array.isArray(raw) ? raw : []).flatMap((message, index) => {
                const role = string(message, "role");
                if (role !== "user" && role !== "assistant") return [];
                const text = contentText(field(message, "content"));
                if (!text) return [];
                return [
                  {
                    id: ids.derive.messageFromProviderItem({
                      driver: OMP_PROVIDER,
                      nativeItemId: `${snapshotThread.nativeThreadRef?.nativeId}:history:${index}`,
                    }),
                    threadId: snapshotThread.appThreadId ?? input.threadId,
                    runId: null,
                    nodeId: null,
                    role,
                    text,
                    attachments: [],
                    streaming: false,
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    createdAt: snapshotThread.createdAt,
                    updatedAt: snapshotThread.updatedAt,
                  },
                ];
              });
              return {
                providerThread: snapshotThread,
                providerTurns: [],
                messages,
                runtimeRequests: [],
              };
            },
            (effect, value) =>
              effect.pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                      driver: OMP_PROVIDER,
                      providerThreadId: value.providerThread.id,
                      cause,
                    }),
                ),
              ),
          ),
          rollbackThread: (value) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterRollbackThreadError({
                driver: OMP_PROVIDER,
                providerThreadId: value.providerThread.id,
              }),
            ),
          forkThread: (value) =>
            Effect.fail(
              new ProviderAdapter.ProviderAdapterForkThreadError({
                driver: OMP_PROVIDER,
                providerThreadId: value.sourceProviderThread.id,
              }),
            ),
        };
        return runtime;
      },
      (effect, value) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: OMP_PROVIDER,
                providerSessionId: value.providerSessionId,
                cause,
              }),
          ),
        ),
    ),
  });
});

export type OmpAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;
export const OmpAdapterV2Driver: ProviderAdapterDriver<OmpSettings, OmpAdapterV2DriverEnv> = {
  driverKind: OMP_PROVIDER,
  configSchema: OmpSettings,
  defaultConfig: () => decodeOmpSettings({}),
  create: Effect.fn("OmpAdapterV2Driver.create")(
    function* (input) {
      const environment = yield* HostProcess.Environment;
      return yield* makeOmpAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, environment),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: OMP_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create Oh My Pi adapter.",
              cause,
            }),
        ),
      ),
  ),
};
