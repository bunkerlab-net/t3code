// @effect-diagnostics preferSchemaOverJson:off -- MCP schemas and tool results are open protocol records.
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import { t3OrchestrationSystemPrompt } from "@t3tools/provider-core/server/orchestrationInstructions";
import { discardResponseBody, responsePayloads } from "@t3tools/shared/mcpResponsePayloads";

import {
  ompRecordField,
  ompRecordString,
  type OmpRpcConnection,
  type OmpRpcRecord,
} from "./rpc.ts";

const TOOL_PREFIX = "mcp__t3_code__";
const ESSENTIAL_TOOLS = new Set(["orchestrator_capabilities", "delegate_task", "task_status"]);

class OmpMcpBridgeError extends Schema.TaggedError<OmpMcpBridgeError>()("OmpMcpBridgeError", {
  operation: Schema.Literals([
    "initialize",
    "notifications/initialized",
    "tools/list",
    "tools/call",
    "disconnect",
  ]),
  reason: Schema.Literals([
    "transport",
    "http-status",
    "missing-response",
    "request-rejected",
    "missing-protocol-version",
    "invalid-catalog",
    "invalid-tool-name",
    "duplicate-tools",
    "repeated-cursor",
    "unknown-tool",
  ]),
  status: Schema.optional(Schema.Number),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    switch (this.reason) {
      case "transport":
        return "T3 Code MCP transport failed.";
      case "http-status":
        return `T3 Code MCP returned HTTP ${this.status}.`;
      case "missing-response":
        return "T3 Code MCP returned no matching response.";
      case "request-rejected":
        return `T3 Code MCP rejected ${this.operation}.`;
      case "missing-protocol-version":
        return "T3 Code MCP did not negotiate a protocol version.";
      case "invalid-catalog":
        return "T3 Code MCP returned an invalid tool catalog.";
      case "invalid-tool-name":
        return "T3 Code MCP returned an invalid tool name.";
      case "duplicate-tools":
        return "T3 Code MCP returned duplicate tools.";
      case "repeated-cursor":
        return "T3 Code MCP repeated a catalog cursor.";
      case "unknown-tool":
        return "Unknown T3 Code host tool.";
    }
  }
}

interface OmpMcpBridgeOptions {
  readonly config: McpProviderSessionConfig;
  readonly connection: Pick<OmpRpcConnection, "request" | "send">;
  readonly fetchImplementation?: (url: string, init?: RequestInit) => Promise<Response>;
}

function modelResult(value: unknown, name: string) {
  const blocks = ompRecordField(value, "content");
  const content: Array<OmpRpcRecord> = [];
  if (Array.isArray(blocks)) {
    for (const block of blocks) {
      if (ompRecordString(block, "type") === "text") {
        const text = ompRecordString(block, "text");
        if (text !== undefined) content.push({ type: "text", text });
      } else if (ompRecordString(block, "type") === "image") {
        const data = ompRecordString(block, "data");
        const mimeType = ompRecordString(block, "mimeType");
        if (data !== undefined && mimeType !== undefined)
          content.push({ type: "image", data, mimeType });
      }
    }
  }
  const structuredContent = ompRecordField(value, "structuredContent");
  if (content.length === 0) {
    content.push({ type: "text", text: JSON.stringify(structuredContent ?? value ?? null) });
  }
  return {
    content,
    details: {
      server: "t3-code",
      tool: name,
      ...(structuredContent === undefined ? {} : { structuredContent }),
    },
  };
}

export const makeOmpMcpBridge = Effect.fn("OmpMcpBridge.make")(function* (
  options: OmpMcpBridgeOptions,
) {
  const scope = yield* Effect.scope;
  const fetchImplementation = options.fetchImplementation ?? fetch;
  const calls = new Map<string, Fiber.Fiber<void>>();
  const names = new Map<string, string>();
  let sessionId: string | undefined;
  let protocolVersion: string | undefined;
  let nextId = 0;

  const headers = () => ({
    authorization: options.config.authorizationHeader,
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
    ...(sessionId === undefined ? {} : { "mcp-session-id": sessionId }),
    ...(protocolVersion === undefined ? {} : { "mcp-protocol-version": protocolVersion }),
  });

  const request = Effect.fn("OmpMcpBridge.request")(function* (
    method: OmpMcpBridgeError["operation"],
    params: unknown,
    notification = false,
  ) {
    const id = ++nextId;
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        fetchImplementation(options.config.endpoint, {
          method: "POST",
          signal,
          headers: headers(),
          body: JSON.stringify({ jsonrpc: "2.0", ...(notification ? {} : { id }), method, params }),
        }),
      catch: (cause) => new OmpMcpBridgeError({ operation: method, reason: "transport", cause }),
    });
    sessionId = response.headers.get("mcp-session-id") ?? sessionId;
    if (!response.ok) {
      yield* discardResponseBody(response);
      return yield* new OmpMcpBridgeError({
        operation: method,
        reason: "http-status",
        status: response.status,
      });
    }
    if (notification) {
      yield* discardResponseBody(response);
      return undefined;
    }
    const payload = yield* responsePayloads(
      response,
      (cause) => new OmpMcpBridgeError({ operation: method, reason: "transport", cause }),
    ).pipe(
      Stream.filter((frame) => ompRecordField(frame, "id") === id),
      Stream.take(1),
      Stream.runCollect,
      Effect.map((frames) => frames[0]),
    );
    if (payload === undefined)
      return yield* new OmpMcpBridgeError({ operation: method, reason: "missing-response" });
    if (ompRecordField(payload, "error") !== undefined) {
      return yield* new OmpMcpBridgeError({
        operation: method,
        reason: "request-rejected",
        cause: ompRecordField(payload, "error"),
      });
    }
    return ompRecordField(payload, "result");
  });

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      yield* Effect.forEach([...calls.values()], Fiber.interrupt, { discard: true });
      calls.clear();
      if (sessionId === undefined) return;
      yield* Effect.tryPromise({
        try: (signal) =>
          fetchImplementation(options.config.endpoint, {
            method: "DELETE",
            headers: headers(),
            signal,
          }),
        catch: (cause) =>
          new OmpMcpBridgeError({ operation: "disconnect", reason: "transport", cause }),
      }).pipe(Effect.flatMap(discardResponseBody), Effect.timeout("5 seconds"), Effect.ignore);
    }),
  );

  const initialized = yield* request("initialize", {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "t3-omp-mcp", version: "1.0.0" },
  }).pipe(Effect.timeout("10 seconds"));
  protocolVersion = ompRecordString(initialized, "protocolVersion");
  if (protocolVersion === undefined)
    return yield* new OmpMcpBridgeError({
      operation: "initialize",
      reason: "missing-protocol-version",
    });
  yield* request("notifications/initialized", {}, true).pipe(Effect.timeout("10 seconds"));

  const tools: Array<OmpRpcRecord> = [];
  const cursors = new Set<string>();
  let cursor: string | undefined;
  do {
    const page = yield* request("tools/list", cursor === undefined ? {} : { cursor }).pipe(
      Effect.timeout("10 seconds"),
    );
    const catalog = ompRecordField(page, "tools");
    if (!Array.isArray(catalog))
      return yield* new OmpMcpBridgeError({ operation: "tools/list", reason: "invalid-catalog" });
    for (const tool of catalog) {
      const name = ompRecordString(tool, "name");
      if (
        name === undefined ||
        !/^[a-zA-Z0-9_-]+$/.test(name) ||
        TOOL_PREFIX.length + name.length > 64
      ) {
        return yield* new OmpMcpBridgeError({
          operation: "tools/list",
          reason: "invalid-tool-name",
        });
      }
      const hostName = `${TOOL_PREFIX}${name}`;
      if (names.has(hostName))
        return yield* new OmpMcpBridgeError({ operation: "tools/list", reason: "duplicate-tools" });
      names.set(hostName, name);
      const schema = ompRecordField(tool, "inputSchema");
      const parameters =
        typeof schema === "object" && schema !== null && !Array.isArray(schema) ? schema : {};
      tools.push({
        name: hostName,
        label: name,
        description: ompRecordString(tool, "description") ?? name,
        parameters: { type: "object", properties: {}, ...parameters },
        loadMode: ESSENTIAL_TOOLS.has(name) ? "essential" : "discoverable",
      });
    }
    cursor = ompRecordString(page, "nextCursor");
    if (cursor !== undefined) {
      if (cursors.has(cursor))
        return yield* new OmpMcpBridgeError({ operation: "tools/list", reason: "repeated-cursor" });
      cursors.add(cursor);
    }
  } while (cursor !== undefined);
  yield* options.connection.request({ type: "set_host_tools", tools });

  const handleEvent = Effect.fn("OmpMcpBridge.handleEvent")(function* (event: OmpRpcRecord) {
    if (event.type === "host_tool_cancel") {
      const targetId = ompRecordString(event, "targetId");
      const call = targetId === undefined ? undefined : calls.get(targetId);
      if (call !== undefined) yield* Fiber.interrupt(call);
      return true;
    }
    if (event.type !== "host_tool_call") return false;
    const id = ompRecordString(event, "id");
    if (id === undefined || calls.has(id)) return true;
    const name = names.get(ompRecordString(event, "toolName") ?? "");
    const execute =
      name === undefined
        ? Effect.fail(new OmpMcpBridgeError({ operation: "tools/call", reason: "unknown-tool" }))
        : request("tools/call", { name, arguments: ompRecordField(event, "arguments") ?? {} });
    const started = yield* Deferred.make<void>();
    const fiber = yield* Deferred.await(started).pipe(
      Effect.andThen(execute),
      Effect.matchEffect({
        onFailure: (error) =>
          options.connection.send({
            type: "host_tool_result",
            id,
            isError: true,
            result: { content: [{ type: "text", text: error.message }] },
          }),
        onSuccess: (result) =>
          options.connection
            .send({
              type: "host_tool_result",
              id,
              ...(ompRecordField(result, "isError") === true ? { isError: true } : {}),
              result: modelResult(result, name ?? "unknown"),
            })
            .pipe(
              Effect.catchTags({
                OmpRpcError: () =>
                  options.connection.send({
                    type: "host_tool_result",
                    id,
                    isError: true,
                    result: {
                      content: [
                        {
                          type: "text",
                          text: "T3 Code MCP tool finished, but its result could not be delivered to OMP.",
                        },
                      ],
                    },
                  }),
              }),
            ),
      }),
      Effect.catch(() => Effect.logWarning("Could not deliver an OMP host-tool result.")),
      Effect.ensuring(Effect.sync(() => calls.delete(id))),
      Effect.forkIn(scope),
    );
    calls.set(id, fiber);
    yield* Deferred.succeed(started, undefined);
    return true;
  });

  const instructions = [
    t3OrchestrationSystemPrompt(true, options.config.capabilities?.has("device") === true),
    ompRecordString(initialized, "instructions"),
  ]
    .filter((value) => value !== undefined)
    .join("\n\n");
  return { handleEvent, instructions };
});
