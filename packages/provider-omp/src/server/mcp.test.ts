import { describe, expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";

import { makeOmpMcpBridge } from "./mcp.ts";
import { OmpRpcError, ompRecordField, type OmpRpcRecord } from "./rpc.ts";

const config = {
  environmentId: EnvironmentId.make("omp-mcp-test"),
  threadId: ThreadId.make("omp-thread-test"),
  providerSessionId: "omp-session-test",
  providerInstanceId: ProviderInstanceId.make("omp"),
  endpoint: "http://127.0.0.1:1/mcp",
  authorizationHeader: "Bearer test-secret",
  browserToolsAvailable: true,
};

function response(id: unknown, result: unknown, sse = false) {
  const body = JSON.stringify({ jsonrpc: "2.0", id, result });
  return new Response(
    sse ? `data: {"jsonrpc":"2.0","method":"notifications/progress"}\n\ndata: ${body}\n\n` : body,
    {
      headers: {
        "content-type": sse ? "text/event-stream" : "application/json",
        "mcp-session-id": "mcp-session",
      },
    },
  );
}

const fixture = Effect.fn("test.ompMcpFixture")(function* (
  options: {
    readonly call?: (params: unknown, signal: AbortSignal) => Promise<unknown>;
    readonly catalog?: unknown;
    readonly initializeFailure?: Error;
    readonly maxOutgoingBytes?: number;
  } = {},
) {
  const frames = yield* Queue.unbounded<OmpRpcRecord>();
  const registrations: OmpRpcRecord[] = [];
  const deliveryAttempts: OmpRpcRecord[] = [];
  const requests: Array<{ method: string; headers: Headers; body: OmpRpcRecord | null }> = [];
  const bridge = yield* makeOmpMcpBridge({
    config,
    connection: {
      request: (frame) =>
        Effect.sync(() => {
          registrations.push(frame);
          return {};
        }),
      send: (frame) =>
        Effect.gen(function* () {
          deliveryAttempts.push(frame);
          if (
            options.maxOutgoingBytes !== undefined &&
            Buffer.byteLength(JSON.stringify(frame)) > options.maxOutgoingBytes
          ) {
            return yield* new OmpRpcError({
              operation: "send",
              detail: "Command exceeds provider frame limit",
            });
          }
          yield* Queue.offer(frames, frame);
        }),
    },
    fetchImplementation: async (_url, init) => {
      const method = init?.method ?? "GET";
      const headers = new Headers(init?.headers);
      const body = typeof init?.body === "string" ? (JSON.parse(init.body) as OmpRpcRecord) : null;
      requests.push({ method, headers, body });
      if (method === "DELETE") return new Response(null, { status: 204 });
      if (body?.method === "notifications/initialized") return new Response(null, { status: 202 });
      if (body?.method === "initialize" && options.initializeFailure !== undefined)
        throw options.initializeFailure;
      if (body?.method === "initialize")
        return response(body.id, {
          protocolVersion: "2025-06-18",
          instructions: "T3 server guidance",
        });
      if (body?.method === "tools/list")
        return response(
          body.id,
          options.catalog ??
            (ompRecordField(body.params, "cursor") === undefined
              ? {
                  tools: [{ name: "orchestrator_capabilities", inputSchema: { type: "object" } }],
                  nextCursor: "page-2",
                }
              : {
                  tools: [
                    {
                      name: "preview_snapshot",
                      inputSchema: { type: "object", properties: { tab: { type: "string" } } },
                    },
                  ],
                }),
        );
      if (body?.method === "tools/call") {
        const result =
          options.call === undefined
            ? { content: [{ type: "text", text: "done" }] }
            : await options.call(body.params, init!.signal!);
        return response(body.id, result, true);
      }
      throw new Error("Unexpected MCP request");
    },
  });
  return { bridge, frames, registrations, requests, deliveryAttempts };
});

describe("OMP native host-tool MCP bridge", () => {
  it.effect(
    "registers a paged catalog and preserves text, images, structured results and tool errors",
    () =>
      Effect.gen(function* () {
        const requests = yield* Effect.scoped(
          Effect.gen(function* () {
            const { bridge, frames, registrations, requests } = yield* fixture({
              call: async () => ({
                content: [
                  { type: "text", text: "inspect this" },
                  { type: "image", data: "aW1hZ2U=", mimeType: "image/png" },
                ],
                structuredContent: { count: 3 },
                _meta: { secret: "not model content" },
                isError: true,
              }),
            });
            expect(registrations).toEqual([
              {
                type: "set_host_tools",
                tools: [
                  {
                    name: "mcp__t3_code__orchestrator_capabilities",
                    label: "orchestrator_capabilities",
                    description: "orchestrator_capabilities",
                    parameters: { type: "object", properties: {} },
                    loadMode: "essential",
                  },
                  {
                    name: "mcp__t3_code__preview_snapshot",
                    label: "preview_snapshot",
                    description: "preview_snapshot",
                    parameters: { type: "object", properties: { tab: { type: "string" } } },
                    loadMode: "discoverable",
                  },
                ],
              },
            ]);
            expect(bridge.instructions).toContain("T3 server guidance");
            yield* bridge.handleEvent({
              type: "host_tool_call",
              id: "host-1",
              toolName: "mcp__t3_code__preview_snapshot",
              arguments: { tab: "tab-1" },
            });
            expect(yield* Queue.take(frames)).toEqual({
              type: "host_tool_result",
              id: "host-1",
              isError: true,
              result: {
                content: [
                  { type: "text", text: "inspect this" },
                  { type: "image", data: "aW1hZ2U=", mimeType: "image/png" },
                ],
                details: {
                  server: "t3-code",
                  tool: "preview_snapshot",
                  structuredContent: { count: 3 },
                },
              },
            });
            expect(
              requests.find((item) => item.body?.method === "tools/call")?.body?.params,
            ).toEqual({ name: "preview_snapshot", arguments: { tab: "tab-1" } });
            expect(requests[0]?.headers.get("mcp-protocol-version")).toBeNull();
            expect(requests[1]?.headers.get("mcp-protocol-version")).toBe("2025-06-18");
            expect(requests[1]?.headers.get("mcp-session-id")).toBe("mcp-session");
            expect(requests[1]?.headers.get("authorization")).toBe("Bearer test-secret");
            return requests;
          }),
        );
        expect(requests.at(-1)?.method).toBe("DELETE");
      }),
  );

  it.effect("cancels one host call without blocking another or sending a cancelled result", () =>
    Effect.gen(function* () {
      const started = Promise.withResolvers<AbortSignal>();
      const { bridge, frames } = yield* fixture({
        call: (params, signal) => {
          if (ompRecordField(params, "name") === "preview_snapshot") {
            started.resolve(signal);
            return new Promise(() => {});
          }
          return Promise.resolve({ structuredContent: { ready: true } });
        },
      });
      yield* bridge.handleEvent({
        type: "host_tool_call",
        id: "slow",
        toolName: "mcp__t3_code__preview_snapshot",
        arguments: {},
      });
      const signal = yield* Effect.promise(() => started.promise);
      yield* bridge.handleEvent({
        type: "host_tool_call",
        id: "fast",
        toolName: "mcp__t3_code__orchestrator_capabilities",
        arguments: {},
      });
      expect(yield* Queue.take(frames)).toMatchObject({
        id: "fast",
        result: { content: [{ type: "text", text: '{"ready":true}' }] },
      });
      yield* bridge.handleEvent({ type: "host_tool_cancel", id: "cancel", targetId: "slow" });
      expect(signal.aborted).toBe(true);
      expect(yield* Queue.size(frames)).toBe(0);
    }).pipe(Effect.scoped),
  );

  it.effect("rejects unknown calls locally and never exposes transport credentials in errors", () =>
    Effect.gen(function* () {
      const { bridge, frames, requests } = yield* fixture({
        call: async () => {
          throw new Error("Bearer test-secret");
        },
      });
      yield* bridge.handleEvent({
        type: "host_tool_call",
        id: "unknown",
        toolName: "bash",
        arguments: {},
      });
      expect(yield* Queue.take(frames)).toMatchObject({ id: "unknown", isError: true });
      expect(requests.some((item) => item.body?.method === "tools/call")).toBe(false);
      yield* bridge.handleEvent({
        type: "host_tool_call",
        id: "failed",
        toolName: "mcp__t3_code__preview_snapshot",
        arguments: {},
      });
      const failed = yield* Queue.take(frames);
      expect(failed).toMatchObject({ id: "failed", isError: true });
      expect(JSON.stringify(failed)).not.toContain("test-secret");
      const cause = new Error("Bearer test-secret");
      const setupError = yield* fixture({ initializeFailure: cause }).pipe(Effect.flip);
      expect(setupError).toMatchObject({
        _tag: "OmpMcpBridgeError",
        operation: "initialize",
        reason: "transport",
        cause,
      });
      expect(setupError.message).not.toContain("test-secret");

      const large = yield* fixture({
        maxOutgoingBytes: 1024 * 1024,
        call: async () => ({
          content: [{ type: "image", data: "a".repeat(1024 * 1024), mimeType: "image/png" }],
        }),
      });
      yield* large.bridge.handleEvent({
        type: "host_tool_call",
        id: "large",
        toolName: "mcp__t3_code__preview_snapshot",
        arguments: {},
      });
      const delivered = yield* Queue.take(large.frames);
      expect(delivered).toMatchObject({ type: "host_tool_result", id: "large", isError: true });
      expect(Buffer.byteLength(JSON.stringify(delivered))).toBeLessThan(1024);
      expect(large.deliveryAttempts).toHaveLength(2);
      expect(large.requests.filter((item) => item.body?.method === "tools/call")).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect("rejects a repeating catalog cursor instead of registering a partial tool set", () =>
    fixture({ catalog: { tools: [], nextCursor: "same" } }).pipe(
      Effect.flip,
      Effect.tap((error) =>
        Effect.sync(() => expect(error.message).toContain("repeated a catalog cursor")),
      ),
      Effect.scoped,
    ),
  );
});
