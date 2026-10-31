import { assert, describe, it } from "@effect/vitest";
import { makeOmpFrameDecoder } from "./framing.ts";

const encode = (value: unknown) => new TextEncoder().encode(`${JSON.stringify(value)}\n`);
const ready = {
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1, 2],
  maxFrameBytes: 1024 * 1024,
  maxReassembledFrameBytes: 64 * 1024 * 1024,
};

function chunks() {
  const message = { type: "message_end", message: { text: "😀\u2028\u2029".repeat(160_000) } };
  const bytes = Buffer.from(JSON.stringify(message));
  const count = Math.ceil(bytes.byteLength / (256 * 1024));
  return {
    message,
    frames: Array.from({ length: count }, (_, index) => ({
      type: "rpc_chunk",
      chunkId: "stream-1",
      index,
      count,
      byteLength: bytes.byteLength,
      data: bytes.subarray(index * 256 * 1024, (index + 1) * 256 * 1024).toString("base64"),
    })),
  };
}

describe("OMP protocol v2 framing", () => {
  it("reassembles physical splits and lossless UTF-8 chunks without treating Unicode separators as lines", () => {
    const decoder = makeOmpFrameDecoder();
    const { message, frames } = chunks();
    assert.deepEqual(decoder.push(encode(ready)), [ready]);
    const result: Record<string, unknown>[] = [];
    for (const frame of frames) {
      const bytes = encode(frame);
      result.push(...decoder.push(bytes.subarray(0, 13)));
      result.push(...decoder.push(bytes.subarray(13)));
    }
    assert.deepEqual(result, [message]);
    const small = { type: "event", text: "€\u2028\u2029" };
    const bytes = Buffer.from(`${JSON.stringify(small)}\r\n`);
    const split = bytes.indexOf(Buffer.from("€")) + 1;
    assert.deepEqual(decoder.push(bytes.subarray(0, split)), []);
    assert.deepEqual(decoder.push(bytes.subarray(split)), [small]);
    decoder.finish();
  });

  it("rejects interrupted, reordered, inconsistent, oversized and incomplete sequences", () => {
    const { frames } = chunks();
    const first = frames[0]!;
    const second = frames[1]!;
    for (const invalid of [
      { type: "agent_end" },
      { ...second, index: 3 },
      { ...second, chunkId: "other" },
      { ...second, byteLength: second.byteLength + 1 },
      { ...second, count: second.count + 1 },
      { ...second, data: "not base64" },
    ]) {
      const decoder = makeOmpFrameDecoder();
      decoder.push(encode(first));
      assert.throws(() => decoder.push(encode(invalid)));
    }
    for (const invalid of [
      { ...first, index: 1 },
      { ...first, byteLength: 64 * 1024 * 1024 + 1 },
      { ...first, count: 257 },
      { ...first, chunkId: "" },
    ])
      assert.throws(() => makeOmpFrameDecoder().push(encode(invalid)));
    const incomplete = makeOmpFrameDecoder();
    incomplete.push(encode(first));
    assert.throws(() => incomplete.finish());
    assert.throws(() => makeOmpFrameDecoder().push(new Uint8Array(1024 * 1024)));
    assert.throws(() => makeOmpFrameDecoder().push(new Uint8Array([0xff, 10])));
    assert.throws(() => makeOmpFrameDecoder().push(encode([])));
    const partial = makeOmpFrameDecoder();
    partial.push(new TextEncoder().encode('{"type":"ready"}'));
    assert.throws(() => partial.finish());
  });
});
