import * as Schema from "effect/Schema";

const MAX_FRAME_BYTES = 1024 * 1024;
const MAX_REASSEMBLED_BYTES = 64 * 1024 * 1024;
const RecordFromJson = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
const decodeRecord = Schema.decodeSync(RecordFromJson);
const Chunk = Schema.Struct({
  type: Schema.Literal("rpc_chunk"),
  chunkId: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  index: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  count: Schema.Int.check(Schema.isBetween({ minimum: 2, maximum: 256 })),
  byteLength: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_REASSEMBLED_BYTES })),
  data: Schema.String,
});
const decodeChunk = Schema.decodeUnknownSync(Chunk);
const utf8 = new TextDecoder("utf-8", { fatal: true });

interface PendingChunks {
  readonly chunkId: string;
  readonly count: number;
  readonly byteLength: number;
  readonly buffers: Uint8Array[];
  received: number;
}

export function makeOmpFrameDecoder() {
  let maxFrameBytes = MAX_FRAME_BYTES;
  let maxReassembledBytes = MAX_REASSEMBLED_BYTES;
  let pending: PendingChunks | undefined;
  let segments: Uint8Array[] = [];
  let lineBytes = 0;

  const decode = (line: Uint8Array): Record<string, unknown> | undefined => {
    const record = decodeRecord(utf8.decode(line));
    if (record.type !== "rpc_chunk") {
      if (pending !== undefined) throw new Error("Interrupted OMP chunk sequence");
      if (record.type === "ready") {
        for (const [key, ceiling] of [
          ["maxFrameBytes", MAX_FRAME_BYTES],
          ["maxReassembledFrameBytes", MAX_REASSEMBLED_BYTES],
        ] as const) {
          const value = record[key];
          if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
            throw new Error("Invalid OMP transport limits");
          }
          if (key === "maxFrameBytes") maxFrameBytes = Math.min(value, ceiling);
          else maxReassembledBytes = Math.min(value, ceiling);
        }
      }
      return record;
    }
    const chunk = decodeChunk(record);
    if (
      chunk.index >= chunk.count ||
      chunk.byteLength < maxFrameBytes ||
      chunk.byteLength > maxReassembledBytes
    ) {
      throw new Error("Invalid OMP chunk bounds");
    }
    if (
      chunk.data.length === 0 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(chunk.data)
    ) {
      throw new Error("Invalid OMP chunk encoding");
    }
    const bytes = Buffer.from(chunk.data, "base64");
    if (bytes.byteLength > 256 * 1024) throw new Error("OMP chunk payload exceeds limit");
    if (bytes.toString("base64") !== chunk.data) throw new Error("Invalid OMP chunk encoding");
    if (pending === undefined) {
      if (chunk.index !== 0) throw new Error("Missing first OMP chunk");
      pending = { ...chunk, buffers: [], received: 0 };
    }
    if (
      chunk.chunkId !== pending.chunkId ||
      chunk.index !== pending.buffers.length ||
      chunk.count !== pending.count ||
      chunk.byteLength !== pending.byteLength ||
      pending.received + bytes.byteLength > pending.byteLength
    ) {
      throw new Error("Inconsistent OMP chunk sequence");
    }
    pending.buffers.push(bytes);
    pending.received += bytes.byteLength;
    if (pending.buffers.length < pending.count) return undefined;
    if (pending.received !== pending.byteLength) throw new Error("Incomplete OMP logical frame");
    const complete = decodeRecord(utf8.decode(Buffer.concat(pending.buffers, pending.byteLength)));
    if (complete.type === "rpc_chunk") throw new Error("Nested OMP chunk sequence");
    pending = undefined;
    return complete;
  };

  return {
    push(chunk: Uint8Array): ReadonlyArray<Record<string, unknown>> {
      const records: Record<string, unknown>[] = [];
      let offset = 0;
      while (offset < chunk.byteLength) {
        const newline = chunk.indexOf(10, offset);
        const end = newline < 0 ? chunk.byteLength : newline;
        const segment = chunk.subarray(offset, end);
        lineBytes += segment.byteLength;
        if (lineBytes + 1 > maxFrameBytes) throw new Error("OMP physical frame exceeds limit");
        if (segment.byteLength > 0) segments.push(segment.slice());
        if (newline < 0) break;
        if (lineBytes > 0) {
          const record = decode(Buffer.concat(segments, lineBytes));
          if (record !== undefined) records.push(record);
        }
        segments = [];
        lineBytes = 0;
        offset = newline + 1;
      }
      return records;
    },
    finish(): void {
      if (lineBytes !== 0 || pending !== undefined) throw new Error("Incomplete OMP frame at EOF");
    },
  };
}
