import { ptr } from "bun:ffi";
import { startTiming, endTiming, emitMetrics } from "@/core/engine/telemetry";
import { createPrefilterStats } from "./stats";
import { getPrefilterProgram, runPrefilter } from "./aot";
import type { PrefilterProgram } from "./aot";
import type { PrefilterStreamOptions } from "./types";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const INITIAL_OUTPUT_SLOTS = 4096; // 4096 uint32 values = 2048 match slots
const MAX_POOL_SIZE = 4;
const bufferPool: Array<Uint32Array> = [];

function acquireOutputBuffer(minSlots: number): Uint32Array {
  for (let i = bufferPool.length - 1; i >= 0; i--) {
    if (bufferPool[i]!.length >= minSlots) {
      return bufferPool.splice(i, 1)[0]!;
    }
  }
  const size = Math.max(INITIAL_OUTPUT_SLOTS, minSlots);
  return new Uint32Array(size);
}

function releaseOutputBuffer(buf: Uint32Array): void {
  if (bufferPool.length < MAX_POOL_SIZE) {
    bufferPool.push(buf);
  }
}

// Selectivity threshold: if estimated match rate is above this, skip prefiltering
// and fall back to bulk JSON.parse (which is faster when most items match).
const SELECTIVITY_THRESHOLD = 0.5;

export function applyNdjsonPrefilter(
  line: string,
  options?: PrefilterStreamOptions<PrefilterProgram>,
): boolean {
  if (!options?.prefilter || options.prefilterMode === "off") {
    return true;
  }
  const plan = options.prefilter;
  const stats = options.stats;
  if (stats) {
    stats.checked += 1;
  }
  const bytes = encoder.encode(line);
  const program =
    options.prefilterProgram ??
    getPrefilterProgram(plan, {
      planId: options.planId,
      timingParent: options.timingParent ?? null,
      trace: options?.trace?.enabled ?? false,
    });
  const traceBuffer = options?.trace?.enabled ? createTraceBuffer() : null;
  const result = runPrefilter(program, bytes, traceBuffer);
  if (result < 0) {
    if (stats) {
      stats.unknown += 1;
    }
    if (traceBuffer) {
      emitPrefilterTrace(options?.planId ?? "", traceBuffer);
    }
    if (stats) {
      stats.parsed += 1;
    }
    return true;
  }
  if (result === 0) {
    if (stats) {
      stats.skipped += 1;
    }
    if (traceBuffer) {
      emitPrefilterTrace(options?.planId ?? "", traceBuffer);
    }
    return false;
  }
  if (stats) {
    stats.matched += 1;
  }
  if (stats) {
    stats.parsed += 1;
  }
  if (traceBuffer) {
    emitPrefilterTrace(options?.planId ?? "", traceBuffer);
  }
  return true;
}

export function batchPrefilterNdjson(
  bytes: Uint8Array,
  options?: PrefilterStreamOptions<PrefilterProgram>,
): Array<string> | null {
  if (!options?.prefilter || options.prefilterMode === "off") {
    return null;
  }
  const plan = options.prefilter;
  // Skip prefilter when estimated selectivity is high - bulk parse is faster
  if (
    plan.estimatedSelectivity != null &&
    plan.estimatedSelectivity > SELECTIVITY_THRESHOLD
  ) {
    return null;
  }
  const stats = options.stats;
  const program =
    options.prefilterProgram ??
    getPrefilterProgram(plan, {
      planId: options.planId,
      timingParent: options.timingParent ?? null,
      trace: options?.trace?.enabled ?? false,
    });
  const planId = options.planId ?? "";
  const tp = options.timingParent ?? null;
  const traceBuffer = options?.trace?.enabled ? createTraceBuffer() : null;

  // Estimate max possible matches. Each JSON object is at minimum ~10 bytes.
  // Use byteLength/10 as a conservative upper bound to avoid massive over-allocation.
  const estimatedMaxItems = Math.max(256, Math.ceil(bytes.byteLength / 10));
  const neededSlots = estimatedMaxItems * 2 + 2;
  const outputBuf = acquireOutputBuffer(neededSlots);

  try {
    const ffiTiming = startTiming(
      "ingress",
      "ingress.prefilter.ffi",
      planId,
      tp,
    );
    const result = program.fnNdjson(
      ptr(bytes),
      bytes.byteLength,
      ptr(outputBuf),
      outputBuf.length,
      traceBuffer ? ptr(traceBuffer) : null,
    );
    endTiming(ffiTiming, { skipData: true });

    if (result < 0) {
      // Overflow or error - fallback to parse-all
      return null;
    }

    const matchCount = result;

    const decodeTiming = startTiming(
      "ingress",
      "ingress.prefilter.decode",
      planId,
      tp,
    );
    const matched: Array<string> = new Array(matchCount);
    for (let i = 0; i < matchCount; i++) {
      const offset = outputBuf[2 + i * 2]!;
      const length = outputBuf[2 + i * 2 + 1]!;
      matched[i] = decoder.decode(bytes.subarray(offset, offset + length));
    }
    endTiming(decodeTiming, { skipData: true });

    if (stats) {
      const totalItems = outputBuf[0]!;
      const unknownItems = outputBuf[1]!;
      stats.checked += totalItems;
      stats.matched += matchCount;
      stats.parsed += matchCount;
      stats.skipped += totalItems - matchCount;
      stats.unknown += unknownItems;
    }

    if (traceBuffer) {
      emitPrefilterTrace(planId, traceBuffer);
    }

    return matched;
  } finally {
    releaseOutputBuffer(outputBuf);
  }
}

export function applyJsonArrayPrefilter(
  bytes: Uint8Array,
  options?: PrefilterStreamOptions<PrefilterProgram>,
): Array<Record<string, unknown>> | null {
  if (!options?.prefilter || options.prefilterMode === "off") {
    return null;
  }
  const plan = options.prefilter;
  // Skip prefilter when estimated selectivity is high - bulk parse is faster
  if (
    plan.estimatedSelectivity != null &&
    plan.estimatedSelectivity > SELECTIVITY_THRESHOLD
  ) {
    return null;
  }
  const stats = options.stats;
  const program =
    options.prefilterProgram ??
    getPrefilterProgram(plan, {
      planId: options.planId,
      timingParent: options.timingParent ?? null,
      trace: options?.trace?.enabled ?? false,
    });
  const planId = options.planId ?? "";
  const tp = options.timingParent ?? null;
  const traceBuffer = options?.trace?.enabled ? createTraceBuffer() : null;

  // Estimate max possible matches. Each JSON object is at minimum ~10 bytes.
  const estimatedMaxItems = Math.max(256, Math.ceil(bytes.byteLength / 10));
  const neededSlots = estimatedMaxItems * 2 + 2;
  const outputBuf = acquireOutputBuffer(neededSlots);

  try {
    const ffiTiming = startTiming(
      "ingress",
      "ingress.prefilter.ffi",
      planId,
      tp,
    );
    const result = program.fnJsonArray(
      ptr(bytes),
      bytes.byteLength,
      ptr(outputBuf),
      outputBuf.length,
      traceBuffer ? ptr(traceBuffer) : null,
    );
    endTiming(ffiTiming, { skipData: true });

    if (result < 0) {
      // Parse error or overflow - fallback to full parse
      return null;
    }

    const matchCount = result;

    if (stats) {
      const totalItems = outputBuf[0]!;
      const unknownItems = outputBuf[1]!;
      stats.checked += totalItems;
      stats.matched += matchCount;
      stats.parsed += matchCount;
      stats.skipped += totalItems - matchCount;
      stats.unknown += unknownItems;
    }

    if (matchCount === 0) {
      return [];
    }

    if (traceBuffer) {
      emitPrefilterTrace(planId, traceBuffer);
    }
    return parseJsonlFromSlices(bytes, outputBuf, matchCount, planId, tp);
  } finally {
    releaseOutputBuffer(outputBuf);
  }
}

/**
@public
*/
const TRACE_PHASES: Array<string> = [
  "scan_string",
  "parse_number",
  "skip_number",
  "skip_nested_value",
  "skip_value",
  "match_string",
  "skip_object_rest",
  "prefilter_object",
  "key_dispatch",
];

function createTraceBuffer(): BigUint64Array {
  const buffer = new BigUint64Array(TRACE_PHASES.length * 2);
  return buffer;
}

function emitPrefilterTrace(planId: string, trace: BigUint64Array): void {
  const counts: Record<string, number> = {};
  const durationsMs: Record<string, number> = {};
  for (let i = 0; i < TRACE_PHASES.length; i++) {
    const count = Number(trace[i]);
    const nanos = Number(trace[i + TRACE_PHASES.length]);
    const key = TRACE_PHASES[i]!;
    counts[key] = count;
    durationsMs[key] = nanos / 1_000_000;
  }
  emitMetrics({
    source: "ingress",
    planId,
    metrics: { counts, durationsMs },
  });
}

function parseJsonlFromSlices(
  bytes: Uint8Array,
  outputBuf: Uint32Array,
  matchCount: number,
  planId: string,
  tp: number | null,
): Record<string, unknown>[] {
  const assembleTiming = startTiming(
    "ingress",
    "ingress.prefilter.assembleJsonl",
    planId,
    tp,
  );

  let totalBytes = 0;
  for (let i = 0; i < matchCount; i++) {
    totalBytes += outputBuf[2 + i * 2 + 1]!;
  }
  totalBytes += matchCount - 1; // newlines

  const assembled = new Uint8Array(totalBytes);
  let pos = 0;
  for (let i = 0; i < matchCount; i++) {
    if (i > 0) assembled[pos++] = 0x0a; // '\n'
    const offset = outputBuf[2 + i * 2]!;
    const length = outputBuf[2 + i * 2 + 1]!;
    assembled.set(bytes.subarray(offset, offset + length), pos);
    pos += length;
  }
  endTiming(assembleTiming, { skipData: true });

  const parseTiming = startTiming(
    "ingress",
    "ingress.prefilter.jsonParseJsonl",
    planId,
    tp,
  );

  const parsed = Bun.JSONL.parse(assembled);
  endTiming(parseTiming, { skipData: true });

  return parsed as Record<string, unknown>[];
}
export const initPrefilterStats = createPrefilterStats;
