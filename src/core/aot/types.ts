import type { FFIFunction } from "bun:ffi";

export type AOTSymbols = Record<string, FFIFunction>;

export type AOTMarker = {
    name: string;
    replaceWith: string;
    condition?: boolean;
};

export type AOTTemplate = {
    id: string;
    source: string;
    markers: ReadonlyArray<AOTMarker>;
    requiredMarkers: ReadonlyArray<string>;
};

export type AOTTraceSource = "execution" | "ingress" | "egress" | "misc";

export type AOTTimingTokenLike = {
    label: string;
    parentId?: number;
    planId: string;
    source: AOTTraceSource;
    spanId: number;
    startMs: number;
};

export type AOTTracer<TToken extends AOTTimingTokenLike> = {
    start: (source: AOTTraceSource, label: string, planId: string, parent?: number | TToken | null) => TToken | null;
    end: (token: TToken | null, extras?: { data?: unknown; skipData?: boolean }) => void;
};

export type AOTProgramOptions<TToken extends AOTTimingTokenLike> = {
    tracer?: AOTTracer<TToken>;
    traceSource?: AOTTraceSource;
};

export type AOTCompileOptions<TSymbols extends AOTSymbols> = {
    flags?: Array<string>;
    symbols: TSymbols;
};
