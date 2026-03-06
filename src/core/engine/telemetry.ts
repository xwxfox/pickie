import { getPlannerDeepMetricsEnabled, getPlannerTimingEnabled, logPlanner } from "./planner-logger";
import { nowMs } from "./telemetry-time";

export type TimingToken = {
    label: string;
    parentId?: number;
    planId: string;
    source: "execution" | "ingress" | "egress" | "misc";
    spanId: number;
    startMs: number;
};

let nextSpanId = 1;
let nextSequenceId = 1;

export type RunReportRow = {
    avgMs: number;
    count: number;
    maxMs: number;
    minMs: number;
    totalMs: number;
};

export type RunTraceNode = {
    label: string;
    durationMs: number;
    startMs: number;
    spanId: number;
    parentId?: number;
    planId?: string;
    children: Array<RunTraceNode>;
    sequence?: number;
};

export type RunReport = {
    summary: {
        planId?: string;
        durationMs?: number;
        inputCount?: number;
        inputEstimated?: boolean;
        outputCount?: number;
        mode?: string;
        path?: string;
        hasSearch?: boolean;
        hasTagger?: boolean;
        hasOrders?: boolean;
        pushdownApplied?: boolean;
        streaming?: boolean;
    };
    trace: {
        roots: Array<RunTraceNode>;
        flat: Array<RunTraceNode>;
    };
    hotspots: {
        timings: Array<{ label: string; totalMs: number; avgMs: number; count: number }>;
        predicates: Array<{ op: string; totalMs: number; avgMs: number; count: number }>;
    };
    metrics: {
        predicateCounts: Record<string, number>;
        predicateDurationsMs: Record<string, number>;
        predicateAvgMs: Record<string, number>;
        planPredicateCounts: Record<string, number>;
        extras: Array<{ source: string; planId?: string; extras: Record<string, unknown> }>;
    };
    events: {
        total: number;
        timings: number;
        metrics: number;
    };
};

type EventLike = {
    source: string;
    type: string;
    planId?: string;
    data?: unknown;
    timing?: { durationMs: number; label: string; startMs: number; spanId: number; parentId?: number };
    metrics?: { counts?: Record<string, number>; durationsMs?: Record<string, number>; extras?: Record<string, unknown> };
    sequence?: number;
};

function recordSummary(bucket: Record<string, RunReportRow>, key: string, duration: number): void {
    const entry = bucket[key] ?? { avgMs: 0, count: 0, maxMs: 0, minMs: Number.POSITIVE_INFINITY, totalMs: 0 };
    entry.count += 1;
    entry.totalMs += duration;
    entry.maxMs = Math.max(entry.maxMs, duration);
    entry.minMs = Math.min(entry.minMs, duration);
    entry.avgMs = entry.totalMs / entry.count;
    bucket[key] = entry;
}

export function buildRunReport(events: Array<EventLike>, options?: { planId?: string; top?: number }): RunReport {
    const timingRows: Record<string, RunReportRow> = {};
    const predicateCounts: Record<string, number> = {};
    const predicateDurations: Record<string, number> = {};
    const planPredicateCounts: Record<string, number> = {};
    const extras: Array<{ source: string; planId?: string; extras: Record<string, unknown> }> = [];
    let timingCount = 0;
    let metricsCount = 0;

    const nodes: Array<RunTraceNode> = [];
    const byId = new Map<number, RunTraceNode>();
    const sequences = new Map<number, number>();

    const summary: RunReport["summary"] = {};
    let planId: string | undefined = options?.planId;

    for (let i = 0; i < events.length; i++) {
        const event = events[i]!;
        if (!planId && event.source === "execution" && event.planId) {
            planId = event.planId;
        }
        if (event.timing && (!options?.planId || event.planId === options.planId)) {
            timingCount += 1;
            recordSummary(timingRows, event.timing.label, event.timing.durationMs);
            const node: RunTraceNode = {
                label: event.timing.label,
                durationMs: event.timing.durationMs,
                startMs: event.timing.startMs,
                spanId: event.timing.spanId,
                parentId: event.timing.parentId,
                planId: event.planId,
                children: [],
                sequence: event.sequence,
            };
            nodes.push(node);
            byId.set(node.spanId, node);
            if (event.sequence !== undefined) {
                sequences.set(node.spanId, event.sequence);
            }
        }
        if (event.metrics) {
            metricsCount += 1;
            if (event.metrics.extras) {
                extras.push({ source: event.source, planId: event.planId, extras: event.metrics.extras });
            }
            if (event.metrics.durationsMs) {
                if (event.metrics.counts) {
                    for (const [key, value] of Object.entries(event.metrics.counts)) {
                        predicateCounts[key] = (predicateCounts[key] ?? 0) + value;
                    }
                }
                for (const [key, value] of Object.entries(event.metrics.durationsMs)) {
                    predicateDurations[key] = (predicateDurations[key] ?? 0) + value;
                }
            } else if (event.metrics.counts) {
                for (const [key, value] of Object.entries(event.metrics.counts)) {
                    planPredicateCounts[key] = (planPredicateCounts[key] ?? 0) + value;
                }
            }
        }
        if (event.source === "egress" && event.type === "final" && event.data && typeof event.data === "object") {
            const data = event.data as { mode?: string; path?: string; hasOrders?: boolean; hasSearch?: boolean; resultCount?: number };
            if (data.mode) {summary.mode = data.mode;}
            if (data.path) {summary.path = data.path;}
            if (typeof data.hasOrders === "boolean") {summary.hasOrders = data.hasOrders;}
            if (typeof data.hasSearch === "boolean") {summary.hasSearch = data.hasSearch;}
            if (typeof data.resultCount === "number") {summary.outputCount = data.resultCount;}
        }
    }

    for (let i = 0; i < nodes.length; i++) {
        const node = nodes[i]!;
        if (node.parentId && byId.has(node.parentId)) {
            byId.get(node.parentId)!.children.push(node);
        }
    }
    const roots = nodes.filter((node) => !node.parentId || !byId.has(node.parentId));
    roots.sort((a, b) => {
        const diff = a.startMs - b.startMs;
        if (diff !== 0) {return diff;}
        const left = a.sequence ?? sequences.get(a.spanId) ?? 0;
        const right = b.sequence ?? sequences.get(b.spanId) ?? 0;
        return left - right;
    });

    const predicateAvg: Record<string, number> = {};
    for (const [key, value] of Object.entries(predicateDurations)) {
        const count = predicateCounts[key] ?? 0;
        predicateAvg[key] = count > 0 ? value / count : 0;
    }

    let runExtras: Record<string, unknown> | undefined;
    for (let i = 0; i < extras.length; i++) {
        const extra = extras[i]!;
        if (extra.extras.runDurationMs !== undefined || extra.extras.runOutputCount !== undefined) {
            runExtras = extra.extras;
        }
    }
    if (runExtras) {
        if (typeof runExtras.runDurationMs === "number") {summary.durationMs = runExtras.runDurationMs;}
        if (typeof runExtras.runInputCount === "number") {summary.inputCount = runExtras.runInputCount;}
        if (typeof runExtras.runInputEstimated === "boolean") {summary.inputEstimated = runExtras.runInputEstimated;}
        if (typeof runExtras.runOutputCount === "number") {summary.outputCount = runExtras.runOutputCount;}
        if (typeof runExtras.runMode === "string") {summary.mode = runExtras.runMode;}
        if (typeof runExtras.runPath === "string") {summary.path = runExtras.runPath;}
        if (typeof runExtras.runHasSearch === "boolean") {summary.hasSearch = runExtras.runHasSearch;}
        if (typeof runExtras.runHasTagger === "boolean") {summary.hasTagger = runExtras.runHasTagger;}
        if (typeof runExtras.runHasOrders === "boolean") {summary.hasOrders = runExtras.runHasOrders;}
        if (typeof runExtras.runPushdownApplied === "boolean") {summary.pushdownApplied = runExtras.runPushdownApplied;}
    }

    if (summary.durationMs === undefined) {
        for (let i = 0; i < nodes.length; i++) {
            const node = nodes[i]!;
            if (node.label === "run.execute") {
                summary.durationMs = node.durationMs;
                break;
            }
        }
    }

    const streaming = Object.keys(timingRows).some((label) => label.includes("executeAsync.stream"));
    if (summary.mode === "async") {summary.streaming = streaming;}

    const timingHotspots = Object.entries(timingRows)
        .map(([label, stats]) => ({ label, totalMs: stats.totalMs, avgMs: stats.avgMs, count: stats.count }))
        .sort((a, b) => b.totalMs - a.totalMs)
        .slice(0, options?.top ?? 10);

    const predicateHotspots = Object.entries(predicateDurations)
        .map(([op, totalMs]) => ({
            op,
            totalMs,
            count: predicateCounts[op] ?? 0,
            avgMs: predicateAvg[op] ?? 0,
        }))
        .sort((a, b) => b.totalMs - a.totalMs)
        .slice(0, options?.top ?? 10);

    return {
        summary: { ...summary, planId },
        trace: { roots, flat: nodes },
        hotspots: { timings: timingHotspots, predicates: predicateHotspots },
        metrics: {
            predicateCounts,
            predicateDurationsMs: predicateDurations,
            predicateAvgMs: predicateAvg,
            planPredicateCounts,
            extras,
        },
        events: {
            total: events.length,
            timings: timingCount,
            metrics: metricsCount,
        },
    };
}

export function formatRunReport(events: Array<EventLike>, options?: { planId?: string; top?: number; maxDepth?: number }): string {
    const report = buildRunReport(events, options);
    const sequenceLookup = new Map<number, number>();
    for (let i = 0; i < events.length; i++) {
        const event = events[i]!;
        if (event.timing && event.sequence !== undefined) {
            sequenceLookup.set(event.timing.spanId, event.sequence);
        }
    }
    const lines: Array<string> = [];
    const summary = report.summary;
    const top = options?.top ?? 10;

    lines.push(`Run Report${summary.planId ? ` [${summary.planId}]` : ""}`);
    lines.push("Summary");
    if (summary.durationMs !== undefined) {lines.push(`- duration: ${summary.durationMs.toFixed(2)}ms`);}
    if (summary.inputCount !== undefined) {
        const estimated = summary.inputEstimated ? " (estimated)" : "";
        lines.push(`- input: ${summary.inputCount}${estimated}`);
    }
    if (summary.outputCount !== undefined) {lines.push(`- output: ${summary.outputCount}`);}
    if (summary.mode) {lines.push(`- mode: ${summary.mode}`);}
    if (summary.path) {lines.push(`- path: ${summary.path}`);}
    if (summary.streaming !== undefined) {lines.push(`- streaming: ${summary.streaming}`);}
    if (summary.hasSearch !== undefined) {lines.push(`- search: ${summary.hasSearch}`);}
    if (summary.hasTagger !== undefined) {lines.push(`- tagger: ${summary.hasTagger}`);}
    if (summary.hasOrders !== undefined) {lines.push(`- orders: ${summary.hasOrders}`);}
    if (summary.pushdownApplied !== undefined) {lines.push(`- pushdown: ${summary.pushdownApplied}`);}

    lines.push("");
    lines.push("Timing Hotspots (ms)");
    for (let i = 0; i < Math.min(top, report.hotspots.timings.length); i++) {
        const row = report.hotspots.timings[i]!;
        lines.push(`- ${row.label}: total=${row.totalMs.toFixed(2)} avg=${row.avgMs.toFixed(2)} count=${row.count}`);
    }

    lines.push("");
    lines.push("Predicate Hotspots (ms)");
    for (let i = 0; i < Math.min(top, report.hotspots.predicates.length); i++) {
        const row = report.hotspots.predicates[i]!;
        lines.push(`- ${row.op}: total=${row.totalMs.toFixed(2)} avg=${row.avgMs.toFixed(5)} count=${row.count}`);
    }

    lines.push("");
    lines.push("Trace");
    const maxDepth = options?.maxDepth ?? 8;
    const formatNode = (node: RunTraceNode, depth: number) => {
        if (depth > maxDepth) {return;}
        const indent = "  ".repeat(depth);
        const plan = node.planId ? ` [${node.planId}]` : "";
        lines.push(`${indent}- ${node.label}: ${node.durationMs.toFixed(2)}ms${plan}`);
        node.children.sort((a, b) => {
            const diff = a.startMs - b.startMs;
            if (diff !== 0) {return diff;}
            const left = a.sequence ?? sequenceLookup.get(a.spanId) ?? 0;
            const right = b.sequence ?? sequenceLookup.get(b.spanId) ?? 0;
            return left - right;
        });
        for (let i = 0; i < node.children.length; i++) {
            formatNode(node.children[i]!, depth + 1);
        }
    };
    for (let i = 0; i < report.trace.roots.length; i++) {
        formatNode(report.trace.roots[i]!, 0);
    }

    lines.push("");
    lines.push("Predicate Metrics");
    for (const [op, count] of Object.entries(report.metrics.predicateCounts)) {
        const total = report.metrics.predicateDurationsMs[op] ?? 0;
        const avg = report.metrics.predicateAvgMs[op] ?? 0;
        lines.push(`- ${op}: count=${count} total=${total.toFixed(2)}ms avg=${avg.toFixed(5)}ms`);
    }

    lines.push("");
    lines.push("Plan Predicate Counts");
    for (const [op, count] of Object.entries(report.metrics.planPredicateCounts)) {
        lines.push(`- ${op}: ${count}`);
    }

    if (report.metrics.extras.length > 0) {
        lines.push("");
        lines.push("Decisions");
        for (let i = 0; i < report.metrics.extras.length; i++) {
            const entry = report.metrics.extras[i]!;
            lines.push(`- ${entry.source}${entry.planId ? ` [${entry.planId}]` : ""}: ${JSON.stringify(entry.extras)}`);
        }
    }

    return lines.join("\n");
}

export function startTiming(
    source: "execution" | "ingress" | "egress" | "misc",
    label: string,
    planId: string,
    parent?: TimingToken | number | null
): TimingToken | null {
    if (!getPlannerTimingEnabled()) {return null;}
    const parentId = typeof parent === "number"
        ? parent
        : parent
            ? parent.spanId
            : undefined;
    return { label, parentId, planId, source, spanId: nextSpanId++, startMs: nowMs() };
}

export function emitMarker(
    source: "execution" | "ingress" | "egress" | "misc",
    label: string,
    planId: string,
    parent?: TimingToken | number | null
): void {
    if (!getPlannerTimingEnabled()) {return;}
    const parentId = typeof parent === "number"
        ? parent
        : parent
            ? parent.spanId
            : undefined;
    const now = nowMs();
    logPlanner({
        source,
        type: "timing",
        planId,
        sequence: nextSequenceId++,
        timing: {
            durationMs: 0,
            endMs: now,
            label,
            parentId,
            spanId: nextSpanId++,
            startMs: now,
        },
    });
}

export function endTiming(
    token: TimingToken | null,
    extras?: { data?: unknown; skipData?: boolean }
): void {
    if (!token) {return;}
    const endMs = nowMs();
    const durationMs = endMs - token.startMs;
    const data = extras?.skipData ? undefined : extras?.data;
    logPlanner({
        source: token.source,
        type: "timing",
        planId: token.planId,
        data,
        sequence: nextSequenceId++,
        timing: {
            durationMs,
            endMs,
            label: token.label,
            parentId: token.parentId,
            spanId: token.spanId,
            startMs: token.startMs,
        },
    });
}

export function emitMetrics(event: {
    source: "execution" | "ingress" | "egress" | "misc";
    planId: string;
    metrics: { counts?: Record<string, number>; durationsMs?: Record<string, number>; extras?: Record<string, unknown> };
    data?: unknown;
}): void {
    if (!getPlannerDeepMetricsEnabled()) {return;}
    logPlanner({
        source: event.source,
        type: "metrics",
        planId: event.planId,
        data: event.data,
        sequence: nextSequenceId++,
        metrics: event.metrics,
    });
}
