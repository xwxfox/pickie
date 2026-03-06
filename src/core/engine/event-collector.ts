import type { PlannerLogEvent } from "./planner-logger";

export type EventCollectorOptions = {
    timings?: boolean;
    deepMetrics?: boolean;
    maxEvents?: number;
};

export type EventCollector = {
    events: Array<PlannerLogEvent>;
    logger: (event: PlannerLogEvent) => void;
};

export function createEventCollector(options?: EventCollectorOptions): EventCollector {
    const events: Array<PlannerLogEvent> = [];
    const maxEvents = options?.maxEvents ?? 50_000;
    return {
        events,
        logger: (event) => {
            if (options?.timings === false && event.type === "timing") {return;}
            if (options?.deepMetrics === false && event.type === "metrics") {return;}
            if (events.length >= maxEvents) {return;}
            if (event.type === "timing" && (!event.data || event.data === undefined)) {
                events.push({ ...event, data: undefined });
                return;
            }
            events.push(event);
        },
    };
}

export type EventTimingSummary = {
    avgMs: number;
    count: number;
    maxMs: number;
    minMs: number;
    totalMs: number;
};

export type EventAnalysis = {
    byLabel: Record<string, EventTimingSummary>;
    bySource: Record<string, EventTimingSummary>;
    bySourceType: Record<string, Record<string, EventTimingSummary>>;
    metrics: {
        counts: Record<string, number>;
        durationsMs: Record<string, number>;
        extras: Record<string, number>;
    };
};

function recordTiming(
    bucket: Record<string, EventTimingSummary>,
    key: string,
    duration: number
): void {
    const entry = bucket[key] ?? { avgMs: 0, count: 0, maxMs: 0, minMs: Number.POSITIVE_INFINITY, totalMs: 0 };
    entry.count += 1;
    entry.totalMs += duration;
    entry.maxMs = Math.max(entry.maxMs, duration);
    entry.minMs = Math.min(entry.minMs, duration);
    entry.avgMs = entry.totalMs / entry.count;
    bucket[key] = entry;
}

export function analyzeEvents(events: Array<PlannerLogEvent>): EventAnalysis {
    const byLabel: Record<string, EventTimingSummary> = {};
    const bySource: Record<string, EventTimingSummary> = {};
    const bySourceType: Record<string, Record<string, EventTimingSummary>> = {};
    const metricsCounts: Record<string, number> = {};
    const metricsDurations: Record<string, number> = {};
    const metricsExtras: Record<string, number> = {};

    for (let i = 0; i < events.length; i++) {
        const event = events[i]!;
        if (event.type === "timing" && event.timing) {
            const label = event.timing.label;
            recordTiming(byLabel, label, event.timing.durationMs);
            recordTiming(bySource, event.source, event.timing.durationMs);
            const sourceKey = event.source;
            if (!bySourceType[sourceKey]) {bySourceType[sourceKey] = {};}
            recordTiming(bySourceType[sourceKey], event.type, event.timing.durationMs);
        }
        if (event.type === "metrics" && event.metrics) {
            if (event.metrics.counts) {
                for (const [key, value] of Object.entries(event.metrics.counts)) {
                    metricsCounts[key] = (metricsCounts[key] ?? 0) + value;
                }
            }
            if (event.metrics.durationsMs) {
                for (const [key, value] of Object.entries(event.metrics.durationsMs)) {
                    metricsDurations[key] = (metricsDurations[key] ?? 0) + value;
                }
            }
            if (event.metrics.extras) {
                for (const [key, value] of Object.entries(event.metrics.extras)) {
                    if (typeof value === "number") {
                        metricsExtras[key] = (metricsExtras[key] ?? 0) + value;
                    }
                }
            }
        }
    }

    return {
        byLabel,
        bySource,
        bySourceType,
        metrics: {
            counts: metricsCounts,
            durationsMs: metricsDurations,
            extras: metricsExtras,
        },
    };
}
