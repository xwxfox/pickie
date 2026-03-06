export type ExecutionEventType = "input" | "merge" | "order" | "pushdown" | "final" | "timing" | "metrics";
export type IngressEventType = "input" | "final" | "timing" | "metrics";
export type EgressEventType = "input" | "pushdown" | "final" | "timing" | "metrics";

export type TimingPayload = {
    parentId?: number;
    durationMs: number;
    endMs: number;
    label: string;
    spanId: number;
    startMs: number;
};

export type MetricsPayload = {
    counts?: Record<string, number>;
    durationsMs?: Record<string, number>;
    extras?: Record<string, unknown>;
};

type BaseLogEvent = {
    data?: unknown;
    metrics?: MetricsPayload;
    sequence?: number;
    timing?: TimingPayload;
};

export type ExecutionLogEvent = BaseLogEvent & {
    source: "execution";
    type: ExecutionEventType;
    planId: string;
};

export type IngressLogEvent = BaseLogEvent & {
    source: "ingress";
    type: IngressEventType;
    planId: string;
};

export type EgressLogEvent = BaseLogEvent & {
    source: "egress";
    type: EgressEventType;
    planId: string;
};

export type MiscLogEvent = BaseLogEvent & {
    source: "misc";
    type: string;
    planId?: string;
};

export type PlannerLogEvent =
    | ExecutionLogEvent
    | IngressLogEvent
    | EgressLogEvent
    | MiscLogEvent;

export type PlannerLogger = (event: PlannerLogEvent) => void;

let plannerLogger: PlannerLogger | null = null;
let plannerDiagnosticsEnabled = false;
let plannerTimingEnabled = false;
let plannerDeepMetricsEnabled = false;

export function setPlannerLogger(
    logger: PlannerLogger | null,
    options?: { includeDiagnostics?: boolean; includeTiming?: boolean; includeDeepMetrics?: boolean }
): void {
    plannerLogger = logger ?? null;
    if (options?.includeDiagnostics !== undefined) {
        plannerDiagnosticsEnabled = options.includeDiagnostics;
    }
    if (options?.includeTiming !== undefined) {
        plannerTimingEnabled = options.includeTiming;
    }
    if (options?.includeDeepMetrics !== undefined) {
        plannerDeepMetricsEnabled = options.includeDeepMetrics;
    }
}

export function setPlannerDiagnostics(enabled: boolean): void {
    plannerDiagnosticsEnabled = enabled;
}

export function setPlannerTiming(enabled: boolean): void {
    plannerTimingEnabled = enabled;
}

export function setPlannerDeepMetrics(enabled: boolean): void {
    plannerDeepMetricsEnabled = enabled;
}

export function getPlannerLogger(): PlannerLogger | null {
    return plannerLogger;
}

export function getPlannerDiagnosticsEnabled(): boolean {
    return plannerDiagnosticsEnabled || plannerLogger !== null;
}

export function getPlannerTimingEnabled(): boolean {
    return plannerTimingEnabled;
}

export function getPlannerDeepMetricsEnabled(): boolean {
    return plannerDeepMetricsEnabled;
}

export function logPlanner(event: PlannerLogEvent): void {
    if (!plannerLogger) {return;}
    plannerLogger(event);
}
