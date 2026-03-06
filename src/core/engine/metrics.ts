import type { PredicateSpec, PredicateOp } from "./plan";
import { getPlannerDeepMetricsEnabled } from "./planner-logger";

export type PredicateExecutionMetrics = {
    counts: Record<PredicateOp, number>;
    durationsMs: Record<PredicateOp, number>;
};

export type ExecutionMetricBuckets = {
    counts: Record<string, number>;
    durationsMs: Record<string, number>;
};

export function createPredicateExecutionMetrics(): PredicateExecutionMetrics {
    return {
        counts: {} as Record<PredicateOp, number>,
        durationsMs: {} as Record<PredicateOp, number>,
    };
}

export function shouldCollectDeepMetrics(): boolean {
    return getPlannerDeepMetricsEnabled();
}

export function aggregatePredicateSpecs<T>(
    specs: Array<PredicateSpec<T>>
): { countsByOp: Record<string, number>; countsByKind: Record<string, number> } {
    const countsByOp: Record<string, number> = {};
    const countsByKind: Record<string, number> = {};
    for (let i = 0; i < specs.length; i++) {
        const spec = specs[i]!;
        countsByOp[spec.op] = (countsByOp[spec.op] ?? 0) + 1;
        countsByKind[spec.kind] = (countsByKind[spec.kind] ?? 0) + 1;
    }
    return { countsByOp, countsByKind };
}
