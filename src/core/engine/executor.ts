import type { QueryPlan } from "./plan";
import type { IngressEngine } from "@/io/ingress";
import { executeSearchPipeline } from "@/core/search/runtime";
import type { SearchCapabilityState, AvailableTags } from "@/types/search";
import type { CompiledTaggerConfig } from "@/core/search/runtime";
import { emitMarker, emitMetrics, endTiming, startTiming } from "@/core/engine/telemetry";
import type { TimingToken } from "@/core/engine/telemetry";
import { createPredicateExecutionMetrics, shouldCollectDeepMetrics } from "@/core/engine/metrics";
import { nowMs } from "@/core/engine/telemetry-time";

export class ExecutionEngine<T extends Record<string, unknown>> {
    execute(ingress: IngressEngine<T>, plan: QueryPlan<T>, options?: { timingParent?: TimingToken | null }): Array<T> {
        const data = ingress.data;
        const predicates = plan.predicates;
        const predicateFn = plan.predicateFn;
        const residualPredicateFn = plan.residualPredicateFn;
        const execTiming = startTiming("execution", "execution.execute", plan.id, options?.timingParent ?? null);
        emitMarker("execution", "execution.execute.start", plan.id, execTiming);
        const collectMetrics = shouldCollectDeepMetrics();
        const predicateMetrics = collectMetrics ? createPredicateExecutionMetrics() : null;
        const hasSearch = plan.searchFilters.length > 0;
        if (plan.alwaysFalse) {
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return [];
        }
        if (predicates.length === 0 && !hasSearch) {
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return [...data];
        }
        if (hasSearch) {
            const searchTiming = startTiming("execution", "execution.searchPipeline", plan.id, execTiming);
            const result = executeSearchPipeline<T, SearchCapabilityState>(
                data,
                predicates,
                predicateFn,
                plan.cache,
                plan.fuzzyConfig,
                plan.taggerConfig as CompiledTaggerConfig<T, AvailableTags<SearchCapabilityState>> | null,
                plan.searchFilters,
                false,
                plan.id,
                execTiming
            );
            endTiming(searchTiming, { skipData: true });
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return result.items;
        }
        const result: Array<T> = [];
        const useMetrics = predicateMetrics !== null && plan.predicateSpecs && plan.predicateSpecs.length > 0;
        if (residualPredicateFn && residualPredicateFn !== predicateFn) {
            for (let i = 0; i < data.length; i++) {
                const item = data[i]!;
                let ok = true;
                if (useMetrics) {
                    for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                        const spec = plan.predicateSpecs![p]!;
                        const start = nowMs();
                        const passed = spec.predicate(item);
                        const elapsed = nowMs() - start;
                        predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                        predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                        if (!passed) {ok = false; break;}
                    }
                    if (!ok) {continue;}
                } else if (!predicateFn(item)) {
                    continue;
                }
                if (!residualPredicateFn(item)) {continue;}
                result.push(item);
            }
            if (useMetrics && predicateMetrics) {
                emitMetrics({
                    source: "execution",
                    planId: plan.id,
                    metrics: {
                        counts: predicateMetrics.counts,
                        durationsMs: predicateMetrics.durationsMs,
                        extras: { phase: "execute" },
                    },
                });
            }
            emitMarker("execution", "execution.execute.end", plan.id, execTiming);
            endTiming(execTiming, { skipData: true });
            return result;
        }
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            let ok = true;
            if (useMetrics) {
                for (let p = 0; p < plan.predicateSpecs!.length; p++) {
                    const spec = plan.predicateSpecs![p]!;
                    const start = nowMs();
                    const passed = spec.predicate(item);
                    const elapsed = nowMs() - start;
                    predicateMetrics!.counts[spec.op] = (predicateMetrics!.counts[spec.op] ?? 0) + 1;
                    predicateMetrics!.durationsMs[spec.op] = (predicateMetrics!.durationsMs[spec.op] ?? 0) + elapsed;
                    if (!passed) {ok = false; break;}
                }
                if (!ok) {continue;}
            } else if (!predicateFn(item)) {
                continue;
            }
            result.push(item);
        }
        if (useMetrics && predicateMetrics) {
            emitMetrics({
                source: "execution",
                planId: plan.id,
                metrics: {
                    counts: predicateMetrics.counts,
                    durationsMs: predicateMetrics.durationsMs,
                    extras: { phase: "execute" },
                },
            });
        }
        emitMarker("execution", "execution.execute.end", plan.id, execTiming);
        endTiming(execTiming, { skipData: true });
        return result;
    }
}
