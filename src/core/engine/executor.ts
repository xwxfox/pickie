import type { QueryPlan } from "./plan";
import type { IngressEngine } from "@/io/ingress";

export class ExecutionEngine<T extends Record<string, unknown>> {
    execute(ingress: IngressEngine<T>, plan: QueryPlan<T>): T[] {
        const data = ingress.data;
        const predicates = plan.predicates;
        if (predicates.length === 0) {
            return [...data];
        }
        const result: T[] = [];
        outer: for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            for (let p = 0; p < predicates.length; p++) {
                if (!predicates[p]!(item)) continue outer;
            }
            result.push(item);
        }
        return result;
    }
}
