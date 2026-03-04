import type { Predicate } from "@/types";

export function hashPlanId<T>(predicates: Predicate<T>[]): string {
    const size = predicates.length;
    return `plan_${size}`;
}
