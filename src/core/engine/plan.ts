import type { Predicate } from "@/types";
import type { CacheState } from "@/core/shared/cache";

export type QueryPlan<T> = {
    id: string;
    predicates: Predicate<T>[];
    cache: CacheState;
};
