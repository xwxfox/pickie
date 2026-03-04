import type {
    Predicate,
    OrderOptions,
    OrderSpec,
    ResolveObject,
    ResolveValue,
    Comparable,
    PathValue,
    Paths,
    NonDatePaths,
    DatePaths,
    SortablePaths,
    GroupablePaths,
    ArrayPathItem,
    ArrayPaths,
} from "@/types";
import type { QueryChain } from "@/core/engine/chains/chain";
import { hashPlanId } from "./hash";
import { getSegments } from "@/core/shared/cache";
import {
    someResolvedWithSegments,
    everyResolvedWithSegments,
    pathExistsWithSegments,
} from "@/core/shared/path";
import { createComparePredicate } from "@/core/engine/predicates/compare";
import { createBetweenPredicate } from "@/core/engine/predicates/range";
import { createDateEqualsPredicate, createDateComparePredicate, createDateBetweenPredicate } from "@/core/engine/predicates/dates";
import type { CacheState } from "@/core/shared/cache";
import type { QueryPlan } from "./plan";
import { ExecutionEngine } from "./executor";
import { EgressEngine } from "@/io/egress";
import { IngressEngine } from "@/io/ingress";

export type QueryBuilderState<T> = {
    predicates: Predicate<T>[];
    cache: CacheState;
};

export class QueryBuilder<T extends Record<string, unknown>> {
    private constructor(
        private readonly ingress: IngressEngine<T>,
        private readonly state: QueryBuilderState<T>
    ) {}

    static from<T extends Record<string, unknown>>(ingress: IngressEngine<T>): QueryBuilder<T> {
        return new QueryBuilder(ingress, { predicates: [], cache: ingress.cache });
    }

    private addPredicate(predicate: Predicate<T>): QueryBuilder<T> {
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, predicate],
        });
    }

    use(chain: QueryChain<T>): QueryBuilder<T> {
        const plan = chain.getPlan();
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [...this.state.predicates, ...plan.predicates],
        });
    }

    compilePlan(): QueryPlan<T> {
        const id = hashPlanId(this.state.predicates);
        return {
            id,
            predicates: this.state.predicates,
            cache: this.state.cache,
        };
    }

    out(): EgressEngine<T> {
        const plan = this.compilePlan();
        return EgressEngine.from(this.ingress, plan);
    }

    // Filter operators
    equals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const target = value as ResolveValue;
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) => c === target)
        );
    }

    notEquals<P extends NonDatePaths<T>>(field: P, value: PathValue<T, P>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const target = value as ResolveValue;
        return this.addPredicate(item =>
            !someResolvedWithSegments(item as ResolveObject, segments, (c) => c === target)
        );
    }

    greaterThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    greaterThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    lessThan<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    lessThanOrEqual<P extends NonDatePaths<T>>(field: P, value: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    between<P extends NonDatePaths<T>>(field: P, min: Extract<PathValue<T, P>, Comparable>, max: Extract<PathValue<T, P>, Comparable>): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    in<P extends NonDatePaths<T>>(field: P, values: PathValue<T, P>[]): QueryBuilder<T> {
        if (values.length === 0) return this.addPredicate(() => false);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) => valueSet.has(c))
        );
    }

    notIn<P extends NonDatePaths<T>>(field: P, values: PathValue<T, P>[]): QueryBuilder<T> {
        if (values.length === 0) return this.addPredicate(() => true);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            !someResolvedWithSegments(item as ResolveObject, segments, (c) => valueSet.has(c))
        );
    }

    dateEquals<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateEqualsPredicate(this.state.cache.parseIsoDate, value);
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    dateAfter<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gt");
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    dateAfterOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "gte");
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    dateBefore<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lt");
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    dateBeforeOrEqual<P extends DatePaths<T>>(field: P, value: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateComparePredicate(this.state.cache.parseIsoDate, value, "lte");
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    dateBetween<P extends DatePaths<T>>(field: P, min: Date | string | number, max: Date | string | number): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        const predicate = createDateBetweenPredicate(this.state.cache.parseIsoDate, min, max);
        if (!predicate) return this.addPredicate(() => false);
        return this.addPredicate(item => someResolvedWithSegments(item as ResolveObject, segments, predicate));
    }

    contains<P extends Paths<T>>(field: P, substring: string, ignoreCase = false): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        if (ignoreCase) {
            const target = substring.toLowerCase();
            return this.addPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                    typeof c === "string" && c.toLowerCase().includes(target)
                )
            );
        }
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                typeof c === "string" && c.includes(substring)
            )
        );
    }

    startsWith<P extends Paths<T>>(field: P, prefix: string, ignoreCase = false): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            return this.addPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                    typeof c === "string" && c.toLowerCase().startsWith(target)
                )
            );
        }
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                typeof c === "string" && c.startsWith(prefix)
            )
        );
    }

    endsWith<P extends Paths<T>>(field: P, suffix: string, ignoreCase = false): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            return this.addPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                    typeof c === "string" && c.toLowerCase().endsWith(target)
                )
            );
        }
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                typeof c === "string" && c.endsWith(suffix)
            )
        );
    }

    matches<P extends Paths<T>>(field: P, regex: RegExp): QueryBuilder<T> {
        const safeRegex = new RegExp(regex.source, regex.flags.replace(/[gy]/g, ""));
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                typeof c === "string" && safeRegex.test(c)
            )
        );
    }

    isNull<P extends Paths<T>>(field: P): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) => c === null)
        );
    }

    valueNotNull<P extends Paths<T>>(field: P): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) => c != null)
        );
    }

    pathExists<P extends Paths<T>>(field: P): QueryBuilder<T> {
        const path = String(field);
        const segments = getSegments(this.state.cache, path);
        return this.addPredicate(item =>
            pathExistsWithSegments(item as ResolveObject, segments)
        );
    }

    pathExistsNullable<P extends Paths<T>>(field: P): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) => c !== undefined)
        );
    }

    arraySome<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    arrayEvery<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            everyResolvedWithSegments(item as ResolveObject, segments, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    arrayNone<P extends Paths<T>>(field: P, predicate: (value: PathValue<T, P>) => boolean): QueryBuilder<T> {
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item =>
            !someResolvedWithSegments(item as ResolveObject, segments, (c) =>
                predicate(c as PathValue<T, P>)
            )
        );
    }

    nested<P extends ArrayPaths<T>>(field: P, builder: (q: QueryBuilder<ArrayPathItem<T, P>>) => QueryBuilder<ArrayPathItem<T, P>>): QueryBuilder<T> {
        const nestedPredicate = builder(
            QueryBuilder.from(this.ingress as IngressEngine<ArrayPathItem<T, P>>)
        ).compilePlan().predicates;
        const segments = getSegments(this.state.cache, String(field));
        return this.addPredicate(item => {
            return someResolvedWithSegments(item as ResolveObject, segments, (c) => {
                if (Array.isArray(c)) {
                    for (let i = 0; i < c.length; i++) {
                        for (let p = 0; p < nestedPredicate.length; p++) {
                            if (!nestedPredicate[p]!(c[i] as ArrayPathItem<T, P>)) return false;
                        }
                        return true;
                    }
                    return false;
                }
                if (c && typeof c === "object") {
                    for (let p = 0; p < nestedPredicate.length; p++) {
                        if (!nestedPredicate[p]!(c as ArrayPathItem<T, P>)) return false;
                    }
                    return true;
                }
                return false;
            });
        });
    }

    and(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        return this.addPredicate((item) => group.every((p: Predicate<T>) => p(item)));
    }

    or(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        const left = this.state.predicates;
        if (group.length === 0) return this;
        if (left.length === 0) {
            return new QueryBuilder(this.ingress, {
                ...this.state,
                predicates: [...group],
            });
        }
        return new QueryBuilder(this.ingress, {
            ...this.state,
            predicates: [
                (item) => left.every((p: Predicate<T>) => p(item)) || group.every((p: Predicate<T>) => p(item)),
            ],
        });
    }

    not(builder: (q: QueryBuilder<T>) => QueryBuilder<T>): QueryBuilder<T> {
        const group = builder(QueryBuilder.from(this.ingress)).compilePlan().predicates;
        return this.addPredicate((item) => !group.every((p: Predicate<T>) => p(item)));
    }

    custom(predicate: Predicate<T>): QueryBuilder<T> {
        return this.addPredicate(predicate);
    }
}
