import type {
    Predicate,
    OrderOptions,
    PaginationOptions,
    PaginationCursor,
    CacheOptions,
    OrderSpec,
    ResolvePredicate,
    ResolveObject,
    ResolveValue,
    Comparable,
    PathValue,
    Paths,
    NonDatePaths,
    DatePaths,
    SortablePaths,
    GroupablePaths,
    GroupKey,
    ArrayPathItem,
    ArrayPaths,
    SortKey,
} from "./types";

import {
    defaultCacheOptions,
    getUseSharedCache,
    setUseSharedCache,
    getSharedCacheState,
    setSharedCacheState,
    createCacheState,
    getSegments,
    toTimestamp,
} from "./core/cache";

import {
    someResolvedWithSegments,
    everyResolvedWithSegments,
    forEachResolvedWithSegments,
    resolveFirstWithSegments,
    pathExistsWithSegments,
    resolveOrderValueWithSegments,
} from "./core/path";

import { createComparePredicate } from "./core/predicates/compare";
import { createBetweenPredicate } from "./core/predicates/range";
import { createDateEqualsPredicate, createDateComparePredicate, createDateBetweenPredicate } from "./core/predicates/dates";

import {
    createComparator,
    heapPush,
    heapReplaceRoot,
} from "./core/compare";

import type {
    Orderable1,
    Orderable2,
    Orderable3,
    OrderableN,
    Orderable,
} from "./core/compare";

import type { CacheState } from "./core/cache";
import type { GroupKeyValue } from "./types/core";

export class FilterEngine<T extends Record<string, unknown>> {
    private constructor(
        private readonly data: readonly T[],
        private readonly predicates: readonly Predicate<T>[],
        private readonly cache: CacheState,
        private readonly orders: readonly OrderSpec[] = [],
        private readonly limitCount: number | null = null,
        private readonly offsetCount: number = 0
    ) { }

    static from<T extends Record<string, unknown>>(data: readonly T[]): FilterEngine<T> {
        let cache: CacheState;
        if (getUseSharedCache()) {
            cache = getSharedCacheState() ?? (() => {
                const newState = createCacheState(defaultCacheOptions);
                setSharedCacheState(newState);
                return newState;
            })();
        } else {
            cache = createCacheState(defaultCacheOptions);
        }
        return new FilterEngine<T>(data, [], cache);
    }

    static configure(options: {
        maxDateCache?: number;
        maxPathCache?: number;
        sharedCache?: boolean;
    }): void {
        if (typeof options.sharedCache === "boolean") setUseSharedCache(options.sharedCache);
        if (typeof options.maxDateCache === "number") defaultCacheOptions.maxDateCache = options.maxDateCache;
        if (typeof options.maxPathCache === "number") defaultCacheOptions.maxPathCache = options.maxPathCache;
        if (getUseSharedCache()) {
            setSharedCacheState(createCacheState(defaultCacheOptions));
        }
    }

    static clearCaches(): void {
        const state = getSharedCacheState();
        if (state) {
            state.pathSegmentsCache.clear();
            state.dateCache.clear();
        }
    }

    private withPredicate(predicate: Predicate<T>): FilterEngine<T> {
        return new FilterEngine(
            this.data,
            [...this.predicates, predicate],
            this.cache,
            this.orders,
            this.limitCount,
            this.offsetCount
        );
    }

    private andPredicate(condition: Predicate<T>): FilterEngine<T> {
        return this.withPredicate(condition);
    }

    private applyPipeline(): T[] {
        const predicate = this.compile();
        const predicateCount = this.predicates.length;
        const hasOrder = this.orders.length > 0;
        const limitCount = this.limitCount;
        const offsetCount = this.offsetCount;
        if (predicateCount === 0) {
            if (!hasOrder) {
                const start = offsetCount > 0 ? offsetCount : 0;
                if (limitCount === null) return start > 0 ? this.data.slice(start) : this.data.slice();
                if (limitCount <= 0) return [];
                return this.data.slice(start, start + limitCount);
            }
        }

        if (!hasOrder && (limitCount !== null || offsetCount > 0)) {
            const result: T[] = [];
            const start = offsetCount > 0 ? offsetCount : 0;
            const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
            if (max <= 0) return result;

            let matched = 0;
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (!predicate(item)) continue;
                if (matched < start) {
                    matched++;
                    continue;
                }
                result.push(item);
                matched++;
                if (result.length >= max) break;
            }
            return result;
        }

        if (!hasOrder) {
            const filtered: T[] = [];
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (predicate(item)) filtered.push(item);
            }

            if (offsetCount > 0 || limitCount !== null) {
                const start = offsetCount > 0 ? offsetCount : 0;
                const end = limitCount === null ? filtered.length : start + limitCount;
                if (start >= filtered.length) return [];
                return filtered.slice(start, end);
            }
            return filtered;
        }

        const orderCount = this.orders.length;
        const compare = createComparator<T>(this.orders);

        if (orderCount === 1 || orderCount === 2 || orderCount === 3) {
            const entries: (Orderable1<T> | Orderable2<T> | Orderable3<T>)[] = [];
            for (let i = 0; i < this.data.length; i++) {
                const item = this.data[i]!;
                if (!predicate(item)) continue;
                if (orderCount === 1) {
                    const order0 = this.orders[0]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    } as Orderable1<T>);
                } else if (orderCount === 2) {
                    const order0 = this.orders[0]!;
                    const order1 = this.orders[1]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                        k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                    } as Orderable2<T>);
                } else {
                    const order0 = this.orders[0]!;
                    const order1 = this.orders[1]!;
                    const order2 = this.orders[2]!;
                    entries.push({
                        item,
                        index: entries.length,
                        k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                        k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                        k2: resolveOrderValueWithSegments(item as ResolveObject, order2.segments, order2.resolve),
                    } as Orderable3<T>);
                }
            }

            const start = offsetCount > 0 ? offsetCount : 0;
            const end = limitCount === null ? entries.length : start + limitCount;
            if (start >= entries.length) return [];

            if (limitCount !== null && limitCount > 0) {
                const desired = start + limitCount;
                if (desired > 0 && desired < entries.length) {
                    const topCount = desired;
                    const heap: (Orderable1<T> | Orderable2<T> | Orderable3<T>)[] = [];
                    for (let i = 0; i < entries.length; i++) {
                        const entry = entries[i]!;
                        if (heap.length < topCount) {
                            heapPush(heap, entry, compare);
                        } else if (compare(entry, heap[0]!) < 0) {
                            heapReplaceRoot(heap, entry, compare);
                        }
                    }
                    heap.sort(compare);
                    const result: T[] = [];
                    const last = end < heap.length ? end : heap.length;
                    for (let i = start; i < last; i++) result.push(heap[i]!.item);
                    return result;
                }
            }

            entries.sort(compare);
            const result: T[] = [];
            const last = end < entries.length ? end : entries.length;
            for (let i = start; i < last; i++) result.push(entries[i]!.item);
            return result;
        }

        const entries: OrderableN<T>[] = [];
        for (let i = 0; i < this.data.length; i++) {
            const item = this.data[i]!;
            if (!predicate(item)) continue;
            const keys = new Array<SortKey | null>(orderCount);
            for (let j = 0; j < orderCount; j++) {
                const order = this.orders[j]!;
                keys[j] = resolveOrderValueWithSegments(item as ResolveObject, order.segments, order.resolve);
            }
            entries.push({ item, index: entries.length, keys });
        }

        const start = offsetCount > 0 ? offsetCount : 0;
        const end = limitCount === null ? entries.length : start + limitCount;
        if (start >= entries.length) return [];

        if (limitCount !== null && limitCount > 0) {
            const desired = start + limitCount;
            if (desired > 0 && desired < entries.length) {
                const topCount = desired;
                const heap: OrderableN<T>[] = [];
                for (let i = 0; i < entries.length; i++) {
                    const entry = entries[i]!;
                    if (heap.length < topCount) {
                        heapPush(heap, entry, compare);
                    } else if (compare(entry, heap[0]!) < 0) {
                        heapReplaceRoot(heap, entry, compare);
                    }
                }
                heap.sort(compare);
                const result: T[] = [];
                const last = end < heap.length ? end : heap.length;
                for (let i = start; i < last; i++) result.push(heap[i]!.item);
                return result;
            }
        }

        entries.sort(compare);

        const result: T[] = [];
        const last = end < entries.length ? end : entries.length;
        for (let i = start; i < last; i++) {
            result.push(entries[i]!.item);
        }
        return result;
    }

    private groupItems(
        items: readonly T[],
        segments: string[],
        convert: (value: ResolveValue) => GroupKeyValue | null
    ): Map<GroupKeyValue, T[]> {
        const result = new Map<GroupKeyValue, T[]>();
        for (let i = 0; i < items.length; i++) {
            const item = items[i]!;
            forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) return;
                const bucket = result.get(key);
                if (bucket) bucket.push(item);
                else result.set(key, [item]);
            });
        }
        return result;
    }

    private createCursorUnordered(
        predicate: Predicate<T>,
        pageSize: number,
        startPage: number,
        totalMode: "none" | "lazy" | "full"
    ): PaginationCursor<T> {
        const data = this.data;
        const length = data.length;
        const predicateCount = this.predicates.length;
        const history: number[] = [];
        let scanIndex = 0;
        let matchedCount = 0;
        let page = startPage > 0 ? startPage : 1;
        let cachedTotal: number | undefined;

        if (totalMode === "full") {
            if (predicateCount === 0) {
                cachedTotal = length;
            } else {
                let total = 0;
                for (let i = 0; i < length; i++) {
                    if (predicate(data[i]!)) total++;
                }
                cachedTotal = total;
            }
        }

        const totalFn = () => {
            if (totalMode === "none") return undefined;
            if (cachedTotal !== undefined) return cachedTotal;
            if (predicateCount === 0) {
                cachedTotal = length;
                return cachedTotal;
            }
            let total = 0;
            for (let i = 0; i < length; i++) {
                if (predicate(data[i]!)) total++;
            }
            cachedTotal = total;
            return total;
        };

        const cursor: PaginationCursor<T> = {
            data: [],
            page,
            total: totalMode === "none" ? undefined : cachedTotal,
            next: () => {
                page++;
                fillPage(cursor.data, page);
                cursor.page = page;
                cursor.total = totalFn();
                return cursor;
            },
            previous: () => {
                if (page <= 1) return cursor;
                page--;
                restorePageStart(page);
                fillPage(cursor.data, page);
                cursor.page = page;
                cursor.total = totalFn();
                return cursor;
            },
        };

        const restorePageStart = (targetPage: number) => {
            const targetIndex = targetPage - 1;
            const idx = targetIndex > 0 ? history[targetIndex - 1] : 0;
            scanIndex = idx ?? 0;
            matchedCount = (targetPage - 1) * pageSize;
        };

        const fillPage = (buffer: T[], targetPage: number) => {
            const targetMatched = (targetPage - 1) * pageSize;
            if (matchedCount > targetMatched) {
                restorePageStart(targetPage);
            } else if (matchedCount < targetMatched) {
                if (predicateCount === 0) {
                    scanIndex = targetMatched;
                    matchedCount = targetMatched;
                } else {
                    while (matchedCount < targetMatched && scanIndex < length) {
                        const item = data[scanIndex++]!;
                        if (predicate(item)) matchedCount++;
                    }
                }
            }

            const pageStart = scanIndex;
            buffer.length = 0;

            let collected = 0;
            if (predicateCount === 0) {
                while (scanIndex < length && collected < pageSize) {
                    buffer[collected++] = data[scanIndex++]!;
                    matchedCount++;
                }
            } else {
                while (scanIndex < length && collected < pageSize) {
                    const item = data[scanIndex++]!;
                    if (!predicate(item)) continue;
                    buffer[collected++] = item;
                    matchedCount++;
                }
            }

            if (history.length < targetPage) history.push(pageStart);
        };

        fillPage(cursor.data, page);
        cursor.total = totalFn();
        return cursor;
    }

    private createCursorOrdered(
        predicate: Predicate<T>,
        pageSize: number,
        startPage: number,
        totalMode: "none" | "lazy" | "full"
    ): PaginationCursor<T> {
        const data = this.data;
        const orderCount = this.orders.length;
        const compare = createComparator<T>(this.orders);
        const predicateCount = this.predicates.length;

        const entries: Orderable<T>[] = [];
        for (let i = 0; i < data.length; i++) {
            const item = data[i]!;
            if (predicateCount > 0 && !predicate(item)) continue;
            if (orderCount === 1) {
                const order0 = this.orders[0]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                } as Orderable1<T>);
            } else if (orderCount === 2) {
                const order0 = this.orders[0]!;
                const order1 = this.orders[1]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                } as Orderable2<T>);
            } else if (orderCount === 3) {
                const order0 = this.orders[0]!;
                const order1 = this.orders[1]!;
                const order2 = this.orders[2]!;
                entries.push({
                    item,
                    index: entries.length,
                    k0: resolveOrderValueWithSegments(item as ResolveObject, order0.segments, order0.resolve),
                    k1: resolveOrderValueWithSegments(item as ResolveObject, order1.segments, order1.resolve),
                    k2: resolveOrderValueWithSegments(item as ResolveObject, order2.segments, order2.resolve),
                } as Orderable3<T>);
            } else {
                const keys = new Array<SortKey | null>(orderCount);
                for (let j = 0; j < orderCount; j++) {
                    const order = this.orders[j]!;
                    keys[j] = resolveOrderValueWithSegments(item as ResolveObject, order.segments, order.resolve);
                }
                entries.push({ item, index: entries.length, keys } as OrderableN<T>);
            }
        }

        entries.sort(compare);

        const total = entries.length;
        const cursor: PaginationCursor<T> = {
            data: [],
            page: startPage > 0 ? startPage : 1,
            total: totalMode === "none" ? undefined : total,
            next: () => {
                cursor.page++;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
            previous: () => {
                if (cursor.page <= 1) return cursor;
                cursor.page--;
                fillPage(cursor.data, cursor.page);
                return cursor;
            },
        };

        const fillPage = (buffer: T[], page: number) => {
            const start = (page - 1) * pageSize;
            buffer.length = 0;
            if (start >= entries.length) return;
            const end = start + pageSize;
            const last = end < entries.length ? end : entries.length;
            for (let i = start; i < last; i++) {
                buffer.push(entries[i]!.item);
            }
        };

        fillPage(cursor.data, cursor.page);
        return cursor;
    }

    public result(): T[] {
        return this.applyPipeline();
    }

    public groupBy<P extends GroupablePaths<T>>(
        field: P,
        options?: { date?: boolean }
    ): Map<GroupKey<T, P>, T[]> {
        const segments = getSegments(this.cache, String(field));
        const convert = options?.date ? this.cache.groupKeyConverterDate : this.cache.groupKeyConverter;

        if (this.orders.length > 0) {
            const items = this.applyPipeline();
            return this.groupItems(items, segments, convert) as Map<GroupKey<T, P>, T[]>;
        }

        if (this.predicates.length === 0) {
            const result = new Map<GroupKeyValue, T[]>();
            const limitCount = this.limitCount;
            const offsetCount = this.offsetCount;
            const start = offsetCount > 0 ? offsetCount : 0;
            const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
            if (max <= 0) return result as Map<GroupKey<T, P>, T[]>;
            const end = max === Number.POSITIVE_INFINITY ? this.data.length : start + max;
            const last = end < this.data.length ? end : this.data.length;
            for (let i = start; i < last; i++) {
                const item = this.data[i]!;
                forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                    const key = convert(value);
                    if (key === null || key === undefined) return;
                    const bucket = result.get(key);
                    if (bucket) bucket.push(item);
                    else result.set(key, [item]);
                });
            }
            return result as Map<GroupKey<T, P>, T[]>;
        }

        const predicate = this.compile();
        const result = new Map<GroupKeyValue, T[]>();
        const limitCount = this.limitCount;
        const offsetCount = this.offsetCount;
        const start = offsetCount > 0 ? offsetCount : 0;
        const max = limitCount === null ? Number.POSITIVE_INFINITY : limitCount;
        if (max <= 0) return result as Map<GroupKey<T, P>, T[]>;

        let matched = 0;
        for (let i = 0; i < this.data.length; i++) {
            const item = this.data[i]!;
            if (!predicate(item)) continue;
            if (matched < start) {
                matched++;
                continue;
            }
            if (matched - start >= max) break;
            matched++;

            forEachResolvedWithSegments(item as ResolveObject, segments, (value) => {
                const key = convert(value);
                if (key === null || key === undefined) return;
                const bucket = result.get(key);
                if (bucket) bucket.push(item);
                else result.set(key, [item]);
            });
        }

        return result as Map<GroupKey<T, P>, T[]>;
    }

    public resultPaginated(options: PaginationOptions): PaginationCursor<T> {
        const safeSize = Number.isFinite(options.pageSize) && options.pageSize > 0
            ? Math.floor(options.pageSize)
            : 1;
        const safePage = Number.isFinite(options.page) && options.page! > 0
            ? Math.floor(options.page!)
            : 1;
        const totalMode = options.total ?? "none";

        const predicate = this.compile();
        const hasOrder = this.orders.length > 0;
        const startPage = safePage;
        const pageSize = safeSize;

        if (!hasOrder) {
            return this.createCursorUnordered(predicate, pageSize, startPage, totalMode);
        }

        return this.createCursorOrdered(predicate, pageSize, startPage, totalMode);
    }

    public orderBy<P extends SortablePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            segments,
            direction,
            nullsFirst,
            resolve: this.cache.orderResolver,
        };
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            [...this.orders, order],
            this.limitCount,
            this.offsetCount
        );
    }

    public orderByDate<P extends DatePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const direction = options?.direction === "desc" ? -1 : 1;
        const nullsFirst = options?.nulls === "first";
        const order: OrderSpec = {
            segments,
            direction,
            nullsFirst,
            resolve: this.cache.orderResolverDate,
        };
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            [...this.orders, order],
            this.limitCount,
            this.offsetCount
        );
    }

    public thenBy<P extends SortablePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        return this.orderBy(field, options);
    }

    public thenByDate<P extends DatePaths<T>>(
        field: P,
        options?: OrderOptions
    ): FilterEngine<T> {
        return this.orderByDate(field, options);
    }

    public limit(count: number): FilterEngine<T> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            safeCount,
            this.offsetCount
        );
    }

    public offset(count: number): FilterEngine<T> {
        const safeCount = Number.isFinite(count) ? Math.max(0, Math.floor(count)) : 0;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            this.limitCount,
            safeCount
        );
    }

    public page(page: number, pageSize: number): FilterEngine<T> {
        const safePage = Number.isFinite(page) && page > 0 ? Math.floor(page) : 1;
        const safeSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 1;
        const offset = (safePage - 1) * safeSize;
        return new FilterEngine(
            this.data,
            this.predicates,
            this.cache,
            this.orders,
            safeSize,
            offset
        );
    }

    public compile(): Predicate<T> {
        const count = this.predicates.length;
        const predicates = this.predicates;
        if (count === 0) return () => true;
        if (count === 1) return predicates[0]!;
        if (count === 2) {
            const p0 = predicates[0]!;
            const p1 = predicates[1]!;
            return (item) => p0(item) && p1(item);
        }
        if (count === 3) {
            const p0 = predicates[0]!;
            const p1 = predicates[1]!;
            const p2 = predicates[2]!;
            return (item) => p0(item) && p1(item) && p2(item);
        }

        return (item) => {
            for (let i = 0; i < count; i++) {
                if (!predicates[i]!(item)) return false;
            }
            return true;
        };
    }

    public and(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const group = builder(new FilterEngine(this.data, [], this.cache));
        return this.andPredicate(group.compile());
    }

    public or(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const groupEngine = builder(new FilterEngine(this.data, [], this.cache));
        if (this.predicates.length === 0) return groupEngine;
        const group = groupEngine.compile();
        const left = this.compile();
        return new FilterEngine(this.data, [
            (item) => left(item) || group(item)
        ], this.cache, this.orders, this.limitCount, this.offsetCount);
    }

    public not(
        builder: (q: FilterEngine<T>) => FilterEngine<T>
    ): FilterEngine<T> {
        const group = builder(new FilterEngine(this.data, [], this.cache)).compile();
        return this.andPredicate((item) => !group(item));
    }

    public equals<P extends NonDatePaths<T>>(
        field: P,
        value: PathValue<T, P>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const target = value as ResolveValue;
        return this.andPredicate(item =>
            someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => candidate === target
            )
        );
    }

    public dateEquals<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateEqualsPredicate(this.cache.parseIsoDate, value);
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public notEquals<P extends NonDatePaths<T>>(
        field: P,
        value: PathValue<T, P>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const target = value as ResolveValue;
        return this.andPredicate(item =>
            !someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => candidate === target
            )
        );
    }

    public greaterThan<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gt");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateAfter<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "gt");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public greaterThanOrEqual<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "gte");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateAfterOrEqual<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "gte");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public lessThan<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lt");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBefore<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "lt");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public lessThanOrEqual<P extends NonDatePaths<T>>(
        field: P,
        value: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createComparePredicate(value as ResolveValue, "lte");
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBeforeOrEqual<P extends DatePaths<T>>(
        field: P,
        value: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateComparePredicate(this.cache.parseIsoDate, value, "lte");
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public between<P extends NonDatePaths<T>>(
        field: P,
        min: Extract<PathValue<T, P>, Comparable>,
        max: Extract<PathValue<T, P>, Comparable>
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createBetweenPredicate(min as ResolveValue, max as ResolveValue);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public dateBetween<P extends DatePaths<T>>(
        field: P,
        min: Date | string | number,
        max: Date | string | number
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        const predicate = createDateBetweenPredicate(this.cache.parseIsoDate, min, max);
        if (!predicate) return this.andPredicate(() => false);
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, predicate)
        );
    }

    public in<P extends NonDatePaths<T>>(
        field: P,
        values: PathValue<T, P>[]
    ): FilterEngine<T> {
        if (values.length === 0) return this.andPredicate(() => false);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => valueSet.has(candidate)
            )
        );
    }

    public notIn<P extends NonDatePaths<T>>(
        field: P,
        values: PathValue<T, P>[]
    ): FilterEngine<T> {
        if (values.length === 0) return this.andPredicate(() => true);
        const valueSet = new Set(values as ResolveValue[]);
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            !someResolvedWithSegments(
                item as ResolveObject,
                segments,
                (candidate) => valueSet.has(candidate)
            )
        );
    }

    public contains<P extends Paths<T>>(
        field: P,
        substring: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = substring.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().includes(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.includes(substring)
            )
        );
    }

    public startsWith<P extends Paths<T>>(
        field: P,
        prefix: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = prefix.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().startsWith(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.startsWith(prefix)
            )
        );
    }

    public endsWith<P extends Paths<T>>(
        field: P,
        suffix: string,
        ignoreCase = false
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        if (ignoreCase) {
            const target = suffix.toLowerCase();
            return this.andPredicate(item =>
                someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                    if (typeof candidate !== "string") return false;
                    return candidate.toLowerCase().endsWith(target);
                })
            );
        }

        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && candidate.endsWith(suffix)
            )
        );
    }

    public matches<P extends Paths<T>>(
        field: P,
        regex: RegExp
    ): FilterEngine<T> {
        const safeRegex = new RegExp(regex.source, regex.flags.replace(/[gy]/g, ""));
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                typeof candidate === "string" && safeRegex.test(candidate)
            )
        );
    }

    public isNull<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate === null)
        );
    }

    public valueNotNull<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate != null)
        );
    }

    public pathExists<P extends Paths<T>>(field: P): FilterEngine<T> {
        const path = String(field);
        const segments = getSegments(this.cache, path);
        return this.andPredicate(item => {
            return pathExistsWithSegments(item as ResolveObject, segments);
        });
    }

    public pathExistsNullable<P extends Paths<T>>(field: P): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) => candidate !== undefined)
        );
    }

    public arraySome<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public arrayEvery<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            everyResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public arrayNone<P extends Paths<T>>(
        field: P,
        predicate: (value: PathValue<T, P>) => boolean
    ): FilterEngine<T> {
        const segments = getSegments(this.cache, String(field));
        return this.andPredicate(item =>
            !someResolvedWithSegments(item as ResolveObject, segments, (candidate) =>
                predicate(candidate as PathValue<T, P>)
            )
        );
    }

    public nested<P extends ArrayPaths<T>>(
        field: P,
        builder: (q: FilterEngine<ArrayPathItem<T, P>>) => FilterEngine<ArrayPathItem<T, P>>
    ): FilterEngine<T> {
        const nestedPredicate = builder(
            new FilterEngine([] as ArrayPathItem<T, P>[], [], this.cache)
        ).compile();
        const segments = getSegments(this.cache, String(field));

        return this.andPredicate(item => {
            return someResolvedWithSegments(item as ResolveObject, segments, (candidate) => {
                if (Array.isArray(candidate)) {
                    for (let i = 0; i < candidate.length; i++) {
                        if (nestedPredicate(candidate[i] as ArrayPathItem<T, P>)) return true;
                    }
                    return false;
                }
                if (candidate && typeof candidate === "object") {
                    return nestedPredicate(candidate as ArrayPathItem<T, P>);
                }
                return false;
            });
        });
    }

    public custom(predicate: Predicate<T>): FilterEngine<T> {
        return this.andPredicate(predicate);
    }
}

export type FilterEngineClassType = typeof FilterEngine;
