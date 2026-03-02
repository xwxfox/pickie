import type { CacheOptions, ResolveSortKey, GroupKeyValue } from "@/types";
import type { ResolveValue } from "@/types/core";

const isoDateRegex = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?$/;

export type CacheState = {
    maxDateCache: number;
    maxPathCache: number;
    pathSegmentsCache: Map<string, string[]>;
    dateCache: Map<string, number | null>;
    parseIsoDate: (value: string) => number | null;
    orderResolver: ResolveSortKey;
    orderResolverDate: ResolveSortKey;
    groupKeyConverter: (value: ResolveValue) => GroupKeyValue | null;
    groupKeyConverterDate: (value: ResolveValue) => GroupKeyValue | null;
};

export const defaultCacheOptions: CacheOptions = {
    maxDateCache: 2048,
    maxPathCache: 2048,
};

let useSharedCache = false;
let sharedCacheState: CacheState | null = null;

export function getUseSharedCache(): boolean {
    return useSharedCache;
}

export function setUseSharedCache(value: boolean): void {
    useSharedCache = value;
}

export function getSharedCacheState(): CacheState | null {
    return sharedCacheState;
}

export function setSharedCacheState(state: CacheState | null): void {
    sharedCacheState = state;
}

function createDateCache(cache: Map<string, number | null>, maxDateCache: number) {
    return (value: string): number | null => {
        const cached = cache.get(value);
        if (cached !== undefined) return cached;

        if (value.length < 10 || !isoDateRegex.test(value)) {
            cache.set(value, null);
            return null;
        }

        const time = Date.parse(value);
        const result = Number.isNaN(time) ? null : time;
        cache.set(value, result);

        while (cache.size > maxDateCache) {
            const firstKey = cache.keys().next().value as string | undefined;
            if (firstKey !== undefined) cache.delete(firstKey);
        }

        return result;
    };
}

export function createCacheState(options: CacheOptions): CacheState {
    const pathSegmentsCache = new Map<string, string[]>();
    const dateCache = new Map<string, number | null>();
    const parseIsoDate = createDateCache(dateCache, options.maxDateCache);
    const orderResolver = createOrderResolver();
    const orderResolverDate = createOrderResolverDate(parseIsoDate);
    const groupKeyConverter = createGroupKeyConverter();
    const groupKeyConverterDate = createGroupKeyConverterDate(parseIsoDate);

    return {
        maxDateCache: options.maxDateCache,
        maxPathCache: options.maxPathCache,
        pathSegmentsCache,
        dateCache,
        parseIsoDate,
        orderResolver,
        orderResolverDate,
        groupKeyConverter,
        groupKeyConverterDate,
    };
}

export function toTimestamp(value: unknown, parseIsoDate: (value: string) => number | null): number | null {
    if (value instanceof Date) {
        const time = value.getTime();
        return Number.isNaN(time) ? null : time;
    }

    if (typeof value === "number") return value;

    if (typeof value !== "string") return null;
    return parseIsoDate(value);
}

export function getSegments(cache: CacheState, path: string): string[] {
    const cached = cache.pathSegmentsCache.get(path);
    if (cached) return cached;
    const segments = path.split(".");
    cache.pathSegmentsCache.set(path, segments);
    while (cache.pathSegmentsCache.size > cache.maxPathCache) {
        const firstKey = cache.pathSegmentsCache.keys().next().value as string | undefined;
        if (firstKey !== undefined) cache.pathSegmentsCache.delete(firstKey);
    }
    return segments;
}

function createOrderResolver(): ResolveSortKey {
    return (value) => {
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return value;
        if (typeof value === "bigint") return value;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        return null;
    };
}

function createOrderResolverDate(parseIsoDate: (value: string) => number | null): ResolveSortKey {
    return (value) => {
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return parseIsoDate(value);
        return null;
    };
}

function createGroupKeyConverter() {
    return (value: ResolveValue): GroupKeyValue | null => {
        if (value == null) return null;
        if (typeof value === "string") return value;
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "boolean") return value;
        if (typeof value === "bigint") return value;
        if (typeof value === "symbol") return value;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        return null;
    };
}

function createGroupKeyConverterDate(parseIsoDate: (value: string) => number | null) {
    return (value: ResolveValue): GroupKeyValue | null => {
        if (value == null) return null;
        if (value instanceof Date) {
            const time = value.getTime();
            return Number.isNaN(time) ? null : time;
        }
        if (typeof value === "number") return Number.isNaN(value) ? null : value;
        if (typeof value === "string") return parseIsoDate(value);
        if (typeof value === "boolean") return value;
        if (typeof value === "bigint") return value;
        if (typeof value === "symbol") return value;
        return null;
    };
}
