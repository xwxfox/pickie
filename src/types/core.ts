export type Predicate<T> = (item: T) => boolean;

export type ScalarValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | null
    | undefined
    | Date;

export type Comparable =
    | number
    | string
    | bigint
    | Date;

export type SortKey = number | string | bigint;

export type OrderDirection = "asc" | "desc";
export type NullOrder = "first" | "last";

export type GroupableValue =
    | string
    | number
    | boolean
    | bigint
    | symbol
    | Date;

export type GroupKeyValue =
    | string
    | number
    | boolean
    | bigint
    | symbol;

export type ResolveValue =
    | ScalarValue
    | { [key: string]: ResolveValue }
    | ResolveValue[];

export type ResolveObject = { [key: string]: ResolveValue };

export type OrderOptions = {
    direction?: OrderDirection;
    nulls?: NullOrder;
};

export type PaginationTotalMode = "none" | "lazy" | "full";

export type PaginationOptions = {
    pageSize: number;
    page?: number;
    total?: PaginationTotalMode;
};

export type PaginationCursor<T> = {
    data: T[];
    page: number;
    total?: number;
    next: () => PaginationCursor<T>;
    previous: () => PaginationCursor<T>;
};

export type CacheOptions = {
    maxDateCache: number;
    maxPathCache: number;
};

export type ResolvePredicate = (value: ResolveValue) => boolean;

export type ResolveSortKey = (value: ResolveValue) => SortKey | null;

export type OrderSpec = {
    segments: string[];
    direction: 1 | -1;
    nullsFirst: boolean;
    resolve: ResolveSortKey;
};

export type GroupSpec = {
    segments: string[];
    convert: (value: ResolveValue) => GroupKeyValue | null;
};
