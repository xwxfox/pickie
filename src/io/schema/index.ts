export type SchemaSource = "inline" | "inferred" | "explicit";

export type Schema<T> = {
    source: SchemaSource;
    sample?: T;
};

export const Schema = {
    inline<T>(): Schema<T> {
        return { source: "inline" } as Schema<T>;
    },
    infer<T>(sample: T): Schema<T> {
        return { source: "inferred", sample } as Schema<T>;
    },
};
