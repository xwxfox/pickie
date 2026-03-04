export type AotPlan = {
    id: string;
};

export type AotExecutor = {
    execute: (plan: AotPlan) => void;
};
