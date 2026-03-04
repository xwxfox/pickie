import { Engine, IngressEngine } from "../src";

type LogEntry = {
    message: string;
    tags: Array<string>;
    type: "CREDIT_MAX_EXCEEDED" | "OTHER";
    when: string;
};

type SampleItem = {
    active: boolean;
    created: Date | string | number;
    id: number;
    label: string;
    logs: Array<LogEntry>;
    owner: {
        name: string;
        nickname?: string | null;
    };
    score: number;
};

const data: Array<SampleItem> = [
    {
        active: true,
        created: new Date("2026-01-01T00:00:00.000Z"),
        id: 1,
        label: "release",
        logs: [
            { message: "over", tags: ["x"], type: "CREDIT_MAX_EXCEEDED", when: "2026-01-03T00:00:00.000Z" },
            { message: "over", tags: ["x"], type: "OTHER", when: "2026-01-03T00:00:00.000Z" },
        ],
        owner: { name: "Alice", nickname: null },
        score: 10,
    },
    {
        active: false,
        created: "2026-01-02T00:00:00.000Z",
        id: 2,
        label: "alpha",
        logs: [
            { message: "ok", tags: [], type: "OTHER", when: "2026-01-04T00:00:00.000Z" },
        ],
        owner: { name: "Bob", nickname: "B" },
        score: 20,
    },
    {
        active: true,
        created: "2026-01-01T00:00:00.000Z",
        id: 3,
        label: "release",
        logs: [],
        owner: { name: "Cara" },
        score: 5,
    },
];

const ingress = IngressEngine.from(data);

const result = Engine.from(ingress)
    .equals("active", true)
    .nested("logs", q => q.equals("type", "CREDIT_MAX_EXCEEDED"))
    .contains("label", "release", true)
    .out()
    .orderByDate("created", { direction: "desc" })
    .result();

console.dir(result, { depth: null });
