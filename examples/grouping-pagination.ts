import { Engine, IngressEngine } from "../src";

type Account = {
    active: boolean;
    created: string;
    id: number;
    owner: {
        team: "infra" | "product" | "sales";
    };
    score: number;
};

const data: Array<Account> = [
    { active: true, created: "2026-03-01T10:00:00.000Z", id: 1, owner: { team: "infra" }, score: 90 },
    { active: true, created: "2026-03-02T10:00:00.000Z", id: 2, owner: { team: "product" }, score: 72 },
    { active: false, created: "2026-03-03T10:00:00.000Z", id: 3, owner: { team: "infra" }, score: 55 },
    { active: true, created: "2026-03-04T10:00:00.000Z", id: 4, owner: { team: "sales" }, score: 88 },
];

const ingress = IngressEngine.from(data);

const page = Engine.from(ingress)
    .equals("active", true)
    .out()
    .orderByDate("created", { direction: "desc" })
    .thenBy("score", { direction: "desc" })
    .paginate({ pageSize: 2, total: "lazy" });

console.dir(page.data, { depth: null });
console.dir(page.next().data, { depth: null });

const grouped = Engine.from(ingress)
    .equals("active", true)
    .out()
    .groupBy("owner.team");

console.dir(grouped, { depth: null });
