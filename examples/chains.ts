import { Engine, IngressEngine, QueryChain, Schema, createChain, compileChain } from "../src";

type Row = {
    active: boolean;
    id: number;
    owner: {
        name: string;
    };
    score: number;
};

const sample: Row = {
    active: true,
    id: 0,
    owner: { name: "sample" },
    score: 50,
};

const chain = QueryChain.from(Schema.infer(sample), q =>
    q.equals("active", true).greaterThan("score", 70)
);

const data: Array<Row> = [
    { active: true, id: 1, owner: { name: "Ada" }, score: 91 },
    { active: true, id: 2, owner: { name: "Bea" }, score: 61 },
    { active: false, id: 3, owner: { name: "Cy" }, score: 88 },
];

const ingress = IngressEngine.from(data);

const fromChain = chain
    .out(ingress)
    .orderBy("score", { direction: "desc" })
    .result();

console.dir(fromChain, { depth: null });

const isHighScore = createChain<Row>(item => item.score >= 80);
const isActive = createChain<Row>(item => item.active);

const compiled = compileChain(isHighScore.and(isActive));

const manual = Engine.from(ingress)
    .custom(compiled)
    .out()
    .result();

console.dir(manual, { depth: null });
