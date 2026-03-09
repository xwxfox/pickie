
import {
  Engine,
  IngressEngine,
  Schema,
  setPlannerDiagnostics,
  setPlannerLogger,
  setPlannerTiming,
  setPlannerDeepMetrics,
  setTimingSource,
  fileSource,
  formatRunReport
} from "@/index.ts";
import { resolve } from "path";

type LargeItem = {
  active: boolean;
  created: Date | string;
  flags: Array<string>;
  id: number;
  Logs: Array<{ tags: Array<string>; type: string; when: Date | string }>;
  meta: {
    owner: {
      name: string;
      nickname?: string | null;
    };
  };
  name: string | null;
  score: number;
};
const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min)) + min;
const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"];
const getRandomNamesArray = (): Array<string> => {
  const upper = randomInt(1, ownerNames.length + 1);
  const out: Array<string> = [];
  for (let i = 0; i < upper; i++) {
    const random = randomInt(0, ownerNames.length)
    out.push(ownerNames[random]!);
  }
  return out;
};

export const run = async () => {
  const events: Array<{ source: string; type: string; planId?: string; data?: unknown; metrics?: { counts?: Record<string, number>; durationsMs?: Record<string, number>; extras?: Record<string, unknown> }; timing?: { durationMs: number; label: string; startMs: number; endMs: number; spanId: number; parentId?: number } }> = [];
  setPlannerLogger((evt) => {
    events.push(evt);
    if (evt.type === "timing") { return; }
    if (evt.type === "metrics") { return; }
    console.log(`[${evt.source}.${evt.type}]<ID: ${evt.planId ?? "N/A"}>:`);
    console.dir(evt.data, { depth: null });

    /*
            if (evt.event === "planner:final") {
        console.log("plan", evt.planId);
        console.dir(evt.data, { depth: null });
    }*/
  }, { includeDiagnostics: true, includeTiming: true, includeDeepMetrics: true });

  setPlannerDiagnostics(true);
  setPlannerTiming(true);
  setPlannerDeepMetrics(true);
  setTimingSource("performance");

  const start = performance.now();
  const source = fileSource<LargeItem>(resolve(import.meta.dir, "../perf/bench-data/large-items/150000.json"),
    {
      format: "json",
      hints: { estimatedCount: 150000 },
      schema: Schema.inline<LargeItem>(),
      prefilterMode: "auto"
    });
  const input = IngressEngine.fromSource(source)
  /*
 
   const data = await Bun.file(
       resolve(import.meta.dir, "../perf/bench-data/large-items/150000.json")
   ).json() as Array<LargeItem>;
 const dataLoadTime = performance.now() - start;
   const input = IngressEngine.from(data) // lsp: const input: IngressEngine<LargeItem>
   */
  const filter = Engine.from(input);
  const res = await filter.in("meta.owner.name", getRandomNamesArray()) /* lsp: const res: any */

    .greaterThan("score", 9)
    .valueNotNull("name")
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .configureTagger({
      tags: ["falgs:green"],
      rules: [
        {
          field: "flags",
          in: ["green"],
          tag: "falgs:green"
        }
      ]
    })
    .out()
    .orderByDate("created", { direction: "desc" })
    .result();
  const end = performance.now();
  console.log(`Query took ${(end - start)}ms - sizes: input=${input.length}, res=${res.length}`);
  console.log("\n");
  console.log(formatRunReport(events));
};


if (import.meta.main) {
  await run();
}


/*
$ bun run examples/debugging.ts
[execution.input]<ID: plan_4__t0>:
{
  predicates: [
    {
      cost: 1.57,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "in",
      pushdown: true,
      reorderable: true,
      selectivity: 0.001,
      value: [ "Eli" ],
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
}
[execution.merge]<ID: plan_4__t0>:
{
  merges: [],
}
[execution.order]<ID: plan_4__t0>:
{
  order: [
    {
      before: [ "p1", "p2", "p3", "p4" ],
      after: [ "p3", "p2", "p1", "p4" ],
      reason: "cost/selectivity",
    }
  ],
}
[execution.pushdown]<ID: plan_4__t0>:
{
  applied: [],
  candidates: [ "p3", "p2", "p1" ],
  full: false,
  residual: [ "p3", "p2", "p1", "p4" ],
}
[execution.final]<ID: plan_4__t0>:
{
  alwaysFalse: false,
  predicates: [
    {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 1.25,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "eq",
      pushdown: true,
      reorderable: true,
      selectivity: 0.1,
      value: "Eli",
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
  pushdownPredicates: [],
  residualPredicates: [
    {
      cost: 0.9,
      field: "name",
      id: "p3",
      ignoreCase: undefined,
      op: "notNull",
      pushdown: true,
      reorderable: true,
      selectivity: 0.9,
      value: undefined,
    }, {
      cost: 1.1,
      field: "score",
      id: "p2",
      ignoreCase: undefined,
      op: "gt",
      pushdown: true,
      reorderable: true,
      selectivity: 0.4,
      value: 9,
    }, {
      cost: 1.25,
      field: "meta.owner.name",
      id: "p1",
      ignoreCase: undefined,
      op: "eq",
      pushdown: true,
      reorderable: true,
      selectivity: 0.1,
      value: "Eli",
    }, {
      cost: 2.6,
      field: "created",
      id: "p4",
      ignoreCase: undefined,
      op: "dateBetween",
      pushdown: false,
      reorderable: true,
      selectivity: 0.25,
      value: {
        max: "2026-02-31",
        min: "2026-01-01",
      },
    }
  ],
}
[egress.input]<ID: plan_4__t0>:
{
  hasSearch: false,
  orders: 1,
  limit: null,
  offset: 0,
}
[egress.pushdown]<ID: plan_4__t0>:
{
  applied: false,
  eligible: false,
  reason: {
    hasResidual: true,
    hasSearch: false,
    hasSearchFilters: false,
    ingressSupportsPushdown: false,
    offsetWithoutLimit: false,
  },
}
[ingress.input]<ID: ingress>:
{
  capabilities: {
    count: false,
    filter: false,
    group: false,
    order: false,
    paginate: false,
    search: false,
  },
  hints: {
    estimatedCount: 150000,
  },
  requiresGrouping: false,
  requiresOrdering: true,
  requiresSearch: false,
}
[ingress.final]<ID: ingress>:
{
  plan: {
    strategy: "eager",
  },
  reason: "ordering/grouping",
  supported: {
    group: false,
    order: false,
  },
}
[egress.final]<ID: plan_4__t0>:
{
  mode: "async",
  path: "local",
  hasOrders: true,
  hasSearch: false,
  residualPredicates: 1,
  resultCount: 22331,
}
Query took 244.07642499999997ms - sizes: input=150000, res=22331


Run Report [plan_4__t0]
Summary
- duration: 239.31ms
- output: 22331
- mode: async
- path: local
- streaming: false
- search: false
- orders: true

Timing Hotspots (ms)
- run.execute: total=239.31 avg=239.31 count=1
- egress.executeAsync: total=238.81 avg=238.81 count=1
- execution.executeAsync: total=223.44 avg=223.44 count=1
- execution.load.materialize: total=195.44 avg=195.44 count=1
- ingress.prefilter.ffi: total=131.39 avg=131.39 count=1
- ingress.prefilter.parse: total=36.07 avg=36.07 count=1
- ingress.file.read: total=17.59 avg=17.59 count=1
- egress.finalizeOrder: total=13.92 avg=13.92 count=1
- ingress.prefilter.assemble: total=9.47 avg=9.47 count=1
- ingress.prefilter.compile: total=8.37 avg=8.37 count=1

Predicate Hotspots (ms)
- dateBetween: total=3.07 avg=0.00014 count=22331
- notNull: total=2.42 avg=0.00011 count=22331
- eq: total=2.09 avg=0.00009 count=22331
- gt: total=1.43 avg=0.00006 count=22331

Trace
- source.create: 0.00ms [ingress]
- builder.create: 0.00ms [engine]
- egress.start: 0.00ms [egress]
- execution.compilePlan: 2.71ms [plan_4__t0]
  - execution.optimizePlan: 2.39ms [plan_4__t0]
    - execution.mergePass: 0.81ms [plan_4__t0]
    - execution.orderPass: 0.09ms [plan_4__t0]
    - execution.pushdownSplit: 0.09ms [plan_4__t0]
- run.execute: 239.31ms [plan_4__t0]
  - egress.executeResult: 0.05ms [plan_4__t0]
    - egress.execute.start: 0.00ms [plan_4__t0]
    - egress.mergeFilters: 0.00ms [plan_4__t0]
  - egress.executeAsync: 238.81ms [plan_4__t0]
    - egress.execute.start: 0.00ms [plan_4__t0]
    - execution.executeAsync: 223.44ms [plan_4__t0]
      - execution.execute.start: 0.00ms [plan_4__t0]
      - ingress.planIngress: 0.08ms [ingress]
      - ingress.plan.start: 0.00ms [ingress]
      - ingress.prefilter.plan: 0.34ms [plan_4__t0]
      - ingress.prefilter.compile: 8.37ms [plan_4__t0]
      - execution.load.materialize: 195.44ms [plan_4__t0]
        - ingress.file.read: 17.59ms [plan_4__t0]
        - ingress.prefilter.ffi: 131.39ms [plan_4__t0]
        - ingress.prefilter.assemble: 9.47ms [plan_4__t0]
        - ingress.prefilter.parse: 36.07ms [plan_4__t0]
      - execution.execute.end: 0.00ms [plan_4__t0]
    - egress.finalizeOrder: 13.92ms [plan_4__t0]
    - egress.result.final: 0.00ms [plan_4__t0]

Predicate Metrics
- notNull: count=22331 total=2.42ms avg=0.00011ms
- gt: count=22331 total=1.43ms avg=0.00006ms
- eq: count=22331 total=2.09ms avg=0.00009ms
- dateBetween: count=22331 total=3.07ms avg=0.00014ms

Plan Predicate Counts
- notNull: 1
- gt: 1
- eq: 1
- dateBetween: 1

Decisions
- execution [plan_4__t0]: {"costByOp":{"in":1.57,"gt":1.1,"notNull":0.9,"dateBetween":2.6},"mergeCount":0,"predicateCountsByKind":{"builtin":4},"pushdownCount":3,"residualCount":1}
- ingress [ingress]: {"reason":"ordering/grouping","strategy":"eager","supported":{"group":false,"order":false}}
- execution [plan_4__t0]: {"phase":"executeAsync"}
- ingress [plan_4__t0]: {"prefilterChecked":150000,"prefilterMatched":22331,"prefilterParsed":22331,"prefilterSkipped":127669,"prefilterUnknown":0,"prefilterFields":4,"prefilterPredicates":5,"phase":"executeAsync.materialize"}
- egress [plan_4__t0]: {"path":"local","hasOrders":true,"hasSearch":false,"limit":null,"offset":0}
*/