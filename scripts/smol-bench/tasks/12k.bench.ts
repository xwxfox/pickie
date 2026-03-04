
import { Engine, IngressEngine } from "../../../src";
import { data, getRandomNamesArray } from "../random_data";

const start = performance.now();
const input = IngressEngine.from(data)
const filter = Engine.from(input);
const res = filter.in("meta.owner.name",getRandomNamesArray())
    .nested("Logs", p =>
        p.arraySome("tags", tag => tag === "x")
    )
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .out()
    .orderByDate("created", { direction: "desc" })
    .limit(5)
    .result();
const grouped = filter
    .equals("active", true)
    .out()
    .groupBy("meta.owner.name");
const cursor = filter
    .out()
    .orderByDate("created", { direction: "desc" })
    .paginate({ pageSize: 2, total: "lazy" });
const end = performance.now();
console.log(`Query took ${end - start}ms`);