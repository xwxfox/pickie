import {
  Engine,
  IngressEngine,
  Schema,
  fileSource,
  convertBytes,
} from "@/index.ts";
import { resolve } from "node:path";

const availableCountsArr = [
  1000, 10_000, 12_000, 50_000, 60_000, 70_000, 80_000, 120_000, 150_000,
  200_000,
] as const;
type availableCounts = (typeof availableCountsArr)[number];

function setItemCount(def: availableCounts): availableCounts {
  const raw = Number.parseInt(Bun.argv[2] ?? String(def));
  if (!availableCountsArr.includes(raw as availableCounts)) {
    console.warn("Invalid item count for bench. Falling back to", def);
    return def;
  } else {
    return Number(raw) as availableCounts;
  }
}

const TEST_DATA_ITEM_COUNT: availableCounts = setItemCount(150_000);

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
const randomInt = (min: number, max: number) =>
  Math.floor(Math.random() * (max - min)) + min;
const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"];
const getRandomNamesArray = (): Array<string> => {
  const upper = randomInt(1, ownerNames.length + 1);
  const out: Array<string> = [];
  for (let i = 0; i < upper; i++) {
    const random = randomInt(0, ownerNames.length);
    out.push(ownerNames[random]!);
  }
  return out;
};

export const run = async () => {
  const start = performance.now();
  const source = fileSource<LargeItem>(
    resolve(
      import.meta.dir,
      "../perf/bench-data/large-items/" + TEST_DATA_ITEM_COUNT + ".json",
    ),
    {
      format: "json",
      hints: { estimatedCount: TEST_DATA_ITEM_COUNT },
      schema: Schema.inline<LargeItem>(),
      prefilterMode: "auto",
    },
  );

  const input = IngressEngine.fromSource(source);
  const filter = Engine.from(input);
  const res = await filter
    .in("meta.owner.name", getRandomNamesArray()) /* lsp: const res: any */

    .greaterThan("score", 9)
    .valueNotNull("name")
    .dateBetween("created", "2026-01-01", "2026-02-31")
    .configureTagger({
      tags: ["flags:green"],
      rules: [
        {
          field: "flags",
          in: ["green"],
          tag: "flags:green",
        },
      ],
    })
    .out()
    .orderByDate("created", { direction: "desc" })
    .result();
  const end = performance.now();
  console.log(
    `Query took ${end - start}ms - sizes: input=${input.length}, res=${res.length}, byte size of input: ${await Bun.file(
      resolve(
        import.meta.dir,
        "../perf/bench-data/large-items/" + TEST_DATA_ITEM_COUNT + ".json",
      ),
    )
      .stat()
      .then((s) => convertBytes(s.size))}`,
  );
};

if (import.meta.main) {
  await run();
}
