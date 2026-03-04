const {
  randomInt,
} = await import('node:crypto');

type LargeItem = {
            id: number;
            active: boolean;
            score: number;
            name: string | null;
            created: Date | string;
            meta: {
                owner: {
                    name: string;
                    nickname?: string | null;
                };
            };
            Logs: Array<{ type: string; tags: string[]; when: Date | string }>;
            flags: string[];
        };

        const mulberry32 = (seed: number) => {
            let t = seed;
            return () => {
                t += 0x6D2B79F5;
                let value = t;
                value = Math.imul(value ^ (value >>> 15), value | 1);
                value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
                return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
            };
        };

        const rng = mulberry32(1337);
        const alphabet = "abcdefghijklmnopqrstuvwxyz";
      
        const randomString = (len: number) => {
            let out = "";
            for (let i = 0; i < len; i++) {
                out += alphabet[randomInt(alphabet.length)]!;
            }
            return out;
        };

        const ownerNames = ["Alice", "Bob", "Cara", "Dee", "Eli"];
        const logTypes = ["WARN", "INFO", "ERROR"];
        const tags = ["red", "green", "blue", "amber"];

        const data: LargeItem[] = [];
        for (let i = 0; i < 12000; i++) {
            const baseName = i % 7 === 0 ? `ab${randomString(4)}` : randomString(6);
            const name = i % 31 === 0 ? null : baseName;
            const score = i % 97 === 0 ? Number.NaN : randomInt(50);
            const created = i % 2 === 0
                ? new Date(2024, 0, (i % 28) + 1)
                : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
            const owner = ownerNames[randomInt(ownerNames.length)]!;
            const nick = rng() > 0.8 ? `${owner[0]}${randomInt(9)}` : null;
            const logCount = randomInt(3);
            const Logs: Array<{ type: string; tags: string[]; when: Date | string }> = [];
            for (let j = 0; j < logCount; j++) {
                const type = logTypes[randomInt(logTypes.length)]!;
                const tagCount = randomInt(3);
                const logTags: string[] = [];
                for (let k = 0; k < tagCount; k++) {
                    logTags.push(tags[randomInt(tags.length)]!);
                }
                const when = j % 2 === 0
                    ? new Date(2024, 0, (i % 28) + 1)
                    : `2024-01-${String((i % 28) + 1).padStart(2, "0")}T00:00:00.000Z`;
                Logs.push({ type, tags: logTags, when });
            }
            const flagCount = randomInt(3);
            const flags: string[] = [];
            for (let j = 0; j < flagCount; j++) {
                flags.push(tags[randomInt(tags.length)]!);
            }

            data.push({
                id: i,
                active: i % 2 === 0,
                score,
                name,
                created,
                meta: { owner: { name: owner, nickname: nick } },
                Logs,
                flags,
            });
        }

        const getRandomNamesArray = (): string[] => {
            const out: string[] = [];
            for (let i = 0; i < randomInt(1, ownerNames.length + 1); i++) {
                out.push(ownerNames[randomInt(ownerNames.length)]!);
            }
            return out;
        }

        export { data, getRandomNamesArray, type LargeItem };