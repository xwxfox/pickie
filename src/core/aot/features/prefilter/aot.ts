import { ptr } from "bun:ffi";
import { AOTProgram, type AOTTemplate, type AOTTracer, type AOTTimingTokenLike } from "@/core/aot";
import templateSource from "./prefilter.c" with { type: "text" };
import type { PrefilterPlan } from "./types";
import valueReaderTemplate from "./templates/value_reader";
import nestedDescentTemplate from "./templates/nested_descent";
import predicateCheckTemplate from "./templates/predicate_check";
import nodeBranchTemplate from "./templates/node_branch";
import memmemGuardTemplate from "./templates/memmem_guard";

const snippetStart = "/* PF_SNIP_START */";
const snippetEnd = "/* PF_SNIP_END */";

function buildTemplateSnippet(
    template: { source: string; requiredMarkers: ReadonlyArray<string> },
    replacements: Record<string, string>
): string {
    for (let i = 0; i < template.requiredMarkers.length; i++) {
        const marker = template.requiredMarkers[i]!;
        if (!template.source.includes(marker)) {
            throw new Error(`AOT template missing marker ${marker}`);
        }
    }
    const startIndex = template.source.indexOf(snippetStart);
    const endIndex = template.source.indexOf(snippetEnd);
    if (startIndex < 0 || endIndex < 0 || endIndex <= startIndex) {
        throw new Error("AOT template missing snippet markers");
    }
    let output = template.source.slice(startIndex + snippetStart.length, endIndex);
    if (output.startsWith("\n")) { output = output.slice(1); }
    if (output.endsWith("\n")) { output = output.slice(0, -1); }
    for (const [marker, value] of Object.entries(replacements)) {
        output = output.split(marker).join(value);
    }
    for (let i = 0; i < template.requiredMarkers.length; i++) {
        const marker = template.requiredMarkers[i]!;
        if (output.includes(marker)) {
            throw new Error(`AOT template has unresolved marker ${marker}`);
        }
    }
    return output;
}

type PrefilterSymbols = {
    prefilter: { args: ["ptr", "usize"]; returns: "i32" };
    prefilter_ndjson: { args: ["ptr", "usize", "ptr", "usize"]; returns: "i32" };
    prefilter_json_array: { args: ["ptr", "usize", "ptr", "usize"]; returns: "i32" };
};

export type PrefilterProgram = {
    fn: ReturnType<AOTProgram<PrefilterSymbols, AOTTimingTokenLike>["compile"]>["symbols"]["prefilter"];
    fnNdjson: ReturnType<AOTProgram<PrefilterSymbols, AOTTimingTokenLike>["compile"]>["symbols"]["prefilter_ndjson"];
    fnJsonArray: ReturnType<AOTProgram<PrefilterSymbols, AOTTimingTokenLike>["compile"]>["symbols"]["prefilter_json_array"];
    key: string;
    cleanup: () => Promise<void>;
};

export type PrefilterProgramOptions = {
    planId?: string;
    timingParent?: number | AOTTimingTokenLike | null;
    tracer?: AOTTracer<AOTTimingTokenLike>;
};

type PrefilterCacheEntry = {
    program: PrefilterProgram;
    compiled: ReturnType<AOTProgram<PrefilterSymbols, AOTTimingTokenLike>["compile"]>;
    path: string;
    key: string;
    closed: boolean;
    cleanupPromise?: Promise<void>;
    aot: AOTProgram<PrefilterSymbols, AOTTimingTokenLike>;
    planId: string;
    timingParent?: number | AOTTimingTokenLike | null;
};

const programCache = new Map<string, PrefilterCacheEntry>();
const programEntries = new WeakMap<PrefilterProgram, PrefilterCacheEntry>();
const cacheOrder: Array<string> = [];
const maxCacheSize = 64;
const encoder = new TextEncoder();
const requiredMarkers = [
    "PF_PREDICATE_STATE",
    "PF_ROOT_MATCHING",
    "PF_MEMMEM_GUARD",
    "PF_MEMMEM_FALLBACK",
    "PF_ALL_SEEN_CHECK",
    "PF_PREDICATE_EVAL",
];

export function getPrefilterProgram(plan: PrefilterPlan, options?: PrefilterProgramOptions): PrefilterProgram {
    const key = plan.key;
    const cached = programCache.get(key);
    if (cached && !cached.closed) {
        return cached.program;
    }
    if (cached?.closed) {
        programCache.delete(key);
    }
    const planId = options?.planId ?? key;
    const aot = new AOTProgram<PrefilterSymbols, AOTTimingTokenLike>({ tracer: options?.tracer, traceSource: "ingress" });
    const template = buildPrefilterTemplate(plan);
    const filename = `prefilter-${key.replaceAll(/[^a-zA-Z0-9_-]/g, "_")}-${Date.now()}.c`;
    const path = `/tmp/pickie/${filename}`;
    const compiled = aot.compile(template, {
        outputPath: path,
        planId,
        timingParent: options?.timingParent ?? null,
        flags: ["-O3"],
        symbols: {
            prefilter: {
                args: ["ptr", "usize"],
                returns: "i32",
            },
            prefilter_ndjson: {
                args: ["ptr", "usize", "ptr", "usize"],
                returns: "i32",
            },
            prefilter_json_array: {
                args: ["ptr", "usize", "ptr", "usize"],
                returns: "i32",
            },
        } satisfies PrefilterSymbols,
    });
    const program: PrefilterProgram = {
        fn: compiled.symbols.prefilter,
        fnNdjson: compiled.symbols.prefilter_ndjson,
        fnJsonArray: compiled.symbols.prefilter_json_array,
        key,
        cleanup: async () => cleanupEntry(program),
    };
    const entry: PrefilterCacheEntry = {
        program,
        compiled,
        path,
        key,
        closed: false,
        aot,
        planId,
        timingParent: options?.timingParent ?? null,
    };
    programCache.set(key, entry);
    programEntries.set(program, entry);
    cacheOrder.push(key);
    if (cacheOrder.length > maxCacheSize) {
        const evicted = cacheOrder.shift();
        if (evicted) {
            const evictedEntry = programCache.get(evicted);
            if (evictedEntry) {
                cleanupEntry(evictedEntry.program);
                programCache.delete(evicted);
            }
        }
    }
    return program;
}

export function runPrefilter(program: PrefilterProgram, bytes: Uint8Array): number {
    const entry = programEntries.get(program);
    if (entry?.closed) {
        return -1;
    }
    const pointer = ptr(bytes);
    return program.fn(pointer, bytes.byteLength);
}

export async function disposePrefilterProgram(program: PrefilterProgram): Promise<void> {
    const entry = programEntries.get(program);
    if (!entry) {return;}
    programCache.delete(entry.key);
    removeFromCacheOrder(entry.key);
    await cleanupEntry(program);
}

export async function clearPrefilterCache(): Promise<void> {
    const entries = Array.from(programCache.values());
    programCache.clear();
    cacheOrder.length = 0;
    for (let i = 0; i < entries.length; i++) {
        await cleanupEntry(entries[i]!.program);
    }
}

async function cleanupEntry(program: PrefilterProgram): Promise<void> {
    const entry = programEntries.get(program);
    if (!entry) {return;}
    if (entry.closed) {
        if (entry.cleanupPromise) {
            await entry.cleanupPromise;
        }
        return;
    }
    entry.closed = true;
    entry.cleanupPromise = (async () => {
        await entry.compiled.close();
        entry.aot.cleanupFile(entry.path, entry.planId, entry.timingParent ?? null);
    })();
    await entry.cleanupPromise;
}

function removeFromCacheOrder(key: string): void {
    const idx = cacheOrder.indexOf(key);
    if (idx >= 0) {
        cacheOrder.splice(idx, 1);
    }
}

// Field tree node for nested path support.
// Each node can be a branch (has children) and/or a leaf (has predicateIndices).
type FieldNode = {
    children: Map<string, FieldNode>;
    predicateIndices: Array<number>;
};

function buildFieldTree(plan: PrefilterPlan): FieldNode {
    const root: FieldNode = { children: new Map(), predicateIndices: [] };
    for (let i = 0; i < plan.predicates.length; i++) {
        const pred = plan.predicates[i]!;
        const segs = pred.segments ? pred.segments : [pred.field];
        let node = root;
        for (let s = 0; s < segs.length - 1; s++) {
            const seg = segs[s]!;
            let child = node.children.get(seg);
            if (!child) {
                child = { children: new Map(), predicateIndices: [] };
                node.children.set(seg, child);
            }
            node = child;
        }
        const lastSeg = segs.at(-1)!;
        let leaf = node.children.get(lastSeg);
        if (!leaf) {
            leaf = { children: new Map(), predicateIndices: [] };
            node.children.set(lastSeg, leaf);
        }
        leaf.predicateIndices.push(i);
    }
    return root;
}

function selectMemmemCandidate(plan: PrefilterPlan): { values: Array<string>; predicateIndex: number } | null {
    let bestCandidate: { values: Array<string>; predicateIndex: number; count: number } | null = null;

    for (let i = 0; i < plan.predicates.length; i++) {
        const pred = plan.predicates[i]!;
        // Only string eq/in predicates are useful for memmem pre-screening
        if (pred.type !== "string") {continue;}

        let values: Array<string>;
        if (pred.op === "eq") {
            values = [String(pred.value ?? "")];
        } else if (pred.op === "in") {
            values = ((pred.value as ReadonlyArray<string> | undefined) ?? []).map(String);
        } else {
            // ne, notIn, gt, gte, lt, lte - skip
            continue;
        }

        if (values.length === 0) {continue;}

        // Pick the candidate with the fewest values (most selective)
        if (!bestCandidate || values.length < bestCandidate.count) {
            bestCandidate = { values, predicateIndex: i, count: values.length };
        }
    }

    return bestCandidate ? { values: bestCandidate.values, predicateIndex: bestCandidate.predicateIndex } : null;
}

function buildPrefilterTemplate(plan: PrefilterPlan): AOTTemplate {
    const predicates = plan.predicates;
    const fieldTree = buildFieldTree(plan);
    const numPredicates = predicates.length;

    const predicateState = [
        ...predicates.map((_pred, index) => `int p${index}_seen = 0; int p${index}_pass = 0;`),
        "int seen_count = 0;",
    ].join("\n");

    // predicateEval for the _ex version: endptr is already set at eval label, so return 0 directly
    const predicateEval = predicates.map((pred, index) => {
        if (pred.op === "ne") {
            return `if (!p${index}_seen) { p${index}_pass = 1; } if (!p${index}_pass) { return 0; }`;
        }
        if (pred.op === "notIn") {
            return `if (!p${index}_seen) { p${index}_pass = 1; } if (!p${index}_pass) { return 0; }`;
        }
        return `if (!p${index}_seen) { p${index}_pass = 0; } if (!p${index}_pass) { return 0; }`;
    }).join("\n");

    const rootMatching = renderNodeMatching(fieldTree, plan, 0);

    // Optimization #3: memmem pre-screening for NDJSON
    const memmemCandidate = selectMemmemCandidate(plan);
    let memmemGuard = "";
    let memmemFallback = "";
    if (memmemCandidate) {
        const checks = memmemCandidate.values.map((value) => {
            const bytes = encoder.encode(`"${value}"`);
            const literal = bytesToCString(bytes);
            const len = bytes.length;
            return `if (memmem(line_start, line_len, "${literal}", ${len}) != NULL) { memmem_hit = 1; }`;
        }).join("\n");
        const memmemBody = `int memmem_hit = 0;\n${checks}\nif (!memmem_hit) {\n  result = 0;\n} else {\n  result = prefilter_object(line_start, line_len);\n}`;
        memmemGuard = buildTemplateSnippet(memmemGuardTemplate, {
            "/* PF_MEMMEM_BODY */": memmemBody,
        });
    } else {
        memmemFallback = "result = prefilter_object(line_start, line_len);";
    }

    // Optimization #2: early exit check after each key-value pair
    const allSeenCheck = `if (seen_count >= ${numPredicates}) {\n  const char* rest = skip_object_rest(p, end, 1);\n  if (!rest) { return -1; }\n  *endptr = rest;\n  goto eval;\n}`;

    const markers = [
        { name: "PF_PREDICATE_STATE", replaceWith: indentBlock(predicateState, 2) },
        { name: "PF_ROOT_MATCHING", replaceWith: indentBlock(rootMatching, 4) },
        { name: "PF_MEMMEM_GUARD", replaceWith: indentBlock(memmemGuard, 4) },
        { name: "PF_MEMMEM_FALLBACK", replaceWith: indentBlock(memmemFallback, 4) },
        { name: "PF_ALL_SEEN_CHECK", replaceWith: indentBlock(allSeenCheck, 4) },
        { name: "PF_PREDICATE_EVAL", replaceWith: indentBlock(predicateEval, 2) },
    ];

    return {
        id: "prefilter",
        source: templateSource,
        markers,
        requiredMarkers,
    };
}

function indentBlock(block: string, spaces: number): string {
    if (block.trim() === "") {return "";}
    const pad = " ".repeat(spaces);
    return block.split(/\r?\n/).map((line) => line.length === 0 ? "" : `${pad}${line}`).join("\n");
}

function renderNodeMatching(node: FieldNode, plan: PrefilterPlan, depth: number): string {
    const entries = Array.from(node.children.entries());
    if (entries.length === 0) {return "";}

    const keyVar = depth === 0 ? "key_start" : `key${depth}_start`;
    const keyLenVar = depth === 0 ? "key_len" : `key${depth}_len`;
    const handledVar = depth === 0 ? "handled" : `handled${depth}`;

    const branches = entries.map(([key, child]) => {
        const keyBytes = encoder.encode(key);
        const keyLiteral = bytesToCString(keyBytes);
        const keyLen = keyBytes.length;

        const hasPredicates = child.predicateIndices.length > 0;
        const hasChildren = child.children.size > 0;

        let body: string;

        if (hasPredicates && !hasChildren) {
            // Pure leaf - read value and check predicates
            body = renderValueReaderByIndices(child.predicateIndices, plan);
        } else if (!hasPredicates && hasChildren) {
            // Pure branch - descend into nested object
            body = renderNestedDescent(child, plan, depth + 1);
        } else if (hasPredicates && hasChildren) {
            // Both leaf and branch - rare but possible
            // If value is '{', descend; otherwise evaluate leaf predicates
            body = `if (*p == '{') {\n        ${renderNestedDescentInner(child, plan, depth + 1)}\n      } else {\n        ${renderValueReaderByIndicesInline(child.predicateIndices, plan)}\n      }`;
        } else {
            body = `if (!skip_value(&p, end)) { return -1; }`;
        }

        const branchBody = body;
        const snippet = buildTemplateSnippet(nodeBranchTemplate, {
            "PF_KEY_COND": `(${keyLenVar} == ${keyLen} && memcmp(${keyVar}, "${keyLiteral}", ${keyLen}) == 0)`,
            "/* PF_HANDLED_SET */": `${handledVar} = 1;`,
            "/* PF_BRANCH_BODY */": branchBody,
        });
        return snippet;
    }).join(" else ");

    return branches;
}

function renderNestedDescent(node: FieldNode, plan: PrefilterPlan, depth: number): string {
    const inner = renderNestedDescentInner(node, plan, depth);
    return `if (*p == '{') {\n        ${inner}\n      } else {\n        // Expected object but got something else - skip\n        if (!skip_value(&p, end)) { return -1; }\n      }`;
}

function renderNestedDescentInner(node: FieldNode, plan: PrefilterPlan, depth: number): string {
    const keyVar = `key${depth}_start`;
    const keyLenVar = `key${depth}_len`;
    const keyUnescVar = `key${depth}_needs_unescape`;
    const handledVar = `handled${depth}`;

    const innerMatching = renderNodeMatching(node, plan, depth);
    const snippet = buildTemplateSnippet(nestedDescentTemplate, {
        PF_KEY_VAR: keyVar,
        PF_KEY_LEN_VAR: keyLenVar,
        PF_KEY_UNESC_VAR: keyUnescVar,
        PF_HANDLED_VAR: handledVar,
        "/* PF_INNER_MATCHING */": innerMatching,
    });
    return snippet;
}

function renderValueReaderByIndices(indices: Array<number>, plan: PrefilterPlan): string {
    const refs = indices.map((index) => ({ pred: plan.predicates[index]!, index }));
    if (refs.length === 0) {return "";}
    const stringChecks = refs.map((entry) => renderPredicateValueCheck(entry.pred, entry.index, "string")).join("\n");
    const numberChecks = refs.map((entry) => renderPredicateValueCheck(entry.pred, entry.index, "number")).join("\n");
    const boolChecks = refs.map((entry) => renderPredicateValueCheck(entry.pred, entry.index, "boolean")).join("\n");
    const nullChecks = refs.map((entry) => renderPredicateValueCheck(entry.pred, entry.index, "null")).join("\n");
    return buildTemplateSnippet(valueReaderTemplate, {
        "/* PF_VALUE_STRING_CHECKS */": stringChecks,
        "/* PF_VALUE_NUMBER_CHECKS */": numberChecks,
        "/* PF_VALUE_BOOL_CHECKS */": boolChecks,
        "/* PF_VALUE_NULL_CHECKS */": nullChecks,
    });
}

// Same as renderValueReaderByIndices but without wrapping - for inline use in combined branch+leaf nodes
function renderValueReaderByIndicesInline(indices: Array<number>, plan: PrefilterPlan): string {
    return renderValueReaderByIndices(indices, plan);
}

function renderPredicateValueCheck(
    pred: PrefilterPlan["predicates"][number],
    index: number,
    valueType: "string" | "number" | "boolean" | "null"
): string {
    const tag = `p${index}`;
    const seen = `if (!${tag}_seen) { seen_count++; } ${tag}_seen = 1;`;
    // Mandatory predicates can trigger early-exit on failure.
    // "ne" and "notIn" are soft - missing field means pass, so no early-exit on fail.
    const isMandatory = pred.op !== "ne" && pred.op !== "notIn";
    const earlyExit = isMandatory ? `if (!${tag}_pass) { goto fail_early; }` : "";

    if (pred.op === "isNull") {
        return buildTemplateSnippet(predicateCheckTemplate, {
            "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = ${valueType === "null" ? "1" : "0"};\n${earlyExit}`,
        });
    }
    if (pred.op === "notNull") {
        return buildTemplateSnippet(predicateCheckTemplate, {
            "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = ${valueType === "null" ? "0" : "1"};\n${earlyExit}`,
        });
    }
    if (pred.op === "gt" || pred.op === "gte" || pred.op === "lt" || pred.op === "lte") {
        if (valueType === "number" && pred.type === "number") {
            const value = Number(pred.value ?? 0);
            const op = pred.op === "gt"
                ? ">"
                : pred.op === "gte"
                    ? ">="
                    : pred.op === "lt"
                        ? "<"
                        : "<=";
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = (number ${op} ${value});\n${earlyExit}`,
            });
        }
        if (valueType === "string" && pred.type === "string") {
            const targetStr = String(pred.value ?? "");
            const targetBytes = encoder.encode(targetStr);
            const targetLiteral = bytesToCString(targetBytes);
            const targetLen = targetBytes.length;
            // Lexicographic string comparison for date range checks.
            // Compare using memcmp on the shorter length, then break ties by length.
            const cmpOp = pred.op === "gt"
                ? "> 0"
                : pred.op === "gte"
                    ? ">= 0"
                    : pred.op === "lt"
                        ? "< 0"
                        : "<= 0";
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\n{\n  size_t min_len = value_len < ${targetLen} ? value_len : ${targetLen};\n  int cmp = memcmp(value_start, "${targetLiteral}", min_len);\n  if (cmp == 0) { cmp = (int)value_len - (int)${targetLen}; }\n  ${tag}_pass = (cmp ${cmpOp});\n}\n${earlyExit}`,
            });
        }
        // Type mismatch - fail this predicate
        return buildTemplateSnippet(predicateCheckTemplate, {
            "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = 0;\n${earlyExit}`,
        });
    }

    if (pred.op === "eq" || pred.op === "ne" || pred.op === "in" || pred.op === "notIn") {
        if (pred.type === "string" && valueType === "string") {
            const values = pred.op === "in" || pred.op === "notIn"
                ? (pred.value as ReadonlyArray<string> | undefined) ?? []
                : [String(pred.value ?? "")];
            const literal = values.map((value) => {
                const bytes = encoder.encode(String(value));
                const encoded = bytesToCString(bytes);
                return { encoded, length: bytes.length };
            });
            const checks = literal
                .map(({ encoded, length }) => `match_string(value_start, value_len, "${encoded}", ${length}, value_needs_unescape) == 1`)
                .join(" || ");
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\nint match = ${checks.length > 0 ? `(${checks})` : "0"};\n${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};\n${earlyExit}`,
            });
        }
        if (pred.type === "number" && valueType === "number") {
            const values = pred.op === "in" || pred.op === "notIn"
                ? (pred.value as ReadonlyArray<number> | undefined) ?? []
                : [Number(pred.value ?? 0)];
            const checks = values.map((value) => `number == ${value}`).join(" || ");
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\nint match = ${checks.length > 0 ? `(${checks})` : "0"};\n${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};\n${earlyExit}`,
            });
        }
        if (pred.type === "boolean" && valueType === "boolean") {
            const values = pred.op === "in" || pred.op === "notIn"
                ? (pred.value as ReadonlyArray<boolean> | undefined) ?? []
                : [Boolean(pred.value)];
            const checks = values.map((value) => `bool_val == ${value ? "1" : "0"}`).join(" || ");
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\nint match = ${checks.length > 0 ? `(${checks})` : "0"};\n${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "!match" : "match"};\n${earlyExit}`,
            });
        }
        if (pred.type === "null" && valueType === "null") {
            return buildTemplateSnippet(predicateCheckTemplate, {
                "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "0" : "1"};\n${earlyExit}`,
            });
        }
        return buildTemplateSnippet(predicateCheckTemplate, {
            "/* PF_PREDICATE_BODY */": `${seen}\n${tag}_pass = ${pred.op === "ne" || pred.op === "notIn" ? "1" : "0"};\n${earlyExit}`,
        });
    }
    return "";
}

function bytesToCString(bytes: Uint8Array): string {
    let output = "";
    for (let i = 0; i < bytes.length; i++) {
        const value = bytes[i]!;
        if (value === 0x22) { output += String.raw`\"`; continue; }
        if (value === 0x5C) { output += String.raw`\\`; continue; }
        if (value >= 0x20 && value <= 0x7E) {
            output += String.fromCharCode(value);
            continue;
        }
        output += `\\x${value.toString(16).padStart(2, "0")}`;
    }
    return output;
}
