import type { Library } from "bun:ffi";
import { cc } from "bun:ffi";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import type {
    AOTCompileOptions,
    AOTProgramOptions,
    AOTSymbols,
    AOTTemplate,
    AOTTimingTokenLike,
} from "./types";

export type {
    AOTCompileOptions,
    AOTMarker,
    AOTProgramOptions,
    AOTSymbols,
    AOTTemplate,
    AOTTimingTokenLike,
    AOTTraceSource,
    AOTTracer,
} from "./types";

export class AOTProgram<TSymbols extends AOTSymbols, TToken extends AOTTimingTokenLike> {
    private readonly tracer?: AOTProgramOptions<TToken>["tracer"];
    private readonly traceSource: AOTProgramOptions<TToken>["traceSource"];

    constructor(options?: AOTProgramOptions<TToken>) {
        this.tracer = options?.tracer;
        this.traceSource = options?.traceSource ?? "misc";
    }

    buildSource(template: AOTTemplate, planId: string, timingParent?: number | TToken | null): string {
        const timing = this.startTrace("aot.build", planId, timingParent ?? null);
        this.ensureRequiredMarkers(template);
        let output = template.source;
        for (let i = 0; i < template.markers.length; i++) {
            const marker = template.markers[i]!;
            if (marker.condition === false) {
                output = this.replaceMarker(output, marker.name, "");
                continue;
            }
            output = this.replaceMarker(output, marker.name, marker.replaceWith ?? "");
        }
        this.assertReplacedRequiredMarkers(output, template);
        this.endTrace(timing);
        return output;
    }

    compile(
        template: AOTTemplate,
        options: AOTCompileOptions<TSymbols> & { outputPath: string; planId: string; timingParent?: number | TToken | null }
    ): Library<TSymbols> {
        const source = this.buildSource(template, options.planId, options.timingParent ?? null);
        const timing = this.startTrace("aot.compile", options.planId, options.timingParent ?? null);
        this.ensureTempDir(options.outputPath);
        writeFileSync(options.outputPath, source);
        const compiled = cc({
            source: options.outputPath,
            flags: options.flags ?? [],
            symbols: options.symbols,
        });
        this.endTrace(timing);
        return compiled;
    }

    cleanupFile(path: string, planId: string, timingParent?: number | TToken | null): void {
        const timing = this.startTrace("aot.cleanup", planId, timingParent ?? null);
        try { rmSync(path); } catch {
            // best effort cleanup
        }
        this.endTrace(timing);
    }

    private ensureRequiredMarkers(template: AOTTemplate): void {
        for (let i = 0; i < template.requiredMarkers.length; i++) {
            const marker = template.requiredMarkers[i]!;
            if (!this.hasMarkerLine(template.source, marker)) {
                throw new Error(`AOT template ${template.id} missing marker ${marker}`);
            }
        }
    }

    private assertReplacedRequiredMarkers(output: string, template: AOTTemplate): void {
        for (let i = 0; i < template.requiredMarkers.length; i++) {
            const marker = template.requiredMarkers[i]!;
            if (this.hasMarkerLine(output, marker)) {
                throw new Error(`AOT template ${template.id} has unresolved marker ${marker}`);
            }
        }
    }

    private replaceMarker(input: string, marker: string, replacement: string): string {
        const lines = input.split(/\r?\n/);
        const output: Array<string> = [];
        const replacementLines = replacement.split(/\r?\n/);
        let replaced = false;
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i]!;
            if (line.trim() === marker) {
                replaced = true;
                if (replacement === "") {
                    output.push("");
                } else {
                    output.push(...replacementLines);
                }
                continue;
            }
            output.push(line);
        }
        return replaced ? output.join("\n") : input;
    }

    private hasMarkerLine(source: string, marker: string): boolean {
        const lines = source.split(/\r?\n/);
        for (let i = 0; i < lines.length; i++) {
            if (lines[i]!.trim() === marker) {
                return true;
            }
        }
        return false;
    }

    private ensureTempDir(path: string): void {
        const idx = path.lastIndexOf("/");
        if (idx <= 0) {return;}
        const dir = path.slice(0, idx);
        try { mkdirSync(dir, { recursive: true }); } catch {
            // best effort temp dir creation
        }
    }

    private startTrace(label: string, planId: string, parent?: number | TToken | null): TToken | null {
        if (!this.tracer) {return null;}
        const source = this.traceSource ?? "misc";
        return this.tracer.start(source, label, planId, parent ?? null);
    }

    private endTrace(token: TToken | null): void {
        if (!this.tracer || !token) {return;}
        this.tracer.end(token, { skipData: true });
    }
}
