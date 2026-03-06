type TimingSource = "performance" | "hrtime";

let timingSource: TimingSource = "performance";

export function setTimingSource(source: TimingSource): void {
    timingSource = source;
}

export function getTimingSource(): TimingSource {
    return timingSource;
}

export function nowMs(): number {
    if (timingSource === "hrtime") {
        return Number(process.hrtime.bigint()) / 1_000_000;
    }
    return performance.now();
}
