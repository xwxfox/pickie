import type { PrefilterStats } from "./types";

export function createPrefilterStats(): PrefilterStats {
    return { checked: 0, matched: 0, parsed: 0, skipped: 0, unknown: 0 };
}
