import { currentRuntime } from "./runtime";

export const readFileToArrayBuffer = async (
  path: string,
): Promise<ArrayBuffer> => {
  switch (currentRuntime) {
    case "bun":
      return Bun.file(path).arrayBuffer();
    case "deno":
      //@ts-expect-error
      return Deno.readFile(path) as Promise<ArrayBuffer>;
    case "browser":
      return fetch(path).then((response) => {
        if (!response.ok) {
          throw new Error(
            `Failed to fetch file at ${path}: ${response.status} ${response.statusText}`,
          );
        }
        return response.arrayBuffer();
      });
    case "node":
    default:
      return import("node:fs/promises").then((fs) =>
        fs.readFile(path).then((file) => Buffer.from(file).buffer),
      );
  }
};
export function convertBytes(
  bytes: number,
  options: { useBinaryUnits?: boolean; decimals?: number } = {},
): string {
  const { useBinaryUnits = false, decimals = 2 } = options;

  if (decimals < 0) {
    throw new Error(`Invalid decimals ${decimals}`);
  }

  const base = useBinaryUnits ? 1024 : 1000;
  const units = useBinaryUnits
    ? ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
    : ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

  const i = Math.floor(Math.log(bytes) / Math.log(base));

  return `${(bytes / Math.pow(base, i)).toFixed(decimals)} ${units[i]}`;
}
