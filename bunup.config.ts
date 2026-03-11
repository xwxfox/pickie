import { defineConfig, type DefineConfigItem } from "bunup";
export default defineConfig({
    name: "picky",
    entry: "src/index.ts",
    dts: {
        inferTypes: true,
        tsgo: true,
    },
    minify: true,
    clean: true,
    format: "esm",
    target: "bun",
    minifySyntax: true,
    minifyWhitespace: true,
    unused: true,
    banner: `/* You can import the TS source directly with --> import { thing } from "pickie/src"; <-- if thats more your style :3 */\n`,
    exports: {
        customExports: () => ({
            "./package.json": "./package.json",
            "./tsconfig.json": "./tsconfig.json",
            "./src": {
                "import": {
                    "types": "./src/index.ts",
                    "default": "./src/index.ts",
                    "bun": "./src/index.ts"
                }
            },
            ".": {
                "import": {
                    "types": "./dist/index.d.ts",
                    "default": "./dist/index.js",
                }
            }
        }),
    },
    sourcemap: false,
}) as DefineConfigItem


