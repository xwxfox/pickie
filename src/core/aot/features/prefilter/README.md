# prefilter (aot)

smol lil C-backed prefilter used by ingress to skip full JSON parsing when we can safely fast-path.

## what it does

- builds plan-specific C code from templates
- compiles + caches small FFI programs
- filters JSON/NDJSON/array inputs before full parse

## api

- `getPrefilterProgram(plan, options)`
- `runPrefilter(program, bytes)`
- `disposePrefilterProgram(program)`
- `clearPrefilterCache()`

## template markers

- `PF_PREDICATE_STATE`
- `PF_ROOT_MATCHING`
- `PF_MEMMEM_GUARD`
- `PF_MEMMEM_FALLBACK`
- `PF_ALL_SEEN_CHECK`
- `PF_PREDICATE_EVAL`
