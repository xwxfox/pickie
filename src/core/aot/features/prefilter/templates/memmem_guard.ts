import source from "./memmem_guard.c" with { type: "text" };

export default {
    id: "memmem_guard",
    source,
    requiredMarkers: [
        "/* PF_SNIP_START */",
        "/* PF_SNIP_END */",
        "/* PF_MEMMEM_BODY */",
    ],
};
