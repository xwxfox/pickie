import source from "./predicate_check.c" with { type: "text" };

export default {
    id: "predicate_check",
    source,
    requiredMarkers: [
        "/* PF_SNIP_START */",
        "/* PF_SNIP_END */",
        "/* PF_PREDICATE_BODY */",
    ],
};
