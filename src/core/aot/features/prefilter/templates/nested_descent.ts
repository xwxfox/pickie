import source from "./nested_descent.c" with { type: "text" };

export default {
    id: "nested_descent",
    source,
    requiredMarkers: [
        "/* PF_SNIP_START */",
        "/* PF_SNIP_END */",
        "PF_KEY_VAR",
        "PF_KEY_LEN_VAR",
        "PF_KEY_UNESC_VAR",
        "PF_HANDLED_VAR",
        "/* PF_INNER_MATCHING */",
    ],
};
