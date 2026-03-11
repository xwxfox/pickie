import source from "./value_reader.c" with { type: "text" };

export default {
    id: "value_reader",
    source,
    requiredMarkers: [
        "/* PF_SNIP_START */",
        "/* PF_SNIP_END */",
        "/* PF_VALUE_STRING_CHECKS */",
        "/* PF_VALUE_NUMBER_CHECKS */",
        "/* PF_VALUE_BOOL_CHECKS */",
        "/* PF_VALUE_NULL_CHECKS */",
    ],
};
