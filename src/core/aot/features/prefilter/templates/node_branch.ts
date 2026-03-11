import source from "./node_branch.c" with { type: "text" };

export default {
    id: "node_branch",
    source,
    requiredMarkers: [
        "/* PF_SNIP_START */",
        "/* PF_SNIP_END */",
        "PF_KEY_COND",
        "/* PF_HANDLED_SET */",
        "/* PF_BRANCH_BODY */",
    ],
};
