#include <stddef.h>
#include <string.h>

#ifndef PF_KEY_COND
#define PF_KEY_COND (key_len == 0 && memcmp(key_start, "", 0) == 0)
#endif

static inline int pf_node_branch_template(void) {
  const char* key_start = NULL;
  size_t key_len = 0;
  int handled = 0;
  (void)key_start;
  (void)key_len;
  (void)handled;

  /* PF_SNIP_START */
  if (PF_KEY_COND) {
    /* PF_HANDLED_SET */
    /* PF_BRANCH_BODY */
  }
  /* PF_SNIP_END */

  return 0;
}
