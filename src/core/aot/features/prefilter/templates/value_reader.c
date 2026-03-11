#include <stddef.h>
#include <stdbool.h>
#include <string.h>

int scan_string(const char** pptr, const char* end, const char** out, size_t* out_len, bool* needs_unescape);
int parse_number(const char** pptr, const char* end, double* out);

static inline int pf_value_reader_template(void) {
  const char* p = NULL;
  const char* end = NULL;
  (void)p;
  (void)end;

  /* PF_SNIP_START */
  if (*p == '"') {
    const char* value_start = NULL; size_t value_len = 0; bool value_needs_unescape = false;
    if (!scan_string(&p, end, &value_start, &value_len, &value_needs_unescape)) { return -1; }
    /* PF_VALUE_STRING_CHECKS */
  } else if (*p == '-' || (*p >= '0' && *p <= '9')) {
    double number = 0;
    if (!parse_number(&p, end, &number)) { return -1; }
    /* PF_VALUE_NUMBER_CHECKS */
  } else if (p + 4 <= end && memcmp(p, "true", 4) == 0) {
    p += 4;
    int bool_val = 1;
    /* PF_VALUE_BOOL_CHECKS */
  } else if (p + 5 <= end && memcmp(p, "false", 5) == 0) {
    p += 5;
    int bool_val = 0;
    /* PF_VALUE_BOOL_CHECKS */
  } else if (p + 4 <= end && memcmp(p, "null", 4) == 0) {
    p += 4;
    /* PF_VALUE_NULL_CHECKS */
  } else {
    return -1;
  }
  /* PF_SNIP_END */

  return 0;
}
