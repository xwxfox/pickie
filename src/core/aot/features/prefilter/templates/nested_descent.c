#include <stddef.h>
#include <stdbool.h>

const char* skip_ws(const char* p, const char* end);
int scan_string(const char** pptr, const char* end, const char** out, size_t* out_len, bool* needs_unescape);
int skip_value(const char** pptr, const char* end);

static inline int pf_nested_descent_template(void) {
  const char* p = NULL;
  const char* end = NULL;
  int _nest_depth = 0;
  (void)p;
  (void)end;
  (void)_nest_depth;

  /* PF_SNIP_START */
  _nest_depth++;
  p++; // skip '{'
  while (p < end) {
    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == '}') { p++; _nest_depth--; break; }

    const char* PF_KEY_VAR = NULL;
    size_t PF_KEY_LEN_VAR = 0;
    bool PF_KEY_UNESC_VAR = false;
    if (!scan_string(&p, end, &PF_KEY_VAR, &PF_KEY_LEN_VAR, &PF_KEY_UNESC_VAR)) { return -1; }
    if (PF_KEY_UNESC_VAR) { return -1; }
    p = skip_ws(p, end);
    if (p >= end || *p != ':') { return -1; }
    p++;
    p = skip_ws(p, end);
    if (p >= end) { return -1; }

    int PF_HANDLED_VAR = 0;
    /* PF_INNER_MATCHING */

    if (!PF_HANDLED_VAR) {
      if (!skip_value(&p, end)) { return -1; }
    }

    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == ',') { p++; continue; }
    if (*p == '}') { p++; _nest_depth--; break; }
    return -1;
  }
  /* PF_SNIP_END */
  return 0;
}
