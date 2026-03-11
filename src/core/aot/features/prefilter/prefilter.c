#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

// Fast path JSON object scanner with nested field support.
// Returns 1 = match, 0 = fail, -1 = unknown (parse required).
// - If we see something we don't understand (arrays/objects, bad escapes), we return -1.
// - That means "fall back to JSON.parse" in JS, so we never drop valid data.

#ifndef PF_PREDICATE_STATE
#define PF_PREDICATE_STATE /* empty */
#endif

#ifndef PF_ROOT_MATCHING
#define PF_ROOT_MATCHING /* empty */
#endif

#ifndef PF_MEMMEM_GUARD
#define PF_MEMMEM_GUARD /* empty */
#endif

#ifndef PF_MEMMEM_FALLBACK
#define PF_MEMMEM_FALLBACK /* empty */
#endif

#ifndef PF_ALL_SEEN_CHECK
#define PF_ALL_SEEN_CHECK /* empty */
#endif

#ifndef PF_PREDICATE_EVAL
#define PF_PREDICATE_EVAL /* empty */
#endif

static inline int is_space(char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

static const char* skip_ws(const char* p, const char* end) {
  while (p < end && is_space(*p)) { p++; }
  return p;
}

static int scan_string(const char** pptr, const char* end, const char** out, size_t* out_len, bool* needs_unescape) {
  const char* p = *pptr;
  if (p >= end || *p != '"') { return 0; }
  p++;
  const char* start = p;
  bool escape = false;
  while (p < end) {
    char c = *p;
    if (c == '\\') {
      escape = true;
      p++;
      if (p >= end) { return 0; }
      p++;
      continue;
    }
    if (c == '"') {
      *out = start;
      *out_len = (size_t)(p - start);
      *needs_unescape = escape;
      p++;
      *pptr = p;
      return 1;
    }
    p++;
  }
  return 0;
}

static int decode_hex(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
  return -1;
}

// Minimal JSON string unescape ("\\", "\"", "\/", "\b", "\f", "\n", "\r", "\t", "\\uXXXX").
// We only run this when a string contains a backslash.
static int unescape_string(const char* input, size_t len, char* output, size_t* out_len) {
  size_t i = 0;
  size_t o = 0;
  while (i < len) {
    char c = input[i++];
    if (c != '\\') {
      output[o++] = c;
      continue;
    }
    if (i >= len) { return 0; }
    char esc = input[i++];
    switch (esc) {
      case '"': output[o++] = '"'; break;
      case '\\': output[o++] = '\\'; break;
      case '/': output[o++] = '/'; break;
      case 'b': output[o++] = '\b'; break;
      case 'f': output[o++] = '\f'; break;
      case 'n': output[o++] = '\n'; break;
      case 'r': output[o++] = '\r'; break;
      case 't': output[o++] = '\t'; break;
      case 'u': {
        if (i + 4 > len) { return 0; }
        int h1 = decode_hex(input[i]);
        int h2 = decode_hex(input[i + 1]);
        int h3 = decode_hex(input[i + 2]);
        int h4 = decode_hex(input[i + 3]);
        if (h1 < 0 || h2 < 0 || h3 < 0 || h4 < 0) { return 0; }
        int code = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
        i += 4;
        if (code <= 0x7F) {
          output[o++] = (char)code;
        } else if (code <= 0x7FF) {
          output[o++] = (char)(0xC0 | ((code >> 6) & 0x1F));
          output[o++] = (char)(0x80 | (code & 0x3F));
        } else {
          output[o++] = (char)(0xE0 | ((code >> 12) & 0x0F));
          output[o++] = (char)(0x80 | ((code >> 6) & 0x3F));
          output[o++] = (char)(0x80 | (code & 0x3F));
        }
        break;
      }
      default:
        return 0;
    }
  }
  *out_len = o;
  return 1;
}

// Hand-rolled fast number parser. Parses integer part via multiply-add,
// only falls back to float arithmetic when '.' or 'e'/'E' is seen.
static int parse_number(const char** pptr, const char* end, double* out) {
  const char* p = *pptr;
  if (p >= end) { return 0; }

  int negative = 0;
  if (*p == '-') { negative = 1; p++; }
  if (p >= end || (*p < '0' || *p > '9')) { return 0; }

  // Integer part via multiply-add
  int64_t int_part = 0;
  if (*p == '0') {
    p++;
  } else {
    while (p < end && *p >= '0' && *p <= '9') {
      int_part = int_part * 10 + (*p - '0');
      p++;
    }
  }

  int has_frac = 0;
  double frac_part = 0.0;
  if (p < end && *p == '.') {
    has_frac = 1;
    p++;
    if (p >= end || *p < '0' || *p > '9') { return 0; }
    double frac_scale = 0.1;
    while (p < end && *p >= '0' && *p <= '9') {
      frac_part += (*p - '0') * frac_scale;
      frac_scale *= 0.1;
      p++;
    }
  }

  int has_exp = 0;
  int exp_val = 0;
  if (p < end && (*p == 'e' || *p == 'E')) {
    has_exp = 1;
    p++;
    int exp_neg = 0;
    if (p < end && *p == '+') { p++; }
    else if (p < end && *p == '-') { exp_neg = 1; p++; }
    if (p >= end || *p < '0' || *p > '9') { return 0; }
    while (p < end && *p >= '0' && *p <= '9') {
      exp_val = exp_val * 10 + (*p - '0');
      p++;
    }
    if (exp_neg) { exp_val = -exp_val; }
  }

  double result;
  if (!has_frac && !has_exp) {
    // Pure integer fast path
    result = (double)int_part;
  } else {
    result = (double)int_part + frac_part;
    if (has_exp) {
      // Apply exponent
      double base = 10.0;
      int abs_exp = exp_val < 0 ? -exp_val : exp_val;
      double factor = 1.0;
      while (abs_exp > 0) {
        if (abs_exp & 1) { factor *= base; }
        base *= base;
        abs_exp >>= 1;
      }
      if (exp_val < 0) {
        result /= factor;
      } else {
        result *= factor;
      }
    }
  }

  if (negative) { result = -result; }
  *out = result;
  *pptr = p;
  return 1;
}

static int skip_nested_value(const char** pptr, const char* end) {
  const char* p = *pptr;
  if (p >= end || (*p != '{' && *p != '[')) { return 0; }
  char stack[64];
  int top = 0;
  stack[top++] = (*p == '{') ? '}' : ']';
  p++;
  bool in_string = false;
  bool escape = false;
  while (p < end) {
    char c = *p;
    if (in_string) {
      if (escape) {
        escape = false;
      } else if (c == '\\') {
        escape = true;
      } else if (c == '"') {
        in_string = false;
      }
      p++;
      continue;
    }
    if (c == '"') {
      in_string = true;
      p++;
      continue;
    }
    if (c == '{' || c == '[') {
      if (top >= (int)(sizeof(stack) / sizeof(stack[0]))) { return 0; }
      stack[top++] = (c == '{') ? '}' : ']';
      p++;
      continue;
    }
    if (c == '}' || c == ']') {
      if (top == 0 || c != stack[top - 1]) { return 0; }
      top--;
      p++;
      if (top == 0) {
        *pptr = p;
        return 1;
      }
      continue;
    }
    p++;
  }
  return 0;
}

static int skip_value(const char** pptr, const char* end) {
  const char* p = *pptr;
  if (p >= end) { return 0; }
  if (*p == '"') {
    const char* value_start = NULL; size_t value_len = 0; bool needs_unescape = false;
    if (!scan_string(&p, end, &value_start, &value_len, &needs_unescape)) { return 0; }
    *pptr = p;
    return 1;
  }
  if (*p == '-' || (*p >= '0' && *p <= '9')) {
    double number = 0;
    if (!parse_number(&p, end, &number)) { return 0; }
    *pptr = p;
    return 1;
  }
  if (p + 4 <= end && memcmp(p, "true", 4) == 0) { p += 4; *pptr = p; return 1; }
  if (p + 5 <= end && memcmp(p, "false", 5) == 0) { p += 5; *pptr = p; return 1; }
  if (p + 4 <= end && memcmp(p, "null", 4) == 0) { p += 4; *pptr = p; return 1; }
  if (*p == '{' || *p == '[') {
    if (!skip_nested_value(&p, end)) { return 0; }
    *pptr = p;
    return 1;
  }
  return 0;
}

// Compare a JSON string literal to a target string.
// Uses byte-level compare when there are no escapes.
static int match_string(const char* input, size_t len, const char* target, size_t target_len, bool needs_unescape) {
  if (!needs_unescape) {
    if (len != target_len) { return 0; }
    return memcmp(input, target, len) == 0 ? 1 : 0;
  }
  // Use a stack buffer for small strings, heap for larger values.
  if (len < 256) {
    char buffer[256];
    size_t out_len = 0;
    if (!unescape_string(input, len, buffer, &out_len)) { return -1; }
    if (out_len != target_len) { return 0; }
    return memcmp(buffer, target, out_len) == 0 ? 1 : 0;
  }
  if (len > 8192) { return -1; }
  char* dyn = (char*)malloc(len + 1);
  if (!dyn) { return -1; }
  size_t out_len = 0;
  int ok = unescape_string(input, len, dyn, &out_len);
  if (!ok) { free(dyn); return -1; }
  int match = (out_len == target_len && memcmp(dyn, target, out_len) == 0) ? 1 : 0;
  free(dyn);
  return match;
}

// Skip remaining content of nested objects, finding the closing '}'.
// initial_depth is how many unclosed braces we're inside of.
// Returns pointer just past the outermost closing '}', or NULL on error.
static const char* skip_object_rest(const char* p, const char* end, int initial_depth) {
  int depth = initial_depth;
  bool in_string = false;
  bool escape = false;
  while (p < end) {
    char c = *p;
    if (in_string) {
      if (escape) { escape = false; }
      else if (c == '\\') { escape = true; }
      else if (c == '"') { in_string = false; }
      p++;
      continue;
    }
    if (c == '"') { in_string = true; p++; continue; }
    if (c == '{' || c == '[') { depth++; p++; continue; }
    if (c == '}' || c == ']') {
      depth--;
      p++;
      if (depth == 0) { return p; }
      continue;
    }
    p++;
  }
  return NULL;
}

// Extended per-object prefilter. Scans a single JSON object at [p, end).
// On success (return 0 or 1), sets *endptr to position just past the closing '}'.
// Returns 1 = match, 0 = fail, -1 = unknown (parse required).
static int prefilter_object_ex(const char* p, const char* end, const char** endptr) {
  p = skip_ws(p, end);
  if (p >= end || *p != '{') { return -1; }
  p++;

  int _nest_depth = 1;
  PF_PREDICATE_STATE

  while (p < end) {
    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == '}') { p++; *endptr = p; goto eval; }

    const char* key_start = NULL;
    size_t key_len = 0;
    bool key_needs_unescape = false;
    if (!scan_string(&p, end, &key_start, &key_len, &key_needs_unescape)) { return -1; }
    // We don't currently support escaped keys.
    if (key_needs_unescape) { return -1; }
    p = skip_ws(p, end);
    if (p >= end || *p != ':') { return -1; }
    p++;
    p = skip_ws(p, end);

    if (p >= end) { return -1; }
    int handled = 0;

    PF_ROOT_MATCHING

    if (!handled) {
      // Skip unknown keys
      if (!skip_value(&p, end)) { return -1; }
    }
    PF_ALL_SEEN_CHECK

    p = skip_ws(p, end);
    if (p >= end) { return -1; }

    if (*p == ',') { p++; continue; }
    if (*p == '}') { p++; *endptr = p; goto eval; }
    return -1;
  }
  return -1;

eval:
  PF_PREDICATE_EVAL
  return 1;

fail_early:
  {
    const char* rest = skip_object_rest(p, end, _nest_depth);
    if (!rest) { return -1; }
    *endptr = rest;
    return 0;
  }
}

// Per-object prefilter. Thin wrapper around prefilter_object_ex.
static int prefilter_object(const char* input, size_t length) {
  const char* endptr = NULL;
  return prefilter_object_ex(input, input + length, &endptr);
}

// Single-object entry point (backwards compatible).
int prefilter(const char* input, size_t length) {
  return prefilter_object(input, length);
}

// NDJSON batch entry point.
// Scans newline-delimited JSON. For each matching object, writes (offset, length) pair
// into the output buffer. Returns number of matching items, or -1 on error/overflow.
int prefilter_ndjson(const char* input, size_t input_len, uint32_t* output, size_t output_capacity) {
  if (output_capacity < 4) { return -1; } // need at least header + 1 match slot
  size_t max_matches = (output_capacity - 2) / 2;
  int32_t count = 0;
  int32_t total = 0;
  const char* p = input;
  const char* end = input + input_len;

  while (p < end) {
    while (p < end && (*p == '\n' || *p == '\r')) { p++; }
    if (p >= end) { break; }

    const char* line_start = p;
    while (p < end && *p != '\n') { p++; }
    const char* line_end = p;

    if (line_end > line_start && *(line_end - 1) == '\r') {
      line_end--;
    }

    size_t line_len = (size_t)(line_end - line_start);
    if (line_len == 0) { continue; }

    total++;
    int result;
    PF_MEMMEM_GUARD
    PF_MEMMEM_FALLBACK

    if (result != 0) {
      if ((size_t)count >= max_matches) { return -1; }
      uint32_t offset = (uint32_t)(line_start - input);
      output[2 + count * 2] = offset;
      output[2 + count * 2 + 1] = (uint32_t)line_len;
      count++;
    }
  }

  output[0] = (uint32_t)total;
  output[1] = 0;
  return count;
}

// JSON array batch entry point (single-pass via prefilter_object_ex).
// Scans a top-level JSON array of objects. For each matching object, writes (offset, length)
// pair into the output buffer. Returns number of matching items, or -1 on error/overflow.
int prefilter_json_array(const char* input, size_t input_len, uint32_t* output, size_t output_capacity) {
  if (output_capacity < 4) { return -1; }
  size_t max_matches = (output_capacity - 2) / 2;
  int32_t count = 0;
  int32_t total = 0;
  const char* p = input;
  const char* end = input + input_len;

  p = skip_ws(p, end);
  if (p >= end || *p != '[') { return -1; }
  p++;

  while (1) {
    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == ']') { break; }

    if (*p != '{') { return -1; }

    const char* obj_start = p;
    const char* endptr = NULL;
    int result = prefilter_object_ex(p, end, &endptr);

    if (result == -1) {
      // Unknown/parse error - fall back to boundary detection for this object,
      // then treat it as a match (include in output for safety).
      const char* fallback = obj_start;
      if (!skip_nested_value(&fallback, end)) { return -1; }
      endptr = fallback;
    }

    size_t obj_len = (size_t)(endptr - obj_start);
    p = endptr;
    total++;

    if (result != 0) {
      if ((size_t)count >= max_matches) { return -1; }
      uint32_t offset = (uint32_t)(obj_start - input);
      output[2 + count * 2] = offset;
      output[2 + count * 2 + 1] = (uint32_t)obj_len;
      count++;
    }

    p = skip_ws(p, end);
    if (p >= end) { return -1; }
    if (*p == ',') { p++; continue; }
    if (*p == ']') { break; }
    return -1;
  }

  output[0] = (uint32_t)total;
  output[1] = 0;
  return count;
}
