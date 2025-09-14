#ifndef JSON_H
#define JSON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>


void parse_str_field(const char *s, const char *key, char *out, size_t outsz);
int parse_int_field(const char *s, const char *key);

void format_json_string(char *out, int outSize, const char *key, const char *val);
void format_json_long(char *out, int outSize, const char *key, long val);
void format_json_string_long(char *out, int outSize, const char *key1, const char *val1, const char *key2, long val2);



#ifdef __cplusplus
}
#endif

#endif /* JSON_H */
