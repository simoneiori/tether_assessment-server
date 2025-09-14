#ifndef TRANSFORM_H
#define TRANSFORM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

#include "server.h"


unsigned long calc_checksum(const char *path);
void transform_upper(const char *in, const char *out);
void transform_reverse_file(const char *in, const char *out);
void transform_nop_file(const char *in, const char *out);



#ifdef __cplusplus
}
#endif

#endif /* TRANSFORM_H */
