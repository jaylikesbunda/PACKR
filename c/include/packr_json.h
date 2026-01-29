/*
 * PACKR - JSON Glue Header
 */

#ifndef PACKR_JSON_H
#define PACKR_JSON_H

#include "packr.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Encodes a JSON string directly to a PACKR encoder instance.
 * Returns 0 on success, non-zero on error.
 */
int json_encode_to_packr(const char *json, size_t len, packr_encoder_t *enc);

#ifdef __cplusplus
}
#endif

#endif
