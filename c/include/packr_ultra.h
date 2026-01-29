#ifndef PACKR_ULTRA_H
#define PACKR_ULTRA_H

#include "packr.h"

typedef enum {
    COL_TYPE_INT,
    COL_TYPE_FLOAT,
    COL_TYPE_STRING,
    COL_TYPE_BOOL,
    COL_TYPE_NULL
} col_type_t;

typedef struct {
    col_type_t type;
    size_t count;
    union {
        int32_t *ints;
        double *floats;
        char **strings;
        uint8_t *bools;
    };
    uint8_t *nulls; /* valid=1, null/missing=0 */
} packr_column_t;

void packr_encode_ultra_columns(packr_encoder_t *ctx, int row_count, int col_count, char **field_names, packr_column_t *columns);

#endif
