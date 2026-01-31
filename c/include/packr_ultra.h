#ifndef PACKR_ULTRA_H
#define PACKR_ULTRA_H

#include "packr.h"

typedef enum {
    COL_TYPE_INT,
    COL_TYPE_FLOAT,
    COL_TYPE_STRING,
    COL_TYPE_BOOL,
    COL_TYPE_NULL,
    COL_TYPE_CUSTOM
} col_type_t;

struct packr_encoder_t; /* Forward declaration */

typedef struct {
    col_type_t type;
    size_t count;
    union {
        int32_t *ints;
        double *floats;
        char **strings;
        uint8_t *bools;
        void **custom_data;
    };
    uint8_t *nulls; /* valid=1, null/missing=0 */
    
    /* Callback for custom encoding */
    int (*custom_encoder)(packr_encoder_t *ctx, void *data);
} packr_column_t;

int packr_encode_ultra_columns(packr_encoder_t *ctx, int row_count, int col_count, char **field_names, packr_column_t *columns, int partial);

#endif
