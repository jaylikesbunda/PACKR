/*
 * PACKR - Ultra Batch / Schema Encoding
 */

#include "packr_ultra.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>

#define MIN_RICE_ITEMS 10

// Rice Encoder Helper
/* MFV Helper */
static int encode_mfv_column(packr_encoder_t *ctx, packr_column_t *col);

typedef struct {
    uint8_t *buf;
    size_t cap;
    size_t pos;
    uint32_t bit_buf;
    int bit_cnt;
} bitwriter_t;

static void bw_init(bitwriter_t *bw, uint8_t *buf, size_t cap) {
    bw->buf = buf;
    bw->cap = cap;
    bw->pos = 0;
    bw->bit_buf = 0;
    bw->bit_cnt = 0;
}

static void bw_write(bitwriter_t *bw, uint32_t val, int bits) {
    for (int i = bits - 1; i >= 0; i--) {
        uint8_t bit = (val >> i) & 1;
        bw->bit_buf = (bw->bit_buf << 1) | bit;
        bw->bit_cnt++;
        if (bw->bit_cnt == 8) {
            if (bw->pos < bw->cap) bw->buf[bw->pos++] = bw->bit_buf & 0xFF;
            bw->bit_buf = 0;
            bw->bit_cnt = 0;
        }
    }
}

static void bw_flush(bitwriter_t *bw) {
    if (bw->bit_cnt > 0) {
        /* Pad with zeros to the right, matching Python's flush */
        bw->bit_buf <<= (8 - bw->bit_cnt);
        if (bw->pos < bw->cap) bw->buf[bw->pos++] = bw->bit_buf & 0xFF;
        bw->bit_buf = 0;
        bw->bit_cnt = 0;
    }
}

static void bw_write_unary(bitwriter_t *bw, uint32_t val) {
    for (uint32_t i = 0; i < val; i++) bw_write(bw, 0, 1);
    bw_write(bw, 1, 1);
}

static int encode_rice_column(packr_encoder_t *ctx, int32_t *deltas, size_t count) {
    if (count < MIN_RICE_ITEMS) return 0;

    // 1. Convert to zig-zag and find Max
    uint32_t *udeltas = packr_malloc(sizeof(uint32_t) * count);
    if (!udeltas) return 0;
    
    uint32_t max_u = 0;
    int32_t max_abs = 0;
    for (size_t i=0; i<count; i++) {
        udeltas[i] = (deltas[i] << 1) ^ (deltas[i] >> 31);
        if (udeltas[i] > max_u) max_u = udeltas[i];
        int32_t a = (deltas[i] < 0) ? -deltas[i] : deltas[i];
        if (a > max_abs) max_abs = a;
    }
    
    // Rice is beneficial when deltas are small-to-medium (< 1024)
    if (max_abs >= 1024) { packr_free(udeltas); return 0; }

    int bl = 0;
    int32_t tmp = max_abs;
    if (tmp > 0) {
        while (tmp > 0) { tmp >>= 1; bl++; }
    }
    
    int k = bl - 2;
    if (k < 0) k = 0;
    if (k > 7) k = 7;
    
    // 3. Encode to temp buffer
    size_t limit = count * 2 + 1024; 
    uint8_t *temp = packr_malloc(limit);
    if (!temp) { packr_free(udeltas); return 0; }
    
    bitwriter_t bw;
    bw_init(&bw, temp, limit);

    for (size_t i=0; i<count; i++) {
        uint32_t val = udeltas[i];
        uint32_t q = val >> k;
        uint32_t r = val & ((1 << k) - 1);
        
        bw_write_unary(&bw, q);
        bw_write(&bw, r, k);
    }
    bw_flush(&bw);

    // 4. Check if actually beneficial (roughly < 1.5 bytes/value)
    if (bw.pos < count * 1.5) {
        packr_encode_token(ctx, TOKEN_RICE_COLUMN);
        packr_encode_varint(ctx, count);
        if (ctx->pos + bw.pos + 1 <= ctx->capacity) {
            ctx->buffer[ctx->pos++] = (uint8_t)k; /* Prepend K */
            memcpy(ctx->buffer + ctx->pos, temp, bw.pos);
            ctx->pos += bw.pos;
            packr_free(udeltas);
            packr_free(temp);
            return 1;
        }
    }
    
    packr_free(udeltas);
    packr_free(temp);
    return 0; 
}

static void encode_numeric_column(packr_encoder_t *ctx, packr_column_t *col, size_t field_idx) {
    if (col->count == 0) return;
    
    
    // Values
    if (col->type == COL_TYPE_FLOAT) {
        double first = col->floats[0];
        packr_encode_float(ctx, first);
        
        if (col->count == 1) return;
        
        // Deltas - use reconstructed values to avoid cumulative error
        int32_t *deltas = packr_malloc(sizeof(int32_t) * (col->count - 1));
        double prev = first;
        int all_small = 1;

        for (size_t i=1; i<col->count; i++) {
            double v = col->floats[i];
            int32_t d = (int32_t)rint((v - prev) * 65536.0);
            deltas[i-1] = d;
            if (d < -8 || d > 7) all_small = 0;
            // Use reconstructed value for next delta to match decoder behavior
            prev = prev + (double)d / 65536.0;
        }
        
        if (all_small) {
            // Heuristic: Check if RLE is better than Bitpacking
            // Bitpacking cost: ~0.5 bytes per value + overhead
            size_t bitpack_cost = (col->count) / 2 + 5;
            
            size_t rle_cost = 0;
            size_t k = 0;
            while (k < col->count - 1) {
                if (deltas[k] == 0) {
                    size_t run = 0;
                    while (k + run < col->count - 1 && deltas[k+run] == 0) run++;
                    
                    if (run > 3) {
                        rle_cost += 2 + (run > 127 ? 1 : 0);
                        k += run;
                        continue;
                    }
                }
                rle_cost += 1;
                k++;
            }
            
            if (rle_cost < bitpack_cost * 0.8) {
                all_small = 0; // Force RLE path
            }
        }

        if (all_small) {
            packr_encode_token(ctx, TOKEN_BITPACK_COL);
            packr_encode_varint(ctx, col->count - 1);

            // Pack - two deltas per byte
            for (size_t i=0; i<col->count-1; i+=2) {
                int d1 = deltas[i];
                // For odd counts, pad with 0 delta (encoded as 8 in 4-bit unsigned)
                int d2 = (i+1 < col->count-1) ? deltas[i+1] : 0;
                uint8_t b = ((d1 + 8) << 4) | ((d2 + 8) & 0x0F);

                if (ctx->pos < ctx->capacity) ctx->buffer[ctx->pos++] = b;
            }
        } else {
             // Try Rice Coding
             if (encode_rice_column(ctx, deltas, col->count - 1)) {
             } else {
                 size_t i = 0;
                 while (i < col->count - 1) {
                     int32_t d = deltas[i];
                     
                     // Check for run of zeros
                     if (d == 0) {
                         size_t run = 0;
                         while (i + run < col->count - 1 && deltas[i+run] == 0) {
                             run++;
                         }
                         
                         if (run > 3) {
                             packr_encode_token(ctx, TOKEN_RLE_REPEAT);
                             packr_encode_varint(ctx, run);
                             i += run;
                             continue;
                         }
                     }

                     if (d == 0) packr_encode_token(ctx, TOKEN_DELTA_ZERO);
                     else if (d == 1) packr_encode_token(ctx, TOKEN_DELTA_ONE);
                     else if (d == -1) packr_encode_token(ctx, TOKEN_DELTA_NEG_ONE);
                     else if (d >= -8 && d <= 7) {
                         packr_encode_token(ctx, (packr_token_t)(0xC3 + d + 8));
                     }
                     else if (d >= -64 && d <= 63) {
                         // Medium
                         if (ctx->pos + 2 <= ctx->capacity) {
                             ctx->buffer[ctx->pos++] = TOKEN_DELTA_MEDIUM;
                             ctx->buffer[ctx->pos++] = (d + 64) & 0x7F;
                             ctx->symbol_count++;
                         }
                     } else {
                         packr_encode_token(ctx, TOKEN_DELTA_LARGE);
                         packr_encode_varint(ctx, zigzag_encode(d));
                     }
                     i++;
                 }
             }
        }
        packr_free(deltas);
        
    } else {
        // Int logic
        int32_t first = col->ints[0];
        packr_encode_int(ctx, first);
        
        if (col->count == 1) return;
        
        // Deltas - use reconstructed values to avoid cumulative error
        int32_t *deltas = packr_malloc(sizeof(int32_t) * (col->count - 1));
        int32_t prev = first;
        int all_small = 1;

        for (size_t i=1; i<col->count; i++) {
            int32_t v = col->ints[i];
            int32_t d = v - prev;
            deltas[i-1] = d;
            if (d < -8 || d > 7) all_small = 0;
            // Use reconstructed value for next delta to match decoder behavior
            prev = prev + d;
        }
        
        if (all_small) {
            // Heuristic: Check if RLE is better than Bitpacking
            size_t bitpack_cost = (col->count) / 2 + 5;
            
            size_t rle_cost = 0;
            size_t k = 0;
            while (k < col->count - 1) {
                if (deltas[k] == 0) {
                    size_t run = 0;
                    while (k + run < col->count - 1 && deltas[k+run] == 0) run++;
                    
                    if (run > 3) {
                        rle_cost += 2 + (run > 127 ? 1 : 0);
                        k += run;
                        continue;
                    }
                }
                rle_cost += 1;
                k++;
            }
            
            if (rle_cost < bitpack_cost * 0.8) {
                all_small = 0; // Force RLE path
            }
        }
        
        if (all_small) {
            packr_encode_token(ctx, TOKEN_BITPACK_COL);
            packr_encode_varint(ctx, col->count - 1);
            // Pack - two deltas per byte
            for (size_t i=0; i<col->count-1; i+=2) {
                int d1 = deltas[i];
                // For odd counts, pad with 0 delta (encoded as 8 in 4-bit unsigned)
                int d2 = (i+1 < col->count-1) ? deltas[i+1] : 0;
                uint8_t b = ((d1 + 8) << 4) | ((d2 + 8) & 0x0F);
                if (ctx->pos < ctx->capacity) ctx->buffer[ctx->pos++] = b;
            }
        } else {
             // Try Rice
             if (encode_rice_column(ctx, deltas, col->count - 1)) {
                 // Success
             } else {
                 // Fallback
                 size_t i = 0;
                 while (i < col->count - 1) {
                     int32_t d = deltas[i];
                     
                     // Check for run of zeros
                     if (d == 0) {
                         size_t run = 0;
                         while (i + run < col->count - 1 && deltas[i+run] == 0) {
                             run++;
                         }
                         
                         if (run > 3) {
                             packr_encode_token(ctx, TOKEN_RLE_REPEAT);
                             packr_encode_varint(ctx, run);
                             i += run;
                             continue;
                         }
                     }
                     
                     if (d == 0) packr_encode_token(ctx, TOKEN_DELTA_ZERO);
                     else if (d == 1) packr_encode_token(ctx, TOKEN_DELTA_ONE);
                     else if (d == -1) packr_encode_token(ctx, TOKEN_DELTA_NEG_ONE);
                     else if (d >= -8 && d <= 7) {
                         packr_encode_token(ctx, (packr_token_t)(0xC3 + d + 8));
                     } else if (d >= -64 && d <= 63) {
                         if (ctx->pos + 2 <= ctx->capacity) {
                             ctx->buffer[ctx->pos++] = TOKEN_DELTA_MEDIUM;
                             ctx->buffer[ctx->pos++] = (d + 64) & 0x7F;
                             ctx->symbol_count++;
                         }
                     } else {
                         packr_encode_token(ctx, TOKEN_DELTA_LARGE);
                         packr_encode_varint(ctx, zigzag_encode(d));
                     }
                     i++;
                 }
             }
        }
        packr_free(deltas);
    }
}

static int encode_mfv_column(packr_encoder_t *ctx, packr_column_t *col) {
    if (col->count < 8) return 0; // Not worth overhead
    
    // 1. Boyer-Moore Voting to find Mode Candidate
    int32_t candidate_i = 0;
    double candidate_f = 0;
    char *candidate_s = NULL;
    uint8_t candidate_b = 0;
    
    int count = 0;
    
    for (size_t i = 0; i < col->count; i++) {
        if (count == 0) {
            if (col->type == COL_TYPE_INT) candidate_i = col->ints[i];
            else if (col->type == COL_TYPE_FLOAT) candidate_f = col->floats[i];
            else if (col->type == COL_TYPE_STRING) candidate_s = col->strings[i];
            else if (col->type == COL_TYPE_BOOL) candidate_b = col->bools[i];
            count = 1;
        } else {
            int match = 0;
            if (col->type == COL_TYPE_INT) match = (col->ints[i] == candidate_i);
            else if (col->type == COL_TYPE_FLOAT) match = (col->floats[i] == candidate_f);
            else if (col->type == COL_TYPE_STRING) match = (strcmp(col->strings[i], candidate_s) == 0);
            else if (col->type == COL_TYPE_BOOL) match = (col->bools[i] == candidate_b);
            
            if (match) count++; else count--;
        }
    }
    
    // 2. Verify Count
    size_t occurrences = 0;
    for (size_t i = 0; i < col->count; i++) {
        int match = 0;
        if (col->type == COL_TYPE_INT) match = (col->ints[i] == candidate_i);
        else if (col->type == COL_TYPE_FLOAT) match = (col->floats[i] == candidate_f);
        else if (col->type == COL_TYPE_STRING) match = (strcmp(col->strings[i], candidate_s) == 0);
        else if (col->type == COL_TYPE_BOOL) match = (col->bools[i] == candidate_b);
        if (match) occurrences++;
    }
    
    // Threshold: 60%
    if (occurrences * 10 < col->count * 6) return 0;
    
    // 3. Encode
    packr_encode_token(ctx, TOKEN_MFV_COLUMN);
    packr_encode_varint(ctx, col->count);
    
    // Write Mode Value
    if (col->type == COL_TYPE_INT) packr_encode_int(ctx, candidate_i);
    else if (col->type == COL_TYPE_FLOAT) packr_encode_float(ctx, candidate_f);
    else if (col->type == COL_TYPE_STRING) packr_encode_string(ctx, candidate_s, strlen(candidate_s));
    else if (col->type == COL_TYPE_BOOL) packr_encode_bool(ctx, candidate_b);
    
    // Write Bitmap (1 = Exception, 0 = Mode)
    size_t bitmap_len = (col->count + 7) / 8;
    for (size_t i = 0; i < col->count; i += 8) {
        uint8_t b = 0;
        for (size_t j = 0; j < 8 && i + j < col->count; j++) {
            int match = 0;
            if (col->type == COL_TYPE_INT) match = (col->ints[i+j] == candidate_i);
            else if (col->type == COL_TYPE_FLOAT) match = (col->floats[i+j] == candidate_f);
            else if (col->type == COL_TYPE_STRING) match = (strcmp(col->strings[i+j], candidate_s) == 0);
            else if (col->type == COL_TYPE_BOOL) match = (col->bools[i+j] == candidate_b);
            
            if (!match) b |= (1 << j);
        }
        if (ctx->pos < ctx->capacity) ctx->buffer[ctx->pos++] = b;
    }
    
    // Write Exceptions
    for (size_t i = 0; i < col->count; i++) {
        int match = 0;
        if (col->type == COL_TYPE_INT) match = (col->ints[i] == candidate_i);
        else if (col->type == COL_TYPE_FLOAT) match = (col->floats[i] == candidate_f);
        else if (col->type == COL_TYPE_STRING) match = (strcmp(col->strings[i], candidate_s) == 0);
        else if (col->type == COL_TYPE_BOOL) match = (col->bools[i] == candidate_b);
        
        if (!match) {
            if (col->type == COL_TYPE_INT) packr_encode_int(ctx, col->ints[i]);
            else if (col->type == COL_TYPE_FLOAT) packr_encode_float(ctx, col->floats[i]);
            else if (col->type == COL_TYPE_STRING) packr_encode_string(ctx, col->strings[i], strlen(col->strings[i]));
            else if (col->type == COL_TYPE_BOOL) packr_encode_bool(ctx, col->bools[i]);
        }
    }
    
    return 1;
}

// Public API
void packr_encode_ultra_columns(packr_encoder_t *ctx, int row_count, int col_count, char **field_names, packr_column_t *columns) {
    if (row_count == 0) return;
    
    packr_encode_token(ctx, TOKEN_ULTRA_BATCH);
    packr_encode_varint(ctx, row_count);
    packr_encode_varint(ctx, col_count);

    // Emit fields and flags
    for (int i=0; i<col_count; i++) {
        packr_encode_field(ctx, field_names[i], strlen(field_names[i]));
        
        // Analyze
        packr_column_t *col = &columns[i];
        int is_constant = 1;
        
        if (col->type == COL_TYPE_INT) {
             int32_t val = col->ints[0];
             for(size_t j=1; j<col->count; j++) if(col->ints[j] != val) { is_constant = 0; break; }
        } else if (col->type == COL_TYPE_FLOAT) {
             double val = col->floats[0];
             for(size_t j=1; j<col->count; j++) if(col->floats[j] != val) { is_constant = 0; break; }
        } else if (col->type == COL_TYPE_STRING) {
             char *val = col->strings[0];
             for(size_t j=1; j<col->count; j++) if(strcmp(col->strings[j], val) != 0) { is_constant = 0; break; }
        } else if (col->type == COL_TYPE_BOOL) {
             uint8_t val = col->bools[0];
             for(size_t j=1; j<col->count; j++) if(col->bools[j] != val) { is_constant = 0; break; }
        }
        
        // Check for nulls
        int has_nulls = 0;
        for(size_t j=0; j<col->count; j++) {
            if (col->nulls[j] == 0) {
                has_nulls = 1;
                break;
            }
        }
        
        uint8_t flags = 0;
        if (has_nulls) flags |= 0x08;
        if (is_constant) {
            flags |= 0x01; // CONSTANT
        } else {
            if (col->type == COL_TYPE_INT || col->type == COL_TYPE_FLOAT) {
                flags |= 0x02; // ALL_DELTA
            } else {
                flags |= 0x04; // RLE
            }
        }
        packr_encode_token(ctx, (packr_token_t)flags);
    }
    
    for (int i=0; i<col_count; i++) {
        packr_column_t *col = &columns[i];
        
        // Check for nulls (Recalculate to match flags)
        int has_nulls = 0;
        for(size_t j=0; j<col->count; j++) {
            if (col->nulls[j] == 0) {
                has_nulls = 1;
                break;
            }
        }
        
        if (has_nulls) {
             for (size_t j = 0; j < col->count; j += 8) {
                uint8_t b = 0;
                for (size_t k = 0; k < 8 && j + k < col->count; k++) {
                    if (col->nulls[j+k]) b |= (1 << k);
                }
                if (ctx->pos < ctx->capacity) ctx->buffer[ctx->pos++] = b;
            }
        }
        
        int is_constant = 1;
        if (col->type == COL_TYPE_INT) {
             int32_t val = col->ints[0];
             for(size_t j=1; j<col->count; j++) if(col->ints[j] != val) { is_constant = 0; break; }
             
             if (is_constant) {
                 packr_encode_int(ctx, val);
             } else {
                 if (!encode_mfv_column(ctx, col)) {
                     encode_numeric_column(ctx, col, i);
                 }
             }
        } 
        else if (col->type == COL_TYPE_FLOAT) {
             double val = col->floats[0];
             for(size_t j=1; j<col->count; j++) if(col->floats[j] != val) { is_constant = 0; break; }

             if (is_constant) {
                 if (val == (double)(int32_t)val) {
                     packr_encode_int(ctx, (int32_t)val);
                 } else {
                     packr_encode_float(ctx, val);
                 }
             } else {
                 if (!encode_mfv_column(ctx, col)) {
                     encode_numeric_column(ctx, col, i);
                 }
             }
        }
        else if (col->type == COL_TYPE_STRING) {
            char *val = col->strings[0];
            for(size_t j=1; j<col->count; j++) if(strcmp(col->strings[j], val) != 0) { is_constant = 0; break; }
            
            if (is_constant) {
                packr_encode_string(ctx, val, strlen(val));
             } else {
                 if (encode_mfv_column(ctx, col)) {
                 } else {
                    // RLE
                    size_t j = 0;
                    while (j < col->count) {
                        char *curr = col->strings[j];
                        size_t run = 1;
                        while (j + run < col->count && strcmp(col->strings[j+run], curr) == 0) {
                            run++;
                        }
                        
                        packr_encode_string(ctx, curr, strlen(curr));
                        if (run > 1) {
                            packr_encode_token(ctx, TOKEN_RLE_REPEAT);
                            packr_encode_varint(ctx, run - 1);
                        }
                        j += run;
                    }
                 }
             }
        }
        else if (col->type == COL_TYPE_BOOL) {
             uint8_t val = col->bools[0];
             for(size_t j=1; j<col->count; j++) if(col->bools[j] != val) { is_constant = 0; break; }
             
             if (is_constant) {
                 packr_encode_bool(ctx, val);
             } else {
                 if (encode_mfv_column(ctx, col)) {
                 } else {
                     // RLE Fallback for Booleans
                     size_t j = 0;
                     while (j < col->count) {
                        uint8_t curr = col->bools[j];
                        size_t run = 1;
                        while (j + run < col->count && col->bools[j+run] == curr) {
                            run++;
                        }
                        
                        packr_encode_bool(ctx, curr);
                        if (run > 1) {
                            packr_encode_token(ctx, TOKEN_RLE_REPEAT);
                            packr_encode_varint(ctx, run - 1);
                        }
                        j += run;
                     }
                 }
             }
        }
    }
}
