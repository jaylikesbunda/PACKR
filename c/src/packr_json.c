/*
 * PACKR JSON Parser & Main Entry
 */

#include "packr_json.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* JSMN-style minimalist parser helpers */

typedef enum {
    J_ERROR,
    J_OBJECT_START,
    J_OBJECT_END,
    J_ARRAY_START,
    J_ARRAY_END,
    J_STRING,
    J_NUMBER,
    J_TRUE,
    J_FALSE,
    J_NULL,
    J_COLON,
    J_COMMA,
    J_EOF
} jtoken_type_t;

typedef struct {
    const char *json;
    size_t len;
    size_t pos;
} jparser_t;

static void skip_whitespace(jparser_t *p) {
    while (p->pos < p->len && isspace((unsigned char)p->json[p->pos])) {
        p->pos++;
    }
}

static jtoken_type_t peek_token(jparser_t *p) {
    size_t save = p->pos;
    skip_whitespace(p);
    if (p->pos >= p->len) { p->pos = save; return J_EOF; }
    
    char c = p->json[p->pos];
    jtoken_type_t type = J_ERROR;
    
    if (c == '{') type = J_OBJECT_START;
    else if (c == '}') type = J_OBJECT_END;
    else if (c == '[') type = J_ARRAY_START;
    else if (c == ']') type = J_ARRAY_END;
    else if (c == ':') type = J_COLON;
    else if (c == ',') type = J_COMMA;
    else if (c == '"') type = J_STRING;
    else if (c == 't') type = J_TRUE;
    else if (c == 'f') type = J_FALSE;
    else if (c == 'n') type = J_NULL;
    else if (c == '-' || (c >= '0' && c <= '9')) type = J_NUMBER;
    
    p->pos = save;
    return type;
}

static jtoken_type_t next_token(jparser_t *p, char **out_start, size_t *out_len) {
    skip_whitespace(p);
    if (p->pos >= p->len) return J_EOF;
    
    char c = p->json[p->pos];
    
    if (c == '{' || c == '}' || c == '[' || c == ']' || c == ':' || c == ',') {
        p->pos++;
        return (c=='{')?J_OBJECT_START:(c=='}')?J_OBJECT_END:
               (c=='[')?J_ARRAY_START:(c==']')?J_ARRAY_END:
               (c==':')?J_COLON:J_COMMA;
    }
    
    if (c == '"') {
        p->pos++;
        *out_start = (char*)(p->json + p->pos);
        while (p->pos < p->len && p->json[p->pos] != '"') {
            if (p->json[p->pos] == '\\') p->pos++;
            p->pos++;
        }
        *out_len = (char*)(p->json + p->pos) - *out_start;
        if (p->pos < p->len) p->pos++; /* skip closing quote */
        return J_STRING;
    }
    
    if (c == 't') { p->pos+=4; return J_TRUE; }
    if (c == 'f') { p->pos+=5; return J_FALSE; }
    if (c == 'n') { p->pos+=4; return J_NULL; }
    
    if (c == '-' || isdigit((unsigned char)c)) {
        *out_start = (char*)(p->json + p->pos);
        if (c == '-') p->pos++;
        while (p->pos < p->len && isdigit((unsigned char)p->json[p->pos])) p->pos++;
        if (p->pos < p->len && p->json[p->pos] == '.') {
            p->pos++;
            while (p->pos < p->len && isdigit((unsigned char)p->json[p->pos])) p->pos++;
        }
        if (p->pos < p->len && (p->json[p->pos] == 'e' || p->json[p->pos] == 'E')) {
            p->pos++;
            if (p->pos < p->len && (p->json[p->pos] == '+' || p->json[p->pos] == '-')) p->pos++;
            while (p->pos < p->len && isdigit((unsigned char)p->json[p->pos])) p->pos++;
        }
        *out_len = (char*)(p->json + p->pos) - *out_start;
        return J_NUMBER;
    }
    
    return J_ERROR;
}

/* Helper to consume an entire JSON object/array string raw */
static char* consume_json_object(jparser_t *p, size_t *out_len) {
    skip_whitespace(p);
    size_t start = p->pos;
    if (start >= p->len) return NULL;
    
    char c = p->json[start];
    if (c != '{' && c != '[') return NULL;
    
    int depth = 0;
    int in_quote = 0;
    
    while (p->pos < p->len) {
        char cur = p->json[p->pos];
        
        if (in_quote) {
            if (cur == '\\') {
                p->pos++; // Skip escaped char
            } else if (cur == '"') {
                in_quote = 0;
            }
        } else {
            if (cur == '"') in_quote = 1;
            else if (cur == '{' || cur == '[') depth++;
            else if (cur == '}' || cur == ']') {
                depth--;
                if (depth == 0) {
                    p->pos++; // Include closing brace
                    *out_len = p->pos - start;
                    char *res = packr_malloc(*out_len + 1);
                    memcpy(res, p->json + start, *out_len);
                    res[*out_len] = 0;
                    printf("ALLOC_BLOB %p\n", res);
                    return res;
                }
            }
        }
        p->pos++;
    }
    return NULL;
}



/* Custom Encoder Callback */
static int encode_json_blob(packr_encoder_t *ctx, void *data) {
    char *json = (char*)data;
    if (!json) return packr_encode_null(ctx);
    // Recursively parse this blob
    return json_encode_to_packr(json, strlen(json), ctx);
}

/* Recursive Encoder */

static int encode_value(jparser_t *p, packr_encoder_t *enc);

static int encode_object(jparser_t *p, packr_encoder_t *enc) {
    char *d1; size_t d2;
    /* Consume { */
    if (next_token(p, &d1, &d2) != J_OBJECT_START) return -1;
    
    packr_encode_token(enc, TOKEN_OBJECT_START);
    
    jtoken_type_t t = peek_token(p);
    if (t == J_OBJECT_END) {
        next_token(p, &d1, &d2);
        return packr_encode_token(enc, TOKEN_OBJECT_END);
    }
    
    while (1) {
        char *key; size_t klen;
        if (next_token(p, &key, &klen) != J_STRING) return -1;
        
        /* Key */
        char kbuf[256];
        if (klen >= 256) klen = 255;
        memcpy(kbuf, key, klen);
        kbuf[klen] = 0;
        if (packr_encode_field(enc, kbuf, klen) != 0) return -1;
        
        if (next_token(p, &d1, &d2) != J_COLON) return -1;
        
        if (encode_value(p, enc) != 0) return -1;
        
        t = peek_token(p);
        if (t == J_COMMA) {
            next_token(p, &d1, &d2);
        } else if (t == J_OBJECT_END) {
            next_token(p, &d1, &d2);
            break;
        } else return -1;
    }
    
    return packr_encode_token(enc, TOKEN_OBJECT_END);
}


#include "packr_ultra.h"

#define MAX_BATCH_ROWS 128
#define MAX_BATCH_COLS 32

/* Helper to skip any JSON value */
static void skip_json_value(jparser_t *p) {
    char *s; size_t sl;
    jtoken_type_t t = peek_token(p);
    if (t == J_OBJECT_START || t == J_ARRAY_START) {
        size_t len;
        char *blob = consume_json_object(p, &len);
        printf("FREE_DIRECT %p\n", blob);
        packr_free(blob);
    } else {
        next_token(p, &s, &sl);
    }
}

static int try_encode_ultra_array(jparser_t *p, packr_encoder_t *enc) {
    size_t save = p->pos;
    char *s; size_t sl;
    if (next_token(p, &s, &sl) != J_ARRAY_START) return 1; // Soft fail
    
    // 2. Check if empty
    jtoken_type_t t = peek_token(p);
    if (t == J_ARRAY_END) return 1; 
    if (t != J_OBJECT_START) return 1;
    
    // 3. Schema Discovery (Union of all keys in batch)
    char *fields[MAX_BATCH_COLS];
    col_type_t types[MAX_BATCH_COLS];
    int col_count = 0;
    
    size_t scan_pos = p->pos; // Start scanning from first object
    int scan_rows = 0;
    
    // Save parser state
    jparser_t scan_p = *p;
    
    while (scan_rows < MAX_BATCH_ROWS) {
        t = peek_token(&scan_p);
        if (t == J_ARRAY_END) break;
        if (t == J_COMMA) next_token(&scan_p, &s, &sl);
        
        if (next_token(&scan_p, &s, &sl) != J_OBJECT_START) break;
        
        while (1) {
            char *key; size_t klen;
            if (next_token(&scan_p, &key, &klen) != J_STRING) break;
            
            // Check if key known
            int found = -1;
            for(int k=0; k<col_count; k++) {
                if (strlen(fields[k]) == klen && strncmp(fields[k], key, klen) == 0) {
                    found = k; break;
                }
            }
            
            if (next_token(&scan_p, &s, &sl) != J_COLON) break;
            
            // Peek value type
            t = peek_token(&scan_p);
            char *v; size_t vl;
            
            col_type_t new_type = COL_TYPE_NULL;

            if (t == J_OBJECT_START || t == J_ARRAY_START) {
                new_type = COL_TYPE_CUSTOM;
                char *blob = consume_json_object(&scan_p, &vl);
                printf("FREE_SCAN %p\n", blob);
                packr_free(blob); // Just scanning, discard data
            } else {
                t = next_token(&scan_p, &v, &vl); // Consume scalar
                
                if (t == J_NUMBER) {
                    int is_float = 0;
                    for(size_t i=0; i<vl; i++) if(v[i]=='.' || v[i]=='e' || v[i]=='E') is_float = 1;
                    new_type = is_float ? COL_TYPE_FLOAT : COL_TYPE_INT;
                } else if (t == J_STRING) new_type = COL_TYPE_STRING;
                else if (t == J_TRUE || t == J_FALSE) new_type = COL_TYPE_BOOL;
                else if (t == J_NULL) new_type = COL_TYPE_NULL;
            }
            
            if (found == -1 && new_type != COL_TYPE_NULL) {
                if (col_count < MAX_BATCH_COLS) {
                    fields[col_count] = packr_malloc(klen + 1);
                    memcpy(fields[col_count], key, klen);
                    fields[col_count][klen] = 0;
                    types[col_count] = new_type;
                    col_count++;
                }
            } else if (found != -1 && new_type != COL_TYPE_NULL) {
                 if (types[found] == COL_TYPE_INT && new_type == COL_TYPE_FLOAT) {
                     types[found] = COL_TYPE_FLOAT;
                 }
            }
            
            t = peek_token(&scan_p);
            if (t == J_COMMA) next_token(&scan_p, &s, &sl);
            else if (t == J_OBJECT_END) { next_token(&scan_p, &s, &sl); break; }
            else break;
        }
        scan_rows++;
    }
    
    if (col_count == 0 || scan_rows == 0) {
        for(int i=0; i<col_count; i++) packr_free(fields[i]);
        return 1;
    }
    if (scan_rows < 4) {
        for(int i=0; i<col_count; i++) packr_free(fields[i]);
        return 1; // Fallback for small arrays to avoid Ultra Batch overhead/bugs
    }

    // 4. Parse Rows using Discovered Schema
    packr_column_t cols[MAX_BATCH_COLS];
    for(int i=0; i<col_count; i++) {
        cols[i].type = types[i];
        cols[i].count = 0;
        if (types[i] == COL_TYPE_INT) {
            cols[i].ints = packr_malloc(sizeof(int32_t) * scan_rows);
            memset(cols[i].ints, 0, sizeof(int32_t) * scan_rows); // Default 0
        }
        else if (types[i] == COL_TYPE_FLOAT) {
            cols[i].floats = packr_malloc(sizeof(double) * scan_rows);
            memset(cols[i].floats, 0, sizeof(double) * scan_rows); // Default 0.0
        }
        else if (types[i] == COL_TYPE_STRING) {
            cols[i].strings = packr_malloc(sizeof(char*) * scan_rows);
            memset(cols[i].strings, 0, sizeof(char*) * scan_rows);
            // Default "" - handled in loop
        }
        else if (types[i] == COL_TYPE_BOOL) {
            cols[i].bools = packr_malloc(sizeof(uint8_t) * scan_rows);
            memset(cols[i].bools, 0, sizeof(uint8_t) * scan_rows); // Default False
        }
        else if (types[i] == COL_TYPE_CUSTOM) {
            cols[i].custom_data = packr_malloc(sizeof(void*) * scan_rows);
            memset(cols[i].custom_data, 0, sizeof(void*) * scan_rows);
            cols[i].custom_encoder = encode_json_blob;
        }
        cols[i].nulls = packr_malloc(scan_rows);
        memset(cols[i].nulls, 0, scan_rows); // Default Missing
    }
    
    int row_count = 0;
    int success = 1;
    int is_streaming = 0;
    
    // Dynamic Flush Tracking
    size_t current_batch_size = 0;
    const size_t MAX_BATCH_BYTES = 4096;
    
    // Reset parser
    p->pos = save;
    next_token(p, &s, &sl); // [
    
    while (1) {
        t = peek_token(p);
        if (t == J_ARRAY_END) { next_token(p, &s, &sl); break; }
        if (t == J_COMMA) next_token(p, &s, &sl);
        
        if (next_token(p, &s, &sl) != J_OBJECT_START) { success = 0; break; }
        
        // Init row defaults for Strings (others memset to 0)
        for(int i=0; i<col_count; i++) {
            if (cols[i].type == COL_TYPE_STRING) {
                cols[i].strings[row_count] = NULL; // Use NULL for empty/default
            }
        }
        
        while (1) {
            char *key; size_t klen;
            if (next_token(p, &key, &klen) != J_STRING) { success = 0; break; }
            
            // Find column
            int col_idx = -1;
            for(int k=0; k<col_count; k++) {
                if (strlen(fields[k]) == klen && strncmp(fields[k], key, klen) == 0) {
                    col_idx = k; break;
                }
            }
            
            if (next_token(p, &s, &sl) != J_COLON) { success = 0; break; }
            
            // Parse Value
            char *val = NULL; size_t vlen = 0;
            
            if (col_idx != -1) {
                packr_column_t *c = &cols[col_idx];
                
                // Mark valid
                c->nulls[row_count] = 1;
                
                t = peek_token(p);
                if (c->type == COL_TYPE_CUSTOM) {
                     if (t == J_OBJECT_START || t == J_ARRAY_START) {
                         packr_free(c->custom_data[row_count]);
                         size_t blob_len = 0;
                         c->custom_data[row_count] = consume_json_object(p, &blob_len);
                         current_batch_size += blob_len;
                     } else {
                         next_token(p, &val, &vlen); // Consume unexpected scalar
                     }
                } else if (c->type == COL_TYPE_STRING) {
                     next_token(p, &val, &vlen);
                     if (c->strings[row_count]) packr_free(c->strings[row_count]);
                     char *sv = packr_malloc(vlen + 1);
                     if (val) memcpy(sv, val, vlen);
                     sv[vlen]=0;
                     c->strings[row_count] = sv;
                     current_batch_size += vlen;
                } else if (c->type == COL_TYPE_INT || c->type == COL_TYPE_FLOAT || c->type == COL_TYPE_BOOL) {
                     next_token(p, &val, &vlen);
                     if (c->type == COL_TYPE_INT && t == J_NUMBER && val) {
                        char tmp[64]; if(vlen>63)vlen=63; memcpy(tmp, val, vlen); tmp[vlen]=0;
                        c->ints[row_count] = (int32_t)strtol(tmp, NULL, 10);
                     } else if (c->type == COL_TYPE_FLOAT && t == J_NUMBER && val) {
                        char tmp[64]; if(vlen>63)vlen=63; memcpy(tmp, val, vlen); tmp[vlen]=0;
                        c->floats[row_count] = strtod(tmp, NULL);
                     } else if (c->type == COL_TYPE_BOOL) {
                        if (t == J_TRUE) c->bools[row_count] = 1;
                        else if (t == J_FALSE) c->bools[row_count] = 0;
                     }
                } else {
                    skip_json_value(p);
                }
            } else {
                // Unknown field - skip
                skip_json_value(p);
            }
            
            t = peek_token(p);
            if (t == J_COMMA) next_token(p, &s, &sl);
            else if (t == J_OBJECT_END) { next_token(p, &s, &sl); break; }
            else { success = 0; break; }
        }
        
        if (!success) break;
        
        if (success) {
            // Inc counts
            for(int i=0; i<col_count; i++) cols[i].count++;
            row_count++;
            
            static int total_debug = 0;
            total_debug++;
            if (total_debug % 100 == 0) fprintf(stderr, "JSON Row %d\n", total_debug);

            // Flush Check
            if (row_count >= MAX_BATCH_ROWS || current_batch_size >= MAX_BATCH_BYTES) {
                if (!is_streaming) {
                    packr_encode_token(enc, TOKEN_ARRAY_STREAM);
                    is_streaming = 1;
                }
                
                if (packr_encode_ultra_columns(enc, row_count, col_count, fields, cols, 1) != 0) {
                    success = 0;
                    break;
                }
                
                // Reset for next batch
                for(int i=0; i<col_count; i++) {
                    if (cols[i].type == COL_TYPE_STRING) {
                         for(int j=0; j<row_count; j++) {
                             if (cols[i].strings[j]) packr_free(cols[i].strings[j]);
                             cols[i].strings[j] = NULL;
                         }
                    }
                    else if (cols[i].type == COL_TYPE_CUSTOM) {
                         for(int j=0; j<row_count; j++) {
                             if (cols[i].custom_data[j]) {
                                 printf("FREE_FLUSH %p\n", cols[i].custom_data[j]);
                                 packr_free(cols[i].custom_data[j]);
                             }
                             cols[i].custom_data[j] = NULL;
                         }
                    }
                    
                    cols[i].count = 0;
                    if (cols[i].type == COL_TYPE_INT) memset(cols[i].ints, 0, sizeof(int32_t) * scan_rows);
                    else if (cols[i].type == COL_TYPE_FLOAT) memset(cols[i].floats, 0, sizeof(double) * scan_rows);
                    else if (cols[i].type == COL_TYPE_BOOL) memset(cols[i].bools, 0, sizeof(uint8_t) * scan_rows);
                    
                    memset(cols[i].nulls, 0, scan_rows);
                }
                row_count = 0;
                current_batch_size = 0;
            }
        }
    }

    // Flush remaining
    if (success && row_count > 0) {
        if (is_streaming) {
             if (packr_encode_ultra_columns(enc, row_count, col_count, fields, cols, 1) != 0) success = 0;
        } else {
             // Standard Ultra Batch (Single)
             if (packr_encode_ultra_columns(enc, row_count, col_count, fields, cols, 0) != 0) success = 0; 
        }
    }
    
    if (success && is_streaming) {
        packr_encode_token(enc, TOKEN_ARRAY_END);
    }
    
    // Cleanup
    for(int i=0; i<col_count; i++) {
        packr_free(fields[i]); 
        if (cols[i].type == COL_TYPE_STRING) {
            for(int j=0; j<scan_rows; j++) packr_free(cols[i].strings[j]);
            packr_free(cols[i].strings);
        } else if (cols[i].type == COL_TYPE_INT) {
            packr_free(cols[i].ints);
        }
        else if (cols[i].type == COL_TYPE_FLOAT) {
            packr_free(cols[i].floats);
        }
        else if (cols[i].type == COL_TYPE_BOOL) {
            packr_free(cols[i].bools);
        }
        else if (cols[i].type == COL_TYPE_CUSTOM) {
             printf("CLEANUP_CUSTOM i=%d type=%d rows=%d ptr=%p\n", i, cols[i].type, scan_rows, cols[i].custom_data);
             for(int j=0; j<scan_rows; j++) {
                 if (cols[i].custom_data[j]) {
                     printf("FREE_CLEANUP %p\n", cols[i].custom_data[j]);
                     packr_free(cols[i].custom_data[j]);
                 }
             }
             packr_free(cols[i].custom_data);
        }
        packr_free(cols[i].nulls);
    }
    
    if (success) return 0;
    
    if (is_streaming) return -1; // Fatal error, cannot rewind
    return 1; // Soft error, fallback valid
}

static int encode_array(jparser_t *p, packr_encoder_t *enc) {
    size_t save = p->pos;
    int ret = try_encode_ultra_array(p, enc);
    if (ret == 0) {
        fprintf(stderr, "PACKR: Successfully encoded using Ultra Batch Streaming\n");
        return 0;
    }
    if (ret == -1) return -1; // Fatal error
    
    fprintf(stderr, "PACKR: Ultra Batch failed (ret=%d), falling back to Recursive Array\n", ret);
    p->pos = save; // Fallback
    
    /* Count elements */
    size_t save_pos = p->pos;
    int count = 0;
    int depth = 0;
    
    size_t scan_pos = p->pos;
    
    /* Ensure we are at [ */
    while (scan_pos < p->len && isspace((unsigned char)p->json[scan_pos])) scan_pos++;
    if (scan_pos >= p->len || p->json[scan_pos] != '[') return -1;
    
    scan_pos++; /* Skip [ */
    
    /* Check empty */
    size_t check_pos = scan_pos;
    while (check_pos < p->len && isspace((unsigned char)p->json[check_pos])) check_pos++;
    if (check_pos < p->len && p->json[check_pos] == ']') {
        count = 0;
    } else {
        count = 1; /* At least one element if not empty */
        depth = 1;
        while (scan_pos < p->len && depth > 0) {
            char c = p->json[scan_pos];
            if (c == '[') depth++;
            else if (c == ']') depth--;
            else if (c == '{') depth++;
            else if (c == '}') depth--;
            else if (c == ',' && depth == 1) count++;
            else if (c == '"') {
                scan_pos++;
                while (scan_pos < p->len && p->json[scan_pos] != '"') {
                    if (p->json[scan_pos] == '\\') scan_pos++;
                    scan_pos++;
                }
            }
            scan_pos++;
        }
    }
    
    packr_encode_token(enc, TOKEN_ARRAY_START);
    packr_encode_varint(enc, count);
    
    /* Consume [ */
    char *d1; size_t d2;
    next_token(p, &d1, &d2);
    
    jtoken_type_t t = peek_token(p);
    if (t == J_ARRAY_END) {
        next_token(p, &d1, &d2);
    } else {
        while (1) {
            if (encode_value(p, enc) != 0) return -1;
            
            t = peek_token(p);
            if (t == J_COMMA) {
                next_token(p, &d1, &d2);
            } else if (t == J_ARRAY_END) {
                next_token(p, &d1, &d2);
                break;
            } else return -1;
        }
    }
    
    return packr_encode_token(enc, TOKEN_ARRAY_END);
}

static int is_mac_address(const char *s, size_t len) {
    if (len != 17) return 0;
    for (int i=0; i<17; i++) {
        char c = s[i];
        if (i % 3 == 2) {
            if (c != ':' && c != '-') return 0;
        } else {
            if (!isxdigit((unsigned char)c)) return 0;
        }
    }
    return 1;
}

static int encode_value(jparser_t *p, packr_encoder_t *enc) {
    char *start; size_t len;
    jtoken_type_t t = next_token(p, &start, &len);
    
    if (t == J_OBJECT_START) {
        p->pos -= 1;
        return encode_object(p, enc);
    }
    else if (t == J_ARRAY_START) {
        p->pos -= 1;
        return encode_array(p, enc);
    }
    else if (t == J_STRING) {
        char buf[256];
        if (len >= 256) len = 255;
        memcpy(buf, start, len);
        buf[len] = 0;
        
        if (is_mac_address(buf, len)) {
            return packr_encode_mac(enc, buf);
        }
        return packr_encode_string(enc, buf, len);
    }
    else if (t == J_NUMBER) {
        /* Check float */
        int is_float = 0;
        for (size_t i=0; i<len; i++) if (start[i]=='.' || start[i]=='e') is_float = 1;
        
        char buf[64];
        if (len >= 64) len = 63;
        memcpy(buf, start, len);
        buf[len] = 0;
        
        if (is_float) {
            double v = strtod(buf, NULL);
            return packr_encode_double(enc, v);
        } else {
            int32_t v = (int32_t)strtol(buf, NULL, 10);
            return packr_encode_int(enc, v);
        }
    }
    else if (t == J_TRUE) return packr_encode_bool(enc, 1);
    else if (t == J_FALSE) return packr_encode_bool(enc, 0);
    else if (t == J_NULL) return packr_encode_null(enc);
    
    return -1;
}

int json_encode_to_packr(const char *json, size_t len, packr_encoder_t *enc) {
    jparser_t p = {json, len, 0};
    return encode_value(&p, enc);
}
