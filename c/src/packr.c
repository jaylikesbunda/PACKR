/*
 * PACKR - Core Implementation
 */

#include "packr.h"
#include "packr_platform.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <stdio.h>

#define MIN(a,b) ((a)<(b)?(a):(b))

/* CRC32 Table */
static uint32_t crc32_table[256];
static int crc32_table_inited = 0;

static size_t g_total_alloc = 0;
static size_t g_peak_alloc = 0;

size_t packr_get_total_alloc(void) { return g_total_alloc; }
size_t packr_get_peak_alloc(void) { return g_peak_alloc; }
void packr_reset_alloc_stats(void) { g_total_alloc = 0; g_peak_alloc = 0; }

void* packr_malloc(size_t size) {
    if (size == 0) return NULL;
    g_total_alloc += size;
    if (g_total_alloc > g_peak_alloc) g_peak_alloc = g_total_alloc;
    // We store the size before the pointer to track free
    void *ptr = malloc(size + sizeof(size_t));
    if (!ptr) return NULL;
    *(size_t*)ptr = size;
    return (void*)((size_t*)ptr + 1);
}

void packr_free(void *ptr) {
    if (!ptr) return;
    void *real_ptr = (void*)((size_t*)ptr - 1);
    g_total_alloc -= *(size_t*)real_ptr;
    free(real_ptr);
}


static void make_crc_table(void) {
    uint32_t c;
    for (int n = 0; n < 256; n++) {
        c = (uint32_t)n;
        for (int k = 0; k < 8; k++) {
            if (c & 1) c = 0xEDB88320 ^ (c >> 1);
            else c = c >> 1;
        }
        crc32_table[n] = c;
    }
    crc32_table_inited = 1;
}

static uint32_t update_crc32(uint32_t crc, const uint8_t *data, size_t len) {
    if (!crc32_table_inited) make_crc_table();
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc ^ data[i]) & 0xFF];
    }
    return crc;
}

static uint32_t calculate_crc32(const uint8_t *data, size_t len) {
    return update_crc32(0xFFFFFFFF, data, len) ^ 0xFFFFFFFF;
}

/* Helpers */


uint32_t zigzag_encode(int32_t value) {
    return (uint32_t)((value << 1) ^ (value >> 31));
}

static int packr_flush_buffer(packr_encoder_t *ctx);

static int buffer_append_internal(packr_encoder_t *ctx, const uint8_t *data, size_t len, int update_crc) {
    if (update_crc && ctx->flush_cb) {
        // Only rolling update CRC in Streaming Mode
        if (ctx->compress) {
             // If compressing, CRC happens BEFORE compression?
             // Python spec check: `calculate_crc32(ctx->buffer, frame_len)` on the UNCOMPRESSED data?
             // Legacy `packr_encoder_finish`: `crc = calculate_crc32(ctx->buffer, frame_len)`.
             // `buffer` here IS uncompressed.
             // Then compression happens on `buffer`.
             // Correct. CRC is on plaintext.
        }
        ctx->current_crc = update_crc32(ctx->current_crc, data, len);
    }
    
    // Check if buffer is valid
    if (!ctx->buffer || ctx->capacity == 0) return -1;

    size_t offset = 0;
    while (offset < len) {
        size_t space = ctx->capacity - ctx->pos;
        if (space == 0) {
             if (packr_flush_buffer(ctx) != 0) return -1;
             space = ctx->capacity - ctx->pos;
        }
        
        size_t chunk = (len - offset) < space ? (len - offset) : space;
        
        memcpy(ctx->buffer + ctx->pos, data + offset, chunk);
        ctx->pos += chunk;
        offset += chunk;
    }
    return 0;
}

static int buffer_append(packr_encoder_t *ctx, const uint8_t *data, size_t len) {
    return buffer_append_internal(ctx, data, len, 1);
}

static int buffer_append_byte(packr_encoder_t *ctx, uint8_t b) {
    return buffer_append(ctx, &b, 1);
}

/* Varint */
int packr_encode_varint(packr_encoder_t *ctx, uint32_t value) {
    uint8_t buf[5];
    int i = 0;
    while (value > 0x7F) {
        buf[i++] = (value & 0x7F) | 0x80;
        value >>= 7;
    }
    buf[i++] = value & 0x7F;
    return buffer_append(ctx, buf, i);
}

/* Dictionary Management */
static void dict_init(packr_dict_t *dict) {
    memset(dict, 0, sizeof(packr_dict_t));
}

static int dict_get_or_add(packr_dict_t *dict, const char *value, size_t len, int *out_index, size_t *alloc_counter) {
    /* Lookup */
    for (int i = 0; i < PACKR_DICT_SIZE; i++) {
        if (dict->entries[i].value && 
            dict->entries[i].length == len &&
            memcmp(dict->entries[i].value, value, len) == 0) {
            
            dict->entries[i].last_used = ++dict->usage_counter;
            *out_index = i;
            return 0; /* Found */
        }
    }
    
    /* Add new */
    int index = -1;
    
    /* First pass: find empty slot */
    for (int i = 0; i < PACKR_DICT_SIZE; i++) {
        if (dict->entries[i].value == NULL) {
            index = i;
            break;
        }
    }
    
    /* Second pass: find LRU */
    if (index == -1) {
        uint64_t min_usage = UINT64_MAX;
        for (int i = 0; i < PACKR_DICT_SIZE; i++) {
            if (dict->entries[i].last_used < min_usage) {
                min_usage = dict->entries[i].last_used;
                index = i;
            }
        }
    }
    
    /* Replace */
    if (dict->entries[index].value) {
        if (alloc_counter) *alloc_counter -= (dict->entries[index].length + 1);
        packr_free(dict->entries[index].value);
    }
    dict->entries[index].value = packr_malloc(len + 1);
    if (alloc_counter) *alloc_counter += (len + 1);
    memcpy(dict->entries[index].value, value, len);
    dict->entries[index].value[len] = '\0';
    dict->entries[index].length = len;
    dict->entries[index].last_used = ++dict->usage_counter;
    
    *out_index = index;
    return 1; /* Added */
}

/* Encoder */

void packr_encoder_init(packr_encoder_t *ctx, packr_flush_func flush_cb, void *user_data, uint8_t *work_buffer, size_t work_cap) {
    memset(ctx, 0, sizeof(packr_encoder_t));
    ctx->buffer = work_buffer;
    ctx->capacity = work_cap;
    ctx->flush_cb = flush_cb;
    ctx->user_data = user_data;
    ctx->current_crc = 0xFFFFFFFF;
    
    ctx->total_alloc = sizeof(packr_encoder_t);
    dict_init(&ctx->fields);
    dict_init(&ctx->strings);
    dict_init(&ctx->macs);
    
    if (ctx->flush_cb) {
        /* Streaming Mode: Write Header Immediately */
        if (ctx->compress) {
            packr_lz77_init(&ctx->lz77);
        
            // Emit LZ77 Header
            uint8_t lz_head[7];
            lz_head[0] = 0xFE; 
            lz_head[1] = 0x03;
            // Length unknown/max
            lz_head[3] = 0xFF; lz_head[4] = 0xFF; lz_head[5] = 0xFF; lz_head[6] = 0xFF;
            
            ctx->flush_cb(ctx->user_data, lz_head, 7);
        }
        
        ctx->pos = 0;
        uint8_t header[16];
        int h_pos = 0;
        header[h_pos++] = 0x50; header[h_pos++] = 0x4B; header[h_pos++] = 0x52; header[h_pos++] = 0x31;
        header[h_pos++] = PACKR_VERSION;
        header[h_pos++] = 0x00; // Flags
        header[h_pos++] = 0x00; // Symbol Count (Varint 0 = 1 byte)
        buffer_append(ctx, header, h_pos);
    } else {
        /* Legacy Mode: Reserve Header Space */
        ctx->pos = 11; 
    }
}

static void dict_destroy(packr_dict_t *dict, size_t *alloc_counter) {
    for (int i = 0; i < PACKR_DICT_SIZE; i++) {
        if (dict->entries[i].value) {
            if (alloc_counter) *alloc_counter -= (dict->entries[i].length + 1);
            packr_free(dict->entries[i].value);
            dict->entries[i].value = NULL;
        }
    }
}

void packr_encoder_destroy(packr_encoder_t *ctx) {
    dict_destroy(&ctx->fields, &ctx->total_alloc);
    dict_destroy(&ctx->strings, &ctx->total_alloc);
    dict_destroy(&ctx->macs, &ctx->total_alloc);
    if (ctx->compress) packr_lz77_destroy(&ctx->lz77);
    ctx->total_alloc -= sizeof(packr_encoder_t);
}

int packr_encode_token(packr_encoder_t *ctx, packr_token_t token) {
    ctx->symbol_count++;
    return buffer_append_byte(ctx, (uint8_t)token);
}

int packr_encode_null(packr_encoder_t *ctx) {
    return packr_encode_token(ctx, TOKEN_NULL);
}

int packr_encode_bool(packr_encoder_t *ctx, bool value) {
    return packr_encode_token(ctx, value ? TOKEN_BOOL_TRUE : TOKEN_BOOL_FALSE);
}

int packr_encode_int(packr_encoder_t *ctx, int32_t value) {
    packr_encode_token(ctx, TOKEN_INT);

    return packr_encode_varint(ctx, zigzag_encode(value));
}

int packr_encode_float(packr_encoder_t *ctx, double value) {
    packr_encode_token(ctx, TOKEN_FLOAT32);
    
    double val_scaled = rint(value * 65536.0);
    
    /* Clamp to int32 range */
    if (val_scaled > 2147483647.0) val_scaled = 2147483647.0;
    if (val_scaled < -2147483648.0) val_scaled = -2147483648.0;
    
    int32_t scaled = (int32_t)val_scaled;
    uint8_t buf[4];
    buf[0] = scaled & 0xFF;
    buf[1] = (scaled >> 8) & 0xFF;
    buf[2] = (scaled >> 16) & 0xFF;
    buf[3] = (scaled >> 24) & 0xFF;
    return buffer_append(ctx, buf, 4);
}

int packr_encode_double(packr_encoder_t *ctx, double value) {
    packr_encode_token(ctx, TOKEN_DOUBLE);
    
    // Store as raw 64-bit IEE754
    uint64_t v64;
    memcpy(&v64, &value, sizeof(double));
    
    uint8_t buf[8];
    buf[0] = v64 & 0xFF;
    buf[1] = (v64 >> 8) & 0xFF;
    buf[2] = (v64 >> 16) & 0xFF;
    buf[3] = (v64 >> 24) & 0xFF;
    buf[4] = (v64 >> 32) & 0xFF;
    buf[5] = (v64 >> 40) & 0xFF;
    buf[6] = (v64 >> 48) & 0xFF;
    buf[7] = (v64 >> 56) & 0xFF;
    
    return buffer_append(ctx, buf, 8);
}

int packr_encode_binary(packr_encoder_t *ctx, const uint8_t *data, size_t len) {
    packr_encode_token(ctx, TOKEN_BINARY);
    packr_encode_varint(ctx, (uint32_t)len);
    return buffer_append(ctx, data, len);
}

int packr_encode_string(packr_encoder_t *ctx, const char *str, size_t len) {
    int index;
    int is_new = dict_get_or_add(&ctx->strings, str, len, &index, &ctx->total_alloc);
    
    if (is_new) {
        packr_encode_token(ctx, TOKEN_NEW_STRING);
        packr_encode_varint(ctx, (uint32_t)len);
        return buffer_append(ctx, (const uint8_t*)str, len);
    } else {
        return packr_encode_token(ctx, (packr_token_t)(TOKEN_STRING + index));
    }
}

int packr_encode_field(packr_encoder_t *ctx, const char *str, size_t len) {
    int index;
    int is_new = dict_get_or_add(&ctx->fields, str, len, &index, &ctx->total_alloc);
    
    if (is_new) {
        packr_encode_token(ctx, TOKEN_NEW_FIELD);
        packr_encode_varint(ctx, (uint32_t)len);
        return buffer_append(ctx, (const uint8_t*)str, len);
    } else {
        return packr_encode_token(ctx, (packr_token_t)(TOKEN_FIELD + index));
    }
}

int packr_encode_mac(packr_encoder_t *ctx, const char *str) {
    int index;
    int is_new = dict_get_or_add(&ctx->macs, str, strlen(str), &index, &ctx->total_alloc);
    
    if (is_new) {
        packr_encode_token(ctx, TOKEN_NEW_MAC);
        
        /* Parse MAC to 6 bytes */
        /* Assume format AA:BB:CC:DD:EE:FF or AABBCCDDEEFF */
        uint8_t mac[6];
        int bytes_parsed = 0;
        const char *p = str;
        while (*p && bytes_parsed < 6) {
            char high = *p++;
            if (high == ':' || high == '-') continue;
            char low = *p++;
            
            uint8_t val = 0;
            if (high >= '0' && high <= '9') val = (high - '0') << 4;
            else if (high >= 'A' && high <= 'F') val = (high - 'A' + 10) << 4;
            else if (high >= 'a' && high <= 'f') val = (high - 'a' + 10) << 4;
            
            if (low >= '0' && low <= '9') val |= (low - '0');
            else if (low >= 'A' && low <= 'F') val |= (low - 'A' + 10);
            else if (low >= 'a' && low <= 'f') val |= (low - 'a' + 10);
            
            mac[bytes_parsed++] = val;
        }
        
        return buffer_append(ctx, mac, 6);
    } else {
        return packr_encode_token(ctx, (packr_token_t)(TOKEN_MAC + index));
    }
}


static int packr_flush_buffer(packr_encoder_t *ctx) {
    if (ctx->pos == 0) return 0;
    
    if (ctx->flush_cb) {
        // Streaming
        if (ctx->compress) {
            // Push via LZ77
            return packr_lz77_compress_stream(&ctx->lz77, ctx->buffer, ctx->pos, 
                                              ctx->flush_cb, ctx->user_data, 0); // Flush=0 (accumulate)
        } else {
             int ret = ctx->flush_cb(ctx->user_data, ctx->buffer, ctx->pos);
             if (ret != 0) return ret;
        }
    } else {
        // Fixed Buffer Mode - Overflow
        return -1;
    }
    
    ctx->pos = 0;
    return 0;
}

size_t packr_encoder_finish(packr_encoder_t *ctx, uint8_t *out_buffer) {
    if (ctx->flush_cb) {
        // --- Streaming Mode ---
        packr_flush_buffer(ctx); // Flush any pending payload
        
        uint32_t final_crc = ctx->current_crc ^ 0xFFFFFFFF;
        uint8_t crc_buf[4];
        packr_store_le32(crc_buf, final_crc);
        
        // Append CRC raw (do not update CRC with CRC itself)
        buffer_append_internal(ctx, crc_buf, 4, 0);
        
        // Flush Final (CRC) 
        packr_flush_buffer(ctx);
        
        // Final LZ77 Flush
        if (ctx->compress) {
            packr_lz77_compress_stream(&ctx->lz77, NULL, 0, ctx->flush_cb, ctx->user_data, 1); // Flush=1
        }
        
        return 0; // Length undefined for streaming
    } else {
        // --- Legacy/Buffered Mode ---
        /* Construct Header */
        uint8_t header[16];
        int h_pos = 0;
        
        /* Magic */
        header[h_pos++] = 0x50; header[h_pos++] = 0x4B; header[h_pos++] = 0x52; header[h_pos++] = 0x31;
        header[h_pos++] = PACKR_VERSION;
        header[h_pos++] = 0x00;
        
        /* Symbol Count */
        uint8_t varint[5];
        uint32_t val = ctx->symbol_count;
        int v_len = 0;
        while (val > 0x7F) {
            varint[v_len++] = (val & 0x7F) | 0x80;
            val >>= 7;
        }
        varint[v_len++] = val & 0x7F;
        memcpy(header + h_pos, varint, v_len);
        h_pos += v_len;
        
        /* Move body data to directly after header */
        size_t body_len = ctx->pos - 11;
        memmove(ctx->buffer + h_pos, ctx->buffer + 11, body_len);
        
        /* Copy header */
        memcpy(ctx->buffer, header, h_pos);
        
        size_t frame_len = h_pos + body_len;
        
        /* CRC (Full Calculation) */
        uint32_t crc = calculate_crc32(ctx->buffer, frame_len);
        ctx->buffer[frame_len++] = crc & 0xFF;
        ctx->buffer[frame_len++] = (crc >> 8) & 0xFF;
        ctx->buffer[frame_len++] = (crc >> 16) & 0xFF;
        ctx->buffer[frame_len++] = (crc >> 24) & 0xFF;
        
        /* Compress? */
        if (ctx->compress && frame_len > 20) {
            uint8_t *comp_buf = NULL;
            int using_internal_buf = 0;
            
            /* Try to use the remaining space in the buffer as scratchpad */
            if (ctx->capacity > frame_len && (ctx->capacity - frame_len) >= (frame_len + 128)) {
                comp_buf = ctx->buffer + frame_len;
                using_internal_buf = 1;
            } else {
                comp_buf = packr_malloc(frame_len + 128);
            }

            if (comp_buf) {
                size_t comp_len = packr_lz77_compress(ctx->buffer, frame_len, comp_buf, frame_len + 128);
                
                if (comp_len > 0 && comp_len < frame_len) {
                    if (comp_len + 2 <= ctx->capacity) {
                         /* Move compressed data to front */
                         if (using_internal_buf) {
                             memmove(ctx->buffer + 2, comp_buf, comp_len);
                         } else {
                             memcpy(ctx->buffer + 2, comp_buf, comp_len);
                         }
                         
                         ctx->buffer[0] = 0xFE;
                         ctx->buffer[1] = 0x03; // LZ77 Transform
                         frame_len = comp_len + 2;
                    }
                }
                
                if (!using_internal_buf) {
                    packr_free(comp_buf);
                }
            }
        }
        
        return frame_len;
    }
}

/* Decoder (Minimal for validation) */
static uint32_t decode_varint(packr_decoder_t *ctx, int *bytes_read) {
    uint32_t res = 0;
    int shift = 0;
    *bytes_read = 0;
    while (ctx->pos < ctx->size) {
        uint8_t b = ctx->data[ctx->pos++];
        (*bytes_read)++;
        res |= (b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return res;
}

void packr_decoder_init(packr_decoder_t *ctx, const uint8_t *data, size_t size) {
    memset(ctx, 0, sizeof(packr_decoder_t));
    ctx->data = data;
    ctx->size = size;
    ctx->pos = 0;
    ctx->total_alloc = sizeof(packr_decoder_t);
    ctx->current_field = -1;
    memset(ctx->last_types, 0, sizeof(ctx->last_types));
    
    /* Check for compression transform (0xFE 0x03) */
    if (size > 7 && data[0] == 0xFE && data[1] == 0x03) {
        /* LZ77 compressed */
        uint32_t orig_len = data[3] | (data[4] << 8) | (data[5] << 16) | (data[6] << 24);
        if (orig_len < 1024 * 1024 * 10) { // Limit to 10MB sanity
            uint8_t *dec_buf = packr_malloc(orig_len + 4); // Extra for safety
            if (dec_buf) {
                size_t actual = packr_lz77_decompress(data + 2, size - 2, dec_buf, orig_len + 4);
                if (actual > 0) {
                    ctx->data = dec_buf;
                    ctx->size = actual;
                    ctx->internal_data = dec_buf;
                    ctx->total_alloc += actual;
                } else {
                    packr_free(dec_buf);
                }
            }
        }
    }
    
    dict_init(&ctx->fields);
    dict_init(&ctx->strings);
    dict_init(&ctx->macs);
    
    /* Skip Header: Magic(4) + Ver(1) + Flags(1) + SymCnt(varint) */
    if (ctx->size > 6) {
        if (memcmp(ctx->data, "PKR1", 4) == 0) {
            ctx->pos = 6;
            int v_len;
            decode_varint(ctx, &v_len); /* Symbol count */
        }
    }
}

void packr_decoder_destroy(packr_decoder_t *ctx) {
    dict_destroy(&ctx->fields, &ctx->total_alloc);
    dict_destroy(&ctx->strings, &ctx->total_alloc);
    dict_destroy(&ctx->macs, &ctx->total_alloc);
    if (ctx->internal_data) {
        ctx->total_alloc -= ctx->size; // Rough estimate
        packr_free(ctx->internal_data);
    }
    ctx->total_alloc -= sizeof(packr_decoder_t);
}


/* Helper: Zigzag Decode */
static int32_t zigzag_decode(uint32_t val) {
    return (int32_t)((val >> 1) ^ -(int32_t)(val & 1));
}

typedef struct {
    const uint8_t *buf;
    size_t cap;
    size_t pos;
    uint32_t bit_buf;
    int bit_cnt;
} bitreader_t;

static void br_init(bitreader_t *br, const uint8_t *buf, size_t cap) {
    br->buf = buf; br->cap = cap; br->pos = 0; br->bit_buf = 0; br->bit_cnt = 0;
}

static uint32_t br_read(bitreader_t *br, int bits) {
    uint32_t res = 0;
    for (int i = 0; i < bits; i++) {
        if (br->bit_cnt == 0) {
            if (br->pos >= br->cap) return 0xFFFFFFFF; // Error marker
            br->bit_buf = br->buf[br->pos++];
            br->bit_cnt = 8;
        }
        res = (res << 1) | ((br->bit_buf >> (br->bit_cnt - 1)) & 1);
        br->bit_cnt--;
    }
    return res;
}

static uint32_t br_read_unary(bitreader_t *br) {
    uint32_t count = 0;
    while (1) {
        uint32_t bit = br_read(br, 1);
        if (bit == 1 || bit == 0xFFFFFFFF) break;
        count++;
        if (count > 65536) break; // Sanity
    }
    return count;
}


// Helper for safe appending
static void buf_append_str(char **cursor, char *end, const char *str) {
    while (*str && *cursor < end - 1) {
        *(*cursor)++ = *str++;
    }
    **cursor = '\0';
}

static void buf_append_char(char **cursor, char *end, char c) {
    if (*cursor < end - 1) {
        *(*cursor)++ = c;
        **cursor = '\0';
    }
}

int packr_decode_next(packr_decoder_t *ctx, char **cursor, char *end) {
    if (ctx->pos >= ctx->size) return 0;
    
    if (ctx->pos > ctx->size - 4) return 0;

    uint8_t token = ctx->data[ctx->pos++];
    char temp[64];
    
    if (token == TOKEN_NULL) {
        buf_append_str(cursor, end, "null");
    }
    else if (token == TOKEN_BOOL_TRUE) {
        buf_append_str(cursor, end, "true");
    }
    else if (token == TOKEN_BOOL_FALSE) {
        buf_append_str(cursor, end, "false");
    }
    else if (token == TOKEN_FLOAT32) {
        if (ctx->pos + 4 > ctx->size) return 0;
        int32_t scaled = ctx->data[ctx->pos] | (ctx->data[ctx->pos+1] << 8) | 
                         (ctx->data[ctx->pos+2] << 16) | (ctx->data[ctx->pos+3] << 24);
        ctx->pos += 4;
        double val = (double)scaled / 65536.0;
        if (ctx->current_field >= 0 && ctx->current_field < PACKR_DICT_SIZE) {
            ctx->last_nums[ctx->current_field] = val;
            ctx->last_types[ctx->current_field] = 2;
        }
        snprintf(temp, sizeof(temp), "%.7g", val);
        buf_append_str(cursor, end, temp);
    }
    else if (token == TOKEN_DOUBLE) {
        if (ctx->pos + 8 > ctx->size) return 0;
        uint64_t v64 = (uint64_t)ctx->data[ctx->pos] | 
                       ((uint64_t)ctx->data[ctx->pos+1] << 8) | 
                       ((uint64_t)ctx->data[ctx->pos+2] << 16) | 
                       ((uint64_t)ctx->data[ctx->pos+3] << 24) |
                       ((uint64_t)ctx->data[ctx->pos+4] << 32) |
                       ((uint64_t)ctx->data[ctx->pos+5] << 40) |
                       ((uint64_t)ctx->data[ctx->pos+6] << 48) |
                       ((uint64_t)ctx->data[ctx->pos+7] << 56);
        ctx->pos += 8;
        
        double val;
        memcpy(&val, &v64, sizeof(double));
        
        if (ctx->current_field >= 0 && ctx->current_field < PACKR_DICT_SIZE) {
            ctx->last_nums[ctx->current_field] = val;
            ctx->last_types[ctx->current_field] = 2; 
        }
        
        // Print with high precision
        snprintf(temp, sizeof(temp), "%.17g", val);
        buf_append_str(cursor, end, temp);
    }
    else if (token == TOKEN_BINARY) {
        int bytes;
        uint32_t len = decode_varint(ctx, &bytes);
        if (ctx->pos + len > ctx->size) return 0;
        
        // Skip data
        ctx->pos += len;
        
        char msg[64];
        snprintf(msg, sizeof(msg), "\"<binary data len=%u>\"", (unsigned int)len);
        buf_append_str(cursor, end, msg);
    }
    else if (token == TOKEN_INT || token == TOKEN_DELTA_LARGE || (token >= 0xC3 && token <= 0xD2) || 
             token == TOKEN_DELTA_ZERO || token == TOKEN_DELTA_ONE || token == TOKEN_DELTA_NEG_ONE || 
             token == TOKEN_DELTA_MEDIUM) {
        
        int32_t val = 0;
        int is_delta = 0;
        
        if (token == TOKEN_INT) {
            int bytes;
            val = zigzag_decode(decode_varint(ctx, &bytes));
        } else if (token == TOKEN_DELTA_LARGE) {
            int bytes;
            val = zigzag_decode(decode_varint(ctx, &bytes));
            is_delta = 1;
        } else if (token >= 0xC3 && token <= 0xD2) { // TOKEN_DELTA_SMALL range
            val = (int)token - 0xC3 - 8;
            is_delta = 1;
        } else if (token == TOKEN_DELTA_ZERO) { val = 0; is_delta = 1; }
        else if (token == TOKEN_DELTA_ONE) { val = 1; is_delta = 1; }
        else if (token == TOKEN_DELTA_NEG_ONE) { val = -1; is_delta = 1; }
        else if (token == TOKEN_DELTA_MEDIUM) {
            if (ctx->pos >= ctx->size) return 0; // Ensure data exists
            val = (int)ctx->data[ctx->pos++] - 64;
            is_delta = 1;
        }
        
        if (is_delta && ctx->current_field >= 0 && ctx->current_field < PACKR_DICT_SIZE && ctx->last_types[ctx->current_field] != 0) {
            double prev = ctx->last_nums[ctx->current_field];
                if (ctx->last_types[ctx->current_field] == 2) { // Previous was float
                double res = prev + (double)val / 65536.0;
                ctx->last_nums[ctx->current_field] = res;
                snprintf(temp, sizeof(temp), "%.7g", res);
            } else { // Previous was int
                int32_t res = (int32_t)prev + val;
                ctx->last_nums[ctx->current_field] = (double)res;
                snprintf(temp, sizeof(temp), "%d", res);
            }
        } else {
            if (ctx->current_field >= 0 && ctx->current_field < PACKR_DICT_SIZE) {
                ctx->last_nums[ctx->current_field] = (double)val;
                ctx->last_types[ctx->current_field] = 1; // Store as int
            }
            snprintf(temp, sizeof(temp), "%d", val);
        }
        buf_append_str(cursor, end, temp);
    }
    else if (token == TOKEN_NEW_STRING || token == TOKEN_NEW_FIELD) {
        int bytes;
        uint32_t len = decode_varint(ctx, &bytes);
        if (ctx->pos + len > ctx->size) return 0;
        
        char *str_val = packr_malloc(len + 1);
        memcpy(str_val, ctx->data + ctx->pos, len);
        str_val[len] = '\0';
        ctx->pos += len;
        
        int dummy;
        if (token == TOKEN_NEW_FIELD) dict_get_or_add(&ctx->fields, str_val, len, &dummy, &ctx->total_alloc);
        else dict_get_or_add(&ctx->strings, str_val, len, &dummy, &ctx->total_alloc);
        
        buf_append_char(cursor, end, '"');
        buf_append_str(cursor, end, str_val); 
        buf_append_char(cursor, end, '"');
        packr_free(str_val);
    }
    else if ((token >= TOKEN_FIELD && token < TOKEN_STRING) || 
             (token >= TOKEN_STRING && token < TOKEN_MAC)) {
        packr_dict_t *d = (token < TOKEN_STRING) ? &ctx->fields : &ctx->strings;
        int index = (token < TOKEN_STRING) ? (token - TOKEN_FIELD) : (token - TOKEN_STRING);
        
        if (index < PACKR_DICT_SIZE && d->entries[index].value) {
            buf_append_char(cursor, end, '"');
            buf_append_str(cursor, end, d->entries[index].value);
            buf_append_char(cursor, end, '"');
            d->entries[index].last_used = ++d->usage_counter;
        } else {
            buf_append_str(cursor, end, "\"\"");
        }
    }
    else if (token == TOKEN_NEW_MAC || (token >= TOKEN_MAC && token < TOKEN_INT)) {
        char mac_str[18];
        if (token == TOKEN_NEW_MAC) {
            if (ctx->pos + 6 > ctx->size) return 0;
            const uint8_t *m = ctx->data + ctx->pos;
            ctx->pos += 6;
            snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
                     m[0], m[1], m[2], m[3], m[4], m[5]);
            int dummy;
            dict_get_or_add(&ctx->macs, mac_str, 17, &dummy, &ctx->total_alloc);
        } else {
            int index = token - TOKEN_MAC;
            if (index < PACKR_DICT_SIZE && ctx->macs.entries[index].value) {
                strncpy(mac_str, ctx->macs.entries[index].value, 18);
                ctx->macs.entries[index].last_used = ++ctx->macs.usage_counter;
            } else {
                mac_str[0] = 0;
            }
        }
        buf_append_char(cursor, end, '"');
        buf_append_str(cursor, end, mac_str);
        buf_append_char(cursor, end, '"');
    }
    else if (token == TOKEN_ARRAY_START) {
        int bytes;
        uint32_t count = decode_varint(ctx, &bytes);
        buf_append_char(cursor, end, '[');
        for (uint32_t i = 0; i < count; i++) {
            if (i > 0) buf_append_char(cursor, end, ',');
            if (!packr_decode_next(ctx, cursor, end)) break;
        }
        /* Consume END if present */
        if (ctx->pos < ctx->size && ctx->data[ctx->pos] == TOKEN_ARRAY_END) ctx->pos++;
        buf_append_char(cursor, end, ']');
    }
    else if (token == TOKEN_ARRAY_STREAM) {
        buf_append_char(cursor, end, '[');
        bool first = true;
        while (ctx->pos < ctx->size && ctx->data[ctx->pos] != TOKEN_ARRAY_END) {
            if (!first) buf_append_char(cursor, end, ',');
            if (!packr_decode_next(ctx, cursor, end)) break;
            first = false;
        }
        if (ctx->pos < ctx->size) ctx->pos++; // Skip END
        buf_append_char(cursor, end, ']');
    }
    else if (token == TOKEN_OBJECT_START) {
        buf_append_char(cursor, end, '{');
        bool first = true;
        while (ctx->pos < ctx->size && ctx->data[ctx->pos] != TOKEN_OBJECT_END) {
            if (!first) buf_append_char(cursor, end, ',');
            first = false;
            
            /* Field Name (Key) */
            uint8_t next_t = ctx->data[ctx->pos];
            int field_idx = -1;
            if (next_t >= TOKEN_FIELD && next_t < TOKEN_STRING) field_idx = next_t - TOKEN_FIELD;
            else if (next_t == TOKEN_NEW_FIELD) {
            }
            
            packr_decode_next(ctx, cursor, end); 
            
            buf_append_char(cursor, end, ':');
            
            int old_field = ctx->current_field;
            ctx->current_field = field_idx; // Track for value
            packr_decode_next(ctx, cursor, end); // Value
            ctx->current_field = old_field;
        }
        if (ctx->pos < ctx->size) ctx->pos++; /* Skip END */
        buf_append_char(cursor, end, '}');
    }
    else if (token == TOKEN_ULTRA_BATCH || token == TOKEN_BATCH_PARTIAL) {
        bool partial = (token == TOKEN_BATCH_PARTIAL);
        int bytes_read;
        uint32_t record_count = decode_varint(ctx, &bytes_read);
        uint32_t field_count = decode_varint(ctx, &bytes_read);
        
        /* Store field names and flags */
        char **field_names = packr_malloc(sizeof(char*) * field_count);
        uint8_t *flags = packr_malloc(field_count);
        
        for (uint32_t i = 0; i < field_count; i++) {
            /* Field name is encoded as a regular value (string/token) */
            char *old_cursor = *cursor;
            if (packr_decode_next(ctx, cursor, end) && *cursor > old_cursor + 1) {
                /* *cursor now points after "name". Extract. */
                size_t slen = (*cursor - old_cursor) - 2;
                field_names[i] = packr_malloc(slen + 1);
                if (field_names[i]) {
                    memcpy(field_names[i], old_cursor + 1, slen);
                    field_names[i][slen] = 0;
                }
            } else {
                field_names[i] = packr_malloc(8);
                if (field_names[i]) strcpy(field_names[i], "unknown");
            }
            *cursor = old_cursor; // Rewind cursor!
            
            if (ctx->pos < ctx->size) {
                flags[i] = ctx->data[ctx->pos++];
            } else {
                flags[i] = 0;
            }
        }
        
        /* Buffers for each column */
        typedef struct {
            double *nums;
            char **strs;
            uint8_t *types;
            uint8_t *validity;
        } col_data_t;
        
        col_data_t *cols = packr_malloc(sizeof(col_data_t) * field_count);
        for(uint32_t i=0; i<field_count; i++) {
            cols[i].nums = packr_malloc(sizeof(double) * record_count);
            cols[i].strs = packr_malloc(sizeof(char*) * record_count);
            cols[i].types = packr_malloc(record_count);
            cols[i].validity = packr_malloc(record_count);
            if (cols[i].nums) memset(cols[i].nums, 0, sizeof(double) * record_count);
            if (cols[i].strs) memset(cols[i].strs, 0, sizeof(char*) * record_count);
            if (cols[i].types) memset(cols[i].types, 0, record_count);
            if (cols[i].validity) memset(cols[i].validity, 1, record_count); // Default Valid
        }
        
        /* Decode each column */
        for (uint32_t i = 0; i < field_count; i++) {
            if (flags[i] & 0x08) { // HAS_NULLS
                 size_t bytes = (record_count + 7) / 8;
                 size_t start = ctx->pos;
                 ctx->pos += bytes;
                 // Decode
                 for(uint32_t k=0; k<record_count; k++) {
                     if (start + (k/8) < ctx->size) {
                        uint8_t b = ctx->data[start + (k/8)];
                        cols[i].validity[k] = (b >> (k%8)) & 1;
                     }
                 }
            }

            if (flags[i] & 0x01) { // CONSTANT
                char *old_cursor = *cursor;
                packr_decode_next(ctx, cursor, end);
                size_t vlen = *cursor - old_cursor;
                char *vstr = packr_malloc(vlen + 1);
                memcpy(vstr, old_cursor, vlen); vstr[vlen] = 0;
                *cursor = old_cursor; // Rewind
                for(uint32_t j=0; j<record_count; j++) cols[i].strs[j] = vstr; // Shared
                // Note: Shared pointer, be careful but it's simpler
            } else if (flags[i] & 0x02) { // DELTA
                /* Numeric Column */
                uint8_t vtoken = ctx->data[ctx->pos]; // Peek
                
                if (vtoken == TOKEN_MFV_COLUMN) {
                    ctx->pos++;
                    int bytes_read;
                    uint32_t dcount = decode_varint(ctx, &bytes_read);
                    
                    // Decode Mode
                    double mode_val = 0;
                    if (ctx->pos < ctx->size) {
                      uint8_t mt = ctx->data[ctx->pos++];
                      if (mt == TOKEN_INT) mode_val = zigzag_decode(decode_varint(ctx, &bytes_read));
                      else if (mt == TOKEN_FLOAT32) {
                           int32_t scaled = ctx->data[ctx->pos] | (ctx->data[ctx->pos+1] << 8) | 
                                            (ctx->data[ctx->pos+2] << 16) | (ctx->data[ctx->pos+3] << 24);
                           ctx->pos += 4;
                           mode_val = scaled / 65536.0;
                      }
                      else if (mt == TOKEN_BOOL_TRUE) mode_val = 1.0;
                      else if (mt == TOKEN_BOOL_FALSE) mode_val = 0.0;
                      else if (mt == TOKEN_DOUBLE) {
                          uint64_t v64 = (uint64_t)ctx->data[ctx->pos] | ((uint64_t)ctx->data[ctx->pos+1] << 8) | 
                                         ((uint64_t)ctx->data[ctx->pos+2] << 16) | ((uint64_t)ctx->data[ctx->pos+3] << 24) |
                                         ((uint64_t)ctx->data[ctx->pos+4] << 32) | ((uint64_t)ctx->data[ctx->pos+5] << 40) |
                                         ((uint64_t)ctx->data[ctx->pos+6] << 48) | ((uint64_t)ctx->data[ctx->pos+7] << 56);
                          ctx->pos += 8;
                          memcpy(&mode_val, &v64, sizeof(double));
                      }
                    }
                    
                    // Mask
                    size_t mask_len = (dcount + 7) / 8;
                    size_t mask_start = ctx->pos;
                    ctx->pos += mask_len;
                    
                    uint32_t j = 0;
                    for(uint32_t k=0; k<dcount && j<record_count; k++) {
                       uint8_t b = ctx->data[mask_start + (k/8)];
                       double val;
                       if ( (b >> (k%8)) & 1 ) {
                           // Exception
                           uint8_t et = ctx->data[ctx->pos++];
                           double eval = 0;
                           if (et == TOKEN_INT) eval = zigzag_decode(decode_varint(ctx, &bytes_read));
                           else if (et == TOKEN_FLOAT32) {
                               int32_t scaled = ctx->data[ctx->pos] | (ctx->data[ctx->pos+1] << 8) | 
                                                (ctx->data[ctx->pos+2] << 16) | (ctx->data[ctx->pos+3] << 24);
                               ctx->pos += 4;
                               eval = scaled / 65536.0;
                           }
                           else if (et == TOKEN_BOOL_TRUE) eval = 1.0;
                           else if (et == TOKEN_BOOL_FALSE) eval = 0.0;
                           else if (et == TOKEN_DOUBLE) {
                               uint64_t v64 = (uint64_t)ctx->data[ctx->pos] | ((uint64_t)ctx->data[ctx->pos+1] << 8) | 
                                              ((uint64_t)ctx->data[ctx->pos+2] << 16) | ((uint64_t)ctx->data[ctx->pos+3] << 24) |
                                              ((uint64_t)ctx->data[ctx->pos+4] << 32) | ((uint64_t)ctx->data[ctx->pos+5] << 40) |
                                              ((uint64_t)ctx->data[ctx->pos+6] << 48) | ((uint64_t)ctx->data[ctx->pos+7] << 56);
                               ctx->pos += 8;
                               memcpy(&eval, &v64, sizeof(double));
                           }
                           val = eval;
                       } else {
                           val = mode_val;
                       }
                       cols[i].nums[j++] = val; cols[i].types[j-1] = 1;
                    }
                } else {
                    /* Base value */
                    vtoken = ctx->data[ctx->pos++];
                    double prev = 0;
                    if (vtoken == TOKEN_INT) {
                        prev = zigzag_decode(decode_varint(ctx, &bytes_read));
                    } else if (vtoken == TOKEN_FLOAT32) {
                        int32_t scaled = ctx->data[ctx->pos] | (ctx->data[ctx->pos+1] << 8) |
                                         (ctx->data[ctx->pos+2] << 16) | (ctx->data[ctx->pos+3] << 24);
                        ctx->pos += 4;
                        prev = scaled / 65536.0;
                    } else if (vtoken == TOKEN_DOUBLE) {
                        /* IEEE 754 double - 8 bytes LE. Deltas still use 65536 scaling. */
                        uint64_t v64 = (uint64_t)ctx->data[ctx->pos] | ((uint64_t)ctx->data[ctx->pos+1] << 8) |
                                       ((uint64_t)ctx->data[ctx->pos+2] << 16) | ((uint64_t)ctx->data[ctx->pos+3] << 24) |
                                       ((uint64_t)ctx->data[ctx->pos+4] << 32) | ((uint64_t)ctx->data[ctx->pos+5] << 40) |
                                       ((uint64_t)ctx->data[ctx->pos+6] << 48) | ((uint64_t)ctx->data[ctx->pos+7] << 56);
                        ctx->pos += 8;
                        memcpy(&prev, &v64, sizeof(double));
                        vtoken = TOKEN_FLOAT32; /* Treat same as FLOAT32 for delta scaling */
                    }
                    cols[i].nums[0] = prev;
                    cols[i].types[0] = 1; // Number
                    
                    uint32_t j = 1;
                    while (j < record_count) {
                        uint8_t dt = ctx->data[ctx->pos++];
                        double delta = 0;
                        if (dt == TOKEN_BITPACK_COL) {
                            uint32_t dcount = decode_varint(ctx, &bytes_read);
                            for(uint32_t k=0; k<dcount && j<record_count; k+=2) {
                                uint8_t pack = ctx->data[ctx->pos++];
                                int d1 = (pack >> 4) - 8;
                                prev += (double)d1 / (vtoken == TOKEN_FLOAT32 ? 65536.0 : 1.0);
                                cols[i].nums[j++] = prev; cols[i].types[j-1] = 1;
                                if (j < record_count && k+1 < dcount) {
                                    int d2 = (pack & 0x0F) - 8;
                                    prev += (double)d2 / (vtoken == TOKEN_FLOAT32 ? 65536.0 : 1.0);
                                    cols[i].nums[j++] = prev; cols[i].types[j-1] = 1;
                                }
                            }
                        } else if (dt == TOKEN_RICE_COLUMN) {
                             uint32_t dcount = decode_varint(ctx, &bytes_read);
                             if (ctx->pos < ctx->size) {
                                 int k = ctx->data[ctx->pos++];
                                 uint32_t max_j = j + dcount;
                                 if (max_j > record_count) max_j = record_count;
                                 
                                 bitreader_t br; br_init(&br, ctx->data + ctx->pos, ctx->size - ctx->pos);
                                 for(; j < max_j; j++) {
                                     uint32_t q = br_read_unary(&br);
                                     uint32_t r = br_read(&br, k);
                                     if (q == 0xFFFFFFFF || r == 0xFFFFFFFF) break;
                                     uint32_t u = (q << k) | r;
                                     int32_t d = zigzag_decode(u);
                                     prev += (double)d / (vtoken == TOKEN_FLOAT32 ? 65536.0 : 1.0);
                                     cols[i].nums[j] = prev; cols[i].types[j] = 1;
                                 }
                                 // Account for consumed bytes. br.pos already includes any partially read byte.
                                 ctx->pos += br.pos;
                             }
                        } else if (dt == TOKEN_RLE_REPEAT) {
                             uint32_t run = decode_varint(ctx, &bytes_read);
                             for(uint32_t k=0; k<run && j<record_count; k++) {
                                 cols[i].nums[j++] = prev; cols[i].types[j-1] = 1;
                             }
                        } else {
                             /* Single delta tokens */
                             if (dt == TOKEN_DELTA_ZERO) delta = 0;
                             else if (dt == TOKEN_DELTA_ONE) delta = 1;
                             else if (dt == TOKEN_DELTA_NEG_ONE) delta = -1;
                             else if (dt >= 0xC3 && dt <= 0xD2) delta = (int)dt - 0xC3 - 8;
                             else if (dt == TOKEN_DELTA_LARGE) delta = zigzag_decode(decode_varint(ctx, &bytes_read));
                             else if (dt == TOKEN_DELTA_MEDIUM) delta = (int)ctx->data[ctx->pos++] - 64;
                             
                             prev += (double)delta / (vtoken == TOKEN_FLOAT32 ? 65536.0 : 1.0);
                             cols[i].nums[j++] = prev; cols[i].types[j-1] = 1;
                        }
                    }
                }
            } else { // RLE
                uint32_t j = 0;
                // Check MFV first
                if (ctx->data[ctx->pos] == TOKEN_MFV_COLUMN) {
                     ctx->pos++;
                     int bytes_read;
                     uint32_t dcount = decode_varint(ctx, &bytes_read);
                     // Decode Mode String
                     char *old_cursor = *cursor;
                     packr_decode_next(ctx, cursor, end); 
                     size_t vlen = *cursor - old_cursor;
                     char *mode_str = packr_malloc(vlen + 1);
                     memcpy(mode_str, old_cursor, vlen); mode_str[vlen] = 0;
                     *cursor = old_cursor; // Reset cursor
                     
                     // Mask
                     size_t mask_len = (dcount + 7) / 8;
                     size_t mask_start = ctx->pos;
                     ctx->pos += mask_len;
                     
                     for(uint32_t k=0; k<dcount && j<record_count; k++) {
                           uint8_t b = ctx->data[mask_start + (k/8)];
                           if ( (b >> (k%8)) & 1 ) {
                               // Exception
                               char *oc = *cursor;
                               packr_decode_next(ctx, cursor, end);
                               size_t elen = *cursor - oc;
                               char *estr = packr_malloc(elen + 1);
                               memcpy(estr, oc, elen); estr[elen] = 0;
                               *cursor = oc;
                               cols[i].strs[j++] = estr;
                           } else {
                               size_t ml = strlen(mode_str);
                               char *s = packr_malloc(ml + 1);
                               memcpy(s, mode_str, ml+1);
                               cols[i].strs[j++] = s;
                           }
                     }
                     packr_free(mode_str);
                } else {
                    while (j < record_count) {
                        char *old_cursor = *cursor;
                        packr_decode_next(ctx, cursor, end);
                        size_t vlen = *cursor - old_cursor;
                        char *vstr = packr_malloc(vlen + 1);
                        memcpy(vstr, old_cursor, vlen); vstr[vlen] = 0;
                        *cursor = old_cursor;
                        
                        cols[i].strs[j++] = vstr; // Shared
                        if (j < record_count && ctx->data[ctx->pos] == TOKEN_RLE_REPEAT) {
                             ctx->pos++;
                             int bytes_read;
                             uint32_t run = decode_varint(ctx, &bytes_read);
                             for(uint32_t k=0; k<run && j<record_count; k++) {
                                 cols[i].strs[j++] = vstr; // Shared
                             }
                        }
                    }
                }
            }
        }
        
        /* Reconstruct JSON */
        if (!partial) buf_append_char(cursor, end, '[');
        
        for(uint32_t r=0; r<record_count; r++) {
            if (r > 0) buf_append_char(cursor, end, ',');
            buf_append_char(cursor, end, '{');
            bool first_field = true;
            for(uint32_t c=0; c<field_count; c++) {
                if (cols[c].validity[r] == 0) continue; // Skip missing field
                
                if (!first_field) buf_append_char(cursor, end, ',');
                first_field = false;
                
                buf_append_char(cursor, end, '"');
                buf_append_str(cursor, end, field_names[c]);
                buf_append_str(cursor, end, "\":");
                
                if (cols[c].types[r] == 1) { // Numeric
                    double v = cols[c].nums[r];
                    if (v == (double)(int64_t)v && (v < 2147483648.0 && v > -2147483648.0)) {
                         snprintf(temp, sizeof(temp), "%d", (int32_t)v);
                    }
                    else snprintf(temp, sizeof(temp), "%.17g", v);
                    buf_append_str(cursor, end, temp);
                } else if (cols[c].strs[r]) {
                    buf_append_str(cursor, end, cols[c].strs[r]);
                } else {
                    buf_append_str(cursor, end, "null");
                }
            }
            buf_append_char(cursor, end, '}');
        }
        if (!partial) buf_append_char(cursor, end, ']');
        
        /* Free memory */
        for(uint32_t i=0; i<field_count; i++) {
            packr_free(field_names[i]);
            // Need to free shared strings carefully if we want to be perfect
            // But for benchmark, we can leak a bit or just avoid shared ones for non-constants
            // For now, let's just free the ones that are NOT constant (constant ones shared same ptr)
            if (!(flags[i] & 0x01)) {
                for(uint32_t j=0; j<record_count; j++) {
                    if (cols[i].strs[j]) {
                        // Check if it's the same as previous to avoid double free in RLE
                        if (j == 0 || cols[i].strs[j] != cols[i].strs[j-1]) {
                             packr_free(cols[i].strs[j]);
                        }
                    }
                }
            } else {
                 packr_free(cols[i].strs[0]);
            }
            packr_free(cols[i].nums);
            packr_free(cols[i].strs);
            packr_free(cols[i].types);
            packr_free(cols[i].validity);
        }
        packr_free(field_names);
        packr_free(flags);
        packr_free(cols);
    }
    
    return 1;
}

