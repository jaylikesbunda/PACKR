/*
 * PACKR - Core Header
 */

#ifndef PACKR_H
#define PACKR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Streaming Callback: Returns 0 on success, non-zero on error */
typedef int (*packr_flush_func)(void *user_data, const uint8_t *data, size_t len);

/* Constants */
#define PACKR_MAGIC         0x31524B50  /* "PKR1" LE */
#define PACKR_VERSION       0x01
#define PACKR_DICT_SIZE     64
#define PACKR_MAX_BUFFER    (1024 * 1024)

/* Token Types */
typedef enum {
    TOKEN_FIELD         = 0x00,
    TOKEN_STRING        = 0x40,
    TOKEN_MAC           = 0x80,
    TOKEN_INT           = 0xC0,
    TOKEN_FLOAT16       = 0xC1,
    TOKEN_FLOAT32       = 0xC2,
    TOKEN_DELTA_SMALL   = 0xC3,
    TOKEN_DELTA_LARGE   = 0xD3,
    TOKEN_NEW_STRING    = 0xD4,
    TOKEN_NEW_FIELD     = 0xD5,
    TOKEN_NEW_MAC       = 0xD6,
    TOKEN_BOOL_TRUE     = 0xD7,
    TOKEN_BOOL_FALSE    = 0xD8,
    TOKEN_NULL          = 0xD9,
    TOKEN_ARRAY_START   = 0xDA,
    TOKEN_ARRAY_END     = 0xDB,
    TOKEN_OBJECT_START  = 0xDC,
    TOKEN_OBJECT_END    = 0xDD,
    
    TOKEN_DOUBLE        = 0xDE,
    TOKEN_BINARY        = 0xDF,

    /* Extended Tokens */
    TOKEN_SCHEMA_DEF    = 0xE0,
    TOKEN_SCHEMA_REF    = 0xE1,
    TOKEN_SCHEMA_REPEAT = 0xE2,
    TOKEN_RECORD_BATCH  = 0xE3,
    TOKEN_COLUMN_BATCH  = 0xE4,
    TOKEN_RLE_REPEAT    = 0xE5,
    TOKEN_DELTA_ZERO    = 0xE6,
    TOKEN_DELTA_ONE     = 0xE7,
    TOKEN_DELTA_NEG_ONE = 0xE8,
    
    TOKEN_ULTRA_BATCH   = 0xE9,
    TOKEN_CONST_COLUMN  = 0xEA,
    TOKEN_BITPACK_COL   = 0xEB,
    TOKEN_DELTA_MEDIUM  = 0xEC,
    TOKEN_RICE_COLUMN   = 0xED,
    TOKEN_MFV_COLUMN    = 0xEE,
    
    /* Streaming Extensions */
    TOKEN_ARRAY_STREAM  = 0xEF,
    TOKEN_BATCH_PARTIAL = 0xF0,
} packr_token_t;

/* Dictionary Entry */
typedef struct {
    char *value;
    size_t length;
    uint64_t last_used;
} dict_entry_t;

/* Dictionary */
typedef struct {
    dict_entry_t entries[PACKR_DICT_SIZE];
    uint64_t usage_counter; /* Monotonic counter for LRU */
} packr_dict_t;

/* LZ77 Streaming Context */
#define LZ77_WINDOW_SIZE 4096
#define LZ77_BUFFER_SIZE (LZ77_WINDOW_SIZE * 2)

typedef struct {
    uint8_t window[LZ77_BUFFER_SIZE];
    uint16_t window_pos;
    uint16_t process_pos;
    uint16_t anchor;
    
    // Opaque hash table pointer to keep header clean
    void *ht;

    // Output scratchpad
    uint8_t out_buf[128];
} packr_lz77_stream_t;

/* Encoder Context */
typedef struct {
    uint8_t *buffer;
    size_t capacity;
    size_t pos;
    size_t symbol_count;

    packr_dict_t fields;
    packr_dict_t strings;
    packr_dict_t macs;

    bool compress;
    size_t total_alloc;

    /* Streaming Support */
    packr_flush_func flush_cb;
    void *user_data;
    uint32_t current_crc;
    packr_lz77_stream_t lz77;
} packr_encoder_t;

/* Decoder Context */
typedef struct {
    const uint8_t *data;
    size_t size;
    size_t pos;
    
    uint8_t *internal_data; /* Decompressed or owned data buffer */
    
    packr_dict_t fields;
    packr_dict_t strings;
    packr_dict_t macs;
    
    double last_nums[PACKR_DICT_SIZE];
    uint8_t last_types[PACKR_DICT_SIZE];
    int current_field;
    
    size_t total_alloc;
} packr_decoder_t;

/* API */
void packr_encoder_init(packr_encoder_t *ctx, bool compress, packr_flush_func flush_cb, void *user_data, uint8_t *work_buffer, size_t work_cap);
int packr_encode_null(packr_encoder_t *ctx);
int packr_encode_bool(packr_encoder_t *ctx, bool value);
int packr_encode_int(packr_encoder_t *ctx, int32_t value);
int packr_encode_float(packr_encoder_t *ctx, double value);
int packr_encode_double(packr_encoder_t *ctx, double value);
int packr_encode_binary(packr_encoder_t *ctx, const uint8_t *data, size_t len);
int packr_encode_string(packr_encoder_t *ctx, const char *str, size_t len);
int packr_encode_field(packr_encoder_t *ctx, const char *str, size_t len);
int packr_encode_mac(packr_encoder_t *ctx, const char *str);
int packr_encode_token(packr_encoder_t *ctx, packr_token_t token);
size_t packr_encoder_finish(packr_encoder_t *ctx, uint8_t *out_buffer);
void packr_encoder_destroy(packr_encoder_t *ctx);

/* LZ77 Streaming Context */
void packr_lz77_init(packr_lz77_stream_t *ctx);
void packr_lz77_destroy(packr_lz77_stream_t *ctx);
int packr_lz77_compress_stream(packr_lz77_stream_t *ctx, const uint8_t *in, size_t in_len, 
                               packr_flush_func flush_cb, void *user_data, int flush);

void packr_decoder_init(packr_decoder_t *ctx, const uint8_t *data, size_t size);
int packr_decode_next(packr_decoder_t *ctx, char **cursor, char *end);
void packr_decoder_destroy(packr_decoder_t *ctx);

/* Helpers needed by JSON parser */
int packr_encode_varint(packr_encoder_t *ctx, uint32_t value);
uint32_t zigzag_encode(int32_t value);

/* Memory Tracking */
void* packr_malloc(size_t size);
void packr_free(void *ptr);
size_t packr_get_total_alloc(void);
size_t packr_get_peak_alloc(void);
void packr_reset_alloc_stats(void);

/* LZ77 / Transform */
size_t packr_lz77_compress(const uint8_t *in, size_t in_len, uint8_t *out, size_t out_cap);
size_t packr_lz77_decompress(const uint8_t *in, size_t in_len, uint8_t *out, size_t out_cap);

#ifdef __cplusplus
}
#endif

#endif
