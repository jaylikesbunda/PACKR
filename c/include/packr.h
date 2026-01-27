/*
 * PACKR - Structure-First Streaming Compression
 * 
 * Core header file with token definitions and API declarations.
 */

#ifndef PACKR_H
#define PACKR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version */
#define PACKR_VERSION_MAJOR 1
#define PACKR_VERSION_MINOR 0

/* Magic number: "PKR1" */
#define PACKR_MAGIC_0 0x50
#define PACKR_MAGIC_1 0x4B
#define PACKR_MAGIC_2 0x52
#define PACKR_MAGIC_3 0x31
#define PACKR_VERSION 0x01

/* Dictionary size */
#define PACKR_DICT_SIZE 64

/* Token types */
typedef enum {
    /* Dictionary references (0x00-0xBF) */
    PACKR_TOKEN_FIELD_BASE   = 0x00,  /* 0x00-0x3F: field dict */
    PACKR_TOKEN_STRING_BASE  = 0x40,  /* 0x40-0x7F: string dict */
    PACKR_TOKEN_MAC_BASE     = 0x80,  /* 0x80-0xBF: MAC dict */
    
    /* Literal values */
    PACKR_TOKEN_INT          = 0xC0,  /* varint follows */
    PACKR_TOKEN_FLOAT16      = 0xC1,  /* 2 bytes fixed16 */
    PACKR_TOKEN_FLOAT32      = 0xC2,  /* 4 bytes fixed32 */
    
    /* Delta encoding (0xC3-0xD2 are small deltas -8 to +7) */
    PACKR_TOKEN_DELTA_BASE   = 0xC3,
    PACKR_TOKEN_DELTA_LARGE  = 0xD3,  /* varint follows */
    
    /* New dictionary entries */
    PACKR_TOKEN_NEW_STRING   = 0xD4,  /* length + bytes */
    PACKR_TOKEN_NEW_FIELD    = 0xD5,  /* length + bytes */
    PACKR_TOKEN_NEW_MAC      = 0xD6,  /* 6 bytes */
    
    /* Literals */
    PACKR_TOKEN_BOOL_TRUE    = 0xD7,
    PACKR_TOKEN_BOOL_FALSE   = 0xD8,
    PACKR_TOKEN_NULL         = 0xD9,
    
    /* Structure */
    PACKR_TOKEN_ARRAY_START  = 0xDA,  /* count follows */
    PACKR_TOKEN_ARRAY_END    = 0xDB,
    PACKR_TOKEN_OBJECT_START = 0xDC,
    PACKR_TOKEN_OBJECT_END   = 0xDD,
    
    /* Extended tokens */
    PACKR_TOKEN_ULTRA_BATCH  = 0xE9,
    PACKR_TOKEN_RLE_REPEAT   = 0xE5,
    PACKR_TOKEN_DELTA_ZERO   = 0xE6,
    PACKR_TOKEN_DELTA_ONE    = 0xE7,
    PACKR_TOKEN_DELTA_NEG1   = 0xE8,
    PACKR_TOKEN_BITPACK      = 0xEB,
} packr_token_t;

/* Column flags */
#define PACKR_COL_CONSTANT  0x01
#define PACKR_COL_DELTA     0x02
#define PACKR_COL_RLE       0x04
#define PACKR_COL_BITPACK   0x08

/* Frame flags */
#define PACKR_FRAME_DICT_UPDATE  0x01
#define PACKR_FRAME_RICE         0x02
#define PACKR_FRAME_DICT_RESET   0x04

/* Error codes */
typedef enum {
    PACKR_OK = 0,
    PACKR_ERR_BUFFER_FULL,
    PACKR_ERR_INVALID_TOKEN,
    PACKR_ERR_DICT_FULL,
    PACKR_ERR_DECODE_ERROR,
    PACKR_ERR_CRC_MISMATCH,
    PACKR_ERR_INVALID_MAGIC,
    PACKR_ERR_VERSION_MISMATCH,
} packr_error_t;

/* Dictionary entry */
typedef struct {
    char *value;
    uint16_t length;
    uint8_t lru_counter;
} packr_dict_entry_t;

/* Dictionary */
typedef struct {
    packr_dict_entry_t entries[PACKR_DICT_SIZE];
    uint8_t count;
    uint8_t next_index;
} packr_dict_t;

/* Encoder context */
typedef struct {
    uint8_t *buffer;
    size_t buffer_size;
    size_t write_pos;
    
    packr_dict_t fields;
    packr_dict_t strings;
    packr_dict_t macs;
    
    int32_t last_values[PACKR_DICT_SIZE];
    uint8_t value_types[PACKR_DICT_SIZE];
    
    uint32_t symbol_count;
    uint8_t flags;
} packr_encoder_t;

/* Decoder context */
typedef struct {
    const uint8_t *data;
    size_t data_size;
    size_t read_pos;
    
    packr_dict_t fields;
    packr_dict_t strings;
    packr_dict_t macs;
    
    int32_t last_values[PACKR_DICT_SIZE];
    uint8_t value_types[PACKR_DICT_SIZE];
    
    int current_field_index;
} packr_decoder_t;

/* Encoder API */
void packr_encoder_init(packr_encoder_t *enc, uint8_t *buffer, size_t size);
void packr_encoder_reset(packr_encoder_t *enc);
packr_error_t packr_encode_int(packr_encoder_t *enc, int32_t value);
packr_error_t packr_encode_float(packr_encoder_t *enc, float value);
packr_error_t packr_encode_string(packr_encoder_t *enc, const char *str, size_t len);
packr_error_t packr_encode_mac(packr_encoder_t *enc, const uint8_t *mac);
packr_error_t packr_encode_bool(packr_encoder_t *enc, bool value);
packr_error_t packr_encode_null(packr_encoder_t *enc);
packr_error_t packr_encode_field(packr_encoder_t *enc, const char *name, size_t len);
packr_error_t packr_encode_object_start(packr_encoder_t *enc);
packr_error_t packr_encode_object_end(packr_encoder_t *enc);
packr_error_t packr_encode_array_start(packr_encoder_t *enc, uint32_t count);
packr_error_t packr_encode_array_end(packr_encoder_t *enc);
size_t packr_encoder_finalize(packr_encoder_t *enc);

/* Decoder API */
void packr_decoder_init(packr_decoder_t *dec, const uint8_t *data, size_t size);
void packr_decoder_reset(packr_decoder_t *dec);
packr_error_t packr_decode_token(packr_decoder_t *dec, uint8_t *token);
packr_error_t packr_decode_int(packr_decoder_t *dec, int32_t *value);
packr_error_t packr_decode_float(packr_decoder_t *dec, float *value);
packr_error_t packr_decode_string(packr_decoder_t *dec, char *buf, size_t buf_size, size_t *out_len);
packr_error_t packr_decode_mac(packr_decoder_t *dec, uint8_t *mac);

/* Utility functions */
size_t packr_encode_varint(uint8_t *buf, uint32_t value);
size_t packr_decode_varint(const uint8_t *buf, size_t len, uint32_t *value);
size_t packr_encode_signed_varint(uint8_t *buf, int32_t value);
size_t packr_decode_signed_varint(const uint8_t *buf, size_t len, int32_t *value);
uint32_t packr_crc32(const uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* PACKR_H */
