/*
 * PACKR - LZ77 Implementation
 * Matches Python implementation exactly
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "packr.h"
#include "packr_platform.h"

// Helper to write safe
static void out_byte(uint8_t **out, uint8_t *end, uint8_t b) {
    if (*out < end) *(*out)++ = b;
}

static void out_bytes(uint8_t **out, uint8_t *end, const uint8_t *data, size_t len) {
    if (*out + len <= end) {
        memcpy(*out, data, len);
        *out += len;
    } else {
        *out = end;
    }
}

size_t packr_lz77_decompress(const uint8_t *in, size_t in_len, uint8_t *out, size_t out_cap) {
    if (in_len < 5) return 0;
    
    uint8_t fmt = in[0];
    if (fmt == 0x00) {
        // Uncompressed
        uint32_t len = in[1] | (in[2] << 8) | (in[3] << 16) | (in[4] << 24);
        if (in_len < 5 + len) return 0;
        if (len > out_cap) len = out_cap;
        memcpy(out, in + 5, len);
        return len;
    }
    
    if (fmt != 0x02) return 0; // Unknown
    
    uint32_t orig_len = in[1] | (in[2] << 8) | (in[3] << 16) | (in[4] << 24);
    if (orig_len > out_cap) return 0; // Output too small
    
    size_t ip = 5;
    size_t op = 0;
    
    while (ip < in_len && op < orig_len) {
        uint8_t ctrl = in[ip++];
        
        uint32_t lit_len = ctrl >> 4;
        uint32_t match_len_code = ctrl & 0x0F;
        
        // Extended literal length
        if (lit_len == 15) {
            while (ip < in_len) {
                uint8_t val = in[ip++];
                lit_len += val;
                if (val < 255) break;
            }
        }
        
        // Copy literals
        if (ip + lit_len > in_len) break;
        if (op + lit_len > out_cap) break;
        memcpy(out + op, in + ip, lit_len);
        op += lit_len;
        ip += lit_len;
        
        if (op >= orig_len || ip >= in_len) break;
        
        // Extended match length
        uint32_t match_len = match_len_code + 3;
        if (match_len_code == 15) {
            while (ip < in_len) {
                uint8_t val = in[ip++];
                match_len += val;
                if (val < 255) break;
            }
        }
        
        // Offset
        if (ip + 2 > in_len) break;
        uint16_t offset = in[ip] | (in[ip+1] << 8);
        ip += 2;
        
        // Copy match
        if (offset > op) break; // Invalid offset
        if (op + match_len > out_cap) break;
        
        // Overlapping copy support
        for (uint32_t i = 0; i < match_len; i++) {
            out[op] = out[op - offset];
            op++;
        }
    }
    
    return op;
}

// 4-byte hash
#define HASH_MASK 0xFFF // 4096 entries
#define WINDOW_SIZE 8192

typedef struct {
    uint32_t head[4096];        // 16KB - absolute positions, 0 = empty
    uint16_t prev[WINDOW_SIZE]; // 16KB - chain links within window
} lz77_hash_t;                  // 32KB total

#define LZ77_NO_ENTRY 0

// Hash helper: XOR-fold for better distribution across 4096 buckets
static inline uint32_t lz77_hash4(const uint8_t *p) {
    uint32_t h = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
    h *= 0x1e35a7bd;
    return (h ^ (h >> 16)) & HASH_MASK;
}

// Insert position into hash table (positions stored as pos+1, 0=empty)
static inline void lz77_hash_insert(lz77_hash_t *ht, const uint8_t *in, size_t pos, size_t in_len) {
    if (pos + 3 < in_len) {
        uint32_t h = lz77_hash4(in + pos);
        uint32_t old_head = ht->head[h];
        ht->prev[pos % WINDOW_SIZE] = (uint16_t)old_head; // truncated for chain
        ht->head[h] = (uint32_t)(pos + 1); // +1 so 0 means empty
    }
}

size_t packr_lz77_compress(const uint8_t *in, size_t in_len, uint8_t *out, size_t out_cap) {
    if (in_len == 0) return 0;

    uint8_t *op = out;
    uint8_t *op_end = out + out_cap;

    // Header
    if (op_end - op < 5) return 0;
    *op++ = 0x02;
    // Write original length
    *op++ = in_len & 0xFF;
    *op++ = (in_len >> 8) & 0xFF;
    *op++ = (in_len >> 16) & 0xFF;
    *op++ = (in_len >> 24) & 0xFF;

    // Entropy pre-check: skip LZ77 for high-entropy data
    {
        size_t sample = (in_len < 1024) ? in_len : 1024;
        uint8_t seen[256] = {0};
        size_t unique = 0;
        for (size_t i = 0; i < sample; i++) {
            if (!seen[in[i]]) { seen[in[i]] = 1; unique++; }
        }
        if (unique * 100 / sample > 80) {
            // High entropy, store uncompressed
            if (in_len + 5 <= out_cap) {
                out[0] = 0x00;
                out[1] = in_len & 0xFF;
                out[2] = (in_len >> 8) & 0xFF;
                out[3] = (in_len >> 16) & 0xFF;
                out[4] = (in_len >> 24) & 0xFF;
                memcpy(out + 5, in, in_len);
                return in_len + 5;
            }
            return 0;
        }
    }

    lz77_hash_t *ht = calloc(1, sizeof(lz77_hash_t));
    if (!ht) return 0;

    size_t ip = 0;
    size_t anchor = 0;

    while (ip < in_len) {
        size_t best_len = 0;
        size_t best_off = 0;

        // Hash check
        if (ip + 3 < in_len) {
            uint32_t h = lz77_hash4(in + ip);
            
            // Find match - head stores pos+1 (0 = empty)
            uint32_t stored = ht->head[h];
            // Insert current position into hash
            lz77_hash_insert(ht, in, ip, in_len);

            int chain_len = 32;
            uint32_t chain_val = stored; // pos+1 format

            while (chain_val != 0 && chain_len-- > 0) {
                size_t curr_match = (size_t)(chain_val - 1);
                // Check distance
                if (ip <= curr_match) break;
                size_t dist = ip - curr_match;
                if (dist > WINDOW_SIZE) break;

                size_t len = 0;
                while (ip + len < in_len && in[ip + len] == in[curr_match + len]) {
                    len++;
                    if (len >= 258) break; // Cap
                }

                if (len >= 3 && len > best_len) {
                    best_len = len;
                    best_off = dist;
                    if (len >= 32) break; // Sufficient
                }

                // Follow chain via prev array (stores pos+1 truncated to 16 bits)
                chain_val = (uint32_t)ht->prev[curr_match % WINDOW_SIZE];
            }
        }
        
        // Determine if we encode match
        int min_match = (ip - anchor > 0) ? 3 : 4;
        
        if (best_len >= min_match) {
            // Flush literals
            size_t lit_len = ip - anchor;
            uint8_t lit_nib = (lit_len >= 15) ? 15 : lit_len;
            uint8_t match_nib = (best_len - 3 >= 15) ? 15 : (best_len - 3);
            
            if (op < op_end) *op++ = (lit_nib << 4) | match_nib;
            
            // Extended lit len
            if (lit_nib == 15) {
                size_t rem = lit_len - 15;
                while (rem >= 255) {
                    if (op < op_end) *op++ = 255;
                    rem -= 255;
                }
                if (op < op_end) *op++ = rem;
            }
            
            // Copy literals
            out_bytes(&op, op_end, in + anchor, lit_len);
            
            // Extended match len
            if (match_nib == 15) {
                size_t rem = (best_len - 3) - 15;
                while (rem >= 255) {
                    if (op < op_end) *op++ = 255;
                    rem -= 255;
                }
                if (op < op_end) *op++ = rem;
            }
            
            // Offset
            if (op + 2 <= op_end) {
                *op++ = best_off & 0xFF;
                *op++ = (best_off >> 8) & 0xFF;
            }

            // Hash-fill: insert hash entries for all positions within the match
            // This allows future matches to reference positions inside this match
            {
                size_t match_end = ip + best_len;
                for (size_t j = ip + 1; j < match_end; j++) {
                    lz77_hash_insert(ht, in, j, in_len);
                }
            }

            // Advance
            ip += best_len;
            anchor = ip;
        } else {
            ip++;
        }
    }
    
    // Final literals
    size_t lit_len = ip - anchor;
    if (lit_len > 0) {
        uint8_t lit_nib = (lit_len >= 15) ? 15 : lit_len;
        if (op < op_end) *op++ = (lit_nib << 4);
        
        if (lit_nib == 15) {
            size_t rem = lit_len - 15;
            while (rem >= 255) {
                if (op < op_end) *op++ = 255;
                rem -= 255;
            }
            if (op < op_end) *op++ = rem;
        }
        out_bytes(&op, op_end, in + anchor, lit_len);
    }
    
    // Check expansion
    size_t out_len = op - out;
    if (out_len >= in_len) {
        // Store uncompressed
        if (in_len + 5 <= out_cap) {
            out[0] = 0x00;
            out[1] = in_len & 0xFF;
            out[2] = (in_len >> 8) & 0xFF;
            out[3] = (in_len >> 16) & 0xFF;
            out[4] = (in_len >> 24) & 0xFF;
            memcpy(out + 5, in, in_len);
            free(ht);
            return in_len + 5;
        }
    }
    
    free(ht);
    return out_len;
}

#define STREAM_WINDOW_SIZE 4096
#define STREAM_HASH_MASK 0x7FF // 2048 entries
typedef struct {
    uint16_t head[2048];
    uint16_t prev[STREAM_WINDOW_SIZE];
} lz77_stream_hash_t;

void packr_lz77_init(packr_lz77_stream_t *ctx) {
    memset(ctx, 0, sizeof(packr_lz77_stream_t));
    ctx->ht = packr_malloc(sizeof(lz77_stream_hash_t));
    if (ctx->ht) memset(ctx->ht, 0, sizeof(lz77_stream_hash_t));
}

void packr_lz77_destroy(packr_lz77_stream_t *ctx) {
    if (ctx->ht) {
        packr_free(ctx->ht);
        ctx->ht = NULL;
    }
}

static int flush_out(packr_lz77_stream_t *ctx, packr_flush_func flush_cb, void *user_data, size_t len) {
    if (len == 0) return 0;
    // fprintf(stderr, "LZ77: Flushing %zu bytes to callback\n", len);
    return flush_cb(user_data, ctx->out_buf, len);
}

static void emit_literal(packr_lz77_stream_t *ctx, uint8_t b, packr_flush_func flush_cb, void *user_data, size_t *out_idx) {
    ctx->out_buf[(*out_idx)++] = b;
    if (*out_idx >= sizeof(ctx->out_buf)) {
        flush_out(ctx, flush_cb, user_data, *out_idx);
        *out_idx = 0;
    }
}

static void emit_match(packr_lz77_stream_t *ctx, size_t dist, size_t len, packr_flush_func flush_cb, void *user_data, size_t *out_idx) {
    // Packr Format
    size_t lit_len = ctx->process_pos - ctx->anchor;
    uint8_t lit_nib = (lit_len >= 15) ? 15 : lit_len;
    uint8_t match_nib = (len - 3 >= 15) ? 15 : (len - 3);
    
    emit_literal(ctx, (lit_nib << 4) | match_nib, flush_cb, user_data, out_idx);
    
    // Extended lit len
    if (lit_nib == 15) {
        size_t rem = lit_len - 15;
        while (rem >= 255) {
            emit_literal(ctx, 255, flush_cb, user_data, out_idx);
            rem -= 255;
        }
        emit_literal(ctx, (uint8_t)rem, flush_cb, user_data, out_idx);
    }
    
    // Copy literals from window
    for (size_t i = 0; i < lit_len; i++) {
        emit_literal(ctx, ctx->window[ctx->anchor + i], flush_cb, user_data, out_idx);
    }
    
    // Extended match len
    if (match_nib == 15) {
        size_t rem = (len - 3) - 15;
        while (rem >= 255) {
            emit_literal(ctx, 255, flush_cb, user_data, out_idx);
            rem -= 255;
        }
        emit_literal(ctx, (uint8_t)rem, flush_cb, user_data, out_idx);
    }
    
    // Offset (Little Endian)
    emit_literal(ctx, dist & 0xFF, flush_cb, user_data, out_idx);
    emit_literal(ctx, (dist >> 8) & 0xFF, flush_cb, user_data, out_idx);
}

// Internal window update
static void slide_window(packr_lz77_stream_t *ctx) {
    lz77_stream_hash_t *ht = (lz77_stream_hash_t*)ctx->ht;
    
    // Move upper half to lower half
    memcpy(ctx->window, ctx->window + STREAM_WINDOW_SIZE, STREAM_WINDOW_SIZE);
    ctx->window_pos -= STREAM_WINDOW_SIZE;
    ctx->process_pos -= STREAM_WINDOW_SIZE;
    ctx->anchor -= STREAM_WINDOW_SIZE;
    
    // Adjust Hash
    if (ht) {
        for(int i=0; i<2048; i++) {
             if (ht->head[i] < STREAM_WINDOW_SIZE) ht->head[i] = 0;
             else ht->head[i] -= STREAM_WINDOW_SIZE;
        }
        for(int i=0; i<STREAM_WINDOW_SIZE; i++) {
             if (ht->prev[i] < STREAM_WINDOW_SIZE) ht->prev[i] = 0;
             else ht->prev[i] -= STREAM_WINDOW_SIZE;
        }
    }
}

int packr_lz77_compress_stream(packr_lz77_stream_t *ctx, const uint8_t *in, size_t in_len, 
                               packr_flush_func flush_cb, void *user_data, int flush) {
    if (!ctx->ht) {
        fprintf(stderr, "LZ77: Error - Hash table not initialized\n");
        return -1;
    }
    lz77_stream_hash_t *ht = (lz77_stream_hash_t*)ctx->ht;
    
    size_t in_processed = 0;
    size_t out_idx = 0; // Scratchpad index
    
    // If we have very little data and no flush, just buffer it
    if (!flush && (ctx->window_pos + in_len < STREAM_WINDOW_SIZE)) {
        memcpy(ctx->window + ctx->window_pos, in, in_len);
        ctx->window_pos += in_len;
        // fprintf(stderr, "LZ77: Buffered small chunk, new window_pos=%u\n", ctx->window_pos);
        return 0;
    }

    size_t loops = 0;
    while (in_processed < in_len || (flush && ctx->process_pos < ctx->window_pos)) {
        loops++;
        if (loops > 20000000) {
            fprintf(stderr, "LZ77: FATAL Loop! in_proc=%zu/%zu flush=%d w_pos=%u p_pos=%u\n",
                    in_processed, in_len, flush, ctx->window_pos, ctx->process_pos);
            return -1;
        }
        // 1. Move Data to Window
        size_t space = sizeof(ctx->window) - ctx->window_pos;
        
        // If window is full (or near full), we must slide
        if (space < 258 && ctx->process_pos >= STREAM_WINDOW_SIZE) {
             size_t lit_len = ctx->process_pos - ctx->anchor;
             if (ctx->anchor < STREAM_WINDOW_SIZE) {
                 if (lit_len > 0) {
                     uint8_t lit_nib = (lit_len >= 15) ? 15 : lit_len;
                     uint8_t match_nib = 0; // len=3, but offset=0 triggers skip in custom decoder
                     
                     emit_literal(ctx, (lit_nib << 4) | match_nib, flush_cb, user_data, &out_idx);
                     if (lit_nib == 15) {
                         size_t rem = lit_len - 15;
                         while (rem >= 255) { emit_literal(ctx, 255, flush_cb, user_data, &out_idx); rem -= 255; }
                         emit_literal(ctx, (uint8_t)rem, flush_cb, user_data, &out_idx);
                     }
                     // Copy literals
                     for (size_t i = 0; i < lit_len; i++) {
                         emit_literal(ctx, ctx->window[ctx->anchor + i], flush_cb, user_data, &out_idx);
                     }
                     // Match len dummy
                     // Offset 0
                     emit_literal(ctx, 0, flush_cb, user_data, &out_idx);
                     emit_literal(ctx, 0, flush_cb, user_data, &out_idx);
                 }
                 ctx->anchor = ctx->process_pos;
             }
        
             // Now safe to slide
             slide_window(ctx);
             space = sizeof(ctx->window) - ctx->window_pos;
        }

        size_t chunk = (in_len - in_processed);
        if (chunk > space) chunk = space;
        if (chunk > 0) {
            memcpy(ctx->window + ctx->window_pos, in + in_processed, chunk);
            ctx->window_pos += chunk;
            in_processed += chunk;
        }
        
        // 2. Compress Loop
        size_t lookahead_limit = (flush && in_processed == in_len) ? ctx->window_pos : (ctx->window_pos - 3);
        if (lookahead_limit > sizeof(ctx->window)) lookahead_limit = 0; // underflow protect
        if (lookahead_limit < ctx->process_pos) lookahead_limit = ctx->process_pos;

        while (ctx->process_pos < lookahead_limit) {
            uint32_t h = (ctx->window[ctx->process_pos] | (ctx->window[ctx->process_pos+1] << 8) | 
                         (ctx->window[ctx->process_pos+2] << 16) | (ctx->window[ctx->process_pos+3] << 24));
            h = (h * 0x1e35a7bd) >> 19; // Tuned for 2048 (11 bits? 32-13=19) -> check mask
            h &= STREAM_HASH_MASK;
            
            size_t match_pos = ht->head[h];
            ht->prev[ctx->process_pos % STREAM_WINDOW_SIZE] = match_pos; // Store in ring
            ht->head[h] = ctx->process_pos;
            
            size_t best_len = 0;
            size_t best_off = 0;
            
            // Check match
            if (match_pos > 0 && ctx->process_pos > match_pos) {
                 size_t dist = ctx->process_pos - match_pos;
                 if (dist <= STREAM_WINDOW_SIZE) {
                      // Check actual match
                      size_t len = 0;
                      size_t max_len = ctx->window_pos - ctx->process_pos;
                      if (max_len > 258) max_len = 258;
                      
                      while (len < max_len && ctx->window[match_pos + len] == ctx->window[ctx->process_pos + len]) {
                          len++;
                      }
                      
                      if (len >= 3) {
                          best_len = len;
                          best_off = dist;
                      }
                 }
            }
            
            if (best_len >= 3) {
                 emit_match(ctx, best_off, best_len, flush_cb, user_data, &out_idx);
                 
                 // Update hashes for matched bytes
                 for (size_t k = 0; k < best_len; k++) {
                     if (ctx->process_pos + k + 3 >= ctx->window_pos) break;
                     uint32_t h2 = (ctx->window[ctx->process_pos+k] | (ctx->window[ctx->process_pos+k+1] << 8) | 
                                  (ctx->window[ctx->process_pos+k+2] << 16) | (ctx->window[ctx->process_pos+k+3] << 24));
                     h2 = (h2 * 0x1e35a7bd) >> 19; 
                     h2 &= STREAM_HASH_MASK;
                     ht->prev[(ctx->process_pos + k) % STREAM_WINDOW_SIZE] = ht->head[h2]; 
                     ht->head[h2] = ctx->process_pos + k;
                 }
                 
                 ctx->process_pos += best_len;
                 ctx->anchor = ctx->process_pos;
            } else {
                 ctx->process_pos++;
            }
        }
        
        if (flush && ctx->process_pos < ctx->window_pos) {
             // Final flush of literals using Offset=0 dummy match
             size_t lit_len = ctx->process_pos - ctx->anchor;
             if (lit_len > 0) {
                 uint8_t lit_nib = (lit_len >= 15) ? 15 : lit_len;
                 uint8_t match_nib = 0; // len=3
                 
                 emit_literal(ctx, (lit_nib << 4) | match_nib, flush_cb, user_data, &out_idx);
                 
                 if (lit_nib == 15) {
                     size_t rem = lit_len - 15;
                     while (rem >= 255) { emit_literal(ctx, 255, flush_cb, user_data, &out_idx); rem -= 255; }
                     emit_literal(ctx, (uint8_t)rem, flush_cb, user_data, &out_idx);
                 }
                 
                 for (size_t i = 0; i < lit_len; i++) {
                     emit_literal(ctx, ctx->window[ctx->anchor + i], flush_cb, user_data, &out_idx);
                 }
                 
                 // Offset 0
                 emit_literal(ctx, 0, flush_cb, user_data, &out_idx);
                 emit_literal(ctx, 0, flush_cb, user_data, &out_idx);
             }
             ctx->anchor = ctx->process_pos;
             ctx->process_pos = ctx->window_pos; 
        }
        
        if (out_idx > 0) {
            flush_out(ctx, flush_cb, user_data, out_idx);
            out_idx = 0;
        }
        
        if (in_processed == in_len && !flush) break; // Wait for more data
        if (flush && in_processed == in_len && ctx->process_pos >= ctx->window_pos) break; // Done
    }
    
    return 0;
}
