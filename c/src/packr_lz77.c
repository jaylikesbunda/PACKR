/*
 * PACKR - LZ77 Implementation
 * Matches Python implementation exactly
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "packr.h"

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
#define HASH_MASK 0xFFF // 4096 entries -> 8KB
#define WINDOW_SIZE 8192

typedef struct {
    uint16_t head[4096];
    uint16_t prev[WINDOW_SIZE];
} lz77_hash_t;

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
    
    lz77_hash_t *ht = calloc(1, sizeof(lz77_hash_t));
    if (!ht) return 0;
    
    
    size_t ip = 0;
    size_t anchor = 0;
    
    while (ip < in_len) {
        size_t best_len = 0;
        size_t best_off = 0;
        
        // Hash check
        if (ip + 3 < in_len) {
            uint32_t h = (in[ip] | (in[ip+1] << 8) | (in[ip+2] << 16) | (in[ip+3] << 24));
            // Mix
            h = (h * 0x1e35a7bd) >> 16; 
            h &= HASH_MASK;
            
            // Find match
            size_t match_pos = ht->head[h];
            // Update head
            ht->prev[ip % WINDOW_SIZE] = match_pos;
            ht->head[h] = ip;
            
            
            int chain_len = 32;
            size_t curr_match = match_pos;
            
            
             while (curr_match != 0 && chain_len-- > 0) {
                 // Check distance
                 if (ip <= curr_match) break; // Should not happen
                 size_t dist = ip - curr_match;
                 if (dist > WINDOW_SIZE) break;
                 if (dist == 0) break;
                 
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
                 
                 // Move to next in chain
                 curr_match = ht->prev[curr_match % WINDOW_SIZE];
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
