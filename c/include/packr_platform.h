/*
 * PACKR - Platform Optimization & Abstraction
 */

#ifndef PACKR_PLATFORM_H
#define PACKR_PLATFORM_H

#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Unaligned Access Macros
 * Architectures like x86, ARMv7+ (usually), ESP32 (Xtensa), and some RISC-V support unaligned access.
 * Others (older ARM, some MIPS, strict RISC-V) do not.
 */

#if defined(__xtensa__) || defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
    #define PACKR_UNALIGNED_ACCESS 1
#elif defined(PACKR_FORCE_UNALIGNED)
    #define PACKR_UNALIGNED_ACCESS 1
#else
    #define PACKR_UNALIGNED_ACCESS 0
#endif

// Little Endian Load/Store helpers
#if PACKR_UNALIGNED_ACCESS

static inline uint32_t packr_load_le32(const void *ptr) {
    return *(const uint32_t *)ptr;
}

static inline uint16_t packr_load_le16(const void *ptr) {
    return *(const uint16_t *)ptr;
}

static inline void packr_store_le32(void *ptr, uint32_t val) {
    *(uint32_t *)ptr = val;
}

#else

static inline uint32_t packr_load_le32(const void *ptr) {
    const uint8_t *p = (const uint8_t *)ptr;
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline uint16_t packr_load_le16(const void *ptr) {
    const uint8_t *p = (const uint8_t *)ptr;
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline void packr_store_le32(void *ptr, uint32_t val) {
    uint8_t *p = (uint8_t *)ptr;
    p[0] = val & 0xFF;
    p[1] = (val >> 8) & 0xFF;
    p[2] = (val >> 16) & 0xFF;
    p[3] = (val >> 24) & 0xFF;
}

#endif

/* 
 * SIMD Hooks (Placeholder for ESP32-S3 PIE or others)
 * Returns length of match or 0 if SIMD not used/available.
 */
static inline size_t packr_simd_match_len(const uint8_t *p1, const uint8_t *p2, size_t max_len) {
    // TODO: Implement ESP32-S3 PIE specific instructions here later
    return 0; 
}


#ifdef __cplusplus
}
#endif

#endif // PACKR_PLATFORM_H
