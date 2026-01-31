/*
 * PACKR Comprehensive Benchmark (Simplified Phase 1)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "packr.h"
#include "packr_json.h"

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <sys/time.h>
#include <sys/resource.h>
#endif

#define MAX_BUFFER_SIZE (10 * 1024 * 1024)

static double get_time_ms(void) {
#ifdef _WIN32
    LARGE_INTEGER frequency, counter;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart * 1000.0 / frequency.QuadPart;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000.0) + (tv.tv_usec / 1000.0);
#endif
}

// Memory helpers removed - using library internal tracking

static char *load_file(const char *path, size_t *size) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    *size = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(*size + 1);
    fread(buf, 1, *size, f);
    buf[*size] = 0;
    fclose(f);
    return buf;
}

static int run_tool_encode(const char *in, const char *out, int compress) {
    size_t in_size;
    char *json = load_file(in, &in_size);
    if (!json) return 1;
    
    uint8_t *buffer = malloc(MAX_BUFFER_SIZE);
    packr_encoder_t enc;
    packr_encoder_init(&enc, NULL, NULL, buffer, MAX_BUFFER_SIZE);
    enc.compress = compress;
    
    if (json_encode_to_packr(json, in_size, &enc) != 0) {
        free(json); free(buffer); return 1;
    }
    
    size_t out_size = packr_encoder_finish(&enc, buffer);
    
    FILE *f = fopen(out, "wb");
    fwrite(buffer, 1, out_size, f);
    fclose(f);
    
    packr_encoder_destroy(&enc);
    
    printf("Debug Peak Alloc: %zu bytes\n", packr_get_peak_alloc());
    
    free(json); free(buffer);
    return 0;
}

static int run_tool_decode(const char *in, const char *out) {
    size_t in_size;
    char *pkr = load_file(in, &in_size);
    if (!pkr) return 1;
    
    char *json_out = malloc(MAX_BUFFER_SIZE);
    json_out[0] = 0;
    
    packr_decoder_t dec;
    packr_decoder_init(&dec, (uint8_t*)pkr, in_size);
    char *cursor = json_out;
    char *end = json_out + MAX_BUFFER_SIZE;
    packr_decode_next(&dec, &cursor, end);
    
    FILE *f = fopen(out, "wb");
    fwrite(json_out, 1, strlen(json_out), f);
    fclose(f);
    
    packr_decoder_destroy(&dec);
    free(pkr); free(json_out);
    return 0;
}

static void run_benchmark(const char *name, const char *path) {
    printf("Benchmarking: %s\n", name);
    size_t size;
    char *json = load_file(path, &size);
    if (!json) { printf("Skipped\n"); return; }
    
    uint8_t *buffer = malloc(MAX_BUFFER_SIZE);
    packr_encoder_t enc;
    
    /* Warmup */
    packr_encoder_init(&enc, NULL, NULL, buffer, MAX_BUFFER_SIZE);
    enc.compress = true;
    json_encode_to_packr(json, size, &enc);
    packr_encoder_finish(&enc, buffer);
    packr_encoder_destroy(&enc);
    
    /* Encode */
    packr_reset_alloc_stats();
    double start = get_time_ms();
    size_t out_size = 0;
    for (int i=0; i<10; i++) {
        packr_encoder_init(&enc, NULL, NULL, buffer, MAX_BUFFER_SIZE);
        enc.compress = true;
        json_encode_to_packr(json, size, &enc);
        out_size = packr_encoder_finish(&enc, buffer);
        packr_encoder_destroy(&enc);
    }
    double enc_ms = (get_time_ms() - start) / 10.0;
    size_t mem_peak = packr_get_peak_alloc();
    size_t mem_delta = mem_peak / 1024;
    
    /* Decode */
    char *json_out = malloc(MAX_BUFFER_SIZE);
    packr_decoder_t dec;
    start = get_time_ms();
    for (int i=0; i<10; i++) {
        json_out[0] = 0;
        char *cursor = json_out;
        char *end = json_out + MAX_BUFFER_SIZE;
        packr_decoder_init(&dec, buffer, out_size);
        packr_decode_next(&dec, &cursor, end);
        packr_decoder_destroy(&dec);
    }
    double dec_ms = (get_time_ms() - start) / 10.0;
    
    printf("PACKR Compressed %zu %.1fx %.2fms %.2fms 0.0ms 0.0MB/s %.1f KB\n",
           out_size, (double)size/out_size, enc_ms, dec_ms, (double)mem_delta);
           
    packr_encoder_destroy(&enc);
    free(json); free(buffer); free(json_out);
}

int main(int argc, char **argv) {
    if (argc > 1) {
        if (strcmp(argv[1], "-e") == 0) return run_tool_encode(argv[2], argv[3], 1);
        if (strcmp(argv[1], "-nc") == 0) return run_tool_encode(argv[2], argv[3], 0);
        if (strcmp(argv[1], "-d") == 0) return run_tool_decode(argv[2], argv[3]);
    }
    
    run_benchmark("Best Case - Highly Repetitive", "test/data_best_case.json");
    run_benchmark("Typical Case - Realistic Telemetry", "test/data_typical_case.json");
    run_benchmark("Worst Case - High Entropy", "test/data_worst_case.json");
    run_benchmark("Sparse Case - Many Nulls", "test/data_sparse_case.json");
    run_benchmark("Bursty Case - Event Driven", "test/data_bursty_case.json");
    run_benchmark("Mixed Case - Real World", "test/data_mixed_case.json");
    run_benchmark("IoT Sensor Fleet - Many Devices", "test/data_iot_fleet.json");
    run_benchmark("Network Metrics - IPs/MACs/Floats", "test/data_network_metrics.json");
    run_benchmark("Log Events - Long Strings", "test/data_log_events.json");
    run_benchmark("Deeply Nested Structures", "test/data_deep_nested.json");
    
    return 0;
}
