/*
 * PACKR Decoder CLI Tool
 * 
 * Usage: packr_dec input.pkr output.json
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "packr.h"

#define BUFFER_SIZE (1024 * 1024)  /* 1 MB */

static void print_usage(const char *program) {
    fprintf(stderr, "Usage: %s input.pkr output.json\n", program);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        print_usage(argv[0]);
        return 1;
    }
    
    const char *input_path = argv[1];
    const char *output_path = argv[2];
    
    /* Read input file */
    FILE *fin = fopen(input_path, "rb");
    if (!fin) {
        fprintf(stderr, "Error: cannot open input file: %s\n", input_path);
        return 1;
    }
    
    fseek(fin, 0, SEEK_END);
    long input_size = ftell(fin);
    fseek(fin, 0, SEEK_SET);
    
    uint8_t *input_data = (uint8_t *)malloc(input_size);
    if (!input_data) {
        fprintf(stderr, "Error: out of memory\n");
        fclose(fin);
        return 1;
    }
    
    fread(input_data, 1, input_size, fin);
    fclose(fin);
    
    /* Verify magic */
    if (input_size < 10 ||
        input_data[0] != PACKR_MAGIC_0 ||
        input_data[1] != PACKR_MAGIC_1 ||
        input_data[2] != PACKR_MAGIC_2 ||
        input_data[3] != PACKR_MAGIC_3) {
        fprintf(stderr, "Error: invalid PACKR file\n");
        free(input_data);
        return 1;
    }
    
    /* Initialize decoder */
    packr_decoder_t decoder;
    packr_decoder_init(&decoder, input_data, input_size);
    
    /* Allocate output buffer */
    char *output_buffer = (char *)malloc(BUFFER_SIZE);
    if (!output_buffer) {
        fprintf(stderr, "Error: out of memory\n");
        free(input_data);
        return 1;
    }
    
    /* Decode tokens */
    size_t output_pos = 0;
    uint8_t token;
    
    while (packr_decode_token(&decoder, &token) == PACKR_OK) {
        if (token == PACKR_TOKEN_NEW_STRING) {
            size_t len;
            char str_buf[4096];
            if (packr_decode_string(&decoder, str_buf, sizeof(str_buf), &len) == PACKR_OK) {
                if (output_pos + len < BUFFER_SIZE) {
                    memcpy(output_buffer + output_pos, str_buf, len);
                    output_pos += len;
                }
            }
        }
        /* Add handlers for other token types as needed */
    }
    
    /* Write output */
    FILE *fout = fopen(output_path, "wb");
    if (!fout) {
        fprintf(stderr, "Error: cannot open output file: %s\n", output_path);
        free(input_data);
        free(output_buffer);
        return 1;
    }
    
    fwrite(output_buffer, 1, output_pos, fout);
    fclose(fout);
    
    printf("Decoded %ld bytes -> %zu bytes\n", input_size, output_pos);
    
    free(input_data);
    free(output_buffer);
    return 0;
}
