/*
 * PACKR Encoder CLI Tool
 * 
 * Usage: packr_enc input.json output.pkr
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "packr.h"

#define BUFFER_SIZE (1024 * 1024)  /* 1 MB */

static void print_usage(const char *program) {
    fprintf(stderr, "Usage: %s input.json output.pkr\n", program);
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
    
    char *input_data = (char *)malloc(input_size + 1);
    if (!input_data) {
        fprintf(stderr, "Error: out of memory\n");
        fclose(fin);
        return 1;
    }
    
    fread(input_data, 1, input_size, fin);
    input_data[input_size] = '\0';
    fclose(fin);
    
    /* Allocate output buffer */
    uint8_t *output_buffer = (uint8_t *)malloc(BUFFER_SIZE);
    if (!output_buffer) {
        fprintf(stderr, "Error: out of memory\n");
        free(input_data);
        return 1;
    }
    
    /* Initialize encoder */
    packr_encoder_t encoder;
    packr_encoder_init(&encoder, output_buffer, BUFFER_SIZE);
    
    /* 
     * Simple JSON encoding - for full implementation, use a JSON parser.
     * This is a placeholder that encodes the raw data.
     */
    packr_encode_string(&encoder, input_data, input_size);
    
    /* Finalize */
    size_t output_size = packr_encoder_finalize(&encoder);
    
    /* Write output */
    FILE *fout = fopen(output_path, "wb");
    if (!fout) {
        fprintf(stderr, "Error: cannot open output file: %s\n", output_path);
        free(input_data);
        free(output_buffer);
        return 1;
    }
    
    fwrite(output_buffer, 1, output_size, fout);
    fclose(fout);
    
    printf("Encoded %ld bytes -> %zu bytes (%.1f:1 compression)\n",
           input_size, output_size, (float)input_size / output_size);
    
    free(input_data);
    free(output_buffer);
    return 0;
}
