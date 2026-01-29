# PACKR C Implementation Makefile

CC = gcc
CFLAGS = -Wall -Wextra -O2 -Iinclude -std=c99
LDFLAGS = -lm

# Directories
SRC_DIR = src
INCLUDE_DIR = include
TOOLS_DIR = tools
TEST_DIR = test
BUILD_DIR = build

# Source files
CORE_SRC = $(SRC_DIR)/packr.c $(SRC_DIR)/packr_json.c $(SRC_DIR)/packr_lz77.c $(SRC_DIR)/packr_rice.c
TOOL_SRC = $(TOOLS_DIR)/packr_enc.c $(TOOLS_DIR)/packr_dec.c

# Object files
CORE_OBJ = $(BUILD_DIR)/packr.o $(BUILD_DIR)/packr_json.o $(BUILD_DIR)/packr_lz77.o $(BUILD_DIR)/packr_rice.o

# Targets
TOOLS = $(BUILD_DIR)/packr_enc $(BUILD_DIR)/packr_dec
LIB = $(BUILD_DIR)/libpackr.a
TEST_BENCHMARK = $(BUILD_DIR)/benchmark_c
TEST_COMPREHENSIVE = $(BUILD_DIR)/benchmark_comprehensive

.PHONY: all clean lib tools test benchmark benchmark-comprehensive

all: lib tools

lib: $(LIB)

tools: $(TOOLS)

benchmark: $(TEST_BENCHMARK)

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Library
$(LIB): $(CORE_OBJ) | $(BUILD_DIR)
	ar rcs $@ $^

$(BUILD_DIR)/packr.o: $(SRC_DIR)/packr.c $(INCLUDE_DIR)/packr.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/packr_json.o: $(SRC_DIR)/packr_json.c $(INCLUDE_DIR)/packr_json.h $(INCLUDE_DIR)/packr.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/packr_lz77.o: $(SRC_DIR)/packr_lz77.c $(INCLUDE_DIR)/packr_lz77.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/packr_rice.o: $(SRC_DIR)/packr_rice.c $(INCLUDE_DIR)/packr_rice.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Tools
$(BUILD_DIR)/packr_enc: $(TOOLS_DIR)/packr_enc.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

$(BUILD_DIR)/packr_dec: $(TOOLS_DIR)/packr_dec.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

# Benchmarks
$(TEST_BENCHMARK): $(TEST_DIR)/benchmark_c.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -lz -o $@ 2>/dev/null || \
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

$(TEST_COMPREHENSIVE): $(TEST_DIR)/benchmark_comprehensive.c $(LIB) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -DHAVE_ZLIB $< $(LIB) $(LDFLAGS) -lz -o $@ 2>/dev/null || \
	$(CC) $(CFLAGS) $< $(LIB) $(LDFLAGS) -o $@

benchmark: $(TEST_BENCHMARK)

benchmark-comprehensive: $(TEST_COMPREHENSIVE)

# Run benchmarks
run-benchmark: $(TEST_BENCHMARK)
	@echo "Running PACKR C benchmark..."
	$(TEST_BENCHMARK)

run-benchmark-comprehensive: $(TEST_COMPREHENSIVE)
	@echo "Running comprehensive benchmark..."
	cd .. && ./c/$(TEST_COMPREHENSIVE)

# Clean
clean:
	rm -rf $(BUILD_DIR)

# Install (optional)
install: all
	@echo "Installing to /usr/local..."
	install -d /usr/local/lib
	install -d /usr/local/include
	install -d /usr/local/bin
	install -m 644 $(LIB) /usr/local/lib/
	install -m 644 $(INCLUDE_DIR)/packr.h /usr/local/include/
	install -m 755 $(TOOLS) /usr/local/bin/

.PHONY: help
help:
	@echo "PACKR C Implementation Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all                       - Build library and tools (default)"
	@echo "  lib                       - Build static library"
	@echo "  tools                     - Build encoder/decoder tools"
	@echo "  benchmark                 - Build basic benchmark"
	@echo "  benchmark-comprehensive   - Build comprehensive benchmark"
	@echo "  run-benchmark             - Build and run basic benchmark"
	@echo "  run-benchmark-comprehensive - Build and run comprehensive benchmark"
	@echo "  clean                     - Remove build artifacts"
	@echo "  install                   - Install to /usr/local"
	@echo "  help                      - Show this help message"
