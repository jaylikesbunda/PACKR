CC = gcc
CFLAGS = -Wall -Wextra -O2 -I../include
LDFLAGS = -lz

SRC_DIR = ../src
BUILD_DIR = ../build
TOOL_DIR = .

SOURCES = $(SRC_DIR)/packr.c
OBJECTS = $(BUILD_DIR)/packr.o

all: $(BUILD_DIR) $(BUILD_DIR)/packr_enc $(BUILD_DIR)/packr_dec

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/packr.o: $(SRC_DIR)/packr.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/packr_enc: packr_enc.c $(BUILD_DIR)/packr.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/packr_dec: packr_dec.c $(BUILD_DIR)/packr.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean
