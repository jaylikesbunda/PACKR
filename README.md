# PACKR

Structure-aware JSON compression for embedded systems.

PACKR is a pure C, dependency-free lossless compressor that tokenizes JSON structure to deduplicate keys and values. It is optimised to run with a minimal memory footprint (<32KB), and targets MCU-class devices like ESP32 and STM32.

WASM Web demo: [https://pack-r.netlify.app/](https://pack-r.netlify.app/)

## Why PACKR?

In IoT links, radio time dominates power use. PACKR maximizes density on structured telemetry so radios stay off longer. It outperforms small-window LZ-style codecs (Heatshrink, Tamp) once redundancy spans beyond a few kilobytes.

*Note on Twitter dataset:* PACKR shines on repetitive schemas; Zstd edges it on twitter.json because the corpus is heavy on unique strings and less structural repetition.

### Key features

- **Extreme density:** Up to ~122x on highly repetitive structured JSON; typical structured telemetry sits in the single-digit to low-20x range, still ~20x better than small-window embedded compressors.
- **JSON-optimized:** Structural tokenization collapses repetitive schemas for LoRaWAN, NB-IoT, BLE, etc.
- **Low memory footprint:** Runs with less than 32KB of RAM
- **Streaming:** Uses a fixed buffer to handle data in chunks while keeping the encoder/decoder state alive for one stream

**Streaming semantics:** PACKR works as a continuous stream: feed chunks to one encoder/decoder instance and it keeps schema/state between chunks. You can reset (re-init) to start a fresh stream, schema is not shared across independent streams. Mid-stream, you can emit records with different shapes; the codec will adapt without needing a full reset, but state is not reused once you end a stream.

**Memory profile:**
- Work buffer: PACKR_WORK_MEM_SIZE is user-tunable. 2–4KB is typical.
- Internal state: schema/string/MAC dictionaries are capped at 64 entries each with LRU eviction, per-field numeric history also tracks 64 slots.
- Total RAM: bounded by (work buffer) + (dictionary strings up to 64 entries per table) + small codec scratch; stays <32KB in typical MCU configs. JSON with many unique keys/strings will churn the capped tables, reducing ratio but not growing memory.

## Benchmarks

Compression ratio (higher is better):

| Dataset | PACKR | Zstd (lvl 1) | zlib (lvl 1) | Heatshrink | Tamp |
|:---|:---:|:---:|:---:|:---:|:---:|
| citm_catalog.json | **122.69x** | 103.41x | 26.47x | 6.34x | 5.48x |
| twitter.json | 8.18x | 12.02x | 7.60x | 2.29x | 2.34x |

Performance (desktop AMD64):

| Algorithm | Comp Speed | Decomp Speed | Best For |
|:---|:---:|:---:|:---|
| PACKR | 15.9 MB/s | 38.2 MB/s | Max density / low bandwidth |
| Tamp | 28.0 MB/s | 208.4 MB/s | Generic stream data |
| Zstd (lvl 1) | 2764 MB/s | 2990 MB/s | Server / desktop |

## Usage

### Simple decompression (C)

```c
#include "packr.h"

// PACKR_WORK_MEM_SIZE is a fixed constant defined in packr.h and is recommended to be sized at 2048-4096 bytes
uint8_t work_mem[PACKR_WORK_MEM_SIZE];

void handle_payload(uint8_t *compressed_data, size_t len) {
    char output[2048];
    size_t out_len = sizeof(output);

    packr_status_t status = packr_decompress(
        compressed_data, len,
        output, &out_len,
        work_mem
    );

    if (status == PACKR_OK) {
        printf("Decompressed JSON: %s\n", output);
    }
}
```

### Python reference

The Python implementation mirrors the C codec and is useful for tooling or cloud-side processing.

```python
from packr import PackrEncoder, PackrDecoder

encoder = PackrEncoder()
packed = encoder.encode_stream(data_list)

decoder = PackrDecoder()
data = decoder.decode_stream(packed)
```

## License
MIT
