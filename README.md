# PACKR

structure-first streaming compression for embedded systems and structured data.

## Overview

PACKR is a domain-aware, low-RAM, streaming compression format designed for:
- Embedded systems (ESP32, MCU targets)
- Structured data (JSON, logs, telemetry)
- Low memory footprint (under 32 KB RAM)

## Compression Performance

| Data Type              | gzip   | PACKR   | Improvement       |
|------------------------|--------|---------|-------------------|
| Repetitive telemetry   | 18.6:1 | 38.8:1  | 2.08x better      |
| Mixed telemetry        | 11.1:1 | 16.3:1  | 1.47x better      |
| Random telemetry       | 18.0:1 | 38.2:1  | 2.13x better      |

Note: PACKR is optimized for structured data. Binary files (images, video) should use domain-specific codecs.

## Quick Start

### Python

```bash
python python/packr_encode.py input.json output.pkr
python python/packr_decode.py output.pkr decoded.json
```

### C

```bash
cd c
make
./build/packr_enc input.json output.pkr
./build/packr_dec output.pkr decoded.json
```

## How It Works

1. **Tokenization** - Repeated field names, strings, and MACs become dictionary references
2. **Constant detection** - Columns with identical values store once
3. **Delta encoding** - Sequential numeric values become small deltas
4. **Bit-packing** - Small deltas packed 2 per byte
5. **RLE** - Run-length encoding for repeated strings
6. **Final compression** - Optional deflate pass for maximum efficiency

## Project Structure

```
PACKR/
├── python/           # Python implementation
│   ├── packr/        # Core library
│   ├── packr_encode.py
│   └── packr_decode.py
├── c/                # C implementation
│   ├── include/      # Headers
│   ├── src/          # Library source
│   └── tools/        # CLI tools
├── spec/             # Format specification
└── test/             # Test data and benchmarks
```

## License

MIT
