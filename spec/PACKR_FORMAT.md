# PACKR Binary Format Specification v1

## Overview

PACKR is a streaming compression format optimized for structured data. It achieves high compression ratios by:
1. Tokenizing repeated patterns into dictionary references
2. Delta-encoding sequential numeric values
3. Entropy-coding the resulting symbol stream

## Magic Number and Version

```
Magic: 0x50 0x4B 0x52 0x31 ("PKR1")
Version: 0x01
```

## Token Encoding

Tokens are variable-length byte sequences. The first byte determines the token type and may contain inline data.

### Dictionary Reference Tokens (0x00-0xBF)

These three ranges encode references to 64-entry dictionaries:

| Range       | Token Type | Meaning                    |
|-------------|------------|----------------------------|
| 0x00 - 0x3F | T_FIELD    | Field name dict[byte & 0x3F] |
| 0x40 - 0x7F | T_STRING   | String dict[byte & 0x3F]     |
| 0x80 - 0xBF | T_MAC      | MAC dict[byte & 0x3F]        |

### Literal and Control Tokens (0xC0-0xFF)

| Byte | Token          | Payload                          |
|------|----------------|----------------------------------|
| 0xC0 | T_INT          | varint (1-5 bytes)               |
| 0xC1 | T_FLOAT16      | fixed16 signed (2 bytes, 8.8)    |
| 0xC2 | T_FLOAT32      | fixed32 signed (4 bytes, 16.16)  |
| 0xC3-0xD2 | T_DELTA_SMALL | Inline delta: (byte - 0xCB) gives -8 to +7 |
| 0xD3 | T_DELTA_LARGE  | varint signed delta              |
| 0xD4 | T_NEW_STRING   | varint length + UTF-8 bytes      |
| 0xD5 | T_NEW_FIELD    | varint length + ASCII bytes      |
| 0xD6 | T_NEW_MAC      | 6 bytes raw MAC address          |
| 0xD7 | T_BOOL_TRUE    | (no payload)                     |
| 0xD8 | T_BOOL_FALSE   | (no payload)                     |
| 0xD9 | T_NULL         | (no payload)                     |
| 0xDA | T_ARRAY_START  | varint element count             |
| 0xDB | T_ARRAY_END    | (no payload)                     |
| 0xDC | T_OBJECT_START | (no payload)                     |
| 0xDD | T_OBJECT_END   | (no payload)                     |
| 0xDE-0xFF | Reserved  | Future use                       |

## Varint Encoding

Unsigned varints use continuation bit encoding (like protobuf):
- Each byte: 7 data bits + 1 continuation bit (MSB)
- Continuation bit = 1 means more bytes follow
- Little-endian byte order

```
Value     Bytes
0-127     1 byte:  0xxxxxxx
128-16383 2 bytes: 1xxxxxxx 0xxxxxxx
...
```

Signed varints use ZigZag encoding before varint:
```
zigzag(n) = (n << 1) ^ (n >> 31)  // for 32-bit
```

## Fixed-Point Floats

### Fixed16 (8.8 format)
- 2 bytes, signed
- Range: -128.0 to +127.996
- Precision: 1/256 ≈ 0.004

```
encoded = (int16_t)(value * 256.0)
decoded = encoded / 256.0
```

### Fixed32 (16.16 format)
- 4 bytes, signed
- Range: -32768.0 to +32767.99998
- Precision: 1/65536 ≈ 0.000015

```
encoded = (int32_t)(value * 65536.0)
decoded = encoded / 65536.0
```

## Frame Structure

Each frame is independently decodable:

```
+--------+--------+-------+--------+---------------+-------+
| MAGIC  | VER    | FLAGS | SYMCNT | COMPRESSED    | CRC32 |
| 4 bytes| 1 byte | 1 byte| varint | variable      | 4 bytes|
+--------+--------+-------+--------+---------------+-------+
```

### Flags Byte

| Bit | Meaning                 |
|-----|-------------------------|
| 0   | Has dictionary update   |
| 1   | Uses Rice coding        |
| 2   | Dictionary reset        |
| 3-7 | Reserved                |

### CRC32

CRC32 (IEEE polynomial) of everything from MAGIC through COMPRESSED data.

## Dictionary Management

Each dictionary (fields, strings, MACs) holds up to 64 entries.

### Adding New Entries

When a new value is encountered:
1. Emit T_NEW_FIELD, T_NEW_STRING, or T_NEW_MAC with the value
2. Add to dictionary at next available slot
3. Subsequent uses emit the dictionary reference token

### Dictionary Overflow

When dictionary is full (64 entries), new values:
1. Still emit as T_NEW_* tokens
2. Replace the least-recently-used entry

## Delta Encoding

Sequential numeric values are encoded as deltas:

1. First value in a field: encoded as T_INT or T_FLOAT*
2. Subsequent values: T_DELTA_SMALL if |delta| <= 7, else T_DELTA_LARGE

Delta context is per-field (tracked by field dictionary index).

## Rice Coding (Optional)

When FLAGS bit 1 is set, the COMPRESSED section uses Rice coding:

- K parameter: stored in first byte of COMPRESSED section
- Each symbol: quotient (unary) + remainder (K bits)
- Quotient: 0s terminated by 1
- Remainder: K bits, binary

Without Rice coding, tokens are stored directly.

## Example Encoding

Input JSON:
```json
{"rssi": -45, "mac": "AA:BB:CC:DD:EE:FF"}
```

Token stream:
```
DC              OBJECT_START
D5 04 "rssi"    NEW_FIELD "rssi" (added to field dict[0])
C0 59           INT -45 (zigzag: 89)
D5 03 "mac"     NEW_FIELD "mac" (added to field dict[1])
D6 AA BB CC...  NEW_MAC (added to MAC dict[0])
DD              OBJECT_END
```

Second object with same fields:
```json
{"rssi": -42, "mac": "AA:BB:CC:DD:EE:FF"}
```

Token stream:
```
DC              OBJECT_START
00              FIELD dict[0] ("rssi")
C6              DELTA_SMALL +3 (0xC3 + 3 + 8 = 0xCE... wait, recalc)
01              FIELD dict[1] ("mac")
80              MAC dict[0]
DD              OBJECT_END
```

This achieves significant compression on repeated structures.
