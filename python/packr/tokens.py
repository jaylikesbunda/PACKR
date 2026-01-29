"""
PACKR Token Types and Encoding Utilities

Defines all token types and provides encoding/decoding functions for:
- Varints (unsigned and signed/zigzag)
- Fixed-point floats (16-bit and 32-bit)
- Token byte construction
"""

from enum import IntEnum
from typing import Tuple
import struct

# Format constants
MAGIC = b'PKR1'
VERSION = 0x01

# Dictionary size
DICT_SIZE = 64


class TokenType(IntEnum):
    """PACKR token types with their base byte values."""
    
    # Dictionary references (0x00-0xBF) - base values, add index
    FIELD = 0x00          # 0x00-0x3F: field dict reference
    STRING = 0x40         # 0x40-0x7F: string dict reference  
    MAC = 0x80            # 0x80-0xBF: MAC dict reference
    
    # Literal values (0xC0-0xC2)
    INT = 0xC0            # followed by varint
    FLOAT16 = 0xC1        # followed by 2 bytes (8.8 fixed)
    FLOAT32 = 0xC2        # followed by 4 bytes (16.16 fixed)
    
    # Delta encoding (0xC3-0xD3)
    DELTA_SMALL_BASE = 0xC3  # 0xC3-0xD2: inline delta -8 to +7
    DELTA_LARGE = 0xD3       # followed by signed varint
    
    # New dictionary entries (0xD4-0xD6)
    NEW_STRING = 0xD4     # followed by length + UTF-8 bytes
    NEW_FIELD = 0xD5      # followed by length + ASCII bytes
    NEW_MAC = 0xD6        # followed by 6 bytes
    
    # Literals (0xD7-0xD9, 0xDE)
    BOOL_TRUE = 0xD7
    BOOL_FALSE = 0xD8
    NULL = 0xD9
    DOUBLE = 0xDE         # followed by 8 bytes (IEEE 754 double precision)
    BINARY = 0xDF         # followed by varint length + raw bytes
    
    # Structure (0xDA-0xDD)
    ARRAY_START = 0xDA    # followed by varint length
    ARRAY_END = 0xDB
    OBJECT_START = 0xDC
    OBJECT_END = 0xDD


# Delta small range
DELTA_SMALL_MIN = -8
DELTA_SMALL_MAX = 7
DELTA_SMALL_OFFSET = 8  # Add to delta to get byte offset from DELTA_SMALL_BASE


def encode_varint(value: int) -> bytes:
    """
    Encode an unsigned integer as a varint.
    
    Uses continuation bit encoding (MSB = 1 means more bytes follow).
    
    Args:
        value: Non-negative integer to encode
        
    Returns:
        Encoded bytes (1-5 bytes for 32-bit values)
    """
    if value < 0:
        raise ValueError(f"encode_varint requires non-negative value, got {value}")
    
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def decode_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode a varint from bytes.
    
    Args:
        data: Byte buffer to read from
        offset: Starting position in buffer
        
    Returns:
        Tuple of (decoded value, number of bytes consumed)
    """
    result = 0
    shift = 0
    bytes_read = 0
    
    while True:
        if offset + bytes_read >= len(data):
            raise ValueError("Incomplete varint")
        
        byte = data[offset + bytes_read]
        bytes_read += 1
        result |= (byte & 0x7F) << shift
        
        if not (byte & 0x80):
            break
        shift += 7
        
        if shift > 35:  # Overflow protection
            raise ValueError("Varint too long")
    
    return result, bytes_read


def zigzag_encode(value: int) -> int:
    """
    Encode a signed integer using ZigZag encoding.
    
    Maps negative values to positive: 0, -1, 1, -2, 2, ... -> 0, 1, 2, 3, 4, ...
    """
    return (value << 1) ^ (value >> 31)


def zigzag_decode(value: int) -> int:
    """
    Decode a ZigZag-encoded value back to signed integer.
    """
    return (value >> 1) ^ -(value & 1)


def encode_signed_varint(value: int) -> bytes:
    """
    Encode a signed integer as a ZigZag varint.
    """
    return encode_varint(zigzag_encode(value))


def decode_signed_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Decode a ZigZag varint to a signed integer.
    
    Returns:
        Tuple of (decoded signed value, number of bytes consumed)
    """
    unsigned, consumed = decode_varint(data, offset)
    return zigzag_decode(unsigned), consumed


def encode_fixed16(value: float) -> bytes:
    """
    Encode a float as fixed16 (8.8 format).
    
    Range: -128.0 to +127.996
    Precision: ~0.004 (1/256)
    """
    scaled = int(round(value * 256))
    # Clamp to int16 range
    scaled = max(-32768, min(32767, scaled))
    return struct.pack('<h', scaled)


def decode_fixed16(data: bytes, offset: int = 0) -> Tuple[float, int]:
    """
    Decode a fixed16 value to float.
    
    Returns:
        Tuple of (decoded float, bytes consumed=2)
    """
    scaled = struct.unpack_from('<h', data, offset)[0]
    return scaled / 256.0, 2


def encode_fixed32(value: float) -> bytes:
    """
    Encode a float as fixed32 (16.16 format).
    
    Range: -32768.0 to +32767.99998
    Precision: ~0.000015 (1/65536)
    """
    scaled = int(round(value * 65536))
    # Clamp to int32 range
    scaled = max(-2147483648, min(2147483647, scaled))
    return struct.pack('<i', scaled)


def decode_fixed32(data: bytes, offset: int = 0) -> Tuple[float, int]:
    """
    Decode a fixed32 value to float.
    
    Returns:
        Tuple of (decoded float, bytes consumed=4)
    """
    scaled = struct.unpack_from('<i', data, offset)[0]
    return scaled / 65536.0, 4


def encode_double(value: float) -> bytes:
    """
    Encode a float as full IEEE 754 double precision (8 bytes).
    """
    return struct.pack('<d', value)


def decode_double(data: bytes, offset: int = 0) -> Tuple[float, int]:
    """
    Decode a full double precision float.
    """
    val = struct.unpack_from('<d', data, offset)[0]
    return val, 8


def is_small_delta(delta: int) -> bool:
    """Check if delta fits in small delta encoding (-8 to +7)."""
    return DELTA_SMALL_MIN <= delta <= DELTA_SMALL_MAX


def encode_small_delta(delta: int) -> int:
    """
    Encode a small delta as a single byte.
    
    Args:
        delta: Value from -8 to +7
        
    Returns:
        Token byte value (0xC3 to 0xD2)
    """
    if not is_small_delta(delta):
        raise ValueError(f"Delta {delta} out of small delta range")
    return TokenType.DELTA_SMALL_BASE + delta + DELTA_SMALL_OFFSET


def decode_small_delta(byte: int) -> int:
    """
    Decode a small delta byte to its value.
    
    Args:
        byte: Token byte (0xC3 to 0xD2)
        
    Returns:
        Delta value (-8 to +7)
    """
    return byte - TokenType.DELTA_SMALL_BASE - DELTA_SMALL_OFFSET


def is_field_token(byte: int) -> bool:
    """Check if byte is a field dictionary reference."""
    return 0x00 <= byte <= 0x3F


def is_string_token(byte: int) -> bool:
    """Check if byte is a string dictionary reference."""
    return 0x40 <= byte <= 0x7F


def is_mac_token(byte: int) -> bool:
    """Check if byte is a MAC dictionary reference."""
    return 0x80 <= byte <= 0xBF


def is_delta_small_token(byte: int) -> bool:
    """Check if byte is a small delta token."""
    return TokenType.DELTA_SMALL_BASE <= byte <= 0xD2


def get_dict_index(byte: int) -> int:
    """Extract dictionary index from a reference token."""
    return byte & 0x3F
