"""
PACKR Maximum Compression Encoder

Ultra-compact encoding with:
- Column-oriented data layout
- Constant column detection (encode once)
- RLE for repeated strings
- Bit-packing for small values
- Rice coding for final compression
"""

import re
from typing import Any, Union, Optional, List, Tuple, Dict
from collections import OrderedDict

from .tokens import (
    TokenType,
    MAGIC,
    VERSION,
    DICT_SIZE,
    encode_varint,
    encode_signed_varint,
    encode_fixed16,
    encode_fixed32,
    zigzag_encode,
)
from .dictionary import DictionarySet
from .frame import FrameBuilder, FrameFlags
from .rice import BitWriter


MAC_PATTERN = re.compile(r'^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$')


def is_mac_address(s: str) -> bool:
    return bool(MAC_PATTERN.match(s))


def parse_mac(s: str) -> bytes:
    cleaned = s.replace(':', '').replace('-', '')
    return bytes.fromhex(cleaned)


# Column type flags
class ColumnFlags:
    CONSTANT = 0x01       # All values identical, stored once
    ALL_DELTA = 0x02      # All values stored as deltas (first is absolute)
    RLE = 0x04            # RLE encoded
    BITPACKED = 0x08      # Bit-packed small values


class ExtendedTokenType:
    SCHEMA_DEF = 0xE0
    SCHEMA_REF = 0xE1
    SCHEMA_REPEAT = 0xE2
    RECORD_BATCH = 0xE3
    COLUMN_BATCH = 0xE4
    RLE_REPEAT = 0xE5
    DELTA_ZERO = 0xE6
    DELTA_ONE = 0xE7
    DELTA_NEG_ONE = 0xE8
    # New ultra-compact tokens
    ULTRA_BATCH = 0xE9    # Maximum compression batch
    CONST_COLUMN = 0xEA   # Constant column (value appears once)
    BITPACK_COLUMN = 0xEB # Bit-packed column


class PackrEncoder:
    """Maximum compression PACKR encoder with optional zlib pass."""
    
    def __init__(self, use_delta: bool = True, use_schema: bool = True, compress: bool = True):
        self._dicts = DictionarySet()
        self._use_delta = use_delta
        self._use_schema = use_schema
        self._compress = compress
        self._frame = FrameBuilder()
        self._last_values: dict = {}
        self._value_types: dict = {}
    
    def encode(self, obj: Any) -> bytes:
        self._frame.reset()
        self._encode_value(obj)
        frame = self._frame.finalize()
        return self._finalize(frame)
    
    def encode_stream(self, objects: list) -> bytes:
        self._frame.reset()
        
        if not objects:
            return self._finalize(self._frame.finalize())
        
        if self._use_schema and self._is_homogeneous_object_array(objects):
            self._encode_ultra_batch(objects)
        else:
            for obj in objects:
                self._encode_value(obj)
        
        frame = self._frame.finalize()
        return self._finalize(frame)
    
    def _finalize(self, frame) -> bytes:
        """Finalize frame with optional compression."""
        import zlib
        raw = frame.serialize()
        
        if self._compress and len(raw) > 20:
            # Apply zlib compression
            compressed = zlib.compress(raw, level=9)
            # Only use compressed if it's actually smaller
            if len(compressed) < len(raw):
                # Prepend marker byte 0xFF to indicate compressed
                return b'\xFF' + compressed
        
        return raw
    
    def _is_homogeneous_object_array(self, objects: list) -> bool:
        if not objects or not isinstance(objects[0], dict):
            return False
        first_keys = tuple(objects[0].keys())
        return all(isinstance(obj, dict) and tuple(obj.keys()) == first_keys for obj in objects[1:])
    
    def _encode_ultra_batch(self, objects: List[dict]) -> None:
        """
        Ultra-compact batch encoding with:
        - Constant column detection
        - Bit-packed deltas
        - RLE for strings
        """
        if not objects:
            return
        
        record_count = len(objects)
        field_names = list(objects[0].keys())
        field_count = len(field_names)
        
        # Analyze columns
        columns = []
        column_info = []  # (is_constant, is_numeric, min_val, max_val)
        
        for name in field_names:
            col = [obj[name] for obj in objects]
            columns.append(col)
            
            # Check if constant
            is_constant = all(v == col[0] for v in col)
            
            # Check if numeric
            is_numeric = all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in col)
            
            column_info.append({
                'constant': is_constant,
                'numeric': is_numeric,
                'values': col
            })
        
        # Emit batch header
        self._frame.add_token(bytes([ExtendedTokenType.ULTRA_BATCH]))
        self._frame.add_raw(encode_varint(record_count))
        self._frame.add_raw(encode_varint(field_count))
        
        # Emit field definitions and column flags
        field_indices = []
        for i, name in enumerate(field_names):
            idx = self._encode_field_def(name)
            field_indices.append(idx)
            
            # Emit column flags
            flags = 0
            if column_info[i]['constant']:
                flags |= ColumnFlags.CONSTANT
            elif column_info[i]['numeric']:
                flags |= ColumnFlags.ALL_DELTA
            else:
                flags |= ColumnFlags.RLE
            
            self._frame.add_token(bytes([flags]))
        
        # Emit each column based on its flags
        for i, name in enumerate(field_names):
            info = column_info[i]
            field_idx = field_indices[i]
            
            if info['constant']:
                # Single value for entire column
                self._encode_single_value(info['values'][0], field_idx)
            elif info['numeric']:
                # Delta-encoded numeric column with bit-packing
                self._encode_numeric_column_packed(info['values'], field_idx)
            else:
                # RLE string column
                self._encode_string_column_rle(info['values'])
    
    def _encode_single_value(self, value: Any, field_idx: int) -> None:
        """Encode a single value (for constant columns)."""
        if isinstance(value, bool):
            self._frame.add_token(bytes([TokenType.BOOL_TRUE if value else TokenType.BOOL_FALSE]))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            is_float = isinstance(value, float) and not value.is_integer()
            if is_float:
                if -128 <= value <= 127:
                    self._frame.add_token(bytes([TokenType.FLOAT16]) + encode_fixed16(value))
                else:
                    self._frame.add_token(bytes([TokenType.FLOAT32]) + encode_fixed32(value))
            else:
                self._frame.add_token(bytes([TokenType.INT]) + encode_signed_varint(int(value)))
        elif isinstance(value, str):
            self._encode_string(value)
        elif value is None:
            self._frame.add_token(bytes([TokenType.NULL]))
        else:
            self._encode_string(str(value))
    
    def _encode_numeric_column_packed(self, values: List[Union[int, float]], field_idx: int) -> None:
        """
        Encode numeric column with:
        1. First value absolute
        2. Remaining as deltas
        3. Bit-packed if deltas fit in small range
        """
        if not values:
            return
        
        # Emit first value
        first = values[0]
        is_float = isinstance(first, float) and not first.is_integer()
        
        if is_float:
            if -128 <= first <= 127:
                self._frame.add_token(bytes([TokenType.FLOAT16]) + encode_fixed16(first))
            else:
                self._frame.add_token(bytes([TokenType.FLOAT32]) + encode_fixed32(first))
        else:
            self._frame.add_token(bytes([TokenType.INT]) + encode_signed_varint(int(first)))
        
        if len(values) == 1:
            return
        
        # Calculate deltas
        deltas = []
        prev = first
        for v in values[1:]:
            if is_float:
                delta = int(round((v - prev) * 256))
            else:
                delta = int(v) - int(prev)
            deltas.append(delta)
            prev = v
        
        # Check if all deltas fit in 4 bits (-8 to +7)
        all_small = all(-8 <= d <= 7 for d in deltas)
        
        if all_small:
            # Bit-pack: 2 deltas per byte
            self._frame.add_token(bytes([ExtendedTokenType.BITPACK_COLUMN]))
            
            packed = bytearray()
            for i in range(0, len(deltas), 2):
                d1 = (deltas[i] + 8) & 0x0F  # Map -8..7 to 0..15
                if i + 1 < len(deltas):
                    d2 = (deltas[i + 1] + 8) & 0x0F
                else:
                    d2 = 0
                packed.append((d1 << 4) | d2)
            
            self._frame.add_raw(encode_varint(len(deltas)))
            self._frame.add_raw(bytes(packed))
        else:
            # Variable-length deltas
            for delta in deltas:
                if delta == 0:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_ZERO]))
                elif delta == 1:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_ONE]))
                elif delta == -1:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_NEG_ONE]))
                elif -8 <= delta <= 7:
                    # Use small delta token
                    self._frame.add_token(bytes([0xC3 + delta + 8]))  # DELTA_SMALL_BASE + offset
                else:
                    self._frame.add_token(bytes([TokenType.DELTA_LARGE]) + encode_signed_varint(delta))
    
    def _encode_string_column_rle(self, values: List[str]) -> None:
        """Encode string column with RLE."""
        if not values:
            return
        
        i = 0
        while i < len(values):
            value = values[i]
            
            # Count run length
            run_length = 1
            while i + run_length < len(values) and values[i + run_length] == value:
                run_length += 1
            
            # Emit value
            self._encode_string(value)
            
            # Emit RLE if beneficial
            if run_length > 1:
                self._frame.add_token(bytes([ExtendedTokenType.RLE_REPEAT]))
                self._frame.add_raw(encode_varint(run_length - 1))
            
            i += run_length
    
    def _encode_field_def(self, name: str) -> int:
        index, is_new = self._dicts.fields.get_or_add(name)
        
        if is_new:
            encoded = name.encode('ascii', errors='replace')
            self._frame.add_token(
                bytes([TokenType.NEW_FIELD]) +
                encode_varint(len(encoded)) +
                encoded
            )
        else:
            self._frame.add_token(bytes([TokenType.FIELD + index]))
        
        return index
    
    def _encode_value(self, value: Any) -> None:
        if value is None:
            self._frame.add_token(bytes([TokenType.NULL]))
        elif isinstance(value, bool):
            self._frame.add_token(bytes([TokenType.BOOL_TRUE if value else TokenType.BOOL_FALSE]))
        elif isinstance(value, int):
            self._frame.add_token(bytes([TokenType.INT]) + encode_signed_varint(value))
        elif isinstance(value, float):
            if -128 <= value <= 127:
                self._frame.add_token(bytes([TokenType.FLOAT16]) + encode_fixed16(value))
            else:
                self._frame.add_token(bytes([TokenType.FLOAT32]) + encode_fixed32(value))
        elif isinstance(value, str):
            self._encode_string(value)
        elif isinstance(value, dict):
            self._encode_object(value)
        elif isinstance(value, (list, tuple)):
            self._encode_array(value)
        else:
            self._encode_string(str(value))
    
    def _encode_string(self, value: str) -> None:
        if is_mac_address(value):
            self._encode_mac(value)
        else:
            self._encode_regular_string(value)
    
    def _encode_regular_string(self, value: str) -> None:
        index, is_new = self._dicts.strings.get_or_add(value)
        
        if is_new:
            encoded = value.encode('utf-8')
            self._frame.add_token(
                bytes([TokenType.NEW_STRING]) +
                encode_varint(len(encoded)) +
                encoded
            )
        else:
            self._frame.add_token(bytes([TokenType.STRING + index]))
    
    def _encode_mac(self, value: str) -> None:
        index, is_new = self._dicts.macs.get_or_add(value)
        
        if is_new:
            mac_bytes = parse_mac(value)
            self._frame.add_token(bytes([TokenType.NEW_MAC]) + mac_bytes)
        else:
            self._frame.add_token(bytes([TokenType.MAC + index]))
    
    def _encode_object(self, obj: dict) -> None:
        self._frame.add_token(bytes([TokenType.OBJECT_START]))
        for key, value in obj.items():
            index, is_new = self._dicts.fields.get_or_add(str(key))
            if is_new:
                encoded = str(key).encode('ascii', errors='replace')
                self._frame.add_token(bytes([TokenType.NEW_FIELD]) + encode_varint(len(encoded)) + encoded)
            else:
                self._frame.add_token(bytes([TokenType.FIELD + index]))
            self._encode_value(value)
        self._frame.add_token(bytes([TokenType.OBJECT_END]))
    
    def _encode_array(self, arr: Union[list, tuple]) -> None:
        self._frame.add_token(bytes([TokenType.ARRAY_START]) + encode_varint(len(arr)))
        for item in arr:
            self._encode_value(item)
        self._frame.add_token(bytes([TokenType.ARRAY_END]))
    
    def reset(self) -> None:
        self._dicts.reset()
        self._last_values.clear()
        self._value_types.clear()
        self._frame.reset()


def encode(obj: Any) -> bytes:
    encoder = PackrEncoder()
    return encoder.encode(obj)


def encode_stream(objects: list) -> bytes:
    encoder = PackrEncoder()
    return encoder.encode_stream(objects)
