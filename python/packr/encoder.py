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
    encode_double,
    zigzag_encode,
)
from .dictionary import DictionarySet
from .frame import FrameBuilder, FrameFlags
from .rice import BitWriter, RiceEncoder


MAC_PATTERN = re.compile(r'^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}$')


def is_mac_address(s: Any) -> bool:
    if not isinstance(s, str):
        return False
    return bool(MAC_PATTERN.match(s))


def parse_mac(s: str) -> bytes:
    cleaned = s.replace(':', '').replace('-', '')
    return bytes.fromhex(cleaned)


# Column type flags
class ColumnFlags:
    CONSTANT = 0x01       # All values identical, stored once
    ALL_DELTA = 0x02      # All values stored as deltas (first is absolute)
    RLE = 0x04            # RLE encoded
    HAS_NULLS = 0x08      # Column has null values (bitmap follows)


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
    RICE_COLUMN = 0xED    # Rice-coded deltas


class PackrEncoder:
    """Maximum compression PACKR encoder with optional compression pass."""

    def __init__(self, use_delta: bool = True, use_schema: bool = True, compress: bool = True):
        """
        Initialize encoder.

        Args:
            use_delta: Enable delta encoding for numeric columns
            use_schema: Enable schema-based batch encoding
            compress: Enable fast LZ77 compression (default True for best compression)
                     Set to False only if you need absolute minimum latency
        """
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
        raw = frame.serialize()

        if self._compress and len(raw) > 20:
            from .transform import compress_transform
            # Apply fast LZ77 transform
            compressed = compress_transform(raw, fast_lz=True)
            # Only use compressed if it's actually smaller
            if len(compressed) < len(raw):
                # Prepend marker byte 0xFE to indicate transform
                return b'\xFE' + compressed

        return raw
    
    def _is_homogeneous_object_array(self, objects: list) -> bool:
        if not objects:
            return False
        # Relaxed check: Just verify they are dicts. 
        return isinstance(objects[0], dict)
    
    def _encode_ultra_batch(self, objects: List[dict]) -> None:
        """
        Ultra-compact batch encoding with:
        - Schema Discovery (Union of keys)
        - Constant column detection
        - Null Bitmaps for sparse data
        - Bit-packed deltas
        - RLE for strings
        """
        if not objects:
            return
        
        record_count = len(objects)
        
        # Schema Discovery: Union of all keys
        # Use dict for deterministic ordering (insertion order)
        all_keys = OrderedDict()
        for obj in objects:
            for k in obj:
                all_keys[k] = None
        
        field_names = list(all_keys.keys())
        field_count = len(field_names)
        
        # Analyze columns
        columns = []
        column_info = []  # (is_constant, is_numeric, min_val, max_val)
        
        for name in field_names:
            # Extract values, filling missing with None
            col = [obj.get(name) for obj in objects]
            columns.append(col)
            
            # Check for Nulls
            has_nulls = any(v is None for v in col)
            
            # If all null, treat as constant None
            if all(v is None for v in col):
                column_info.append({
                    'constant': True,
                    'numeric': False, # Doesn't matter
                    'has_nulls': True, # Explicitly all null
                    'values': col
                })
                continue
            
            # Filter non-nulls for type checking
            valid_values = [v for v in col if v is not None]
            first_val = valid_values[0]
            
            is_constant = not has_nulls and all(v == first_val for v in col)
             
            # Check if numeric (all valid values are numbers)
            # Boolean is handled separately in Packr usually, but here we treat as numeric/bool
            is_numeric = all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in valid_values)
            
            # Prepare values for underlying encoders (Fill None with dummy to keep stream sync)
            prepared_values = []
            if has_nulls:
                if is_numeric:
                    # Fill with 0 or previous value to minimize deltas
                    prev = 0
                    if valid_values: prev = valid_values[0] # Start with valid
                    
                    for v in col:
                        if v is None:
                            prepared_values.append(prev) # Repeat prev (delta 0)
                        else:
                            prepared_values.append(v)
                            prev = v
                else:
                    # Strings/Others: Fill with empty or first
                    dummy = first_val if valid_values else ""
                    for v in col:
                        prepared_values.append(v if v is not None else dummy)
            else:
                prepared_values = col

            column_info.append({
                'constant': is_constant,
                'numeric': is_numeric,
                'has_nulls': has_nulls,
                'values': prepared_values,
                'original_col': col # Keep original for boolean/other checks if needed
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
            
            info = column_info[i]
            
            # Emit column flags
            flags = 0
            if info['has_nulls']:
                flags |= ColumnFlags.HAS_NULLS
            
            if info['constant']:
                flags |= ColumnFlags.CONSTANT
            elif info['numeric']:
                flags |= ColumnFlags.ALL_DELTA
            else:
                flags |= ColumnFlags.RLE
            
            self._frame.add_token(bytes([flags]))
        
        # Emit each column based on its flags
        for i, name in enumerate(field_names):
            info = column_info[i]
            field_idx = field_indices[i]
            
            # 1. Emit Null Bitmap if needed
            if info['has_nulls']:
                col = info['original_col'] if 'original_col' in info else info['values']
                bitmap = bytearray((record_count + 7) // 8)
                for r in range(record_count):
                    if col[r] is not None:
                        bitmap[r // 8] |= (1 << (r % 8))
                
                self._frame.add_raw(bytes(bitmap))
            
            # 2. Emit Values
            if info['constant']:
                # Single value for entire column
                # If all null, emit NULL token
                if info.get('has_nulls') and not any(v is not None for v in info.get('original_col', [])):
                     self._frame.add_token(bytes([TokenType.NULL]))
                else:
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
                self._frame.add_token(bytes([TokenType.DOUBLE]) + encode_double(value))
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
        3. Choose best encoding: bit-pack, Rice, or varint
        """
        if not values:
            return

        # Check if ANY value in column has a fractional part - if so, treat as float column
        # This ensures deltas are calculated with 65536 scaling when needed
        is_float = any(
            isinstance(v, float) and not v.is_integer()
            for v in values
        )

        # Emit first value - use FLOAT32 if column has any floats so decoder knows to scale
        first = values[0]
        if is_float:
            self._frame.add_token(bytes([TokenType.DOUBLE]) + encode_double(first))
        else:
            self._frame.add_token(bytes([TokenType.INT]) + encode_signed_varint(int(first)))

        if len(values) == 1:
            return

        # Calculate deltas using reconstructed values to avoid cumulative error
        # This ensures decoder reconstructs the same sequence we're encoding
        deltas = []
        prev = first
        float_scale = 65536 if is_float else 1
        for v in values[1:]:
            if is_float:
                delta = int(round((v - prev) * float_scale))
                # Use reconstructed value for next delta to match decoder behavior
                prev = prev + delta / float_scale
            else:
                delta = int(v) - int(prev)
                prev = int(prev) + delta
            deltas.append(delta)

        # Check if all deltas fit in 4 bits (-8 to +7)
        all_small = all(-8 <= d <= 7 for d in deltas)

        if all_small:
            bitpack_cost = len(deltas) * 0.5 + 5 
            rle_cost = 0
            i = 0
            while i < len(deltas):
                if deltas[i] == 0:
                     run = 0
                     while i + run < len(deltas) and deltas[i+run] == 0:
                         run += 1
                     if run > 3:
                         rle_cost += 2 + (1 if run > 127 else 0) # Token + Varint overhead
                         i += run
                         continue
                
                rle_cost += 1 # Small delta is 1 byte
                i += 1
            
            if rle_cost < bitpack_cost * 0.8:
                 all_small = False

        if all_small:
            # Bit-pack: 2 deltas per byte (best for very small deltas)
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
            # Fast heuristic: Use Rice for numeric telemetry (typically small deltas)
            # Skip if deltas are too large or too variable
            max_delta = max(abs(d) for d in deltas)

            # Rice is beneficial when deltas are small-to-medium (< 1024)
            # and the column has at least 10 values (overhead amortization)
            if len(deltas) >= 10 and max_delta < 1024:
                # Convert to unsigned for Rice (zigzag encode)
                unsigned_deltas = [(d << 1) ^ (d >> 31) for d in deltas]

                # Fast k selection: estimate from max value
                optimal_k = max(0, min(7, max_delta.bit_length() - 2)) if max_delta > 0 else 2

                # Encode with Rice
                rice_encoder = RiceEncoder(k=optimal_k)
                for ud in unsigned_deltas:
                    rice_encoder.encode(ud)
                rice_data = rice_encoder.finish()

                # Simple size check: Rice is good if < 1.5 bytes/value
                if len(rice_data) < len(deltas) * 1.5:
                    self._frame.add_token(bytes([ExtendedTokenType.RICE_COLUMN]))
                    self._frame.add_raw(encode_varint(len(deltas)))
                    self._frame.add_raw(rice_data)
                    return

            # Fall back to optimized varint encoding with RLE for 0-deltas (repeats)
            i = 0
            while i < len(deltas):
                delta = deltas[i]
                
                # Check for run of zeros (identical values)
                if delta == 0:
                     run_length = 0
                     while i + run_length < len(deltas) and deltas[i + run_length] == 0:
                         run_length += 1
                     
                     if run_length > 3:
                         self._frame.add_token(bytes([ExtendedTokenType.RLE_REPEAT]))
                         self._frame.add_raw(encode_varint(run_length))
                         i += run_length
                         continue

                if delta == 0:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_ZERO]))
                elif delta == 1:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_ONE]))
                elif delta == -1:
                    self._frame.add_token(bytes([ExtendedTokenType.DELTA_NEG_ONE]))
                elif -8 <= delta <= 7:
                    # Use small delta token (single byte)
                    self._frame.add_token(bytes([0xC3 + delta + 8]))  # DELTA_SMALL_BASE + offset
                elif -64 <= delta <= 63:
                    # Medium delta: use DELTA_MEDIUM + 1 byte
                    self._frame.add_token(bytes([0xEC, (delta + 64) & 0x7F]))
                else:
                    self._frame.add_token(bytes([TokenType.DELTA_LARGE]) + encode_signed_varint(delta))
                
                i += 1
    
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
            self._frame.add_token(bytes([TokenType.DOUBLE]) + encode_double(value))
        elif isinstance(value, str):
            self._encode_string(value)
        elif isinstance(value, dict):
            self._encode_object(value)
        elif isinstance(value, (list, tuple)):
            self._encode_array(value)
        elif isinstance(value, (bytes, bytearray)):
            self._encode_binary(value)
        else:
            self._encode_string(str(value))
    
    def _encode_string(self, value: str) -> None:
        if is_mac_address(value):
            self._encode_mac(value)
        else:
            self._encode_regular_string(value)
    
    def _encode_regular_string(self, value: Any) -> None:
        str_val = str(value) if not isinstance(value, str) else value
        index, is_new = self._dicts.strings.get_or_add(str_val)
        
        if is_new:
            encoded = str_val.encode('utf-8')
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

    def _encode_binary(self, data: Union[bytes, bytearray]) -> None:
        self._frame.add_token(bytes([TokenType.BINARY]) + encode_varint(len(data)))
        self._frame.add_raw(data)
    
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
