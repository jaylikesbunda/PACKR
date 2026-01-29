"""
PACKR Maximum Compression Decoder

Handles ultra-compact encoding with:
- Constant columns
- Bit-packed deltas
- RLE strings
"""

from typing import Any, List, Tuple, Optional

from .tokens import (
    TokenType,
    decode_varint,
    decode_signed_varint,
    decode_fixed16,
    decode_fixed32,
    decode_double,
    is_field_token,
    is_string_token,
    is_mac_token,
    is_delta_small_token,
    get_dict_index,
    decode_small_delta,
)
from .dictionary import DictionarySet
from .frame import FrameParser, Frame
from .rice import RiceDecoder


def format_mac(mac_bytes: bytes) -> str:
    return ':'.join(f'{b:02X}' for b in mac_bytes)


class ColumnFlags:
    CONSTANT = 0x01
    ALL_DELTA = 0x02
    RLE = 0x04
    HAS_NULLS = 0x08


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
    ULTRA_BATCH = 0xE9
    CONST_COLUMN = 0xEA
    BITPACK_COLUMN = 0xEB
    RICE_COLUMN = 0xED


class PackrDecoder:
    """Maximum compression PACKR decoder."""
    
    def __init__(self):
        self._dicts = DictionarySet()
        self._parser = FrameParser()
        self._data: bytes = b''
        self._offset: int = 0
        self._last_values: dict = {}
        self._value_types: dict = {}
        self._current_field_index: Optional[int] = None
    
    def decode(self, data: bytes) -> Any:
        data = self._maybe_decompress(data)
        frame = self._parser.parse(data)
        self._data = frame.data
        self._offset = 0
        
        if len(self._data) > 0:
            first_byte = self._peek_byte()
            if first_byte == ExtendedTokenType.RECORD_BATCH:
                return self._decode_record_batch()
            elif first_byte == ExtendedTokenType.COLUMN_BATCH:
                return self._decode_column_batch()
            elif first_byte == ExtendedTokenType.ULTRA_BATCH:
                return self._decode_ultra_batch()
        
        return self._decode_value()
    
    def decode_stream(self, data: bytes) -> List[Any]:
        data = self._maybe_decompress(data)
        frame = self._parser.parse(data)
        self._data = frame.data
        self._offset = 0
        
        objects = []
        while self._offset < len(self._data):
            try:
                byte = self._peek_byte()
                if byte == ExtendedTokenType.RECORD_BATCH:
                    objects.extend(self._decode_record_batch())
                elif byte == ExtendedTokenType.COLUMN_BATCH:
                    objects.extend(self._decode_column_batch())
                elif byte == ExtendedTokenType.ULTRA_BATCH:
                    objects.extend(self._decode_ultra_batch())
                else:
                    objects.append(self._decode_value())
            except (IndexError, ValueError):
                break
        
        return objects
    
    def _decode_ultra_batch(self) -> List[dict]:
        """Decode ultra-compact batch."""
        self._read_byte()  # ULTRA_BATCH token
        
        record_count = self._read_varint()
        field_count = self._read_varint()
        
        # Read field definitions and flags
        field_names = []
        field_indices = []
        column_flags = []
        
        for _ in range(field_count):
            name, idx = self._decode_field()
            field_names.append(name)
            field_indices.append(idx)
            flags = self._read_byte()
            column_flags.append(flags)
        
        # Read columns
        columns = []
        for i in range(field_count):
            flags = column_flags[i]
            field_idx = field_indices[i]
            self._current_field_index = field_idx
            
            # 1. Read Null Bitmap if present
            validity_mask = None
            if flags & ColumnFlags.HAS_NULLS:
                byte_count = (record_count + 7) // 8
                bitmap = self._read_bytes(byte_count)
                validity_mask = []
                for r in range(record_count):
                    is_valid = (bitmap[r // 8] >> (r % 8)) & 1
                    validity_mask.append(bool(is_valid))

            # 2. Read Values (Decode potentially dummy-filled stream)
            col_values = []
            if flags & ColumnFlags.CONSTANT:
                # Single value for all records
                if self._peek_byte() == TokenType.NULL:
                    self._read_byte()
                    val = None
                else:
                    val = self._decode_value()
                col_values = [val] * record_count
            elif flags & ColumnFlags.ALL_DELTA:
                # Delta-encoded numeric column
                col_values = self._decode_numeric_column_packed(record_count)
            else:
                # RLE string column
                col_values = self._decode_string_column_rle(record_count)
            
            # 3. Apply Validity Mask (restore None)
            if validity_mask:
                final_col = []
                for r in range(record_count):
                    if validity_mask[r]:
                        final_col.append(col_values[r])
                    else:
                        final_col.append(None)
                columns.append(final_col)
            else:
                columns.append(col_values)
        
        self._current_field_index = None
        
        # Transpose to records
        records = []
        for row_idx in range(record_count):
            record = {}
            for col_idx, name in enumerate(field_names):
                val = columns[col_idx][row_idx]
                # Only add if not None
                if val is not None:
                    record[name] = val
            records.append(record)
        
        return records
    
    def _decode_numeric_column_packed(self, count: int) -> List[Any]:
        """Decode delta-encoded numeric column with optional bit-packing or Rice coding."""
        values = []

        # First value is absolute
        first = self._decode_value()
        values.append(first)

        if count == 1:
            return values

        # Check for bit-packed deltas
        if self._peek_byte() == ExtendedTokenType.BITPACK_COLUMN:
            self._read_byte()  # consume token
            delta_count = self._read_varint()
            byte_count = (delta_count + 1) // 2
            packed = self._read_bytes(byte_count)

            is_float = isinstance(first, float)
            prev = first

            for i in range(delta_count):
                byte_idx = i // 2
                if i % 2 == 0:
                    d = (packed[byte_idx] >> 4) - 8
                else:
                    d = (packed[byte_idx] & 0x0F) - 8

                if is_float:
                    new_val = prev + (d / 65536.0)
                else:
                    new_val = int(prev) + d

                values.append(int(new_val) if not is_float else new_val)
                prev = new_val
        elif self._peek_byte() == ExtendedTokenType.RICE_COLUMN:
            # Rice-coded deltas
            self._read_byte()  # consume token
            delta_count = self._read_varint()

            # Read all Rice data (rest goes to Rice decoder)
            rice_data_start = self._offset
            # We need to figure out how much data the Rice decoder will consume
            # For now, read until we've decoded delta_count values

            # Create a temporary view of remaining data for Rice decoder
            remaining_data = self._data[self._offset:]
            rice_decoder = RiceDecoder(remaining_data)

            deltas = []
            for _ in range(delta_count):
                unsigned = rice_decoder.decode()
                # Zigzag decode
                delta = (unsigned >> 1) ^ -(unsigned & 1)
                deltas.append(delta)

            # Advance offset by consumed bytes
            # Rice decoder tells us position
            byte_pos, bit_pos = rice_decoder._reader.get_position()
            # Add 1 for K parameter byte + actual bytes consumed
            self._offset += 1 + byte_pos
            if bit_pos < 7:  # Partial byte was consumed
                self._offset += 1

            # Apply deltas
            is_float = isinstance(first, float)
            prev = first

            for delta in deltas:
                if is_float:
                    new_val = prev + (delta / 65536.0)
                else:
                    new_val = int(prev) + delta

                values.append(int(new_val) if not is_float else new_val)
                prev = new_val
        else:
            # Variable-length deltas
            is_float = isinstance(first, float)
            prev = first
            
            while len(values) < count:
                byte = self._peek_byte()

                if byte == ExtendedTokenType.DELTA_ZERO:
                    self._read_byte()
                    delta = 0
                elif byte == ExtendedTokenType.DELTA_ONE:
                    self._read_byte()
                    delta = 1
                elif byte == ExtendedTokenType.DELTA_NEG_ONE:
                    self._read_byte()
                    delta = -1
                elif is_delta_small_token(byte):
                    self._read_byte()
                    delta = decode_small_delta(byte)
                elif byte == 0xEC:
                    # Medium delta: -64 to +63 in 2 bytes
                    self._read_byte()
                    delta = self._read_byte() - 64
                elif byte == TokenType.DELTA_LARGE:
                    self._read_byte()
                    delta = self._read_signed_varint()
                elif byte == ExtendedTokenType.RLE_REPEAT:
                    # Repeat LAST decoded value
                    self._read_byte()
                    repeat_count = self._read_varint()
                    
                    # Repeatedly append prev value
                    for _ in range(repeat_count):
                        values.append(int(prev) if not is_float else prev)
                    
                    continue # Skip delta application
                else:
                    raise ValueError(f"Unexpected token in numeric column: {byte:#x}")

                if is_float:
                    new_val = prev + (delta / 65536.0)
                else:
                    new_val = int(prev) + delta

                values.append(int(new_val) if not is_float else new_val)
                prev = new_val

        return values
    
    def _decode_string_column_rle(self, count: int) -> List[Any]:
        """Decode RLE string column."""
        values = []
        
        while len(values) < count:
            value = self._decode_value()
            values.append(value)
            
            # Check for RLE
            if self._offset < len(self._data) and self._peek_byte() == ExtendedTokenType.RLE_REPEAT:
                self._read_byte()
                repeat_count = self._read_varint()
                for _ in range(repeat_count):
                    values.append(value)
        
        return values[:count]
    
    def _decode_column_batch(self) -> List[dict]:
        self._read_byte()  # COLUMN_BATCH token
        
        record_count = self._read_varint()
        field_count = self._read_varint()
        
        field_names = []
        field_indices = []
        for _ in range(field_count):
            name, idx = self._decode_field()
            field_names.append(name)
            field_indices.append(idx)
        
        columns = []
        for i in range(field_count):
            self._current_field_index = field_indices[i]
            self._last_values.pop(field_indices[i], None)
            self._value_types.pop(field_indices[i], None)
            column = self._decode_string_column_rle(record_count)
            columns.append(column)
        
        self._current_field_index = None
        
        records = []
        for row_idx in range(record_count):
            record = {}
            for col_idx, name in enumerate(field_names):
                record[name] = columns[col_idx][row_idx]
            records.append(record)
        
        return records
    
    def _decode_record_batch(self) -> List[dict]:
        self._read_byte()
        
        record_count = self._read_varint()
        field_count = self._read_varint()
        
        field_names = []
        field_indices = []
        for _ in range(field_count):
            name, idx = self._decode_field()
            field_names.append(name)
            field_indices.append(idx)
        
        records = []
        for _ in range(record_count):
            record = {}
            for i, name in enumerate(field_names):
                self._current_field_index = field_indices[i]
                value = self._decode_value()
                record[name] = value
            records.append(record)
        
        self._current_field_index = None
        return records
    
    def _peek_byte(self) -> int:
        if self._offset >= len(self._data):
            raise IndexError("End of data")
        return self._data[self._offset]
    
    def _read_byte(self) -> int:
        if self._offset >= len(self._data):
            raise IndexError("End of data")
        byte = self._data[self._offset]
        self._offset += 1
        return byte
    
    def _read_bytes(self, count: int) -> bytes:
        if self._offset + count > len(self._data):
            raise IndexError("End of data")
        result = self._data[self._offset:self._offset + count]
        self._offset += count
        return result
    
    def _read_varint(self) -> int:
        value, consumed = decode_varint(self._data, self._offset)
        self._offset += consumed
        return value
    
    def _read_signed_varint(self) -> int:
        value, consumed = decode_signed_varint(self._data, self._offset)
        self._offset += consumed
        return value
    
    def _decode_value(self) -> Any:
        byte = self._read_byte()
        
        if is_field_token(byte):
            raise ValueError(f"Unexpected field token {byte:#x} in value position")
        
        if is_string_token(byte):
            index = get_dict_index(byte)
            value = self._dicts.strings.get_value(index)
            if value is None:
                raise ValueError(f"Unknown string index {index}")
            return value
        
        if is_mac_token(byte):
            index = get_dict_index(byte)
            value = self._dicts.macs.get_value(index)
            if value is None:
                raise ValueError(f"Unknown MAC index {index}")
            return value
        
        if byte == TokenType.INT:
            value = self._read_signed_varint()
            self._track_value(value, is_float=False)
            return value
        
        if byte == TokenType.FLOAT16:
            value, _ = decode_fixed16(self._data, self._offset)
            self._offset += 2
            self._track_value(value, is_float=True)
            return value
        
        if byte == TokenType.FLOAT32:
            value, _ = decode_fixed32(self._data, self._offset)
            self._offset += 4
            self._track_value(value, is_float=True)
            return value
        
        if byte == TokenType.DOUBLE:
            value, _ = decode_double(self._data, self._offset)
            self._offset += 8
            self._track_value(value, is_float=True)
            return value

        if byte == TokenType.BINARY:
            length = self._read_varint()
            value = self._read_bytes(length)
            return value
        
        if byte == ExtendedTokenType.DELTA_ZERO:
            return self._apply_delta(0)
        
        if byte == ExtendedTokenType.DELTA_ONE:
            return self._apply_delta(1)
        
        if byte == ExtendedTokenType.DELTA_NEG_ONE:
            return self._apply_delta(-1)
        
        if is_delta_small_token(byte):
            delta = decode_small_delta(byte)
            return self._apply_delta(delta)

        if byte == 0xEC:
            # Medium delta: -64 to +63 in 2 bytes
            delta = self._read_byte() - 64
            return self._apply_delta(delta)

        if byte == TokenType.DELTA_LARGE:
            delta = self._read_signed_varint()
            return self._apply_delta(delta)
        
        if byte == TokenType.NEW_STRING:
            length = self._read_varint()
            string_bytes = self._read_bytes(length)
            value = string_bytes.decode('utf-8')
            self._dicts.strings.get_or_add(value)
            return value
        
        if byte == TokenType.NEW_FIELD:
            raise ValueError("Unexpected NEW_FIELD token in value position")
        
        if byte == TokenType.NEW_MAC:
            mac_bytes = self._read_bytes(6)
            value = format_mac(mac_bytes)
            self._dicts.macs.get_or_add(value)
            return value
        
        if byte == TokenType.BOOL_TRUE:
            return True
        
        if byte == TokenType.BOOL_FALSE:
            return False
        
        if byte == TokenType.NULL:
            return None
        
        if byte == TokenType.ARRAY_START:
            return self._decode_array()
        
        if byte == TokenType.OBJECT_START:
            return self._decode_object()
        
        raise ValueError(f"Unknown token type: {byte:#x}")
    
    def _track_value(self, value: Any, is_float: bool) -> None:
        if self._current_field_index is not None:
            self._last_values[self._current_field_index] = value
            self._value_types[self._current_field_index] = 'float' if is_float else 'int'
    
    def _apply_delta(self, delta: int) -> Any:
        if self._current_field_index is None:
            raise ValueError("Delta without field context")
        
        field_idx = self._current_field_index
        if field_idx not in self._last_values:
            raise ValueError(f"No previous value for field {field_idx}")
        
        last_value = self._last_values[field_idx]
        is_float = self._value_types.get(field_idx) == 'float'

        if is_float:
            new_value = last_value + (delta / 65536.0)
        else:
            new_value = int(last_value) + delta
        
        self._last_values[field_idx] = new_value
        return int(new_value) if not is_float else new_value
    
    def _decode_array(self) -> List[Any]:
        length = self._read_varint()
        items = []
        for _ in range(length):
            if self._peek_byte() == TokenType.ARRAY_END:
                break
            items.append(self._decode_value())
        self._read_byte()  # ARRAY_END
        return items
    
    def _decode_object(self) -> dict:
        obj = {}
        while True:
            if self._peek_byte() == TokenType.OBJECT_END:
                self._read_byte()
                break
            name, idx = self._decode_field()
            old = self._current_field_index
            self._current_field_index = idx
            obj[name] = self._decode_value()
            self._current_field_index = old
        return obj
    
    def _decode_field(self) -> Tuple[str, int]:
        byte = self._read_byte()
        
        if is_field_token(byte):
            index = get_dict_index(byte)
            name = self._dicts.fields.get_value(index)
            if name is None:
                raise ValueError(f"Unknown field index {index}")
            return name, index
        
        if byte == TokenType.NEW_FIELD:
            length = self._read_varint()
            field_bytes = self._read_bytes(length)
            name = field_bytes.decode('ascii', errors='replace')
            index, _ = self._dicts.fields.get_or_add(name)
            return name, index
        
        raise ValueError(f"Expected field token, got {byte:#x}")
    
    def _maybe_decompress(self, data: bytes) -> bytes:
        """Decompress data if it has the compression marker."""
        while len(data) > 0:
            # Check for Transform flags defined in transform.py
            if data[0] in (0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06):
                from .transform import decompress_transform
                data = decompress_transform(data)
                continue
                
            if len(data) > 1:
                if data[0] == 0xFE:
                    # MTF + Zero-RLE transform (legacy frame-wrapped)
                    from .transform import decompress_transform
                    data = decompress_transform(data[1:])
                    continue
                elif data[0] == 0xFF:
                    # Legacy zlib compression
                    import zlib
                    data = zlib.decompress(data[1:])
                    continue
            
            # If no markers match, we are done
            break
            
        return data
    
    def reset(self) -> None:
        self._dicts.reset()
        self._last_values.clear()
        self._value_types.clear()
        self._data = b''
        self._offset = 0
        self._current_field_index = None


def decode(data: bytes) -> Any:
    decoder = PackrDecoder()
    return decoder.decode(data)


def decode_stream(data: bytes) -> List[Any]:
    decoder = PackrDecoder()
    return decoder.decode_stream(data)
