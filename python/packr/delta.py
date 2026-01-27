"""
PACKR Delta Encoding

Provides delta encoding for sequential numeric values.
Tracks previous values per field to encode differences efficiently.
"""

from typing import Dict, Tuple, Optional, Union

from .tokens import (
    TokenType,
    is_small_delta,
    encode_small_delta,
    decode_small_delta,
    encode_signed_varint,
    decode_signed_varint,
    encode_fixed16,
    decode_fixed16,
    encode_fixed32,
    decode_fixed32,
)


NumericValue = Union[int, float]


class DeltaEncoder:
    """
    Encodes numeric values as deltas from previous values.
    
    Tracks the last value for each field (by field index) and encodes
    subsequent values as small deltas when possible.
    """
    
    def __init__(self):
        """Initialize with no previous values."""
        # field_index -> last value
        self._last_values: Dict[int, NumericValue] = {}
        # field_index -> value type ('int' or 'float')
        self._value_types: Dict[int, str] = {}
    
    def encode(self, field_index: int, value: NumericValue) -> Tuple[bytes, bool]:
        """
        Encode a numeric value, using delta if possible.
        
        Args:
            field_index: The field dictionary index
            value: The numeric value to encode
            
        Returns:
            Tuple of (encoded bytes, is_delta) where is_delta indicates
            if delta encoding was used
        """
        is_float = isinstance(value, float) and not value.is_integer()
        
        # Check if we have a previous value for this field
        if field_index in self._last_values:
            last_value = self._last_values[field_index]
            last_type = self._value_types[field_index]
            
            # Only delta encode if same type
            if (is_float and last_type == 'float') or (not is_float and last_type == 'int'):
                if is_float:
                    # For floats, calculate delta in fixed-point
                    delta = value - last_value
                    # Round to fixed16 precision
                    delta_scaled = int(round(delta * 256))
                    if is_small_delta(delta_scaled):
                        self._last_values[field_index] = value
                        return bytes([encode_small_delta(delta_scaled)]), True
                    else:
                        # Use large delta for floats (as fixed16 scaled)
                        self._last_values[field_index] = value
                        return bytes([TokenType.DELTA_LARGE]) + encode_signed_varint(delta_scaled), True
                else:
                    # Integer delta
                    delta = int(value) - int(last_value)
                    if is_small_delta(delta):
                        self._last_values[field_index] = int(value)
                        return bytes([encode_small_delta(delta)]), True
                    else:
                        self._last_values[field_index] = int(value)
                        return bytes([TokenType.DELTA_LARGE]) + encode_signed_varint(delta), True
        
        # No delta - encode absolute value
        self._last_values[field_index] = value
        
        if is_float:
            self._value_types[field_index] = 'float'
            # Choose fixed16 or fixed32 based on value range
            if -128 <= value <= 127:
                return bytes([TokenType.FLOAT16]) + encode_fixed16(value), False
            else:
                return bytes([TokenType.FLOAT32]) + encode_fixed32(value), False
        else:
            self._value_types[field_index] = 'int'
            return bytes([TokenType.INT]) + encode_signed_varint(int(value)), False
    
    def reset(self) -> None:
        """Clear all tracked values."""
        self._last_values.clear()
        self._value_types.clear()
    
    def reset_field(self, field_index: int) -> None:
        """Clear tracked value for a specific field."""
        self._last_values.pop(field_index, None)
        self._value_types.pop(field_index, None)


class DeltaDecoder:
    """
    Decodes delta-encoded numeric values.
    
    Tracks previous values per field to reconstruct absolute values from deltas.
    """
    
    def __init__(self):
        """Initialize with no previous values."""
        # field_index -> last value
        self._last_values: Dict[int, NumericValue] = {}
        # field_index -> is float
        self._is_float: Dict[int, bool] = {}
    
    def decode_absolute_int(self, field_index: int, value: int) -> int:
        """
        Decode an absolute integer value.
        
        Args:
            field_index: The field dictionary index
            value: The absolute value
            
        Returns:
            The value (unchanged, but tracked for future deltas)
        """
        self._last_values[field_index] = value
        self._is_float[field_index] = False
        return value
    
    def decode_absolute_float16(self, field_index: int, encoded: bytes, offset: int = 0) -> Tuple[float, int]:
        """
        Decode a fixed16 float value.
        
        Args:
            field_index: The field dictionary index
            encoded: Bytes containing the fixed16 value
            offset: Offset into bytes
            
        Returns:
            Tuple of (decoded float, bytes consumed)
        """
        value, consumed = decode_fixed16(encoded, offset)
        self._last_values[field_index] = value
        self._is_float[field_index] = True
        return value, consumed
    
    def decode_absolute_float32(self, field_index: int, encoded: bytes, offset: int = 0) -> Tuple[float, int]:
        """
        Decode a fixed32 float value.
        
        Args:
            field_index: The field dictionary index
            encoded: Bytes containing the fixed32 value
            offset: Offset into bytes
            
        Returns:
            Tuple of (decoded float, bytes consumed)
        """
        value, consumed = decode_fixed32(encoded, offset)
        self._last_values[field_index] = value
        self._is_float[field_index] = True
        return value, consumed
    
    def decode_delta_small(self, field_index: int, delta_byte: int) -> NumericValue:
        """
        Decode a small delta (inline in token byte).
        
        Args:
            field_index: The field dictionary index
            delta_byte: The token byte (0xC3-0xD2)
            
        Returns:
            Reconstructed absolute value
        """
        delta = decode_small_delta(delta_byte)
        return self._apply_delta(field_index, delta)
    
    def decode_delta_large(self, field_index: int, encoded: bytes, offset: int = 0) -> Tuple[NumericValue, int]:
        """
        Decode a large delta (varint encoded).
        
        Args:
            field_index: The field dictionary index
            encoded: Bytes containing the signed varint delta
            offset: Offset into bytes
            
        Returns:
            Tuple of (reconstructed value, bytes consumed)
        """
        delta, consumed = decode_signed_varint(encoded, offset)
        value = self._apply_delta(field_index, delta)
        return value, consumed
    
    def _apply_delta(self, field_index: int, delta: int) -> NumericValue:
        """
        Apply a delta to the last value for a field.
        
        Args:
            field_index: The field dictionary index
            delta: The delta value (scaled for floats)
            
        Returns:
            The reconstructed absolute value
        """
        if field_index not in self._last_values:
            raise ValueError(f"No previous value for field {field_index}")
        
        last_value = self._last_values[field_index]
        is_float = self._is_float.get(field_index, False)
        
        if is_float:
            # Delta is in fixed16 scale (1/256)
            new_value = last_value + (delta / 256.0)
        else:
            new_value = int(last_value) + delta
        
        self._last_values[field_index] = new_value
        return new_value
    
    def reset(self) -> None:
        """Clear all tracked values."""
        self._last_values.clear()
        self._is_float.clear()
    
    def set_last_value(self, field_index: int, value: NumericValue, is_float: bool = False) -> None:
        """
        Manually set the last value for a field.
        
        Used when decoding absolute values outside the delta decoder.
        
        Args:
            field_index: The field dictionary index
            value: The value to set
            is_float: Whether this is a float value
        """
        self._last_values[field_index] = value
        self._is_float[field_index] = is_float
