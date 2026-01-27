"""
PACKR Rice Coding

Provides Rice/Golomb coding for entropy compression of symbol streams.
Rice coding is efficient for data with geometric distributions.
"""

from typing import List, Tuple


class BitWriter:
    """
    Bit-level writer for constructing compressed bitstreams.
    
    Accumulates bits and outputs complete bytes.
    """
    
    def __init__(self):
        """Initialize empty bit buffer."""
        self._buffer = bytearray()
        self._current_byte = 0
        self._bit_count = 0
    
    def write_bit(self, bit: int) -> None:
        """
        Write a single bit.
        
        Args:
            bit: 0 or 1
        """
        self._current_byte = (self._current_byte << 1) | (bit & 1)
        self._bit_count += 1
        
        if self._bit_count == 8:
            self._buffer.append(self._current_byte)
            self._current_byte = 0
            self._bit_count = 0
    
    def write_bits(self, value: int, count: int) -> None:
        """
        Write multiple bits from an integer.
        
        Args:
            value: Integer containing the bits (MSB first)
            count: Number of bits to write
        """
        for i in range(count - 1, -1, -1):
            self.write_bit((value >> i) & 1)
    
    def write_unary(self, value: int) -> None:
        """
        Write a value in unary coding (value zeros followed by one).
        
        Args:
            value: Non-negative integer to encode
        """
        for _ in range(value):
            self.write_bit(0)
        self.write_bit(1)
    
    def flush(self) -> bytes:
        """
        Flush remaining bits and return the complete buffer.
        
        Pads with zeros to complete the last byte.
        
        Returns:
            The encoded bytes
        """
        if self._bit_count > 0:
            # Pad to complete byte
            self._current_byte <<= (8 - self._bit_count)
            self._buffer.append(self._current_byte)
            self._current_byte = 0
            self._bit_count = 0
        
        return bytes(self._buffer)
    
    def get_bit_length(self) -> int:
        """Return total bits written so far."""
        return len(self._buffer) * 8 + self._bit_count


class BitReader:
    """
    Bit-level reader for parsing compressed bitstreams.
    """
    
    def __init__(self, data: bytes):
        """
        Initialize with data to read.
        
        Args:
            data: Compressed bytes
        """
        self._data = data
        self._byte_pos = 0
        self._bit_pos = 7  # MSB first
    
    def read_bit(self) -> int:
        """
        Read a single bit.
        
        Returns:
            0 or 1
            
        Raises:
            IndexError: If no more bits available
        """
        if self._byte_pos >= len(self._data):
            raise IndexError("No more bits to read")
        
        bit = (self._data[self._byte_pos] >> self._bit_pos) & 1
        self._bit_pos -= 1
        
        if self._bit_pos < 0:
            self._byte_pos += 1
            self._bit_pos = 7
        
        return bit
    
    def read_bits(self, count: int) -> int:
        """
        Read multiple bits as an integer.
        
        Args:
            count: Number of bits to read
            
        Returns:
            Integer value (MSB first)
        """
        value = 0
        for _ in range(count):
            value = (value << 1) | self.read_bit()
        return value
    
    def read_unary(self) -> int:
        """
        Read a unary-coded value.
        
        Returns:
            The decoded value (number of zeros before the terminating one)
        """
        count = 0
        while self.read_bit() == 0:
            count += 1
        return count
    
    def has_more(self) -> bool:
        """Check if more bits are available."""
        return self._byte_pos < len(self._data)
    
    def get_position(self) -> Tuple[int, int]:
        """Return current (byte_pos, bit_pos)."""
        return self._byte_pos, self._bit_pos


class RiceEncoder:
    """
    Rice coding encoder for efficient compression of small integers.
    
    Rice coding splits each value into:
    - Quotient: value >> k, encoded in unary
    - Remainder: value & ((1 << k) - 1), encoded in k bits
    """
    
    def __init__(self, k: int = 3):
        """
        Initialize Rice encoder.
        
        Args:
            k: Rice parameter (remainder bits). Default 3 is good for
               data with mean around 4-8.
        """
        self.k = k
        self._writer = BitWriter()
    
    def encode(self, value: int) -> None:
        """
        Encode a non-negative integer.
        
        Args:
            value: Value to encode (must be >= 0)
        """
        if value < 0:
            raise ValueError(f"Rice coding requires non-negative values, got {value}")
        
        quotient = value >> self.k
        remainder = value & ((1 << self.k) - 1)
        
        # Encode quotient in unary
        self._writer.write_unary(quotient)
        
        # Encode remainder in k bits
        self._writer.write_bits(remainder, self.k)
    
    def encode_signed(self, value: int) -> None:
        """
        Encode a signed integer using zigzag + Rice.
        
        Args:
            value: Signed integer to encode
        """
        # Zigzag encode: maps ..., -2, -1, 0, 1, 2, ... -> ..., 3, 1, 0, 2, 4, ...
        unsigned = (value << 1) ^ (value >> 31)
        self.encode(unsigned)
    
    def finish(self) -> bytes:
        """
        Finish encoding and return compressed bytes.
        
        Returns:
            Compressed data including K parameter byte
        """
        # Prepend K parameter
        data = self._writer.flush()
        return bytes([self.k]) + data
    
    def reset(self) -> None:
        """Reset encoder for new data."""
        self._writer = BitWriter()


class RiceDecoder:
    """
    Rice coding decoder.
    """
    
    def __init__(self, data: bytes):
        """
        Initialize decoder with compressed data.
        
        Args:
            data: Compressed bytes (first byte is K parameter)
        """
        if len(data) < 1:
            raise ValueError("Rice data must contain at least K parameter")
        
        self.k = data[0]
        self._reader = BitReader(data[1:])
    
    def decode(self) -> int:
        """
        Decode one non-negative integer.
        
        Returns:
            Decoded value
        """
        # Decode quotient from unary
        quotient = self._reader.read_unary()
        
        # Decode remainder from k bits
        remainder = self._reader.read_bits(self.k)
        
        return (quotient << self.k) | remainder
    
    def decode_signed(self) -> int:
        """
        Decode one signed integer (zigzag + Rice).
        
        Returns:
            Decoded signed value
        """
        unsigned = self.decode()
        # Zigzag decode
        return (unsigned >> 1) ^ -(unsigned & 1)
    
    def has_more(self) -> bool:
        """Check if more values can be decoded."""
        return self._reader.has_more()


def rice_encode_all(values: List[int], k: int = 3) -> bytes:
    """
    Convenience function to Rice-encode a list of values.
    
    Args:
        values: List of non-negative integers
        k: Rice parameter
        
    Returns:
        Compressed bytes
    """
    encoder = RiceEncoder(k)
    for v in values:
        encoder.encode(v)
    return encoder.finish()


def rice_decode_all(data: bytes, count: int) -> List[int]:
    """
    Convenience function to Rice-decode a known number of values.
    
    Args:
        data: Compressed bytes
        count: Number of values to decode
        
    Returns:
        List of decoded values
    """
    decoder = RiceDecoder(data)
    values = []
    for _ in range(count):
        values.append(decoder.decode())
    return values
