"""
PACKR Frame Handling

Provides frame construction and parsing for the PACKR format.
Each frame is independently decodable with magic number, version, and CRC.
"""

import struct
import zlib
from dataclasses import dataclass, field
from typing import List, Optional
from enum import IntFlag

from .tokens import MAGIC, VERSION, encode_varint, decode_varint


class FrameFlags(IntFlag):
    """Frame flag bits."""
    HAS_DICT_UPDATE = 0x01
    USES_RICE = 0x02
    DICT_RESET = 0x04


@dataclass
class Frame:
    """
    Represents a PACKR frame.
    
    Attributes:
        version: Format version number
        flags: Frame flags
        symbol_count: Number of symbols in this frame
        data: Raw or compressed symbol data
        crc: CRC32 of frame (excluding CRC field itself)
    """
    version: int = VERSION
    flags: int = 0
    symbol_count: int = 0
    data: bytes = field(default_factory=bytes)
    crc: int = 0
    
    def compute_crc(self) -> int:
        """
        Compute CRC32 for this frame.
        
        CRC covers: MAGIC + VERSION + FLAGS + SYMBOL_COUNT + DATA
        """
        content = self._serialize_content()
        return zlib.crc32(content) & 0xFFFFFFFF
    
    def _serialize_content(self) -> bytes:
        """Serialize frame content (everything except CRC)."""
        parts = [
            MAGIC,
            bytes([self.version, self.flags]),
            encode_varint(self.symbol_count),
            self.data
        ]
        return b''.join(parts)
    
    def serialize(self) -> bytes:
        """
        Serialize frame to bytes.
        
        Returns:
            Complete frame including header and CRC
        """
        content = self._serialize_content()
        crc = zlib.crc32(content) & 0xFFFFFFFF
        return content + struct.pack('<I', crc)


class FrameBuilder:
    """
    Incrementally builds a PACKR frame.
    
    Example:
        builder = FrameBuilder()
        builder.add_token(b'\\xDC')  # OBJECT_START
        builder.add_token(b'\\xD5\\x04rssi')  # NEW_FIELD
        frame = builder.finalize()
    """
    
    def __init__(self):
        """Initialize empty frame builder."""
        self._data = bytearray()
        self._symbol_count = 0
        self._flags = 0
    
    def add_token(self, token_bytes: bytes) -> None:
        """
        Add a token to the frame.
        
        Args:
            token_bytes: Complete encoded token
        """
        self._data.extend(token_bytes)
        self._symbol_count += 1
    
    def add_raw(self, data: bytes) -> None:
        """
        Add raw bytes without incrementing symbol count.
        
        Used for continuation data that's part of a previous token.
        
        Args:
            data: Raw bytes to append
        """
        self._data.extend(data)
    
    def set_flag(self, flag: FrameFlags) -> None:
        """
        Set a frame flag.
        
        Args:
            flag: Flag to set
        """
        self._flags |= int(flag)
    
    def clear_flag(self, flag: FrameFlags) -> None:
        """
        Clear a frame flag.
        
        Args:
            flag: Flag to clear
        """
        self._flags &= ~int(flag)
    
    def get_symbol_count(self) -> int:
        """Return current symbol count."""
        return self._symbol_count
    
    def get_data_size(self) -> int:
        """Return current data size in bytes."""
        return len(self._data)
    
    def finalize(self) -> Frame:
        """
        Finalize and return the frame.
        
        Returns:
            Complete Frame object
        """
        return Frame(
            version=VERSION,
            flags=self._flags,
            symbol_count=self._symbol_count,
            data=bytes(self._data)
        )
    
    def reset(self) -> None:
        """Reset builder for new frame."""
        self._data.clear()
        self._symbol_count = 0
        self._flags = 0


class FrameParser:
    """
    Parses PACKR frames from bytes.
    
    Example:
        parser = FrameParser()
        frame = parser.parse(data)
        if frame:
            print(f"Got {frame.symbol_count} symbols")
    """
    
    def __init__(self, verify_crc: bool = True):
        """
        Initialize parser.
        
        Args:
            verify_crc: Whether to verify CRC32 (default True)
        """
        self.verify_crc = verify_crc
    
    def parse(self, data: bytes) -> Optional[Frame]:
        """
        Parse a frame from bytes.
        
        Args:
            data: Bytes containing a complete frame
            
        Returns:
            Parsed Frame, or None if invalid
            
        Raises:
            ValueError: If frame is malformed or CRC fails
        """
        offset = 0
        
        # Check minimum size
        if len(data) < 10:  # MAGIC(4) + VER(1) + FLAGS(1) + SYMCNT(1) + CRC(4)
            raise ValueError("Frame too short")
        
        # Verify magic
        if data[offset:offset + 4] != MAGIC:
            raise ValueError(f"Invalid magic: expected {MAGIC!r}, got {data[offset:offset+4]!r}")
        offset += 4
        
        # Read version
        version = data[offset]
        offset += 1
        
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")
        
        # Read flags
        flags = data[offset]
        offset += 1
        
        # Read symbol count
        symbol_count, consumed = decode_varint(data, offset)
        offset += consumed
        
        # Data is everything between here and CRC
        data_end = len(data) - 4  # CRC is last 4 bytes
        if offset > data_end:
            raise ValueError("Frame truncated")
        
        frame_data = data[offset:data_end]
        
        # Read and verify CRC
        stored_crc = struct.unpack_from('<I', data, data_end)[0]
        
        if self.verify_crc:
            content = data[:data_end]
            computed_crc = zlib.crc32(content) & 0xFFFFFFFF
            if computed_crc != stored_crc:
                raise ValueError(f"CRC mismatch: expected {stored_crc:08X}, got {computed_crc:08X}")
        
        return Frame(
            version=version,
            flags=flags,
            symbol_count=symbol_count,
            data=frame_data,
            crc=stored_crc
        )
    
    def parse_stream(self, data: bytes) -> List[Frame]:
        """
        Parse multiple frames from a byte stream.
        
        Args:
            data: Bytes potentially containing multiple frames
            
        Returns:
            List of parsed frames
        """
        frames = []
        offset = 0
        
        while offset < len(data):
            # Find next magic
            magic_pos = data.find(MAGIC, offset)
            if magic_pos < 0:
                break
            
            # Try to parse frame starting here
            # We need to find the frame end, which requires parsing
            # For now, try progressively larger sizes
            remaining = data[magic_pos:]
            
            for end in range(10, len(remaining) + 1):
                try:
                    frame = self.parse(remaining[:end])
                    frames.append(frame)
                    offset = magic_pos + end
                    break
                except ValueError:
                    continue
            else:
                # Couldn't parse a frame, skip this magic
                offset = magic_pos + 1
        
        return frames
