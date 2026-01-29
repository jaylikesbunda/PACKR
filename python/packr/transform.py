"""
PACKR Byte Transforms

Pre-processing transforms that eliminate redundancy without entropy coding.
These replace the need for zlib by catching the same patterns at the byte level.

Transforms:
1. MTF (Move-to-Front) - Converts repeated bytes to zeros
2. RLE (Run-Length Encoding) - Compresses runs of zeros/repeated bytes
3. Delta on sorted runs - For numeric-heavy data
"""

from typing import Tuple


def mtf_encode(data: bytes) -> bytes:
    """
    Move-to-Front transform.

    Converts repeated/recent bytes to small values (mostly 0s and 1s).
    This is what makes the data highly compressible with simple RLE.

    Args:
        data: Input bytes

    Returns:
        MTF-transformed bytes (same length)
    """
    # Initialize symbol list (all possible bytes in order)
    symbols = list(range(256))
    output = bytearray(len(data))

    for i, byte in enumerate(data):
        # Find position of byte in list
        pos = symbols.index(byte)
        output[i] = pos

        # Move to front
        if pos > 0:
            symbols.pop(pos)
            symbols.insert(0, byte)

    return bytes(output)


def mtf_decode(data: bytes) -> bytes:
    """
    Inverse Move-to-Front transform.

    Args:
        data: MTF-encoded bytes

    Returns:
        Original bytes
    """
    symbols = list(range(256))
    output = bytearray(len(data))

    for i, pos in enumerate(data):
        byte = symbols[pos]
        output[i] = byte

        # Move to front
        if pos > 0:
            symbols.pop(pos)
            symbols.insert(0, byte)

    return bytes(output)


def rle_encode(data: bytes, min_run: int = 3) -> bytes:
    """
    Run-Length Encoding optimized for MTF output (lots of 0s and small values).

    Format:
    - Literal byte (0x00-0xFD): output as-is
    - 0xFE <byte> <count-3>: run of 3-258 identical bytes
    - 0xFF <count-1> <bytes...>: literal run of 2-256 bytes (escape)

    Args:
        data: Input bytes (ideally MTF-transformed)
        min_run: Minimum run length to encode (default 3)

    Returns:
        RLE-compressed bytes
    """
    if not data:
        return b''

    output = bytearray()
    i = 0
    n = len(data)

    while i < n:
        # Count run of identical bytes
        run_byte = data[i]
        run_len = 1
        while i + run_len < n and data[i + run_len] == run_byte and run_len < 258:
            run_len += 1

        if run_len >= min_run:
            # Encode as run
            if run_byte == 0xFE or run_byte == 0xFF:
                # Need escape for special bytes even in runs
                output.append(0xFE)
                output.append(run_byte)
                output.append(run_len - 3)
            else:
                output.append(0xFE)
                output.append(run_byte)
                output.append(run_len - 3)
            i += run_len
        elif run_byte == 0xFE or run_byte == 0xFF:
            # Escape special bytes
            output.append(0xFF)
            output.append(0)  # count-1 = 0 means 1 byte
            output.append(run_byte)
            i += 1
        else:
            # Literal byte
            output.append(run_byte)
            i += 1

    return bytes(output)


def rle_decode(data: bytes) -> bytes:
    """
    Decode RLE-compressed data.

    Args:
        data: RLE-compressed bytes

    Returns:
        Original bytes
    """
    if not data:
        return b''

    output = bytearray()
    i = 0
    n = len(data)

    while i < n:
        byte = data[i]

        if byte == 0xFE:
            # Run encoding
            if i + 2 >= n:
                raise ValueError("Truncated RLE run")
            run_byte = data[i + 1]
            run_len = data[i + 2] + 3
            output.extend([run_byte] * run_len)
            i += 3
        elif byte == 0xFF:
            # Literal escape
            if i + 1 >= n:
                raise ValueError("Truncated RLE escape")
            count = data[i + 1] + 1
            if i + 2 + count > n:
                raise ValueError("Truncated RLE literal run")
            output.extend(data[i + 2:i + 2 + count])
            i += 2 + count
        else:
            # Literal byte
            output.append(byte)
            i += 1

    return bytes(output)


def zero_rle_encode(data: bytes) -> bytes:
    """
    Specialized RLE that only compresses runs of zeros.

    This is even more efficient for MTF output where zeros dominate.

    Format:
    - 0x00: single zero
    - 0x01-0xFD: literal byte
    - 0xFE <byte>: escape for literal 0xFE or 0xFF
    - 0xFF <count>: run of (count+2) zeros (so 0xFF 0x00 = 2 zeros, 0xFF 0xFD = 255 zeros)

    Args:
        data: Input bytes

    Returns:
        Zero-RLE compressed bytes
    """
    if not data:
        return b''

    output = bytearray()
    i = 0
    n = len(data)

    while i < n:
        if data[i] == 0:
            # Count zeros
            zero_count = 0
            while i + zero_count < n and data[i + zero_count] == 0 and zero_count < 255:
                zero_count += 1

            if zero_count == 1:
                output.append(0x00)
            elif zero_count >= 2:
                # Encode run: 0xFF <count-2>
                output.append(0xFF)
                output.append(zero_count - 2)
            i += zero_count
        elif data[i] == 0xFE or data[i] == 0xFF:
            # Escape special bytes
            output.append(0xFE)
            output.append(data[i])
            i += 1
        else:
            output.append(data[i])
            i += 1

    return bytes(output)


def zero_rle_decode(data: bytes) -> bytes:
    """
    Decode zero-RLE compressed data.

    Args:
        data: Zero-RLE compressed bytes

    Returns:
        Original bytes
    """
    if not data:
        return b''

    output = bytearray()
    i = 0
    n = len(data)

    while i < n:
        byte = data[i]

        if byte == 0xFF:
            # Run of zeros
            if i + 1 >= n:
                raise ValueError("Truncated zero RLE run")
            count = data[i + 1] + 2
            output.extend([0] * count)
            i += 2
        elif byte == 0xFE:
            # Escaped byte (0xFE or 0xFF literal)
            if i + 1 >= n:
                raise ValueError("Truncated zero RLE escape")
            output.append(data[i + 1])
            i += 2
        else:
            output.append(byte)
            i += 1

    return bytes(output)


def block_sort_transform(data: bytes, block_size: int = 256) -> Tuple[bytes, bytes]:
    """
    Simplified block sorting (BWT-lite).

    Instead of full BWT, we sort small blocks and store the permutation indices.
    This groups similar bytes together without the complexity of full BWT.

    Args:
        data: Input bytes
        block_size: Size of each block to sort (default 256)

    Returns:
        Tuple of (sorted_data, index_data)
    """
    if not data:
        return b'', b''

    output = bytearray()
    indices = bytearray()

    # Process in blocks
    for start in range(0, len(data), block_size):
        block = data[start:start + block_size]

        # Create (byte, original_index) pairs and sort
        pairs = [(b, i) for i, b in enumerate(block)]
        pairs.sort(key=lambda x: x[0])

        # Extract sorted bytes and indices
        for byte, idx in pairs:
            output.append(byte)
            indices.append(idx)

    return bytes(output), bytes(indices)


def block_sort_inverse(sorted_data: bytes, indices: bytes, block_size: int = 256) -> bytes:
    """
    Inverse block sort transform.

    Args:
        sorted_data: Block-sorted bytes
        indices: Original position indices
        block_size: Block size used in transform

    Returns:
        Original bytes
    """
    if not sorted_data:
        return b''

    output = bytearray(len(sorted_data))

    # Process in blocks
    for start in range(0, len(sorted_data), block_size):
        end = min(start + block_size, len(sorted_data))
        block_len = end - start

        # Reconstruct original order
        for i in range(block_len):
            orig_idx = indices[start + i]
            output[start + orig_idx] = sorted_data[start + i]

    return bytes(output)


def lz_compress(data: bytes, min_match: int = 3, max_offset: int = 8191, fast_mode: bool = False) -> bytes:
    """
    LZ77-style compression optimized for PACKR output and ESP32 constraints.

    Uses a token-based format where each token starts with a control byte:
    - Control byte high nibble (4 bits): literal count (0-15)
    - Control byte low nibble (4 bits): match length - 3 (0-15 = lengths 3-18)

    If literal count is 15, read another byte for extended count.
    If match length is 15, read another byte for extended length.

    After control byte: <literals> <match_offset (2 bytes, little-endian)>

    Uses both 3-byte and 4-byte hash tables for better matching.

    Args:
        data: Input bytes
        min_match: Minimum match length (default 3)
        max_offset: Maximum back-reference distance (default 8191 for ESP32)

    Returns:
        LZ-compressed bytes with header
    """
    if len(data) == 0:
        return b'\x00' + (0).to_bytes(4, 'little')

    if len(data) < 4:
        return b'\x00' + len(data).to_bytes(4, 'little') + data

    # Quick entropy check: sample first 1KB
    # If too many unique bytes, likely high-entropy
    sample_size = min(1024, len(data))
    unique_bytes = len(set(data[:sample_size]))
    entropy_ratio = unique_bytes / sample_size

    # If > 80% unique bytes in sample, skip LZ compression
    if entropy_ratio > 0.80:
        return b'\x00' + len(data).to_bytes(4, 'little') + data

    # Adaptive search limit based on entropy
    # Higher entropy = fewer positions to check
    adaptive_search_limit = 8 if entropy_ratio > 0.70 else (16 if entropy_ratio > 0.50 else 32)

    output = bytearray()
    output.append(0x02)  # Format marker v2
    output.extend(len(data).to_bytes(4, 'little'))

    # Build hash tables for 4-byte sequences (most effective)
    hash4 = {}  # 4-byte hash -> list of positions (limited to last 32 for speed)

    # High-entropy detection: track match success rate
    attempts = 0
    matches = 0

    def calc_hash4(p):
        if p + 3 >= len(data):
            return None
        return (data[p] | (data[p+1] << 8) | (data[p+2] << 16) | (data[p+3] << 24)) & 0xFFFFFF

    def find_match(p):
        """Find best match at position p."""
        nonlocal attempts, matches
        best_len = 0
        best_off = 0

        # Fast mode: only check recent positions
        # Use adaptive limit based on entropy
        search_limit = 8 if fast_mode else adaptive_search_limit

        # Use 4-byte hash (best speed/compression trade-off)
        h4 = calc_hash4(p)
        if h4 is not None and h4 in hash4:
            attempts += 1
            positions = hash4[h4]
            # Search backwards through recent positions
            # Limit to last N positions for speed
            start_idx = max(0, len(positions) - search_limit)
            for i in range(len(positions) - 1, start_idx - 1, -1):
                prev_pos = positions[i]
                offset = p - prev_pos
                if offset > max_offset or offset < 1:
                    continue

                # Count match length
                match_len = 0
                max_len = min(273, len(data) - p)
                while match_len < max_len and data[prev_pos + match_len] == data[p + match_len]:
                    match_len += 1

                if match_len >= 3 and match_len > best_len:
                    best_len = match_len
                    best_off = offset
                    matches += 1
                    # Early exit if we found a good match
                    if match_len >= 32:
                        return best_len, best_off

        return best_len, best_off

    def update_hash(p):
        """Add position to hash table."""
        h4 = calc_hash4(p)
        if h4 is not None:
            if h4 not in hash4:
                hash4[h4] = []
            positions = hash4[h4]
            positions.append(p)
            # Keep only last 32 positions for speed
            if len(positions) > 32:
                hash4[h4] = positions[-32:]

    pos = 0
    literal_start = 0
    abort_compression = False

    while pos < len(data):
        # Early abort heuristic for high-entropy data
        # After sampling 200 positions, if match rate < 8%, abort LZ compression
        # This catches high-entropy data quickly while allowing normal data to compress
        if attempts > 200 and matches < attempts * 0.08:
            abort_compression = True
            break

        best_len, best_off = find_match(pos)

        # Match is worth it if: match_len > overhead
        # Overhead: 1 byte control + 2 byte offset = 3 bytes minimum
        # For 3-byte match: saves 0 bytes (wash) - skip unless literal pending
        # For 4-byte match: saves 1 byte
        min_worthwhile = 3 if (pos - literal_start) > 0 else 4

        if best_len >= min_worthwhile:
            # Emit token: literals + match
            lit_count = pos - literal_start
            match_len_code = best_len - 3  # 0 = length 3

            # Control byte
            lit_nibble = min(15, lit_count)
            match_nibble = min(15, match_len_code)
            output.append((lit_nibble << 4) | match_nibble)

            # Extended literal count
            if lit_nibble == 15:
                remaining = lit_count - 15
                while remaining >= 255:
                    output.append(255)
                    remaining -= 255
                output.append(remaining)

            # Literals
            output.extend(data[literal_start:pos])

            # Extended match length
            if match_nibble == 15:
                remaining = match_len_code - 15
                while remaining >= 255:
                    output.append(255)
                    remaining -= 255
                output.append(remaining)

            # Match offset (2 bytes LE)
            output.append(best_off & 0xFF)
            output.append((best_off >> 8) & 0xFF)

            # Update hash tables for all positions in match
            for i in range(best_len):
                update_hash(pos + i)

            pos += best_len
            literal_start = pos
        else:
            # No worthwhile match, add to hash and continue
            update_hash(pos)
            pos += 1

    # If aborted, return uncompressed immediately
    if abort_compression:
        return b'\x00' + len(data).to_bytes(4, 'little') + data

    # Emit final literals (if any)
    if literal_start < len(data):
        lit_count = len(data) - literal_start
        lit_nibble = min(15, lit_count)
        output.append(lit_nibble << 4)  # match_nibble = 0, no match follows

        # Extended literal count
        if lit_nibble == 15:
            remaining = lit_count - 15
            while remaining >= 255:
                output.append(255)
                remaining -= 255
            output.append(remaining)

        # Literals
        output.extend(data[literal_start:])

    # Only use if smaller
    if len(output) < len(data):
        return bytes(output)
    else:
        return b'\x00' + len(data).to_bytes(4, 'little') + data


def lz_decompress(data: bytes) -> bytes:
    """
    Decompress LZ77-style compressed data.

    Args:
        data: LZ-compressed bytes

    Returns:
        Original bytes
    """
    if len(data) < 5:
        raise ValueError("LZ data too short")

    fmt = data[0]
    orig_len = int.from_bytes(data[1:5], 'little')

    if fmt == 0x00:
        # Uncompressed
        return data[5:5+orig_len]

    if fmt != 0x02:
        raise ValueError(f"Unknown LZ format: {fmt}")

    output = bytearray()
    pos = 5

    while pos < len(data) and len(output) < orig_len:
        # Read control byte
        ctrl = data[pos]
        pos += 1

        lit_count = ctrl >> 4
        match_len_code = ctrl & 0x0F

        # Extended literal count
        if lit_count == 15:
            while pos < len(data):
                extra = data[pos]
                pos += 1
                lit_count += extra
                if extra < 255:
                    break

        # Copy literals
        output.extend(data[pos:pos + lit_count])
        pos += lit_count

        # Check if we're done (last token has no match)
        if pos >= len(data) or len(output) >= orig_len:
            break

        # Extended match length
        match_len = match_len_code + 3
        if match_len_code == 15:
            while pos < len(data):
                extra = data[pos]
                pos += 1
                match_len += extra
                if extra < 255:
                    break

        # Read offset
        if pos + 1 >= len(data):
            break
        offset = data[pos] | (data[pos + 1] << 8)
        pos += 2

        # Copy match
        start = len(output) - offset
        for i in range(match_len):
            output.append(output[start + i])

    return bytes(output[:orig_len])


def huffman_compress(data: bytes) -> bytes:
    """
    Simple canonical Huffman compression.

    Format:
    - Byte 0: 0x01 (compressed marker)
    - Bytes 1-4: original length (LE)
    - Bytes 5-6: number of symbols with non-zero count
    - For each symbol: <symbol> <code_length> (code lengths 1-15)
    - Bit-packed Huffman-encoded data
    - Final byte may have padding bits (high bits)

    Args:
        data: Input bytes

    Returns:
        Huffman-compressed bytes
    """
    if len(data) == 0:
        return b'\x00\x00\x00\x00\x00'

    from collections import Counter
    import heapq

    # Count frequencies
    counts = Counter(data)

    if len(counts) == 1:
        # Single symbol - special case
        sym = list(counts.keys())[0]
        return b'\x02' + len(data).to_bytes(4, 'little') + bytes([sym])

    # Build Huffman tree using heap
    # Each entry: (count, tie_breaker, node)
    # node is either (symbol,) for leaf or (left, right) for internal
    heap = []
    for sym, cnt in counts.items():
        heapq.heappush(heap, (cnt, sym, (sym,)))

    tie = 256
    while len(heap) > 1:
        cnt1, _, node1 = heapq.heappop(heap)
        cnt2, _, node2 = heapq.heappop(heap)
        heapq.heappush(heap, (cnt1 + cnt2, tie, (node1, node2)))
        tie += 1

    # Extract code lengths via tree traversal
    code_lengths = {}

    def traverse(node, depth):
        if len(node) == 1:
            # Leaf
            code_lengths[node[0]] = min(15, depth) if depth > 0 else 1
        else:
            traverse(node[0], depth + 1)
            traverse(node[1], depth + 1)

    if heap:
        traverse(heap[0][2], 0)

    # Limit code lengths to 15 and rebuild if necessary
    max_len = max(code_lengths.values())
    if max_len > 15:
        # Package-merge algorithm would be better, but for now just truncate
        for sym in code_lengths:
            if code_lengths[sym] > 15:
                code_lengths[sym] = 15

    # Build canonical codes
    # Sort by (length, symbol)
    sorted_syms = sorted(code_lengths.keys(), key=lambda s: (code_lengths[s], s))

    codes = {}
    code = 0
    prev_len = 0

    for sym in sorted_syms:
        length = code_lengths[sym]
        if length > prev_len:
            code <<= (length - prev_len)
        codes[sym] = (code, length)
        code += 1
        prev_len = length

    # Build output
    output = bytearray()
    output.append(0x01)  # Compressed marker
    output.extend(len(data).to_bytes(4, 'little'))

    # Symbol table
    output.extend(len(counts).to_bytes(2, 'little'))
    for sym in sorted_syms:
        output.append(sym)
        output.append(code_lengths[sym])

    # Encode data
    bit_buffer = 0
    bit_count = 0

    for byte in data:
        code_val, code_len = codes[byte]
        bit_buffer = (bit_buffer << code_len) | code_val
        bit_count += code_len

        while bit_count >= 8:
            bit_count -= 8
            output.append((bit_buffer >> bit_count) & 0xFF)

    # Flush remaining bits
    if bit_count > 0:
        output.append((bit_buffer << (8 - bit_count)) & 0xFF)

    return bytes(output)


def huffman_decompress(data: bytes) -> bytes:
    """
    Decompress Huffman-compressed data.

    Args:
        data: Huffman-compressed bytes

    Returns:
        Original bytes
    """
    if len(data) < 5:
        raise ValueError("Huffman data too short")

    marker = data[0]

    if marker == 0x00:
        # Uncompressed (empty)
        return b''

    orig_len = int.from_bytes(data[1:5], 'little')

    if marker == 0x02:
        # Single symbol repeated
        sym = data[5]
        return bytes([sym] * orig_len)

    if marker != 0x01:
        raise ValueError(f"Unknown Huffman marker: {marker}")

    pos = 5

    # Read symbol table
    num_syms = int.from_bytes(data[pos:pos+2], 'little')
    pos += 2

    code_lengths = {}
    for _ in range(num_syms):
        sym = data[pos]
        length = data[pos + 1]
        code_lengths[sym] = length
        pos += 2

    # Rebuild canonical codes
    sorted_syms = sorted(code_lengths.keys(), key=lambda s: (code_lengths[s], s))

    codes = {}
    code = 0
    prev_len = 0

    for sym in sorted_syms:
        length = code_lengths[sym]
        if length > prev_len:
            code <<= (length - prev_len)
        codes[code] = (sym, length)
        code += 1
        prev_len = length

    # Build decode table: for each code length, map code -> symbol
    decode_table = {}
    for code_val, (sym, length) in codes.items():
        if length not in decode_table:
            decode_table[length] = {}
        decode_table[length][code_val] = sym

    # Decode
    output = bytearray()
    bit_buffer = 0
    bit_count = 0
    data_pos = pos

    while len(output) < orig_len:
        # Ensure we have enough bits
        while bit_count < 15 and data_pos < len(data):
            bit_buffer = (bit_buffer << 8) | data[data_pos]
            bit_count += 8
            data_pos += 1

        # Try each code length
        found = False
        for length in sorted(decode_table.keys()):
            if bit_count < length:
                continue

            # Extract top 'length' bits
            code_val = (bit_buffer >> (bit_count - length)) & ((1 << length) - 1)

            if code_val in decode_table[length]:
                output.append(decode_table[length][code_val])
                bit_count -= length
                bit_buffer &= (1 << bit_count) - 1
                found = True
                break

        if not found:
            break

    return bytes(output[:orig_len])


# Composite transforms

def compress_transform(data: bytes, fast_lz: bool = True) -> bytes:
    """
    Apply compression transform optimized for streaming.

    Uses fast LZ77 compression by default for real-time performance.

    Format:
    - Byte 0: Transform flags
      - 0x00 = uncompressed
      - 0x03 = LZ77 only

    Args:
        data: Raw PACKR frame data
        fast_lz: Use fast LZ mode (fewer hash lookups)

    Returns:
        Transformed data with header
    """
    if not data:
        return b'\x00' + (0).to_bytes(4, 'little')

    # LZ77 compression (fast or full)
    lz_out = lz_compress(data, fast_mode=fast_lz)

    # Only use compression if it's actually smaller
    if len(lz_out) + 1 < len(data) + 5:
        return b'\x03' + lz_out
    else:
        # Uncompressed
        return b'\x00' + len(data).to_bytes(4, 'little') + data


def decompress_transform(data: bytes) -> bytes:
    """
    Reverse compression transform.

    Args:
        data: Transformed data with header

    Returns:
        Original PACKR frame data
    """
    if len(data) < 2:
        raise ValueError("Transform data too short")

    flags = data[0]

    if flags == 0x00:
        # No transform
        if len(data) < 5:
            raise ValueError("Transform data too short")
        orig_len = int.from_bytes(data[1:5], 'little')
        return data[5:5+orig_len]
    elif flags == 0x01:
        # MTF + Zero-RLE (legacy)
        if len(data) < 5:
            raise ValueError("Transform data too short")
        orig_len = int.from_bytes(data[1:5], 'little')
        payload = data[5:]
        rle_decoded = zero_rle_decode(payload)
        mtf_decoded = mtf_decode(rle_decoded)
        return mtf_decoded[:orig_len]
    elif flags == 0x03:
        # LZ77 only
        return lz_decompress(data[1:])
    elif flags == 0x04:
        # Huffman only
        return huffman_decompress(data[1:])
    elif flags == 0x05:
        # LZ77 + Huffman (decompress Huffman first, then LZ)
        lz_data = huffman_decompress(data[1:])
        return lz_decompress(lz_data)
    elif flags == 0x06:
        # Huffman + LZ (decompress LZ first, then Huffman)
        huff_data = lz_decompress(data[1:])
        return huffman_decompress(huff_data)
    else:
        raise ValueError(f"Unknown transform flags: {flags:#x}")


def compress_transform_v2(data: bytes) -> bytes:
    """
    Enhanced transform: Block-sort + MTF + Zero-RLE.

    Block sorting groups similar bytes, MTF converts to small values,
    Zero-RLE compresses the runs of zeros.

    Format:
    - Byte 0: Transform flags (0x02 = BlockSort+MTF+ZRLE)
    - Bytes 1-4: Original length (little-endian)
    - Bytes 5-8: Block size used
    - Rest: Transformed data + indices

    Args:
        data: Raw PACKR frame data

    Returns:
        Transformed data with header
    """
    if not data:
        return b'\x02\x00\x00\x00\x00\x00\x01\x00\x00'

    block_size = 256

    # Block sort
    sorted_data, indices = block_sort_transform(data, block_size)

    # MTF on sorted data (should have lots of runs now)
    mtf_data = mtf_encode(sorted_data)

    # Zero-RLE
    rle_data = zero_rle_encode(mtf_data)

    # Indices also benefit from MTF+RLE (they have patterns too)
    indices_mtf = mtf_encode(indices)
    indices_rle = zero_rle_encode(indices_mtf)

    # Total size: header + rle_data_len + rle_data + indices_rle
    total_transformed = len(rle_data) + len(indices_rle) + 8  # +8 for two length fields

    if total_transformed < len(data):
        header = b'\x02' + len(data).to_bytes(4, 'little') + block_size.to_bytes(4, 'little')
        body = (len(rle_data).to_bytes(4, 'little') + rle_data +
                len(indices_rle).to_bytes(4, 'little') + indices_rle)
        return header + body
    else:
        # Fall back to simpler transform
        return compress_transform(data)


def decompress_transform_v2(data: bytes) -> bytes:
    """
    Reverse enhanced transform.

    Args:
        data: Transformed data with header

    Returns:
        Original PACKR frame data
    """
    if len(data) < 5:
        raise ValueError("Transform data too short")

    flags = data[0]

    if flags == 0x00 or flags == 0x01:
        # Delegate to v1
        return decompress_transform(data)

    if flags != 0x02:
        raise ValueError(f"Unknown transform flags: {flags:#x}")

    orig_len = int.from_bytes(data[1:5], 'little')
    block_size = int.from_bytes(data[5:9], 'little')

    pos = 9

    # Read RLE data
    rle_len = int.from_bytes(data[pos:pos+4], 'little')
    pos += 4
    rle_data = data[pos:pos+rle_len]
    pos += rle_len

    # Read indices RLE
    indices_rle_len = int.from_bytes(data[pos:pos+4], 'little')
    pos += 4
    indices_rle = data[pos:pos+indices_rle_len]

    # Decode
    rle_decoded = zero_rle_decode(rle_data)
    mtf_decoded = mtf_decode(rle_decoded)

    indices_decoded = zero_rle_decode(indices_rle)
    indices = mtf_decode(indices_decoded)

    # Inverse block sort
    original = block_sort_inverse(mtf_decoded, indices, block_size)

    return original[:orig_len]
