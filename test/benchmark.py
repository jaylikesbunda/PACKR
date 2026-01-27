#!/usr/bin/env python3
"""
PACKR Benchmark Script

Compares PACKR compression against gzip and zstd (if available).
"""

import argparse
import gzip
import json
import os
import sys
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'python'))

from packr import PackrEncoder, PackrDecoder

try:
    import zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False


def benchmark_file(path: str, iterations: int = 10) -> None:
    """Run compression benchmarks on a file."""
    
    print(f"Benchmarking: {path}\n")
    print("=" * 60)
    
    # Read and parse
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        lines = content.strip().split('\n')
        data = [json.loads(line) for line in lines if line.strip()]
    
    original_size = len(content.encode('utf-8'))
    print(f"Original size: {original_size:,} bytes")
    print()
    
    # PACKR
    encoder = PackrEncoder()
    decoder = PackrDecoder()
    
    # Warmup
    if isinstance(data, list):
        packr_data = encoder.encode_stream(data)
    else:
        packr_data = encoder.encode(data)
    
    # Time encoding
    encoder.reset()
    start = time.perf_counter()
    for _ in range(iterations):
        encoder.reset()
        if isinstance(data, list):
            packr_data = encoder.encode_stream(data)
        else:
            packr_data = encoder.encode(data)
    encode_time = (time.perf_counter() - start) / iterations * 1000
    
    packr_size = len(packr_data)
    
    # Time decoding
    start = time.perf_counter()
    for _ in range(iterations):
        decoder.reset()
        decoder.decode(packr_data)
    decode_time = (time.perf_counter() - start) / iterations * 1000
    
    print(f"PACKR:")
    print(f"  Size:        {packr_size:,} bytes ({original_size/packr_size:.1f}:1)")
    print(f"  Encode time: {encode_time:.2f} ms")
    print(f"  Decode time: {decode_time:.2f} ms")
    print()
    
    # gzip
    content_bytes = content.encode('utf-8')
    
    start = time.perf_counter()
    for _ in range(iterations):
        gzip_data = gzip.compress(content_bytes)
    gzip_encode_time = (time.perf_counter() - start) / iterations * 1000
    
    gzip_size = len(gzip_data)
    
    start = time.perf_counter()
    for _ in range(iterations):
        gzip.decompress(gzip_data)
    gzip_decode_time = (time.perf_counter() - start) / iterations * 1000
    
    print(f"gzip:")
    print(f"  Size:        {gzip_size:,} bytes ({original_size/gzip_size:.1f}:1)")
    print(f"  Encode time: {gzip_encode_time:.2f} ms")
    print(f"  Decode time: {gzip_decode_time:.2f} ms")
    print()
    
    # zstd (if available)
    if HAS_ZSTD:
        start = time.perf_counter()
        for _ in range(iterations):
            zstd_data = zstd.compress(content_bytes)
        zstd_encode_time = (time.perf_counter() - start) / iterations * 1000
        
        zstd_size = len(zstd_data)
        
        start = time.perf_counter()
        for _ in range(iterations):
            zstd.decompress(zstd_data)
        zstd_decode_time = (time.perf_counter() - start) / iterations * 1000
        
        print(f"zstd:")
        print(f"  Size:        {zstd_size:,} bytes ({original_size/zstd_size:.1f}:1)")
        print(f"  Encode time: {zstd_encode_time:.2f} ms")
        print(f"  Decode time: {zstd_decode_time:.2f} ms")
        print()
    
    # Summary
    print("=" * 60)
    print("Summary:")
    print(f"  PACKR vs gzip:  {gzip_size/packr_size:.2f}x smaller")
    if HAS_ZSTD:
        print(f"  PACKR vs zstd:  {zstd_size/packr_size:.2f}x smaller")
    
    # Verify roundtrip
    print()
    print("Verifying roundtrip...")
    decoder.reset()
    
    # Use decode_stream for lists, decode for single objects
    if isinstance(data, list):
        decoded = decoder.decode_stream(packr_data)
    else:
        decoded = decoder.decode(packr_data)
    
    # Compare
    original_json = json.dumps(data, sort_keys=True)
    decoded_json = json.dumps(decoded, sort_keys=True)
    
    if original_json == decoded_json:
        print("✓ Roundtrip successful - data matches!")
    else:
        print("✗ Roundtrip failed - data mismatch!")
        print(f"  Original: {original_json[:100]}...")
        print(f"  Decoded:  {decoded_json[:100]}...")


def main():
    parser = argparse.ArgumentParser(description='PACKR compression benchmark')
    parser.add_argument('file', help='JSON file to benchmark')
    parser.add_argument('-n', '--iterations', type=int, default=10,
                        help='Number of iterations for timing (default: 10)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: file not found: {args.file}")
        sys.exit(1)
    
    benchmark_file(args.file, args.iterations)


if __name__ == '__main__':
    main()
