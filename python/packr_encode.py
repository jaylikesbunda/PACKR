#!/usr/bin/env python3
"""
PACKR CLI Encoder

Usage:
    packr_encode.py input.json output.pkr
    packr_encode.py --stream < input.jsonl > output.pkr
    packr_encode.py --stats input.json output.pkr
"""

import argparse
import json
import sys
import os
import gzip

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from packr import PackrEncoder


def encode_file(input_path: str, output_path: str, show_stats: bool = False) -> None:
    """Encode a JSON file to PACKR format."""
    
    # Read input
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Parse JSON (handle single object or array or JSONL)
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Try as JSONL
        lines = content.strip().split('\n')
        data = [json.loads(line) for line in lines if line.strip()]
    
    # Encode
    encoder = PackrEncoder()
    if isinstance(data, list):
        compressed = encoder.encode_stream(data)
    else:
        compressed = encoder.encode(data)
    
    # Write output
    with open(output_path, 'wb') as f:
        f.write(compressed)
    
    if show_stats:
        original_size = len(content.encode('utf-8'))
        packr_size = len(compressed)
        gzip_compressed = gzip.compress(content.encode('utf-8'))
        gzip_size = len(gzip_compressed)
        
        print(f"Original:     {original_size:,} bytes")
        print(f"gzip:         {gzip_size:,} bytes ({original_size/gzip_size:.1f}:1)")
        print(f"PACKR:        {packr_size:,} bytes ({original_size/packr_size:.1f}:1)")
        print(f"PACKR vs gzip: {gzip_size/packr_size:.2f}x better")


def encode_stream() -> None:
    """Encode JSONL from stdin to PACKR on stdout."""
    encoder = PackrEncoder()
    objects = []
    
    for line in sys.stdin:
        line = line.strip()
        if line:
            try:
                obj = json.loads(line)
                objects.append(obj)
            except json.JSONDecodeError as e:
                print(f"Warning: skipping invalid JSON: {e}", file=sys.stderr)
    
    if objects:
        compressed = encoder.encode_stream(objects)
        sys.stdout.buffer.write(compressed)


def main():
    parser = argparse.ArgumentParser(
        description='Encode JSON to PACKR format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  packr_encode.py data.json data.pkr
  packr_encode.py --stats data.json data.pkr
  cat data.jsonl | packr_encode.py --stream > data.pkr
"""
    )
    
    parser.add_argument('input', nargs='?', help='Input JSON file')
    parser.add_argument('output', nargs='?', help='Output PACKR file')
    parser.add_argument('--stream', action='store_true',
                        help='Stream mode: read JSONL from stdin, write PACKR to stdout')
    parser.add_argument('--stats', action='store_true',
                        help='Show compression statistics')
    
    args = parser.parse_args()
    
    if args.stream:
        encode_stream()
    elif args.input and args.output:
        encode_file(args.input, args.output, args.stats)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
