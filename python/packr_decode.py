#!/usr/bin/env python3
"""
PACKR CLI Decoder

Usage:
    packr_decode.py input.pkr output.json
    packr_decode.py --stream < input.pkr > output.jsonl
    packr_decode.py --pretty input.pkr output.json
"""

import argparse
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from packr import PackrDecoder


def decode_file(input_path: str, output_path: str, pretty: bool = False) -> None:
    """Decode a PACKR file to JSON format."""
    
    # Read input
    with open(input_path, 'rb') as f:
        compressed = f.read()
    
    # Decode
    decoder = PackrDecoder()
    
    # Try to decode as stream first, fall back to single object
    try:
        data = decoder.decode_stream(compressed)
        if len(data) == 1:
            data = data[0]
    except Exception:
        decoder.reset()
        data = decoder.decode(compressed)
    
    # Write output
    indent = 2 if pretty else None
    with open(output_path, 'w', encoding='utf-8') as f:
        if isinstance(data, list) and not pretty:
            # Write as JSONL
            for item in data:
                f.write(json.dumps(item) + '\n')
        else:
            json.dump(data, f, indent=indent)
            f.write('\n')
    
    print(f"Decoded {len(compressed):,} bytes to {output_path}")


def decode_stream(pretty: bool = False) -> None:
    """Decode PACKR from stdin to JSON/JSONL on stdout."""
    compressed = sys.stdin.buffer.read()
    
    decoder = PackrDecoder()
    data = decoder.decode_stream(compressed)
    
    if pretty:
        print(json.dumps(data, indent=2))
    else:
        for item in data:
            print(json.dumps(item))


def main():
    parser = argparse.ArgumentParser(
        description='Decode PACKR format to JSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  packr_decode.py data.pkr data.json
  packr_decode.py --pretty data.pkr data.json
  cat data.pkr | packr_decode.py --stream > data.jsonl
"""
    )
    
    parser.add_argument('input', nargs='?', help='Input PACKR file')
    parser.add_argument('output', nargs='?', help='Output JSON file')
    parser.add_argument('--stream', action='store_true',
                        help='Stream mode: read PACKR from stdin, write JSON to stdout')
    parser.add_argument('--pretty', action='store_true',
                        help='Pretty-print JSON output')
    
    args = parser.parse_args()
    
    if args.stream:
        decode_stream(args.pretty)
    elif args.input and args.output:
        decode_file(args.input, args.output, args.pretty)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
