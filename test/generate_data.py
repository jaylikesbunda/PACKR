#!/usr/bin/env python3
"""Generate larger sample data for benchmarking."""

import json
import random
import sys
import os

def generate_telemetry(count: int, output_path: str):
    """Generate sample WiFi telemetry data."""
    macs = [
        "AA:BB:CC:DD:EE:01",
        "AA:BB:CC:DD:EE:02", 
        "AA:BB:CC:DD:EE:03",
        "11:22:33:44:55:66",
        "DE:AD:BE:EF:CA:FE",
    ]
    
    ssids = [
        "HomeNetwork",
        "GuestNetwork",
        "OfficeWiFi",
        "CafeHotspot",
    ]
    
    channels = [1, 6, 11, 36, 40, 44]
    
    records = []
    timestamp = 1706345000
    
    for i in range(count):
        mac = random.choice(macs)
        ssid = random.choice(ssids)
        channel = random.choice(channels)
        rssi = random.randint(-80, -30)
        
        records.append({
            "timestamp": timestamp + i,
            "rssi": rssi,
            "mac": mac,
            "channel": channel,
            "ssid": ssid
        })
    
    with open(output_path, 'w') as f:
        json.dump(records, f, indent=2)
    
    print(f"Generated {count} records to {output_path}")
    print(f"File size: {os.path.getsize(output_path):,} bytes")


if __name__ == '__main__':
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    output = sys.argv[2] if len(sys.argv) > 2 else 'test/sample_large.json'
    generate_telemetry(count, output)
