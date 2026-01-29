# PACKR

structural telemetry compression for embedded systems.

PACKR is a specialized compression format for JSON-like telemetry data. It combines dictionary tokenization, schema awareness, and numeric delta-encoding and operates with a small memory footprint suitable for low-power MCUs like the ESP32 and STM32.

Web Demo: [https://pack-r.netlify.app/](https://pack-r.netlify.app/)

## Compression

PACKR efficiency is driven by the structural regularity of the data. 

| Data Pattern | Typical Ratio | Key Mechanism |
|:---|:---:|:---|
| **High Redundancy** | 500x - 800x | Constant field elimination & RLE |
| **Gradual Variation** | 20x - 50x | Delta-encoding & bit-packing |
| **Sparse Data** | 10x - 20x | Null-mapping & schema elision |
| **High Entropy** | 2x - 6x | Transform-pass overhead reduction |

## Implementation

### C
The C implementation is designed for MCU targets such as ESP32 and STM32.
```c
#include "packr.h"

// Initialize context and decode
packr_ctx_t ctx;
packr_init(&ctx, buffer, size);
packr_decode_json(&ctx, &output_callback);
```

### Python
The Python implementation serves as a reference and cloud-side processing bridge.
```python
from packr import PackrEncoder, PackrDecoder

encoder = PackrEncoder()
packed = encoder.encode_stream(data_list)

decoder = PackrDecoder()
data = decoder.decode_stream(packed)
```

## License
MIT
