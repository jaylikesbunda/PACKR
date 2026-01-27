"""
PACKR - Structure-First Streaming Compression
"""

__version__ = "0.1.0"

from .tokens import TokenType, MAGIC, VERSION
from .encoder import PackrEncoder
from .decoder import PackrDecoder

__all__ = [
    "TokenType",
    "MAGIC", 
    "VERSION",
    "PackrEncoder",
    "PackrDecoder",
]
