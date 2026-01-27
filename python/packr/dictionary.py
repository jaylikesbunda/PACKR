"""
PACKR Dictionary Management

Provides LRU-based dictionaries for field names, strings, and MAC addresses.
Each dictionary holds up to 64 entries and tracks usage for LRU eviction.
"""

from typing import Dict, Optional, Tuple, List, Any
from collections import OrderedDict

from .tokens import DICT_SIZE


class Dictionary:
    """
    LRU dictionary for PACKR token compression.
    
    Stores up to 64 entries and evicts least-recently-used entries when full.
    """
    
    def __init__(self, max_size: int = DICT_SIZE):
        """
        Initialize an empty dictionary.
        
        Args:
            max_size: Maximum number of entries (default 64)
        """
        self.max_size = max_size
        # value -> index mapping
        self._value_to_index: Dict[Any, int] = {}
        # index -> value mapping  
        self._index_to_value: Dict[int, Any] = {}
        # LRU order: most recent at end
        self._lru: OrderedDict[int, None] = OrderedDict()
        # Next available index (for initial population)
        self._next_index = 0
    
    def lookup(self, value: Any) -> Optional[int]:
        """
        Look up a value in the dictionary.
        
        Args:
            value: The value to find
            
        Returns:
            Index if found, None otherwise
        """
        index = self._value_to_index.get(value)
        if index is not None:
            # Move to end of LRU (mark as recently used)
            self._lru.move_to_end(index)
        return index
    
    def get_or_add(self, value: Any) -> Tuple[int, bool]:
        """
        Get existing index or add new entry.
        
        Args:
            value: The value to find or add
            
        Returns:
            Tuple of (index, is_new) where is_new indicates if entry was added
        """
        # Check if already exists
        existing = self.lookup(value)
        if existing is not None:
            return existing, False
        
        # Need to add new entry
        if self._next_index < self.max_size:
            # Dictionary not full yet, use next slot
            index = self._next_index
            self._next_index += 1
        else:
            # Dictionary full, evict LRU entry
            # Get the least recently used (first in OrderedDict)
            lru_index = next(iter(self._lru))
            old_value = self._index_to_value[lru_index]
            
            # Remove old entry
            del self._value_to_index[old_value]
            del self._lru[lru_index]
            
            index = lru_index
        
        # Add new entry
        self._value_to_index[value] = index
        self._index_to_value[index] = value
        self._lru[index] = None
        self._lru.move_to_end(index)
        
        return index, True
    
    def get_value(self, index: int) -> Optional[Any]:
        """
        Get value by index.
        
        Args:
            index: Dictionary index
            
        Returns:
            Value if found, None otherwise
        """
        value = self._index_to_value.get(index)
        if value is not None:
            self._lru.move_to_end(index)
        return value
    
    def add_at_index(self, index: int, value: Any) -> None:
        """
        Add or update entry at specific index.
        
        Used during decoding when we assign values to specific slots.
        
        Args:
            index: The index to use
            value: The value to store
        """
        # Remove old value at this index if exists
        if index in self._index_to_value:
            old_value = self._index_to_value[index]
            del self._value_to_index[old_value]
            if index in self._lru:
                del self._lru[index]
        
        # Add new entry
        self._value_to_index[value] = index
        self._index_to_value[index] = value
        self._lru[index] = None
        self._lru.move_to_end(index)
        
        # Update next_index if needed
        if index >= self._next_index:
            self._next_index = index + 1
    
    def reset(self) -> None:
        """Clear all entries from the dictionary."""
        self._value_to_index.clear()
        self._index_to_value.clear()
        self._lru.clear()
        self._next_index = 0
    
    def __len__(self) -> int:
        """Return number of entries in dictionary."""
        return len(self._index_to_value)
    
    def __contains__(self, value: Any) -> bool:
        """Check if value exists in dictionary."""
        return value in self._value_to_index
    
    def entries(self) -> List[Tuple[int, Any]]:
        """Return all entries as (index, value) pairs."""
        return [(i, v) for i, v in self._index_to_value.items()]


class DictionarySet:
    """
    Container for all three PACKR dictionaries.
    
    Provides field, string, and MAC dictionaries together.
    """
    
    def __init__(self):
        """Initialize all three dictionaries."""
        self.fields = Dictionary()
        self.strings = Dictionary()
        self.macs = Dictionary()
    
    def reset(self) -> None:
        """Reset all dictionaries."""
        self.fields.reset()
        self.strings.reset()
        self.macs.reset()
