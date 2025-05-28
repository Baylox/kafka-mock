"""
Utility functions for KafkaMock application.
"""

from typing import Any
from functools import lru_cache

# Implement lazy loading for utilities
def __getattr__(name: str) -> Any:
    """
    Lazy import utility modules when they are first accessed.
    
    Args:
        name: The name of the utility to import
        
    Returns:
        The requested utility module or function
    """
    try:
        return __import__(name, globals(), locals(), [], 1)
    except ImportError:
        raise AttributeError(f'Module {__name__} has no attribute {name}')

# Cache decorator for expensive operations
def memoize(func):
    """Decorator to cache function results"""
    return lru_cache(maxsize=None)(func)

# Export utility functions
__all__ = ['memoize'] 