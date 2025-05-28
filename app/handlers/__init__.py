"""
Handlers module for KafkaMock application.
Contains all the message handling logic.
"""

from importlib import import_module
from typing import Dict, Any, Callable

# Dictionary to store handler functions
_handlers: Dict[str, Callable] = {}

def get_handler(name: str) -> Callable:
    """
    Lazy load and return a handler function.
    
    Args:
        name: The name of the handler to load
        
    Returns:
        The handler function
    """
    if name not in _handlers:
        try:
            module = import_module(f'.{name}', package=__package__)
            _handlers[name] = module.handle
        except ImportError:
            raise ValueError(f'Handler {name} not found')
    return _handlers[name]

# Clean up namespace
__all__ = ['get_handler'] 