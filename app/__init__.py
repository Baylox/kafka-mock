"""
KafkaMock - A Kafka Mock Application
"""

__version__ = '0.1.0'
__author__ = 'Baylo'

from . import utils
from . import handlers

# Enable relative imports
__all__ = ['utils', 'handlers'] 