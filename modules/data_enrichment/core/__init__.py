# modules_new/core/__init__.py
"""
核心模块

提供项目的基础类、异常、常量等核心功能
"""

from .base import (
    BaseProcessor, BaseValidator, BaseAnalyzer, 
    Singleton, BaseConfig, EventEmitter, ProgressTracker, BaseManager
)
from .exceptions import (
    ProcessingError, DataValidationError, APIError, 
    FileOperationError, ConfigurationError, ModuleError,
    with_error_handling
)

from .constants import (
    VERSION, PROJECT_NAME, DEFAULT_ENCODING,
    PROPERTIES_COLUMNS, REQUIRED_COLUMNS, COLUMN_MAPPINGS,
    ProcessingStatus, LogLevel, DataType, ValidationLevel,
    APIService, ErrorCodes, Messages
)

from .processor import CoreProcessor
from .status_tracker import StatusTracker

__all__ = [
    # Base classes
    'BaseProcessor', 'BaseValidator', 'BaseAnalyzer', 'BaseConfig', 'BaseManager',
    'EventEmitter', 'ProgressTracker',
    
    # Exceptions
    'ProcessingError', 'DataValidationError', 'APIError', 
    'FileOperationError', 'ConfigurationError', 'ModuleError',
    'with_error_handling',
    
    # Constants
    'VERSION', 'PROJECT_NAME', 'DEFAULT_ENCODING',
    'PROPERTIES_COLUMNS', 'REQUIRED_COLUMNS', 'COLUMN_MAPPINGS',
    'ProcessingStatus', 'LogLevel', 'DataType', 'ValidationLevel',
    'APIService', 'ErrorCodes', 'Messages'
]
