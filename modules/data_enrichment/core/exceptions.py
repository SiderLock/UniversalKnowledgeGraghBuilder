# modules_new/core/exceptions.py
"""
异常定义模块

定义项目中使用的所有自定义异常类
"""

from typing import Optional, Any


class ChemicalDataError(Exception):
    """化学品数据处理相关异常的基类"""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[dict] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
    
    def __str__(self):
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class FileOperationError(ChemicalDataError):
    """文件操作异常"""
    
    def __init__(self, message: str, file_path: Optional[str] = None, operation: Optional[str] = None):
        super().__init__(message, "FILE_ERROR")
        self.file_path = file_path
        self.operation = operation
        if file_path:
            self.details["file_path"] = file_path
        if operation:
            self.details["operation"] = operation


class DataValidationError(ChemicalDataError):
    """数据验证异常"""
    
    def __init__(self, message: str, column: Optional[str] = None, row_index: Optional[int] = None, value: Optional[Any] = None):
        super().__init__(message, "VALIDATION_ERROR")
        self.column = column
        self.row_index = row_index
        self.value = value
        if column:
            self.details["column"] = column
        if row_index is not None:
            self.details["row_index"] = row_index
        if value is not None:
            self.details["value"] = str(value)


class ConfigurationError(ChemicalDataError):
    """配置异常"""
    
    def __init__(self, message: str, config_key: Optional[str] = None):
        super().__init__(message, "CONFIG_ERROR")
        self.config_key = config_key
        if config_key:
            self.details["config_key"] = config_key


class APIError(ChemicalDataError):
    """API调用异常"""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[dict] = None):
        super().__init__(message, "API_ERROR")
        self.status_code = status_code
        self.response_data = response_data
        if status_code:
            self.details["status_code"] = status_code
        if response_data:
            self.details["response_data"] = response_data


class ProcessingError(ChemicalDataError):
    """数据处理异常"""
    
    def __init__(self, message: str, stage: Optional[str] = None, chemical_name: Optional[str] = None):
        super().__init__(message, "PROCESSING_ERROR")
        self.stage = stage
        self.chemical_name = chemical_name
        if stage:
            self.details["stage"] = stage
        if chemical_name:
            self.details["chemical_name"] = chemical_name


class ValidationRuleError(ChemicalDataError):
    """验证规则异常"""
    
    def __init__(self, message: str, rule_name: Optional[str] = None):
        super().__init__(message, "VALIDATION_RULE_ERROR")
        self.rule_name = rule_name
        if rule_name:
            self.details["rule_name"] = rule_name


class ColumnMappingError(ChemicalDataError):
    """列名映射异常"""
    
    def __init__(self, message: str, column_name: Optional[str] = None, expected_columns: Optional[list] = None):
        super().__init__(message, "COLUMN_MAPPING_ERROR")
        self.column_name = column_name
        self.expected_columns = expected_columns
        if column_name:
            self.details["column_name"] = column_name
        if expected_columns:
            self.details["expected_columns"] = expected_columns


class BackupError(ChemicalDataError):
    """备份操作异常"""
    
    def __init__(self, message: str, backup_path: Optional[str] = None, operation: Optional[str] = None):
        super().__init__(message, "BACKUP_ERROR")
        self.backup_path = backup_path
        self.operation = operation
        if backup_path:
            self.details["backup_path"] = backup_path
        if operation:
            self.details["operation"] = operation


class ModuleError(ChemicalDataError):
    """模块相关错误"""
    
    def __init__(self, message: str, module_name: Optional[str] = None):
        super().__init__(message, "MODULE_ERROR")
        self.module_name = module_name
        if module_name:
            self.details["module_name"] = module_name


# 异常处理装饰器
from functools import wraps
import logging


def with_error_handling(default_return: Any = None):
    """
    统一的错误处理装饰器
    
    Args:
        default_return: 发生异常时返回的默认值
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ChemicalDataError as e:
                logging.error(f"在 {func.__name__} 中发生业务错误: {e}")
                return default_return
            except Exception as e:
                logging.error(f"在 {func.__name__} 中发生未知错误: {e}", exc_info=True)
                return default_return
        return wrapper
    return decorator


def safe_execute(func, default=None, logger: Optional[logging.Logger] = None):
    """
    安全执行函数，捕获所有异常
    
    Args:
        func: 要执行的函数
        default: 异常时返回的默认值
        logger: 日志记录器
    
    Returns:
        函数结果或默认值
    """
    try:
        return func()
    except Exception as e:
        if logger:
            logger.error(f"安全执行失败: {e}")
        return default


# 验证函数
def validate_positive_int(value: Any, name: str = "值") -> int:
    """验证正整数"""
    try:
        int_value = int(value)
        if int_value <= 0:
            raise DataValidationError(f"{name}必须是正整数，当前值: {value}")
        return int_value
    except (ValueError, TypeError):
        raise DataValidationError(f"{name}必须是有效的整数，当前值: {value}")


def validate_non_negative_float(value: Any, name: str = "值") -> float:
    """验证非负浮点数"""
    try:
        float_value = float(value)
        if float_value < 0:
            raise DataValidationError(f"{name}不能为负数，当前值: {value}")
        return float_value
    except (ValueError, TypeError):
        raise DataValidationError(f"{name}必须是有效的数字，当前值: {value}")


def validate_file_path(path: str, must_exist: bool = False) -> str:
    """验证文件路径"""
    import os
    
    if not path or not isinstance(path, str):
        raise FileOperationError("文件路径不能为空")
    
    abs_path = os.path.abspath(path)
    
    if must_exist and not os.path.exists(abs_path):
        raise FileOperationError(f"文件不存在: {abs_path}", file_path=abs_path)
    
    return abs_path


def validate_dataframe(df, min_rows: int = 1) -> bool:
    """验证DataFrame有效性"""
    import pandas as pd
    
    if df is None or not isinstance(df, pd.DataFrame):
        return False
    
    if len(df) < min_rows:
        return False
    
    return True


def ensure_directory_exists(directory: str):
    """确保目录存在"""
    import os
    from pathlib import Path
    
    if not directory:
        raise FileOperationError("目录路径不能为空")
    
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise FileOperationError(f"创建目录失败: {directory}", file_path=directory, operation="create_directory") from e


def create_safe_filename(filename: str, max_length: int = 255) -> str:
    """创建安全的文件名"""
    import re
    import os
    
    if not filename:
        raise FileOperationError("文件名不能为空")
    
    # 替换不安全字符
    unsafe_chars = '<>:"/\\|?*'
    safe_filename = filename
    for char in unsafe_chars:
        safe_filename = safe_filename.replace(char, '_')
    
    # 限制长度
    if len(safe_filename) > max_length:
        name, ext = os.path.splitext(safe_filename)
        safe_filename = name[:max_length - len(ext)] + ext
    
    return safe_filename
