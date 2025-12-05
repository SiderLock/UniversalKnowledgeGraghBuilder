"""
通用数据清洗工具包

一个用于处理实体数据并与参考数据进行比对的Python工具。
"""

__version__ = "2.1.0"
__author__ = "User"
__email__ = "user@example.com"

from .data_processor import process_data


from .data_processor import main, process_data, process_all_files

__all__ = ["main", "process_data", "process_all_files"]
