"""
化学品数据处理工具包

一个用于处理化学品数据并与国家危化品清单进行比对的Python工具。
"""

__version__ = "2.1.0"
__author__ = "User"
__email__ = "user@example.com"

from .process_chemicals import process_data


from .process_chemicals import main, process_data, process_all_files

__all__ = ["main", "process_data", "process_all_files"]
