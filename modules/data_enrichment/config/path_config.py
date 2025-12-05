# modules_new/config/path_config.py
"""
路径配置管理器
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass
import logging

from ..core.base import BaseConfig
from ..core.exceptions import ConfigurationError


@dataclass 
class PathConfig:
    """路径配置数据类"""
    input_csv: str = '危化品需补充.csv'
    output_csv: str = 'output_enriched.csv'
    output_folder: str = 'output_batches'
    backup_folder: str = 'backups'
    log_folder: str = 'logs'
    config_file: str = 'api_config.json'


class PathConfigManager(BaseConfig):
    """路径配置管理器"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._path_config = PathConfig()
    
    def get_path_config(self) -> PathConfig:
        """获取路径配置"""
        return self._path_config
    
    def update_paths(self, **kwargs):
        """更新路径配置"""
        for key, value in kwargs.items():
            if hasattr(self._path_config, key):
                setattr(self._path_config, key, value)
        
        self.logger.info(f"路径配置已更新: {kwargs}")
    
    def ensure_directories(self):
        """确保所有目录存在"""
        dirs = [
            self._path_config.output_folder,
            self._path_config.backup_folder, 
            self._path_config.log_folder
        ]
        
        for dir_path in dirs:
            try:
                os.makedirs(dir_path, exist_ok=True)
                self.logger.info(f"目录已准备: {dir_path}")
            except Exception as e:
                raise ConfigurationError(f"创建目录失败: {dir_path}, {e}")


# modules_new/config/column_mapper.py
"""
列名映射器
"""

from typing import Dict, List
import pandas as pd
import logging

from ..core.constants import COLUMN_MAPPINGS


class ColumnMapper:
    """列名映射器"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.column_mappings = COLUMN_MAPPINGS
    
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """映射DataFrame的列名"""
        mapped_df = df.copy()
        mapping_applied = {}
        
        for col in df.columns:
            col_lower = col.lower().strip()
            for standard_name, variants in self.column_mappings.items():
                if col_lower in [v.lower() for v in variants]:
                    mapping_applied[col] = standard_name
                    break
        
        if mapping_applied:
            mapped_df = mapped_df.rename(columns=mapping_applied)
            self.logger.info(f"列名映射已应用: {mapping_applied}")
        
        return mapped_df
    
    def get_standard_columns(self) -> List[str]:
        """获取标准列名列表"""
        return list(self.column_mappings.keys())
    
    def add_mapping(self, standard_name: str, variants: List[str]):
        """添加新的列名映射"""
        if standard_name not in self.column_mappings:
            self.column_mappings[standard_name] = []
        
        self.column_mappings[standard_name].extend(variants)
        self.logger.info(f"添加列名映射: {standard_name} -> {variants}")
