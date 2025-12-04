# modules_new/config/column_mapper.py
"""
列名映射器

负责CSV文件列名的标准化映射
"""

from typing import Dict, List
import pandas as pd
import logging

from ..core.constants import COLUMN_MAPPINGS


class ColumnMapper:
    """列名映射器"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.column_mappings = COLUMN_MAPPINGS.copy()
    
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
    
    def get_mapping_suggestions(self, columns: List[str]) -> Dict[str, str]:
        """获取列名映射建议"""
        suggestions = {}
        
        for col in columns:
            col_lower = col.lower().strip()
            for standard_name, variants in self.column_mappings.items():
                # 模糊匹配
                for variant in variants:
                    if variant.lower() in col_lower or col_lower in variant.lower():
                        suggestions[col] = standard_name
                        break
                if col in suggestions:
                    break
        
        return suggestions
    
    def validate_required_columns(self, df: pd.DataFrame) -> Dict[str, bool]:
        """验证必需列是否存在"""
        required_columns = ['化学品名称', 'CAS号']  # 核心必需列
        validation_result = {}
        
        for req_col in required_columns:
            validation_result[req_col] = req_col in df.columns
        
        return validation_result
