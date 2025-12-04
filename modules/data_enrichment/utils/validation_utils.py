# modules_new/utils/validation_utils.py
"""
验证工具类

提供各种数据验证的实用工具
"""

import re
import pandas as pd
from typing import Any, List, Dict, Optional, Union
import logging


class ValidationUtils:
    """验证工具类"""
    
    # 验证模式
    CAS_PATTERN = re.compile(r'^\d{2,7}-\d{2}-\d$')
    MOLECULAR_FORMULA_PATTERN = re.compile(r'^[A-Z][a-z]?(\d*[A-Z][a-z]?\d*)*$')
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    @staticmethod
    def is_valid_cas_number(cas: Any) -> bool:
        """验证CAS号格式"""
        if pd.isna(cas) or not isinstance(cas, str):
            return False
        
        cas = cas.strip()
        return bool(ValidationUtils.CAS_PATTERN.match(cas))
    
    @staticmethod
    def is_valid_cas(cas: Any) -> bool:
        """验证CAS号格式（简化方法名）"""
        return ValidationUtils.is_valid_cas_number(cas)
    
    @staticmethod
    def is_valid_molecular_formula(formula: Any) -> bool:
        """验证分子式格式"""
        if pd.isna(formula) or not isinstance(formula, str):
            return False
        
        formula = formula.strip()
        return bool(ValidationUtils.MOLECULAR_FORMULA_PATTERN.match(formula))
    
    @staticmethod
    def is_valid_chemical_name(name: Any, min_length: int = 2, max_length: int = 100) -> bool:
        """验证化学品名称"""
        if pd.isna(name) or not isinstance(name, str):
            return False
        
        name = name.strip()
        
        # 长度检查
        if len(name) < min_length or len(name) > max_length:
            return False
        
        # 不能是纯数字
        if name.isdigit():
            return False
        
        # 不能为空或只有空格
        if not name or name.isspace():
            return False
        
        return True
    
    @staticmethod
    def is_valid_number(value: Any, min_val: Optional[float] = None, max_val: Optional[float] = None) -> bool:
        """验证数字"""
        try:
            num = float(value)
            
            # 检查范围
            if min_val is not None and num < min_val:
                return False
            if max_val is not None and num > max_val:
                return False
            
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def is_valid_email(email: Any) -> bool:
        """验证电子邮件格式"""
        if pd.isna(email) or not isinstance(email, str):
            return False
        
        return bool(ValidationUtils.EMAIL_PATTERN.match(email.strip()))
    
    @staticmethod
    def is_valid_url(url: Any) -> bool:
        """验证URL格式"""
        if pd.isna(url) or not isinstance(url, str):
            return False
        
        url = url.strip()
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return bool(url_pattern.match(url))
    
    @staticmethod
    def validate_dataframe_schema(df: pd.DataFrame, required_columns: List[str], 
                                optional_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """验证DataFrame结构"""
        validation_result = {
            'is_valid': True,
            'missing_required': [],
            'unexpected_columns': [],
            'total_columns': len(df.columns),
            'total_rows': len(df)
        }
        
        df_columns = set(df.columns)
        required_set = set(required_columns)
        optional_set = set(optional_columns) if optional_columns else set()
        expected_set = required_set | optional_set
        
        # 检查缺失的必需列
        missing_required = required_set - df_columns
        if missing_required:
            validation_result['is_valid'] = False
            validation_result['missing_required'] = list(missing_required)
        
        # 检查意外的列
        unexpected = df_columns - expected_set
        if unexpected:
            validation_result['unexpected_columns'] = list(unexpected)
        
        return validation_result
    
    @staticmethod
    def validate_data_types(df: pd.DataFrame, type_constraints: Dict[str, type]) -> Dict[str, Any]:
        """验证数据类型"""
        validation_result = {
            'is_valid': True,
            'type_errors': {},
            'conversion_suggestions': {}
        }
        
        for column, expected_type in type_constraints.items():
            if column not in df.columns:
                continue
            
            series = df[column].dropna()
            if len(series) == 0:
                continue
            
            # 检查类型兼容性
            if expected_type in [int, float]:
                try:
                    pd.to_numeric(series, errors='raise')
                except (ValueError, TypeError):
                    validation_result['is_valid'] = False
                    validation_result['type_errors'][column] = f"无法转换为{expected_type.__name__}"
                    validation_result['conversion_suggestions'][column] = "检查数据中的非数值内容"
            
            elif expected_type == str:
                # 字符串类型较为宽松，大多数数据都可以转换
                pass
            
            elif expected_type == bool:
                # 检查布尔值
                bool_values = series.astype(str).str.lower()
                valid_bool = bool_values.isin(['true', 'false', '1', '0', 'yes', 'no', 'y', 'n'])
                if not valid_bool.all():
                    validation_result['is_valid'] = False
                    validation_result['type_errors'][column] = "包含无效的布尔值"
        
        return validation_result
    
    @staticmethod
    def validate_data_range(df: pd.DataFrame, range_constraints: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
        """验证数据范围"""
        validation_result = {
            'is_valid': True,
            'range_errors': {},
            'out_of_range_count': 0
        }
        
        for column, constraints in range_constraints.items():
            if column not in df.columns:
                continue
            
            series = pd.to_numeric(df[column], errors='coerce').dropna()
            if len(series) == 0:
                continue
            
            min_val = constraints.get('min')
            max_val = constraints.get('max')
            
            violations = []
            
            if min_val is not None:
                below_min = series < min_val
                if below_min.any():
                    violations.append(f"{below_min.sum()} 个值小于最小值 {min_val}")
            
            if max_val is not None:
                above_max = series > max_val
                if above_max.any():
                    violations.append(f"{above_max.sum()} 个值大于最大值 {max_val}")
            
            if violations:
                validation_result['is_valid'] = False
                validation_result['range_errors'][column] = violations
                validation_result['out_of_range_count'] += len(violations)
        
        return validation_result
    
    @staticmethod
    def validate_uniqueness(df: pd.DataFrame, unique_columns: List[str]) -> Dict[str, Any]:
        """验证唯一性约束"""
        validation_result = {
            'is_valid': True,
            'duplicate_errors': {},
            'total_duplicates': 0
        }
        
        for column in unique_columns:
            if column not in df.columns:
                continue
            
            series = df[column].dropna()
            duplicates = series.duplicated()
            
            if duplicates.any():
                validation_result['is_valid'] = False
                duplicate_count = duplicates.sum()
                validation_result['duplicate_errors'][column] = {
                    'count': duplicate_count,
                    'duplicate_values': series[duplicates].unique().tolist()[:10]  # 只显示前10个
                }
                validation_result['total_duplicates'] += duplicate_count
        
        return validation_result
    
    @staticmethod
    def comprehensive_validation(df: pd.DataFrame, validation_config: Dict[str, Any]) -> Dict[str, Any]:
        """综合验证"""
        result = {
            'overall_valid': True,
            'validations': {},
            'summary': {
                'total_errors': 0,
                'total_warnings': 0,
                'validation_score': 0.0
            }
        }
        
        # 结构验证
        if 'schema' in validation_config:
            schema_result = ValidationUtils.validate_dataframe_schema(
                df, 
                validation_config['schema'].get('required', []),
                validation_config['schema'].get('optional', [])
            )
            result['validations']['schema'] = schema_result
            if not schema_result['is_valid']:
                result['overall_valid'] = False
                result['summary']['total_errors'] += 1
        
        # 类型验证
        if 'types' in validation_config:
            type_result = ValidationUtils.validate_data_types(df, validation_config['types'])
            result['validations']['types'] = type_result
            if not type_result['is_valid']:
                result['overall_valid'] = False
                result['summary']['total_errors'] += len(type_result['type_errors'])
        
        # 范围验证
        if 'ranges' in validation_config:
            range_result = ValidationUtils.validate_data_range(df, validation_config['ranges'])
            result['validations']['ranges'] = range_result
            if not range_result['is_valid']:
                result['overall_valid'] = False
                result['summary']['total_warnings'] += range_result['out_of_range_count']
        
        # 唯一性验证
        if 'unique' in validation_config:
            unique_result = ValidationUtils.validate_uniqueness(df, validation_config['unique'])
            result['validations']['unique'] = unique_result
            if not unique_result['is_valid']:
                result['overall_valid'] = False
                result['summary']['total_errors'] += unique_result['total_duplicates']
        
        # 计算验证得分
        total_checks = len(result['validations'])
        passed_checks = sum(1 for v in result['validations'].values() if v.get('is_valid', False))
        result['summary']['validation_score'] = (passed_checks / total_checks) * 100 if total_checks > 0 else 100
        
        return result
    
    @staticmethod
    def validate_and_fix_cas_number(cas: Any) -> tuple[bool, str, str]:
        """
        验证并尝试修复CAS号格式
        
        Args:
            cas: 待验证的CAS号
            
        Returns:
            tuple: (是否有效, 修复后的CAS号, 错误信息)
        """
        if pd.isna(cas) or not cas:
            return False, "", "CAS号为空"
        
        cas_str = str(cas).strip()
        
        # 移除可能的前后缀
        cas_str = cas_str.replace("CAS:", "").replace("CAS ", "").replace("cas:", "").replace("cas ", "")
        cas_str = cas_str.strip()
        
        # 基本格式检查
        if not ValidationUtils.CAS_PATTERN.match(cas_str):
            # 尝试修复常见的格式问题
            # 移除多余的空格和特殊字符
            cas_clean = re.sub(r'[^\d-]', '', cas_str)
            
            # 检查是否是纯数字，尝试添加分隔符
            if cas_clean.isdigit() and len(cas_clean) >= 5:
                # 尝试按照CAS号规则添加分隔符
                if len(cas_clean) == 5:  # 最短CAS号 XX-XX-X
                    cas_clean = f"{cas_clean[:2]}-{cas_clean[2:4]}-{cas_clean[4]}"
                elif len(cas_clean) >= 6:
                    # 标准格式：最后一位是检验位，倒数第二、三位用-分隔
                    cas_clean = f"{cas_clean[:-3]}-{cas_clean[-3:-1]}-{cas_clean[-1]}"
            
            # 再次验证修复后的格式
            if ValidationUtils.CAS_PATTERN.match(cas_clean):
                return True, cas_clean, "格式已修复"
            else:
                return False, cas_str, f"CAS号格式不正确: {cas_str}"
        
        # 验证校验位（CAS号最后一位是校验位）
        if ValidationUtils._validate_cas_checksum(cas_str):
            return True, cas_str, "格式正确"
        else:
            return False, cas_str, "CAS号校验位错误"
    
    @staticmethod
    def _validate_cas_checksum(cas: str) -> bool:
        """
        验证CAS号的校验位
        
        CAS号校验规则：
        1. 从右往左第二位开始，每位数字乘以其位置权重
        2. 权重从1开始递增
        3. 所有乘积之和模10应等于校验位
        """
        try:
            # 移除分隔符
            digits = cas.replace('-', '')
            if not digits.isdigit():
                return False
            
            # 校验位是最后一位
            check_digit = int(digits[-1])
            
            # 计算校验和
            checksum = 0
            weight = 1
            
            # 从倒数第二位开始，向前计算
            for i in range(len(digits) - 2, -1, -1):
                checksum += int(digits[i]) * weight
                weight += 1
            
            # 校验和模10应等于校验位
            return (checksum % 10) == check_digit
            
        except (ValueError, IndexError):
            return False
    
    @staticmethod
    def extract_cas_numbers_from_text(text: str) -> List[str]:
        """
        从文本中提取所有可能的CAS号
        
        Args:
            text: 待搜索的文本
            
        Returns:
            找到的CAS号列表
        """
        if not text:
            return []
        
        # 使用正则表达式查找所有CAS号模式
        cas_pattern = r'\b\d{2,7}-\d{2}-\d\b'
        potential_cas = re.findall(cas_pattern, text)
        
        # 验证每个找到的CAS号
        valid_cas = []
        for cas in potential_cas:
            if ValidationUtils._validate_cas_checksum(cas):
                valid_cas.append(cas)
        
        return valid_cas
