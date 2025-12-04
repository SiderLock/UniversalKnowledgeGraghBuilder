# modules_new/utils/cas_query.py
"""
CAS号查询工具

专门用于查询和补充缺失的CAS注册号
"""

import re
import json
import logging
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple
import time

from ..core.exceptions import with_error_handling
from .validation_utils import ValidationUtils


class CASQueryEngine:
    """CAS号查询引擎"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.query_cache = {}  # 简单缓存避免重复查询
    
    @with_error_handling()
    def query_cas_number(self, chemical_name: str, use_grounding: bool = True) -> Optional[str]:
        """
        根据化学品名称查询CAS号
        
        Args:
            chemical_name: 化学品名称
            use_grounding: 是否使用联网搜索
            
        Returns:
            查询到的CAS号，如果未找到返回None
        """
        if not chemical_name or not chemical_name.strip():
            return None
        
        chemical_name = chemical_name.strip()
        
        # 检查缓存
        if chemical_name in self.query_cache:
            self.logger.info(f"从缓存获取CAS号: {chemical_name} -> {self.query_cache[chemical_name]}")
            return self.query_cache[chemical_name]
        
        # 首先尝试从常见化学品数据库查询
        cas_number = self._query_from_common_database(chemical_name)
        
        if not cas_number and use_grounding:
            # 使用联网搜索查询
            cas_number = self._query_from_grounding_search(chemical_name)
        
        # 验证查询到的CAS号
        if cas_number:
            is_valid, fixed_cas, _ = ValidationUtils.validate_and_fix_cas_number(cas_number)
            if is_valid:
                cas_number = fixed_cas
                # 缓存查询结果
                self.query_cache[chemical_name] = cas_number
                self.logger.info(f"成功查询到CAS号: {chemical_name} -> {cas_number}")
                return cas_number
            else:
                self.logger.warning(f"查询到的CAS号格式无效: {cas_number}")
        
        self.logger.info(f"未找到有效的CAS号: {chemical_name}")
        return None
    
    def _query_from_common_database(self, chemical_name: str) -> Optional[str]:
        """
        从常见化学品数据库查询CAS号
        
        这里包含一些常见化学品的CAS号映射
        """
        # 常见化学品CAS号数据库（可以扩展）
        common_chemicals = {
            # 基础有机化合物
            "水": "7732-18-5",
            "乙醇": "64-17-5", "酒精": "64-17-5", "无水乙醇": "64-17-5",
            "甲醇": "67-56-1", "木醇": "67-56-1",
            "丙酮": "67-64-1", "二甲基酮": "67-64-1",
            "苯": "71-43-2",
            "甲苯": "108-88-3", "甲基苯": "108-88-3",
            "二甲苯": "1330-20-7", "dimethylbenzene": "1330-20-7",
            "氯仿": "67-66-3", "三氯甲烷": "67-66-3",
            "四氯化碳": "56-23-5", "四氯甲烷": "56-23-5",
            
            # 无机化合物
            "盐酸": "7647-01-0", "氯化氢": "7647-01-0",
            "硫酸": "7664-93-9",
            "硝酸": "7697-37-2",
            "氢氧化钠": "1310-73-2", "烧碱": "1310-73-2", "苛性钠": "1310-73-2",
            "氯化钠": "7647-14-5", "食盐": "7647-14-5",
            "碳酸钠": "497-19-8", "纯碱": "497-19-8",
            "氨": "7664-41-7", "液氨": "7664-41-7", "氨气": "7664-41-7",
            
            # 有机溶剂
            "乙酸乙酯": "141-78-6", "醋酸乙酯": "141-78-6",
            "二氯甲烷": "75-09-2", "亚甲基氯": "75-09-2",
            "乙醚": "60-29-7", "二乙醚": "60-29-7",
            "丁酮": "78-93-3", "甲基乙基酮": "78-93-3",
            "异丙醇": "67-63-0", "2-丙醇": "67-63-0",
            "正己烷": "110-54-3", "己烷": "110-54-3",
            
            # 酸碱化合物
            "乙酸": "64-19-7", "醋酸": "64-19-7", "冰醋酸": "64-19-7",
            "甲酸": "64-18-6", "蚁酸": "64-18-6",
            "草酸": "144-62-7", "乙二酸": "144-62-7",
            "氢氟酸": "7664-39-3", "氟化氢": "7664-39-3",
            
            # 危险化学品
            "苯胺": "62-53-3", "氨基苯": "62-53-3",
            "甲醛": "50-00-0", "福尔马林": "50-00-0",
            "苯酚": "108-95-2", "石炭酸": "108-95-2",
            "氰化钠": "143-33-9",
            "氰化钾": "151-50-8",
        }
        
        # 标准化化学品名称进行匹配
        name_normalized = chemical_name.lower().strip()
        
        # 精确匹配
        for key, cas in common_chemicals.items():
            if key.lower() == name_normalized:
                self.logger.info(f"从常见化学品数据库找到CAS号: {chemical_name} -> {cas}")
                return cas
        
        # 模糊匹配（包含关系）
        for key, cas in common_chemicals.items():
            if key.lower() in name_normalized or name_normalized in key.lower():
                self.logger.info(f"通过模糊匹配找到CAS号: {chemical_name} -> {cas} (匹配: {key})")
                return cas
        
        return None
    
    def _query_from_grounding_search(self, chemical_name: str) -> Optional[str]:
        """
        使用联网搜索查询CAS号
        """
        try:
            from ..api.gemini_grounding import get_chemical_properties_with_grounding
            
            # 专门针对CAS号查询的提示词
            cas_query_prompt = f"""
🔍 专业CAS号查询任务

**目标**: 查询化学品 "{chemical_name}" 的准确CAS注册号

**数据源优先级**:
1. 🌐 PubChem (美国国家生物技术信息中心)
2. 🏛️ ECHA (欧洲化学品管理局)
3. 🔬 GESTIS国际化学品数据库
4. 🏭 Sigma-Aldrich、Fisher Scientific、Merck等供应商数据库

**查询要求**:
• 请搜索权威化学数据库中 "{chemical_name}" 的CAS注册号
• 如果有多个CAS号，请提供最常用的主要CAS号
• 确保CAS号格式正确 (XXXX-XX-X 或 XXXXX-XX-X 或更长)
• 优先从官方机构数据库获取

**输出格式** (严格按照以下JSON格式):
```json
{{
    "chemical_name": "{chemical_name}",
    "cas_number": "XXXX-XX-X",
    "source": "数据来源(如PubChem, ECHA等)",
    "confidence": "high/medium/low",
    "alternative_names": ["别名1", "别名2"],
    "notes": "补充说明"
}}
```

**如果未找到CAS号，请输出**:
```json
{{
    "chemical_name": "{chemical_name}",
    "cas_number": null,
    "source": "searched but not found",
    "confidence": "not_found",
    "alternative_names": [],
    "notes": "在权威数据库中未找到对应的CAS号"
}}
```

现在开始联网搜索CAS号:
"""
            
            self.logger.info(f"正在联网搜索CAS号: {chemical_name}")
            response = get_chemical_properties_with_grounding(cas_query_prompt)
            
            if response:
                # 尝试从响应中提取CAS号
                cas_number = self._extract_cas_from_response(response, chemical_name)
                if cas_number:
                    self.logger.info(f"联网搜索成功找到CAS号: {chemical_name} -> {cas_number}")
                    return cas_number
                else:
                    self.logger.warning(f"联网搜索响应中未找到有效CAS号: {chemical_name}")
            else:
                self.logger.warning(f"联网搜索无响应: {chemical_name}")
                
        except Exception as e:
            self.logger.error(f"联网搜索CAS号时发生错误: {e}")
        
        return None
    
    def _extract_cas_from_response(self, response: str, chemical_name: str) -> Optional[str]:
        """
        从API响应中提取CAS号
        """
        if not response:
            return None
        
        try:
            # 尝试解析JSON响应
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
            if json_match:
                json_data = json.loads(json_match.group(1))
                cas_number = json_data.get('cas_number')
                if cas_number and cas_number != "null":
                    return cas_number
            
            # 如果JSON解析失败，使用正则表达式查找CAS号
            cas_numbers = ValidationUtils.extract_cas_numbers_from_text(response)
            if cas_numbers:
                # 返回第一个找到的有效CAS号
                return cas_numbers[0]
            
        except json.JSONDecodeError:
            self.logger.warning("JSON解析失败，尝试正则表达式提取CAS号")
            # 使用正则表达式查找CAS号
            cas_numbers = ValidationUtils.extract_cas_numbers_from_text(response)
            if cas_numbers:
                return cas_numbers[0]
        
        return None
    
    def batch_query_missing_cas(self, df, name_column: str = 'chemical_name', 
                               cas_column: str = 'cas_number') -> Dict[str, str]:
        """
        批量查询缺失的CAS号
        
        Args:
            df: 数据框
            name_column: 化学品名称列名
            cas_column: CAS号列名
            
        Returns:
            字典，键为化学品名称，值为查询到的CAS号
        """
        missing_cas = {}
        
        for idx, row in df.iterrows():
            chemical_name = row.get(name_column, '')
            cas_number = row.get(cas_column, '')
            
            # 检查是否缺少CAS号或CAS号无效
            if not cas_number or pd.isna(cas_number) or not ValidationUtils.is_valid_cas_number(cas_number):
                if chemical_name and not pd.isna(chemical_name):
                    self.logger.info(f"查询缺失的CAS号: {chemical_name}")
                    queried_cas = self.query_cas_number(chemical_name)
                    if queried_cas:
                        missing_cas[chemical_name] = queried_cas
                    
                    # 添加延迟避免API限制
                    time.sleep(0.5)
        
        self.logger.info(f"批量CAS号查询完成，找到 {len(missing_cas)} 个CAS号")
        return missing_cas


# 全局CAS查询引擎实例
_cas_query_engine = None


def get_cas_query_engine() -> CASQueryEngine:
    """获取CAS查询引擎实例"""
    global _cas_query_engine
    if _cas_query_engine is None:
        _cas_query_engine = CASQueryEngine()
    return _cas_query_engine


def query_cas_for_chemical(chemical_name: str) -> Optional[str]:
    """
    为化学品查询CAS号的便捷函数
    
    Args:
        chemical_name: 化学品名称
        
    Returns:
        查询到的CAS号
    """
    engine = get_cas_query_engine()
    return engine.query_cas_number(chemical_name)
