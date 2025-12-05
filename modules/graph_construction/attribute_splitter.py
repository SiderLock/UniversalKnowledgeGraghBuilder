"""
属性切分工具
用于将复杂的复合属性拆分为更精确的子属性
"""

import re
import pandas as pd
from typing import List, Dict, Any, Optional
from .text_normalizer import TextNormalizer


class AttributeSplitter:
    def __init__(self, normalizer: TextNormalizer):
        """初始化属性切分器"""
        self.normalizer = normalizer
    
    def split_aliases(self, alias_text: str) -> List[str]:
        """分割别名"""
        if not alias_text:
            return []
        
        return self.normalizer.split_list_string(alias_text)
    
    def split_storage_conditions(self, storage_text: str) -> Dict[str, List[str]]:
        """分割存储条件"""
        if not storage_text:
            return {}
        
        conditions = {
            '温度条件': [],
            '环境条件': [],
            '容器要求': [],
            '隔离要求': [],
            '其他要求': []
        }
        
        # 分割条件
        parts = self.normalizer.split_list_string(storage_text, [',', '，', ';', '；'])
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # 根据关键词分类
            if any(keyword in part for keyword in ['温度', '°C', '℃', '低温', '冷藏']):
                conditions['温度条件'].append(part)
            elif any(keyword in part for keyword in ['通风', '干燥', '避光', '阴凉']):
                conditions['环境条件'].append(part)
            elif any(keyword in part for keyword in ['密闭', '容器', '储存']):
                conditions['容器要求'].append(part)
            elif any(keyword in part for keyword in ['远离', '避免', '禁止', '分开']):
                conditions['隔离要求'].append(part)
            else:
                conditions['其他要求'].append(part)
        
        # 移除空分类
        return {k: v for k, v in conditions.items() if v}
    
    def split_solubility(self, solubility_text: str) -> Dict[str, Any]:
        """
        分割溶解性信息，区分定性和定量信息。
        """
        if not solubility_text:
            return {}
        
        solubility_data = {
            'qualitative': {}, # 定性描述
            'quantitative': []  # 定量数据
        }
        
        # 1. 提取定量数据
        quantitative_results = self.normalizer.extract_quantitative_solubility(solubility_text)
        if quantitative_results:
            solubility_data['quantitative'] = quantitative_results

        # 2. 提取定性描述
        # 使用分句来分割复杂的描述
        sentences = self.normalizer.split_list_string(solubility_text, [',', '，', ';', '；', '。'])
        
        # 常见溶剂和溶解性关键词
        solvent_keywords = ['水', '乙醇', '乙醚', '苯', '氯仿', '丙酮', '酸', '碱']
        solubility_keywords = ['溶于', '不溶于', '微溶于', '易溶于', '难溶于', '混溶', '任意比例混溶']

        for sentence in sentences:
            # 避免重复处理已提取的定量部分
            if any(char.isdigit() for char in sentence) and ('g/mL' in sentence or 'g/L' in sentence):
                continue

            found_solvent = None
            for solvent in solvent_keywords:
                if solvent in sentence:
                    found_solvent = solvent
                    break
            
            if found_solvent:
                found_solubility = '未知'
                for solubility in solubility_keywords:
                    if solubility in sentence:
                        found_solubility = solubility
                        break
                
                # 避免覆盖更具体的信息
                if found_solvent not in solubility_data['qualitative']:
                    solubility_data['qualitative'][found_solvent] = found_solubility

        return solubility_data

    def extract_physical_properties(self, row: pd.Series) -> Dict[str, Any]:
        """提取物理性质"""
        properties = {}
        
        # 提取温度相关属性
        if '熔点/凝固点' in row and pd.notna(row['熔点/凝固点']):
            temps = self.normalizer.extract_temperature(str(row['熔点/凝固点']))
            if temps:
                properties['熔点'] = temps
        
        if '沸点/沸程' in row and pd.notna(row['沸点/沸程']):
            temps = self.normalizer.extract_temperature(str(row['沸点/沸程']))
            if temps:
                properties['沸点'] = temps
        
        if '闪点' in row and pd.notna(row['闪点']):
            temps = self.normalizer.extract_temperature(str(row['闪点']))
            if temps:
                properties['闪点'] = temps
        
        if '自燃温度' in row and pd.notna(row['自燃温度']):
            temps = self.normalizer.extract_temperature(str(row['自燃温度']))
            if temps:
                properties['自燃温度'] = temps
        
        # 提取密度
        if '密度/相对密度' in row and pd.notna(row['密度/相对密度']):
            densities = self.normalizer.extract_density(str(row['密度/相对密度']))
            if densities:
                properties['密度'] = densities
        
        # 提取压力相关属性
        if '饱和蒸气压' in row and pd.notna(row['饱和蒸气压']):
            pressures = self.normalizer.extract_pressure(str(row['饱和蒸气压']))
            if pressures:
                properties['饱和蒸气压'] = pressures
        
        # 提取爆炸极限
        if '爆炸极限（LEL/UEL）' in row and pd.notna(row['爆炸极限（LEL/UEL）']):
            limits = self.normalizer.extract_explosion_limits(str(row['爆炸极限（LEL/UEL）']))
            if limits:
                properties['爆炸极限'] = limits
        
        return properties
    
    def extract_hazard_info(self, row: pd.Series) -> Dict[str, Any]:
        """提取危害信息"""
        hazards = {}
        
        # 物理危害
        if '物理危害' in row and pd.notna(row['物理危害']):
            physical_hazards = self.normalizer.split_list_string(str(row['物理危害']))
            if physical_hazards:
                hazards['物理危害'] = physical_hazards
        
        # 健康危害
        if '健康危害' in row and pd.notna(row['健康危害']):
            health_hazards = self.normalizer.categorize_hazards(str(row['健康危害']))
            if health_hazards:
                hazards['健康危害'] = health_hazards
        
        # 环境危害
        if '环境危害' in row and pd.notna(row['环境危害']):
            env_hazards = self.normalizer.split_list_string(str(row['环境危害']))
            if env_hazards:
                hazards['环境危害'] = env_hazards
        
        return hazards
    
    def process_row(self, row: pd.Series) -> Dict[str, Any]:
        """处理单行数据"""
        processed = {
            'basic_info': {},
            'aliases': [],
            'physical_properties': {},
            'storage_conditions': {},
            'solubility': {},
            'hazards': {}
        }
        
        # 基本信息
        basic_fields = ['品名', 'CAS号', '外观与性状', '室温状态']
        for field in basic_fields:
            if field in row and pd.notna(row[field]):
                normalized = self.normalizer.normalize_text(str(row[field]))
                if normalized:
                    processed['basic_info'][field] = normalized
        
        # 处理别名
        if '别名' in row and pd.notna(row['别名']):
            processed['aliases'] = self.split_aliases(str(row['别名']))
        
        # 处理物理性质
        processed['physical_properties'] = self.extract_physical_properties(row)
        
        # 处理存储条件
        if '建议存储条件' in row and pd.notna(row['建议存储条件']):
            processed['storage_conditions'] = self.split_storage_conditions(str(row['建议存储条件']))
        
        # 处理溶解性
        if '溶解性' in row and pd.notna(row['溶解性']):
            processed['solubility'] = self.split_solubility(str(row['溶解性']))
        
        # 处理危害信息
        processed['hazards'] = self.extract_hazard_info(row)
        
        return processed
    
    def split_compound_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """分割复合字段"""
        processed_data = []
        
        for _, row in df.iterrows():
            processed_row = self.process_row(row)
            processed_data.append(processed_row)
        
        return pd.DataFrame(processed_data)
