"""
主要数据处理类
集成所有数据预处理功能
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import os
import json
from .text_normalizer import TextNormalizer
from .attribute_splitter import AttributeSplitter


class DataProcessor:
    def __init__(self, config_path: Optional[str] = None):
        """初始化数据处理器"""
        self.config_path = config_path
        self.normalizer = TextNormalizer(config_path)
        self.splitter = AttributeSplitter(self.normalizer)
        self.processed_data = None
        self.original_data = None
    
    def load_data(self, file_path: str) -> pd.DataFrame:
        """加载数据文件，尝试多种编码格式"""
        try:
            if file_path.endswith('.csv'):
                encodings_to_try = ['utf-8', 'utf-8-sig', 'gb18030', 'gbk']
                df = None
                for encoding in encodings_to_try:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        print(f"成功使用 '{encoding}' 编码加载文件。")
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    raise ValueError(f"无法使用支持的编码格式 {encodings_to_try} 解码文件。")
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"不支持的文件格式: {file_path}")
            
            print(f"成功加载数据: {len(df)} 行, {len(df.columns)} 列")
            self.original_data = df.copy()
            return df
        
        except Exception as e:
            print(f"加载数据失败: {e}")
            raise
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗"""
        print("开始数据清洗...")
        
        # 移除完全空的行
        df = df.dropna(how='all')
        
        # 处理无效值
        invalid_values = ['N/A', '处理异常', '', 'null', 'NULL']
        df = df.replace(invalid_values, np.nan)
        
        # 标准化文本
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            df[col] = df[col].apply(lambda x: self.normalizer.normalize_text(str(x)) if pd.notna(x) else x)
        
        print(f"清洗后数据: {len(df)} 行")
        return df
    
    def detect_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """检测重复数据"""
        print("检测重复数据...")
        
        duplicates_info = {}
        
        # 检测完全重复的行
        full_duplicates = df.duplicated().sum()
        duplicates_info['完全重复行数'] = full_duplicates
        
        # 检测基于品名的重复
        if '品名' in df.columns:
            name_duplicates = df.duplicated(subset=['品名']).sum()
            duplicates_info['品名重复行数'] = name_duplicates
        
        # 检测基于CAS号的重复
        if 'CAS号' in df.columns:
            cas_duplicates = df.duplicated(subset=['CAS号']).sum()
            duplicates_info['CAS号重复行数'] = cas_duplicates
        
        # 使用模糊匹配检测相似品名
        if '品名' in df.columns:
            chemical_names = df['品名'].dropna().unique().tolist()
            similar_groups = self.find_similar_chemicals(chemical_names)
            duplicates_info['相似品名组数'] = len(similar_groups)
            duplicates_info['相似品名详情'] = similar_groups
        
        return duplicates_info
    
    def find_similar_chemicals(self, names: List[str], threshold: int = 85) -> List[List[str]]:
        """查找相似的化学品名称"""
        similar_groups = []
        processed = set()
        
        for name in names:
            if name in processed:
                continue
            
            similar = self.normalizer.find_similar_terms(name, names, threshold)
            if len(similar) > 1:
                similar_groups.append(similar)
                processed.update(similar)
        
        return similar_groups
    
    def standardize_chemicals(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化化学品名称"""
        print("标准化化学品名称...")
        
        if '品名' not in df.columns:
            return df
        
        # 获取所有化学品名称
        all_names = []
        for _, row in df.iterrows():
            # 确保品名是字符串
            if pd.notna(row['品名']):
                all_names.append(str(row['品名']))

            if '别名' in row and pd.notna(row['别名']):
                # 确保别名是字符串
                aliases = self.splitter.split_aliases(str(row['别名']))
                all_names.extend(aliases)
        
        # 标准化名称映射
        # 确保列表中的所有名称都是字符串
        all_names = [name for name in all_names if isinstance(name, str)]
        name_mapping = self.normalizer.standardize_chemical_names(list(set(all_names)))
        
        # 应用标准化
        df_copy = df.copy()
        # 在替换前，确保'品名'列是字符串类型
        df_copy['品名'] = df_copy['品名'].astype(str)
        for old_name, new_name in name_mapping.items():
            df_copy['品名'] = df_copy['品名'].replace(old_name, new_name)
        
        print(f"标准化了 {len(name_mapping)} 个化学品名称")
        return df_copy
    
    def process_attributes(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """处理所有属性"""
        print("处理复合属性...")
        
        processed_records = []
        
        for idx, row in df.iterrows():
            try:
                processed_row = self.splitter.process_row(row)
                processed_row['原始行号'] = idx
                processed_records.append(processed_row)
            except Exception as e:
                print(f"处理第 {idx} 行时出错: {e}")
                continue
        
        print(f"成功处理 {len(processed_records)} 条记录")
        return processed_records
    
    def generate_statistics(self, processed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成数据统计信息"""
        stats = {
            '总记录数': len(processed_data),
            '有别名的记录数': 0,
            '有物理性质的记录数': 0,
            '有存储条件的记录数': 0,
            '有溶解性信息的记录数': 0,
            '有危害信息的记录数': 0,
            '属性分布': {}
        }
        
        for record in processed_data:
            if record.get('aliases'):
                stats['有别名的记录数'] += 1
            if record.get('physical_properties'):
                stats['有物理性质的记录数'] += 1
            if record.get('storage_conditions'):
                stats['有存储条件的记录数'] += 1
            if record.get('solubility'):
                stats['有溶解性信息的记录数'] += 1
            if record.get('hazards'):
                stats['有危害信息的记录数'] += 1
        
        # 统计物理性质分布
        property_counts = {}
        for record in processed_data:
            for prop_name in record.get('physical_properties', {}).keys():
                property_counts[prop_name] = property_counts.get(prop_name, 0) + 1
        
        stats['属性分布'] = property_counts
        return stats
    
    def save_processed_data(self, processed_data: List[Dict[str, Any]], output_dir: str):
        """保存处理后的数据"""
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存完整的处理结果
        output_file = os.path.join(output_dir, 'processed_chemicals.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
        
        # 生成扁平化的CSV文件
        flattened_data = self.flatten_processed_data(processed_data)
        csv_file = os.path.join(output_dir, 'processed_chemicals.csv')
        flattened_df = pd.DataFrame(flattened_data)
        flattened_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        # 保存统计信息
        stats = self.generate_statistics(processed_data)
        stats_file = os.path.join(output_dir, 'processing_statistics.json')
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        print(f"处理结果已保存到: {output_dir}")
        return output_file, csv_file, stats_file
    
    def flatten_processed_data(self, processed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """扁平化处理后的数据，以适应新的详细结构"""
        flattened = []
        
        for record in processed_data:
            base_record = record.get('basic_info', {}).copy()
            base_record['原始行号'] = record.get('原始行号')
            
            # 添加别名（合并为字符串）
            if record.get('aliases'):
                base_record['别名_处理后'] = '; '.join(record['aliases'])
            
            # 添加物理性质
            for prop_name, prop_values in record.get('physical_properties', {}).items():
                if isinstance(prop_values, list):
                    # 处理字典列表，例如温度、压力等
                    for i, prop_data in enumerate(prop_values):
                        prefix = f'{prop_name}_{i+1}'
                        if isinstance(prop_data, dict):
                            for key, value in prop_data.items():
                                base_record[f'{prefix}_{key}'] = value
                elif isinstance(prop_values, dict):
                    # 处理单个字典，例如爆炸极限
                    for key, value in prop_values.items():
                        base_record[f'{prop_name}_{key}'] = value
            
            # 添加存储条件
            for condition_type, conditions in record.get('storage_conditions', {}).items():
                base_record[f'存储_{condition_type}'] = '; '.join(conditions)
            
            # 添加溶解性
            solubility_info = record.get('solubility', {})
            # 定性溶解性
            qual_sol = solubility_info.get('qualitative', {})
            if qual_sol:
                qual_str = '; '.join([f"{solvent}: {desc}" for solvent, desc in qual_sol.items()])
                base_record['溶解性_定性'] = qual_str
            
            # 定量溶解性
            quant_sol_list = solubility_info.get('quantitative', [])
            for i, quant_data in enumerate(quant_sol_list):
                prefix = f'溶解性_定量_{i+1}'
                if isinstance(quant_data, dict):
                    for key, value in quant_data.items():
                        base_record[f'{prefix}_{key}'] = value
            
            # 添加危害信息
            for hazard_type, hazards in record.get('hazards', {}).items():
                if isinstance(hazards, list):
                    base_record[f'危害_{hazard_type}'] = '; '.join(hazards)
                elif isinstance(hazards, dict):
                    for sub_type, sub_hazards in hazards.items():
                        base_record[f'危害_{hazard_type}_{sub_type}'] = '; '.join(sub_hazards)
            
            flattened.append(base_record)
        
        return flattened
    
    def process_complete_pipeline(self, input_file: str, output_dir: str) -> Dict[str, Any]:
        """完整的数据处理流水线"""
        print("开始完整数据处理流水线...")
        
        # 1. 加载数据
        df = self.load_data(input_file)
        
        # 2. 数据清洗
        df_cleaned = self.clean_data(df)
        
        # 3. 检测重复
        duplicates_info = self.detect_duplicates(df_cleaned)
        print(f"重复检测结果: {duplicates_info}")
        
        # 4. 标准化化学品名称
        df_standardized = self.standardize_chemicals(df_cleaned)
        
        # 5. 处理属性
        processed_data = self.process_attributes(df_standardized)
        
        # 6. 保存结果
        output_files = self.save_processed_data(processed_data, output_dir)
        
        # 7. 生成统计
        stats = self.generate_statistics(processed_data)
        
        self.processed_data = processed_data
        
        result = {
            'original_records': len(df),
            'processed_records': len(processed_data),
            'duplicates_info': duplicates_info,
            'statistics': stats,
            'output_files': output_files
        }
        
        print("数据处理完成!")
        return result
