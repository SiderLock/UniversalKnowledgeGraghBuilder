#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
化学品数据处理主模块
用于合并、清洗化学品数据并为Neo4j图数据库做准备
实现阶段一：数据合并与预处理，阶段二：图数据格式化与关系提取
"""

import os
import pandas as pd
import numpy as np
import re
from pathlib import Path
import chardet
from typing import List, Dict, Tuple, Optional, Set
import logging
from datetime import datetime
import hashlib
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chemical_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ChemicalProcessor:
    """化学品数据处理器 - 实现完整的数据处理流程"""
    
    def __init__(self, base_path: str):
        """
        初始化数据处理器
        
        Args:
            base_path: 工作目录基础路径
        """
        self.base_path = Path(base_path)
        self.input_dir = self.base_path / "已补全文件"
        self.dangerous_dir = self.base_path / "危化品目录"
        self.success_dir = self.base_path / "处理成功"
        self.failed_dir = self.base_path / "处理失败"
        
        # 创建输出目录
        self.success_dir.mkdir(exist_ok=True)
        self.failed_dir.mkdir(exist_ok=True)
        
        # 创建增量更新相关目录
        self.incremental_dir = self.base_path / "增量更新"
        self.metadata_dir = self.base_path / "元数据"
        self.incremental_dir.mkdir(exist_ok=True)
        self.metadata_dir.mkdir(exist_ok=True)
        
        # 存储危化品目录数据
        self.dangerous_chemicals = None
        
        # 增量更新状态管理
        self.processed_files_record = self.metadata_dir / "processed_files.json"
        self.data_fingerprint_record = self.metadata_dir / "data_fingerprints.json"
        
        # 缓存化学品名称，避免在关系提取时重复加载文件
        self._cached_chemical_names = None
        
        logger.info(f"初始化化学品数据处理器")
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"危化品目录: {self.dangerous_dir}")
        logger.info(f"成功输出目录: {self.success_dir}")
        logger.info(f"失败输出目录: {self.failed_dir}")
        logger.info(f"增量更新目录: {self.incremental_dir}")
        logger.info(f"元数据目录: {self.metadata_dir}")

    def detect_encoding(self, file_path: Path) -> str:
        """检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except:
            return 'utf-8'

    def load_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """安全加载CSV文件"""
        try:
            encoding = self.detect_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"成功加载文件: {file_path.name}, 共{len(df)}条记录")
            return df
        except Exception as e:
            logger.error(f"加载文件失败 {file_path.name}: {e}")
            return None

    def load_dangerous_chemicals(self) -> pd.DataFrame:
        """加载危化品目录数据"""
        try:
            dangerous_file = self.dangerous_dir / "危化品需补充_enriched.csv"
            if dangerous_file.exists():
                self.dangerous_chemicals = self.load_csv_file(dangerous_file)
                if self.dangerous_chemicals is not None:
                    logger.info(f"成功加载危化品目录，共{len(self.dangerous_chemicals)}条记录")
                    return self.dangerous_chemicals
                else:
                    logger.warning("危化品目录文件加载失败")
                    return pd.DataFrame()
            else:
                logger.warning("危化品目录文件不存在")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"加载危化品目录失败: {e}")
            return pd.DataFrame()

    def calculate_file_fingerprint(self, file_path: Path) -> str:
        """计算文件指纹用于增量更新检测"""
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                return hashlib.md5(content).hexdigest()
        except Exception as e:
            logger.error(f"计算文件指纹失败 {file_path}: {e}")
            return ""

    def load_processed_files_record(self) -> Dict[str, str]:
        """加载已处理文件记录"""
        try:
            if self.processed_files_record.exists():
                with open(self.processed_files_record, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"加载已处理文件记录失败: {e}")
        return {}

    def save_processed_files_record(self, record: Dict[str, str]):
        """保存已处理文件记录"""
        try:
            with open(self.processed_files_record, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存已处理文件记录失败: {e}")

    def get_new_files(self) -> List[Path]:
        """获取需要增量处理的新文件"""
        processed_record = self.load_processed_files_record()
        new_files = []
        
        csv_files = list(self.input_dir.glob("*.csv"))
        
        for file_path in csv_files:
            current_fingerprint = self.calculate_file_fingerprint(file_path)
            file_key = str(file_path.relative_to(self.base_path))
            
            # 检查文件是否是新的或已修改
            if (file_key not in processed_record or 
                processed_record[file_key] != current_fingerprint):
                new_files.append(file_path)
                
        logger.info(f"发现 {len(new_files)} 个新文件或已修改文件需要处理")
        return new_files

    def validate_chemical_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """数据验证规则 - 增加更严格的数据质量检查"""
        if df.empty:
            return df, pd.DataFrame(), []

        logger.info("开始执行数据验证规则...")
        validation_errors = []
        failed_records = []
        
        # 验证规则1: 中文名称必须存在且为有效中文
        chinese_name_pattern = r'^[\u4e00-\u9fa5\u3400-\u4dbf\u20000-\u2a6df\u2a700-\u2b73f\u2b740-\u2b81f\u2ceb0-\u2ebef\uff00-\uffef]+'
        name_invalid = df['中文名称'].isna() | (df['中文名称'].astype(str).str.strip() == '') | (~df['中文名称'].astype(str).str.match(chinese_name_pattern))
        
        if name_invalid.any():
            invalid_names = df[name_invalid].copy()
            invalid_names['validation_error'] = '中文名称无效或缺失'
            failed_records.append(invalid_names)
            validation_errors.append(f"中文名称验证失败: {name_invalid.sum()} 条记录")
        
        # 验证规则2: CAS号格式验证
        cas_pattern = r'^\d{1,7}-\d{2}-\d$'
        cas_invalid = df['CAS号或流水号'].notna() & (~df['CAS号或流水号'].astype(str).str.match(cas_pattern)) & (df['CAS号或流水号'].astype(str).str.strip() != '')
        
        if cas_invalid.any():
            invalid_cas = df[cas_invalid].copy()
            invalid_cas['validation_error'] = 'CAS号格式无效'
            failed_records.append(invalid_cas)
            validation_errors.append(f"CAS号格式验证失败: {cas_invalid.sum()} 条记录")
        
        # 验证规则3: 分子式格式验证
        molecular_formula_pattern = r'^[A-Za-z0-9\(\)\[\]\.·\-\+]+$'
        formula_invalid = df['分子式'].notna() & (~df['分子式'].astype(str).str.match(molecular_formula_pattern)) & (df['分子式'].astype(str).str.strip() != '')
        
        if formula_invalid.any():
            invalid_formula = df[formula_invalid].copy()
            invalid_formula['validation_error'] = '分子式格式无效'
            failed_records.append(invalid_formula)
            validation_errors.append(f"分子式格式验证失败: {formula_invalid.sum()} 条记录")
        
        # 验证规则4: 危化品必须有危害信息
        dangerous_missing_hazard = (df['是否为危化品'].astype(str).str.strip() == '是') & (df['危害'].isna() | (df['危害'].astype(str).str.strip() == ''))
        
        if dangerous_missing_hazard.any():
            missing_hazard = df[dangerous_missing_hazard].copy()
            missing_hazard['validation_error'] = '危化品缺少危害信息'
            failed_records.append(missing_hazard)
            validation_errors.append(f"危化品危害信息验证失败: {dangerous_missing_hazard.sum()} 条记录")
        
        # 验证规则5: 数据完整性检查
        required_fields = ['中文名称', 'CAS号或流水号']
        for field in required_fields:
            if field in df.columns:
                field_missing = df[field].isna() | (df[field].astype(str).str.strip() == '')
                if field_missing.any():
                    validation_errors.append(f"必填字段 {field} 缺失: {field_missing.sum()} 条记录")
        
        # 验证规则6: 重复记录检测
        duplicate_mask = df.duplicated(subset=['中文名称', 'CAS号或流水号'], keep='first')
        if duplicate_mask.any():
            duplicate_records = df[duplicate_mask].copy()
            duplicate_records['validation_error'] = '重复记录'
            failed_records.append(duplicate_records)
            validation_errors.append(f"发现重复记录: {duplicate_mask.sum()} 条")
        
        # 合并所有验证失败的记录
        all_failed_mask = name_invalid | cas_invalid | formula_invalid | dangerous_missing_hazard | duplicate_mask
        valid_data = df[~all_failed_mask].copy()
        
        if failed_records:
            failed_df = pd.concat(failed_records, ignore_index=True)
        else:
            failed_df = pd.DataFrame()
        
        logger.info(f"数据验证完成: 有效记录 {len(valid_data)} 条, 失败记录 {len(failed_df)} 条")
        
        return valid_data, failed_df, validation_errors

    def get_known_chemical_names(self) -> Set[str]:
        """获取已知的化学品名称库"""
        known_names = set()
        
        # 从危化品目录获取
        if self.dangerous_chemicals is not None:
            if '品名' in self.dangerous_chemicals.columns:
                known_names.update(self.dangerous_chemicals['品名'].dropna().astype(str))
        
        # 从已处理的数据获取
        success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
        for file_path in success_files:
            df = self.load_csv_file(file_path)
            if df is not None and '中文名称' in df.columns:
                known_names.update(df['中文名称'].dropna().astype(str))
        
        # 常见化学品名称库
        common_chemicals = {
            '硫酸', '盐酸', '硝酸', '氢氧化钠', '氢氧化钾', '氨水', '乙醇', '甲醇',
            '丙酮', '甲苯', '苯', '乙醚', '氯仿', '四氯化碳', '乙酸', '甲酸',
            '氢气', '氧气', '氮气', '二氧化碳', '一氧化碳', '氯气', '氨气',
            '苯酚', '甲醛', '乙醛', '乙烯', '丙烯', '丁烷', '甲烷', '乙烷',
            # 添加更多基础化学品
            '石油', '天然气', '煤', '石蜡', '汽油', '柴油', '煤油',
            '丙烷', '丁烯', '异丁烷', '环己烷', '正己烷', '庚烷', '辛烷',
            '苯乙烯', '甲苯', '二甲苯', '萘', '蒽', '菲',
            '丙烯酸', '醋酸', '丙酸', '丁酸', '戊酸',
            '聚乙烯', '聚丙烯', '聚苯乙烯', '聚氯乙烯',
            '水', '重水', '过氧化氢', '硫化氢', '二氧化硫', '三氧化硫'
        }
        known_names.update(common_chemicals)
        
        return known_names

    def get_all_chemical_names_from_data(self) -> Set[str]:
        """从所有可用数据源获取化学品名称列表 - 使用缓存避免重复加载"""
        # 如果已经缓存了，直接返回
        if self._cached_chemical_names is not None:
            return self._cached_chemical_names
        
        logger.info("首次加载化学品名称库...")
        all_chemicals = set()
        
        # 从危化品目录获取
        if self.dangerous_chemicals is not None:
            if '品名' in self.dangerous_chemicals.columns:
                all_chemicals.update(self.dangerous_chemicals['品名'].dropna().astype(str))
        
        # 添加常见的基础化学品（避免重复文件读取）
        basic_chemicals = {
            '硫酸', '盐酸', '硝酸', '氢氧化钠', '氢氧化钾', '氨水', '乙醇', '甲醇',
            '丙酮', '甲苯', '苯', '乙醚', '氯仿', '四氯化碳', '乙酸', '甲酸',
            '氢气', '氧气', '氮气', '二氧化碳', '一氧化碳', '氯气', '氨气',
            '苯酚', '甲醛', '乙醛', '乙烯', '丙烯', '丁烷', '甲烷', '乙烷',
            '石油', '天然气', '煤', '石蜡', '汽油', '柴油', '煤油',
            '丙烷', '丁烯', '异丁烷', '环己烷', '正己烷', '庚烷', '辛烷',
            '苯乙烯', '甲苯', '二甲苯', '萘', '蒽', '菲',
            '丙烯酸', '醋酸', '丙酸', '丁酸', '戊酸',
            '聚乙烯', '聚丙烯', '聚苯乙烯', '聚氯乙烯',
            '水', '重水', '过氧化氢', '硫化氢', '二氧化硫', '三氧化硫',
            # 新增重要的基础石化化学品
            '乙苯', '异丙苯', '邻二甲苯', '间二甲苯', '对二甲苯',
            '环己烷', '甲基环己烷', '1,3-丁二烯', '异戊二烯',
            '正丁烷', '异丁烷', '正戊烷', '异戊烷', '新戊烷',
            '环丙烷', '环丁烷', '环戊烷', '环庚烷', '环辛烷',
            '丙炔', '1-丁炔', '2-丁炔', '1-戊炔', '苯炔',
            '甲苯胺', '苯胺', '二甲苯胺', '硝基苯', '氯苯',
            '对氯甲苯', '邻氯甲苯', '间氯甲苯', '三氯苯',
            '呋喃', '噻吩', '吡啶', '吲哚', '喹啉', '咔唑',
            '原油', '石脑油', '重油', '润滑油', '沥青'
        }
        all_chemicals.update(basic_chemicals)
        
        # 过滤掉无效的化学品名称
        valid_chemicals = set()
        for chem in all_chemicals:
            if isinstance(chem, str) and len(chem) >= 2 and len(chem) <= 20:
                # 移除明显不是化学品的词汇
                if not any(word in chem for word in ['工业', '产业', '公司', '企业', '有限', '股份']):
                    valid_chemicals.add(chem)
        
        # 缓存结果
        self._cached_chemical_names = valid_chemicals
        logger.info(f"化学品名称库加载完成，共 {len(valid_chemicals)} 个化学品")
        
        return valid_chemicals

    def is_valid_chemical_name(self, name: str) -> bool:
        """验证是否为有效的化学品名称 - 简化版本"""
        if not name or len(name) < 1 or len(name) > 20:
            return False
        
        # 排除明显不是化学品的词汇
        invalid_words = {
            '等', '类', '中', '上', '下', '前', '后', '左', '右', '内', '外', 
            '高', '低', '大', '小', '多', '少', '好', '坏', '新', '旧', '快', '慢', 
            '热', '冷', '工业', '农业', '医药', '食品', '化工', '生产', '制造', 
            '加工', '处理', '使用', '应用', '方法', '技术', '工艺', '公司', 
            '企业', '有限', '股份', '集团', '反应', '制备', '合成', '包括',
            '通过石油', '经过石油', '利用石油', '使用石油', '乙烯氢气', '通过'
        }
        
        if name in invalid_words:
            return False
        
        # 检查是否包含无效字符
        if re.search(r'[0-9%％\-\+\=\*\#\@\$\&\^\~\`\|\\\"\'\[\]<>《》]', name):
            return False
        
        # 特殊重要化学品（单独列出）
        special_chemicals = {'苯', '甲苯', '二甲苯', '石油', '煤', '天然气', '水', '氨', '氢气', '氧气', '氮气'}
        if name in special_chemicals:
            return True
        
        # 基于化学品命名规律的简单验证
        chemical_indicators = [
            # 常见化学后缀
            '酸', '碱', '盐', '醇', '醛', '酮', '酯', '醚', '烷', '烯', '炔', '苯', '酚', '腈',
            # 常见化学前缀
            '甲', '乙', '丙', '丁', '戊', '己', '庚', '辛', '壬', '癸', '正', '异', '叔', '仲',
            # 位置指示词
            '邻', '间', '对', '顺', '反',
            # 常见元素
            '氢', '氧', '氮', '氯', '硫', '钠', '钾', '钙', '镁', '铁', '铜', '锌', '铅', '汞', '银',
            # 其他化学品特征
            '聚', '环', '二', '三', '四', '五', '六', '七', '八', '九', '十', '气', '油', '脂', '胶'
        ]
        
        # 如果包含任何化学品指示词，认为是有效的
        return any(indicator in name for indicator in chemical_indicators)

    def add_new_chemical_to_database(self, chemical_name: str):
        """将新化学品添加到数据库中"""
        try:
            # 创建新化学品记录
            new_chemical_record = {
                '中文名称': chemical_name,
                'CAS号或流水号': '',  # 新发现的化学品暂时没有CAS号
                '分子式': '',
                '英文名称': '',
                '是否为危化品': '',  # 待确定
                '用途': '',
                '危害': '',
                '防范': '',
                '生产来源': '',
                '上游原料': '',
                '工业生产原料': '',
                'data_source': '系统自动识别',
                '备注': f'从生产来源文本中自动识别的新化学品，添加时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            }
            
            # 保存到新发现化学品文件
            new_chemicals_file = self.metadata_dir / "new_discovered_chemicals.csv"
            
            if new_chemicals_file.exists():
                # 文件存在，追加记录
                existing_df = pd.read_csv(new_chemicals_file, encoding='utf-8-sig')
                # 检查是否已经存在
                if chemical_name not in existing_df['中文名称'].values:
                    new_df = pd.concat([existing_df, pd.DataFrame([new_chemical_record])], ignore_index=True)
                    new_df.to_csv(new_chemicals_file, index=False, encoding='utf-8-sig')
                    logger.info(f"新增化学品到数据库: {chemical_name}")
            else:
                # 文件不存在，创建新文件
                new_df = pd.DataFrame([new_chemical_record])
                new_df.to_csv(new_chemicals_file, index=False, encoding='utf-8-sig')
                logger.info(f"创建新发现化学品文件并添加: {chemical_name}")
            
            # 更新缓存中的化学品名称列表，避免重复加载
            if self._cached_chemical_names is not None:
                self._cached_chemical_names.add(chemical_name)
                
        except Exception as e:
            logger.warning(f"添加新化学品到数据库失败 {chemical_name}: {e}")

    def extract_batch_number(self, filename: str) -> Optional[int]:
        """从文件名提取批次号"""
        # 匹配 batch_数字 格式
        batch_match = re.search(r'batch_(\d+)', filename)
        if batch_match:
            return int(batch_match.group(1))
        
        # 匹配 part_数字 格式  
        part_match = re.search(r'part_(\d+)', filename)
        if part_match:
            return int(part_match.group(1))
        
        return None

    def calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """计算数据完整度得分"""
        if df is None or len(df) == 0:
            return 0.0
        
        # 重要字段权重
        important_fields = {
            '中文名称': 3,
            'CAS号或流水号': 3,
            '分子式': 2,
            '英文名称': 2,
            '是否为危化品': 2,
            '用途': 1,
            '危害': 1,
            '防范': 1
        }
        
        total_score = 0
        max_score = 0
        
        for field, weight in important_fields.items():
            if field in df.columns:
                # 计算非空值比例
                non_null_ratio = df[field].notna().sum() / len(df)
                # 计算平均文本长度（对于文本字段）
                if df[field].dtype == 'object':
                    avg_length = df[field].dropna().astype(str).str.len().mean()
                    length_score = min(avg_length / 50, 1.0)  # 假设50字符为满分
                else:
                    length_score = 1.0
                
                field_score = non_null_ratio * length_score * weight
                total_score += field_score
            
            max_score += weight
        
        return total_score / max_score if max_score > 0 else 0.0
    
    def organize_files_by_batch(self) -> Dict[int, List[Path]]:
        """阶段一：按序号组织文件"""
        logger.info("开始按序号组织文件...")
        
        csv_files = list(self.input_dir.glob("*.csv"))
        files_by_batch = {}
        
        for file_path in csv_files:
            batch_num = self.extract_batch_number(file_path.name)
            if batch_num is not None:
                if batch_num not in files_by_batch:
                    files_by_batch[batch_num] = []
                files_by_batch[batch_num].append(file_path)
        
        logger.info(f"找到 {len(csv_files)} 个文件，按序号分组为 {len(files_by_batch)} 个批次")
        return files_by_batch

    def merge_duplicate_batches(self, files_by_batch: Dict[int, List[Path]]) -> Dict[int, pd.DataFrame]:
        """合并重复批次文件，选择最优版本"""
        logger.info("开始合并重复批次文件...")
        merged_batches = {}
        
        for batch_num, file_list in files_by_batch.items():
            if len(file_list) == 1:
                # 只有一个文件，直接加载
                df = self.load_csv_file(file_list[0])
                if df is not None:
                    merged_batches[batch_num] = df
            else:
                # 多个文件，选择最优版本
                logger.info(f"批次 {batch_num} 有 {len(file_list)} 个文件，选择最优版本...")
                best_df = None
                best_score = -1
                best_file = None
                
                for file_path in file_list:
                    df = self.load_csv_file(file_path)
                    if df is not None:
                        score = self.calculate_completeness_score(df)
                        logger.info(f"  文件 {file_path.name} 完整度得分: {score:.3f}")
                        
                        if score > best_score:
                            best_score = score
                            best_df = df
                            best_file = file_path
                
                if best_df is not None and best_file is not None:
                    merged_batches[batch_num] = best_df
                    logger.info(f"  批次 {batch_num} 选择文件: {best_file.name}")
        
        return merged_batches

    def check_missing_batches(self, merged_batches: Dict[int, pd.DataFrame]) -> List[int]:
        """检查缺失的批次序号"""
        if not merged_batches:
            return []
        
        batch_numbers = sorted(merged_batches.keys())
        missing_batches = []
        
        for i in range(min(batch_numbers), max(batch_numbers) + 1):
            if i not in batch_numbers:
                missing_batches.append(i)
        
        if missing_batches:
            logger.warning(f"发现缺失的批次序号: {missing_batches}")
        
        return missing_batches

    def identify_failed_records(self, merged_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """识别失败记录"""
        failed_records = pd.DataFrame()
        success_records = merged_data.copy()
        
        # 检查processing_status字段
        if 'processing_status' in merged_data.columns:
            failed_mask = merged_data['processing_status'].str.contains('失败', na=False)
            if failed_mask.any():
                failed_records = merged_data[failed_mask].copy()
                success_records = merged_data[~failed_mask].copy()
                logger.info(f"发现 {len(failed_records)} 条失败记录")
        
        return success_records, failed_records

    def find_missing_dangerous_chemicals(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """检查危化品目录中未被导入的化学品"""
        if self.dangerous_chemicals is None or len(self.dangerous_chemicals) == 0:
            return pd.DataFrame()
        
        logger.info("检查危化品目录中缺失的化学品...")
        missing_records = []
        
        # 创建已处理数据的查找集合
        processed_names = set()
        processed_cas = set()
        
        if len(processed_data) > 0:
            if '中文名称' in processed_data.columns:
                processed_names = set(processed_data['中文名称'].dropna().astype(str))
            if 'CAS号或流水号' in processed_data.columns:
                processed_cas = set(processed_data['CAS号或流水号'].dropna().astype(str))
        
        for _, dangerous_row in self.dangerous_chemicals.iterrows():
            dangerous_name = str(dangerous_row.get('品名', '')).strip()
            dangerous_cas = str(dangerous_row.get('CAS号', '')).strip()
            
            found = False
            
            # 按CAS号查找
            if dangerous_cas and dangerous_cas in processed_cas:
                found = True
            
            # 按名称查找
            if not found and dangerous_name and dangerous_name in processed_names:
                found = True
            
            if not found:
                missing_record = {
                    '序号': dangerous_row.get('序号', ''),
                    '中文名称': dangerous_name,
                    'CAS号或流水号': dangerous_cas,
                    '是否为危化品': '是',
                    '备注': '未在已补全文件中找到',
                    'data_source': '危化品目录'
                }
                missing_records.append(missing_record)
        
        if missing_records:
            logger.info(f"发现 {len(missing_records)} 条缺失的危化品")
            return pd.DataFrame(missing_records)
        else:
            logger.info("未发现缺失的危化品")
            return pd.DataFrame()

    def save_failed_data(self, failed_records: pd.DataFrame, missing_batches: List[int], missing_dangerous: pd.DataFrame):
        """保存失败数据"""
        all_failed_data = []
        
        # 添加失败记录
        if not failed_records.empty:
            all_failed_data.append(failed_records)
        
        # 添加缺失批次记录
        if missing_batches:
            missing_batch_records = pd.DataFrame([
                {'序号': batch_num, '备注': f'批次{batch_num}文件缺失'} 
                for batch_num in missing_batches
            ])
            all_failed_data.append(missing_batch_records)
        
        # 添加缺失危化品记录
        if not missing_dangerous.empty:
            all_failed_data.append(missing_dangerous)
        
        if all_failed_data:
            combined_failed = pd.concat(all_failed_data, ignore_index=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failed_filename = f"failed_chemicals_{timestamp}.csv"
            failed_filepath = self.failed_dir / failed_filename
            combined_failed.to_csv(failed_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存失败数据: {failed_filename}, 共{len(combined_failed)}条记录")
        else:
            logger.info("没有失败数据需要保存")

    def save_batch_data(self, data: pd.DataFrame, batch_size: int = 3000):
        """阶段一：按批次保存成功数据"""
        if len(data) == 0:
            logger.warning("没有数据需要保存")
            return
        
        num_batches = (len(data) + batch_size - 1) // batch_size
        logger.info(f"将 {len(data)} 条记录分为 {num_batches} 个批次保存")
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_data = data.iloc[start_idx:end_idx]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processed_chemicals_batch_{i+1}_{timestamp}.csv"
            filepath = self.success_dir / filename
            
            batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存批次 {i+1}: {filename}, 共{len(batch_data)}条记录")

    def format_for_neo4j(self, df: pd.DataFrame) -> pd.DataFrame:
        """阶段二：将数据格式化以适应Neo4j导入，包含名称清理"""
        if df.empty:
            return pd.DataFrame()

        logger.info("开始格式化数据以适应Neo4j...")

        # 列映射
        column_mapping = {
            '中文名称': 'name:ID',
            'CAS号或流水号': 'cas:string',
            '中文别名': 'aliases:string[]',
            '英文名称': 'english_name:string',
            '英文别名': 'english_aliases:string[]',
            '分子式': 'molecular_formula:string',
            '分子量': 'molecular_weight:float',
            '危害': 'hazard:string',
            '防范': 'prevention:string',
            '危害处置': 'hazard_disposal:string',
            '用途': 'uses:string',
            '自然来源': 'natural_source:string',
            '生产来源': 'production_source:string',
            '工业生产原料': 'downstream_industries:string',
            '上游原料': 'upstream_materials:string',
            '性质': 'properties:string',
            'data_source': 'source:string',
            '浓度阈值': 'concentration_threshold:string'
        }

        # 筛选存在的列
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        formatted_df = df[existing_columns.keys()].copy()
        formatted_df.rename(columns=existing_columns, inplace=True)

        # 清理化学品名称（保留-和,，去除其他符号）
        if 'name:ID' in formatted_df.columns:
            formatted_df['name:ID'] = formatted_df['name:ID'].astype(str).apply(self.clean_chemical_name_for_neo4j)
            # 过滤掉清理后为空的记录
            formatted_df = formatted_df[formatted_df['name:ID'].str.strip() != '']
        
        # 清理英文名称
        if 'english_name:string' in formatted_df.columns:
            formatted_df['english_name:string'] = formatted_df['english_name:string'].astype(str).apply(self.clean_chemical_name_for_neo4j)

        # 清理CAS号中的中文字符
        if 'cas:string' in formatted_df.columns:
            chinese_char_pattern = re.compile(r'[\u4e00-\u9fa5]')
            formatted_df['cas:string'] = formatted_df['cas:string'].astype(str).apply(
                lambda x: '' if chinese_char_pattern.search(x) else x
            )

        # 创建节点标签
        if '是否为危化品' in df.columns:
            formatted_df[':LABEL'] = np.where(
                df['是否为危化品'].astype(str).str.strip() == '是',
                'DangerousChemical;Chemical',
                'Chemical'
            )
        else:
            formatted_df[':LABEL'] = 'Chemical'

        # 过滤非危化品的浓度阈值
        if 'concentration_threshold:string' in formatted_df.columns and '是否为危化品' in df.columns:
            formatted_df.loc[df['是否为危化品'].astype(str).str.strip() != '是', 'concentration_threshold:string'] = ''

        # 处理数组列格式，并清理别名
        array_columns = ['aliases:string[]', 'english_aliases:string[]']
        for col in array_columns:
            if col in formatted_df.columns:
                # 清理别名中的符号，然后格式化
                formatted_df[col] = formatted_df[col].astype(str).apply(
                    lambda x: '|'.join([self.clean_chemical_name_for_neo4j(alias.strip()) 
                                      for alias in re.split(r'[;；|]', x) 
                                      if self.clean_chemical_name_for_neo4j(alias.strip())])
                )

        # 处理数据来源字段，应用花括号内容提取和本地搜索标识
        if 'source:string' in formatted_df.columns:
            formatted_df['source:string'] = formatted_df['source:string'].astype(str).apply(self.process_data_source_field)
        elif 'data_source' in df.columns:
            # 如果原始数据中有data_source字段，也要处理
            formatted_df['source:string'] = df['data_source'].astype(str).apply(self.process_data_source_field)

        # 处理生产来源字段，移除生产商信息
        if 'production_source:string' in formatted_df.columns:
            formatted_df['production_source:string'] = formatted_df['production_source:string'].astype(str).apply(self.remove_producer_info)

        # 填充空值
        formatted_df.fillna('', inplace=True)

        logger.info(f"数据格式化完成，生成 {len(formatted_df)} 条记录")
        return formatted_df

    def deduplicate_relationships(self, relationships: List[dict]) -> List[dict]:
        """去除重复和相近的关系"""
        if not relationships:
            return relationships
        
        # 使用集合去除完全相同的关系
        unique_relationships = []
        seen_relationships = set()
        
        for rel in relationships:
            # 清理关系中的名称
            start_id = self.clean_chemical_name_for_neo4j(str(rel[':START_ID']))
            end_id = self.clean_chemical_name_for_neo4j(str(rel[':END_ID']))
            rel_type = rel[':TYPE']
            
            # 跳过空名称
            if not start_id or not end_id:
                continue
            
            # 创建关系的唯一标识符
            rel_key = (start_id, end_id, rel_type)
            
            if rel_key not in seen_relationships:
                seen_relationships.add(rel_key)
                unique_relationships.append({
                    ':START_ID': start_id,
                    ':END_ID': end_id,
                    ':TYPE': rel_type
                })
        
        # 进一步处理相近关系：如果两个关系的起点和终点相似度很高，保留较短的名称
        filtered_relationships = []
        grouped_by_type = {}
        
        # 按关系类型分组
        for rel in unique_relationships:
            rel_type = rel[':TYPE']
            if rel_type not in grouped_by_type:
                grouped_by_type[rel_type] = []
            grouped_by_type[rel_type].append(rel)
        
        # 对每种关系类型进行去重
        for rel_type, rels in grouped_by_type.items():
            processed_rels = []
            
            for rel in rels:
                is_similar = False
                start_id = rel[':START_ID']
                end_id = rel[':END_ID']
                
                # 检查是否与已处理的关系相似
                for processed_rel in processed_rels:
                    proc_start = processed_rel[':START_ID']
                    proc_end = processed_rel[':END_ID']
                    
                    # 计算相似度：如果一个名称包含另一个名称，认为相似
                    start_similar = (start_id in proc_start or proc_start in start_id)
                    end_similar = (end_id in proc_end or proc_end in end_id)
                    
                    if start_similar and end_similar:
                        is_similar = True
                        # 如果当前关系的名称更短（更简洁），替换已处理的关系
                        if len(start_id) + len(end_id) < len(proc_start) + len(proc_end):
                            processed_rels.remove(processed_rel)
                            processed_rels.append(rel)
                        break
                
                if not is_similar:
                    processed_rels.append(rel)
            
            filtered_relationships.extend(processed_rels)
        
        logger.info(f"关系去重：从 {len(relationships)} 条减少到 {len(filtered_relationships)} 条")
        return filtered_relationships

    def extract_relationships(self, df: pd.DataFrame) -> pd.DataFrame:
        """阶段二：正确的关系提取逻辑 - 上游原料和下游产业关系，包含去重"""
        if df.empty:
            logger.info("没有数据，跳过关系提取")
            return pd.DataFrame()

        logger.info("开始提取上游原料和下游产业关系...")
        relationships = []
        
        # 获取已知化学品名称库
        known_chemicals = self.get_known_chemical_names()
        logger.info(f"已知化学品库包含 {len(known_chemicals)} 个化学品名称")
        
        for _, row in df.iterrows():
            chemical_name = self.clean_chemical_name_for_neo4j(row['name:ID'])
            
            # 1. 提取上游原料关系：生产来源 → 工艺 → 当前化学品
            if 'production_source:string' in row.index and pd.notna(row['production_source:string']):
                source_text = str(row['production_source:string'])
                upstream_materials = self.extract_upstream_materials(source_text, known_chemicals, chemical_name)
                
                if upstream_materials:
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical_name)
                    if clean_chemical:
                        # 为每个目标化学品创建一个统一的工艺节点
                        process_name = f"{clean_chemical}_生产工艺"
                        
                        # 工艺 -> 当前化学品 (此关系对每个目标化学品只创建一次)
                        relationships.append({
                            ':START_ID': process_name,
                            ':END_ID': clean_chemical,
                            ':TYPE': '生产产品'
                        })

                        # 所有上游原料都指向这一个工艺节点
                        for material in upstream_materials:
                            clean_material = self.clean_chemical_name_for_neo4j(material)
                            if clean_material and clean_material != clean_chemical:
                                relationships.append({
                                    ':START_ID': clean_material,
                                    ':END_ID': process_name,
                                    ':TYPE': '参与工艺'
                                })
            
            # 2. 提取下游产业关系：当前化学品 → 下游产业 → 具体产业分块
            if 'downstream_industries:string' in row.index and pd.notna(row['downstream_industries:string']):
                industry_text = str(row['downstream_industries:string'])
                downstream_industries = self.extract_downstream_industries(industry_text, chemical_name)
                
                for industry in downstream_industries:
                    # 清理产业名称
                    clean_industry = self.clean_chemical_name_for_neo4j(industry)
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical_name)
                    
                    if clean_industry and clean_chemical:
                        # 当前化学品 → 下游产业
                        relationships.append({
                            ':START_ID': clean_chemical,
                            ':END_ID': clean_industry,
                            ':TYPE': '应用于产业'
                        })
                        
                        # 进一步分解产业到具体分块
                        industry_blocks = self.extract_industry_blocks(industry)
                        for block in industry_blocks:
                            clean_block = self.clean_chemical_name_for_neo4j(block)
                            if clean_block:
                                # 下游产业 → 产业分块
                                relationships.append({
                                    ':START_ID': clean_industry,
                                    ':END_ID': clean_block,
                                    ':TYPE': '包含领域'
                                })

        # 去重复关系
        relationships = self.deduplicate_relationships(relationships)
        
        logger.info(f"关系提取完成，共提取 {len(relationships)} 条关系")
        return pd.DataFrame(relationships)

    def extract_process_nodes_from_relationships(self, relationships_df: pd.DataFrame) -> pd.DataFrame:
        """从关系中提取工艺节点并创建节点数据"""
        if relationships_df.empty:
            return pd.DataFrame()
        
        process_nodes = []
        process_names = set()
        
        # 从关系中收集所有工艺节点名称
        for _, row in relationships_df.iterrows():
            start_id = str(row.get(':START_ID', ''))
            end_id = str(row.get(':END_ID', ''))
            
            # 识别工艺节点（包含"工艺"关键字的节点）
            if '工艺' in start_id and start_id not in process_names:
                process_names.add(start_id)
            if '工艺' in end_id and end_id not in process_names:
                process_names.add(end_id)
        
        # 为每个工艺节点创建节点数据
        for process_name in process_names:
            # 解析工艺名称
            if '_制备_' in process_name and process_name.endswith('_工艺'):
                parts = process_name.replace('_工艺', '').split('_制备_')
                if len(parts) == 2:
                    upstream_material = parts[0]
                    target_chemical = parts[1]
                    
                    process_nodes.append({
                        'name:ID': process_name,
                        ':LABEL': 'Process',
                        'process_type:string': '化学制备工艺',
                        'upstream_material:string': upstream_material,
                        'target_product:string': target_chemical,
                        'description:string': f'使用{upstream_material}制备{target_chemical}的工艺流程',
                        'source:string': 'system_generated',
                        'data_status:string': 'placeholder'
                    })
        
        logger.info(f"从关系中提取了 {len(process_nodes)} 个工艺节点")
        return pd.DataFrame(process_nodes)

    def clean_chemical_name_for_neo4j(self, name: str) -> str:
        """清理化学品名称用于Neo4j录入，保留-和,，去除其他符号"""
        if not name:
            return name
        
        # 保留中文字符、英文字母、数字、连字符(-)和逗号(,)
        # 去除其他特殊符号如：（）[]{}【】""''：:；;等等·！!？?
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\-,，]', '', name)
        
        # 清理多余的逗号和连字符
        cleaned = re.sub(r'[-,，]+$', '', cleaned)  # 去除末尾的连字符和逗号
        cleaned = re.sub(r'^[-,，]+', '', cleaned)  # 去除开头的连字符和逗号
        cleaned = re.sub(r'[-]{2,}', '-', cleaned)  # 多个连字符合并为一个
        cleaned = re.sub(r'[,，]{2,}', ',', cleaned)  # 多个逗号合并为一个
        
        return cleaned.strip()

    def extract_upstream_materials(self, source_text: str, known_chemicals: Set[str], target_chemical: str) -> Set[str]:
        """从生产来源文本中提取上游原料化学品 - 增强的提取逻辑，包含名称清理"""
        upstream_materials = set()
        
        # 先排除生产商信息
        cleaned_text = self.remove_producer_info(source_text)
        
        # 获取所有已知化学品名称 - 使用缓存版本，避免重复文件加载
        all_chemicals = self.get_all_chemical_names_from_data()
        all_chemicals.update(known_chemicals)
        
        # 方法1: 针对常见格式模式提取，增加更多模式
        upstream_patterns = [
            r'上游原料[：:]?\s*包括\s*([^。；;]+)',
            r'原料(?:包括)?[：:]\s*([^。；;]+)',
            r'以\s*([^为]+?)\s*为原料',
            r'由\s*([^制]+?)\s*(?:制得|制成|制备)',
            r'从\s*([^中]+?)\s*中(?:提取|分离|得到)',
            r'([^。；;，,]+?)\s*(?:裂解|氧化|分解)(?:得到|制得|产生)',
            # 新增模式：更好地匹配化学品列表
            r'原料.*?包括\s*([^。；;]+)',
            r'主要原料[：:]?\s*([^。；;]+)',
            r'所需原料[：:]?\s*([^。；;]+)',
            r'以\s*([^。；;，,]*(?:、[^。；;，,]*)*)\s*(?:等\s*)?(?:化学品|原料|物质)',
            r'使用\s*([^。；;，,]*(?:、[^。；;，,]*)*)\s*(?:等\s*)?(?:作为|为)\s*原料',
            r'从\s*([^。；;，,]*(?:、[^。；;，,]*)*)\s*(?:等\s*)?中',
        ]
        
        for pattern in upstream_patterns:
            matches = re.findall(pattern, cleaned_text, re.IGNORECASE)
            for match in matches:
                # 更智能的分割：处理"、"、"和"、"与"、","等分隔符
                chemicals_in_match = re.split(r'[,，、;；和与及或等]+', match.strip())
                
                for chem_candidate in chemicals_in_match:
                    clean_name = chem_candidate.strip()
                    # 移除常见前缀和后缀
                    clean_name = re.sub(r'^(?:通过|经过|利用|使用|主要|从|由)\s*', '', clean_name)
                    clean_name = re.sub(r'\s*(?:等|类|反应|制得|制备|制成|裂解|氧化|分解|化学品|原料|物质).*$', '', clean_name)
                    clean_name = clean_name.strip()
                    
                    # 对化学品名称进行清理（去除除了-和,以外的符号）
                    clean_name = self.clean_chemical_name_for_neo4j(clean_name)
                    
                    # 检查是否为有效化学品名称
                    if (clean_name and 
                        len(clean_name) >= 2 and 
                        clean_name != target_chemical and
                        not re.search(r'[制得制备制成反应裂解氧化分解]', clean_name)):  # 排除包含动作词的片段
                        
                        if clean_name in all_chemicals:
                            # 已知化学品，直接添加
                            upstream_materials.add(clean_name)
                            logger.debug(f"发现已知上游化学品: {clean_name}")
                        else:
                            # 未知化学品，先添加到数据库，再加入集合
                            if self.add_new_chemical_to_database(clean_name):
                                upstream_materials.add(clean_name)
                                logger.info(f"发现并添加新上游化学品: {clean_name}")
                                # 注意：add_new_chemical_to_database 已经会更新缓存
        
        # 方法2: 直接扫描已知化学品名称 - 使用更高效的方法
        sorted_chemicals = sorted(all_chemicals, key=len, reverse=True)
        for chemical in sorted_chemicals:
            if chemical != target_chemical and len(chemical) >= 2 and chemical in cleaned_text:
                # 确保是完整的化学品名称词汇，不是其他词的一部分
                pattern = r'(?:^|[^a-zA-Z\u4e00-\u9fa5])' + re.escape(chemical) + r'(?:[^a-zA-Z\u4e00-\u9fa5]|$)'
                if re.search(pattern, cleaned_text):
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical)
                    if clean_chemical:
                        upstream_materials.add(clean_chemical)
                        logger.debug(f"通过扫描发现上游化学品: {clean_chemical}")
        
        # 如果发现的上游原料很少，尝试更宽松的匹配
        if len(upstream_materials) >= 5:
            logger.info(f"为 {target_chemical} 提取到上游原料: {upstream_materials}")
        
        return upstream_materials

    def extract_downstream_industries(self, application_text: str, target_chemical: str) -> Set[str]:
        """从应用文本中提取下游产业"""
        downstream_industries = set()
        
        if not application_text or application_text == 'nan' or pd.isna(application_text):
            return downstream_industries
        
        # 清理应用文本
        industry_text = str(application_text).lower()
        
        # 产业提取模式
        industry_patterns = [
            r'在([^工业制造生产领域行业中]+)中(?:广泛)?应用',
            r'作为([^工业制造生产领域行业的]+)的(?:原料|材料)',
            r'主要用于([^工业制造生产领域行业。；,，]+)',
            r'下游产品包括[：:].*?(\d+\.\s*[^；。;]+)',
            r'下游.*?包括[：:].*?([^；。;，,]+)',
        ]
        
        # 常见产业关键词
        industry_keywords = {
            '医药': '医药产业', '制药': '医药产业', '药': '医药产业',
            '化工': '化工产业',
            '石化': '石油化工产业',
            '农药': '农业化学产业',
            '涂料': '涂料工业',
            '塑料': '塑料工业',
            '橡胶': '橡胶工业',
            '纺织': '纺织工业',
            '食品': '食品工业',
            '电子': '电子工业',
            '汽车': '汽车工业',
            '建筑': '建筑材料工业',
            '钢铁': '钢铁工业',
            '有色金属': '有色金属工业',
            '玻璃': '玻璃工业',
            '陶瓷': '陶瓷工业',
            '香料': '香料工业', '香精': '香料工业',
            '清洁': '清洁用品工业', '消毒': '清洁用品工业',
            '化妆品': '化妆品工业', '护肤': '化妆品工业',
            '农业': '农业', '种植': '农业',
            '按摩': '保健品工业', '保健': '保健品工业',
        }
        
        # 使用模式提取
        for pattern in industry_patterns:
            matches = re.findall(pattern, industry_text, re.IGNORECASE)
            for match in matches:
                clean_match = match.strip()
                if len(clean_match) > 1:
                    # 检查是否包含已知产业关键词
                    for keyword, industry_name in industry_keywords.items():
                        if keyword in clean_match:
                            downstream_industries.add(industry_name)
                    
                    # 直接添加提取的产业名称（清理后）
                    industry_name = self.clean_industry_name(clean_match)
                    if industry_name:
                        downstream_industries.add(industry_name)
        
        # 直接扫描已知产业关键词
        for keyword, industry_name in industry_keywords.items():
            if keyword in industry_text:
                downstream_industries.add(industry_name)
        
        return downstream_industries

    def extract_industry_blocks(self, industry: str) -> Set[str]:
        """将产业进一步分解为具体的产业分块"""
        industry_blocks = set()
        
        # 产业分块映射
        industry_block_mapping = {
            '化工产业': {'基础化工', '精细化工', '专用化学品'},
            '石油化工产业': {'石油炼制', '石化中间体', '合成材料'},
            '医药产业': {'原料药', '制剂', '生物制药', '中药'},
            '农业化学产业': {'农药', '化肥', '植物生长调节剂'},
            '塑料工业': {'工程塑料', '通用塑料', '塑料制品'},
            '橡胶工业': {'合成橡胶', '橡胶制品', '轮胎'},
            '纺织工业': {'化纤', '染料', '纺织助剂'},
            '食品工业': {'食品添加剂', '食品包装', '营养保健品'},
        }
        
        if industry in industry_block_mapping:
            industry_blocks.update(industry_block_mapping[industry])
        else:
            # 如果没有具体映射，直接使用产业名称
            industry_blocks.add(industry)
        
        return industry_blocks

    def clean_industry_name(self, name: str) -> str:
        """清理产业名称"""
        if not name or name == 'nan' or pd.isna(name):
            return ""
        
        # 移除不必要的词汇
        exclude_words = ['生产', '制造', '领域', '方面', '中', '等']
        
        clean_name = name.strip()
        for word in exclude_words:
            clean_name = clean_name.replace(word, '')
        
        # 确保是有效的产业名称
        if len(clean_name) < 2 or len(clean_name) > 15:
            return ""
        
        # 添加"产业"后缀（如果没有的话）
        if not any(suffix in clean_name for suffix in ['产业', '工业', '行业', '领域']):
            clean_name += '产业'
        
        return clean_name

    def remove_producer_info(self, text: str) -> str:
        """移除生产商信息 - 增强版，包括生厂商和生产商后面的所有文字"""
        if not text or text == 'nan' or pd.isna(text):
            return ""
        
        text = str(text)  # 确保是字符串
        
        # 移除生产商信息模式
        producer_patterns = [
            r'由.*?(?:公司|集团|企业|厂|生产|制造|工厂).*',  # 由...公司/厂生产的内容
            r'生产商[：:].*',  # 生产商: 后面的内容
            r'制造商[：:].*',  # 制造商: 后面的内容
            r'厂家[：:].*',   # 厂家: 后面的内容
            r'供应商[：:].*', # 供应商: 后面的内容
        ]
        
        for pattern in producer_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text.strip()

    def clean_chemical_name_for_neo4j(self, name: str) -> str:
        """为Neo4j清理化学品名称，保留关键字符"""
        if not name or name == 'nan' or pd.isna(name):
            return ""
        
        # 去除多余空格并trim
        clean_name = str(name).strip()
        
        # 移除不必要的括号内容（但保留一些重要的）
        clean_name = re.sub(r'\([^)]*工业级[^)]*\)', '', clean_name)
        clean_name = re.sub(r'\([^)]*食品级[^)]*\)', '', clean_name)
        clean_name = re.sub(r'\([^)]*医药级[^)]*\)', '', clean_name)
        
        # 保留化学结构相关的字符：英文字母、数字、中文、连字符、逗号
        clean_name = re.sub(r'[^\w\u4e00-\u9fa5\-,，]', ' ', clean_name)
        
        # 清理多余空格
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        return clean_name

    def extract_chemicals_from_text(self, text: str, known_chemicals: Set[str]) -> Set[str]:
        """从文本中提取化学品名称 - 改进的智能提取"""
        if not text or text == 'nan' or pd.isna(text):
            return set()
        
        extracted = set()
        text = str(text)
        
        # 按长度排序已知化学品，优先匹配长名称
        sorted_chemicals = sorted(known_chemicals, key=len, reverse=True)
        
        for chemical in sorted_chemicals:
            if len(chemical) >= 2 and chemical in text:
                # 确保是完整的化学品名称词汇，不是其他词的一部分
                pattern = r'(?:^|[^a-zA-Z\u4e00-\u9fa5])' + re.escape(chemical) + r'(?:[^a-zA-Z\u4e00-\u9fa5]|$)'
                if re.search(pattern, text):
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical)
                    if clean_chemical:
                        extracted.add(clean_chemical)
        
        return extracted
            '电子工业': {'电子化学品', '半导体材料', '显示材料'},
        # 生产商信息识别模式 - 更全面的匹配 '汽车化学品', '汽车涂料'},
        producer_patterns = ['玻璃', '保温材料', '防水材料'},
            # 匹配"生产商"、"生厂商"及其后的所有内容直到句子结束
            r'生产商.*?(?=[。；;，,]|$)',
            r'生厂商.*?(?=[。；;，,]|$)',ck_mapping:
            r'主要生产商.*?(?=[。；;，,]|$)',dustry_block_mapping[industry])
            r'制造商.*?(?=[。；;，,]|$)', 
            r'生产企业.*?(?=[。；;，,]|$)',
            r'制造企业.*?(?=[。；;，,]|$)',ndustry}_主要应用")
            # 匹配包含公司、企业、厂、集团等的整个短语{industry}_细分领域")
            r'[^。；;，,]*(?:公司|企业|厂|集团|有限责任公司|股份有限公司|工厂|制造厂)[^。；;，,]*(?:[。；;，,]|$)',
            # 匹配"主要生产商包括/有"这种模式后的所有内容
            r'主要生产商[有包括：:].+?(?=[。；;]|$)',
            r'生产商[有包括：:].+?(?=[。；;]|$)',str) -> str:
            r'制造商[有包括：:].+?(?=[。；;]|$)',
        ]f not name:
            return ""
        cleaned_text = text
        for pattern in producer_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE), '广泛'}
        clean_name = name.strip()
        # 只清理多余的标点符号，保留列表分隔符（逗号）和字段分隔符（冒号）
        # 移除连续的标点符号，但保留单个逗号和冒号rds:
        cleaned_text = re.sub(r'[；;]+', '', cleaned_text)  # 移除分号
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)   # 标准化空格
        # 确保是有效的产业名称
        return cleaned_text.strip()en(clean_name) > 15:
            return ""
    def extract_braced_content(self, text: str) -> List[str]:
        """提取花括号{}内的内容并按符号分隔"""
        if not text:uffix in clean_name for suffix in ['产业', '工业', '行业', '领域']):
            return []e += '产业'
        
        # 提取所有花括号内的内容name
        braced_pattern = r'\{([^}]+)\}'
        matches = re.findall(braced_pattern, text)tr:
        """移除生产商信息 - 增强版，包括生厂商和生产商后面的所有文字"""
        result = [] or text == 'nan' or pd.isna(text):
        for match in matches:
            # 按常见分隔符分割内容，但要考虑特殊情况
            # 首先按主要分隔符分割  # 确保是字符串
            main_separators = [';', '；', '、']
            items = [match]配
            ucer_patterns = [
            # 使用主要分隔符进行分割商"及其后的所有内容直到句子结束
            for sep in main_separators:
                new_items = []|$)',
                for item in items:)',
                    new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                items = new_items)',
            r'制造企业.*?(?=[。；;，,]|$)',
            # 如果没有主要分隔符，再尝试次要分隔符短语
            if len(items) == 1:|厂|集团|有限责任公司|股份有限公司|工厂|制造厂)[^。；;，,]*(?:[。；;，,]|$)',
                secondary_separators = [',', '，']
                for sep in secondary_separators:
                    new_items = []]|$)',
                    for item in items:',
                        # 对于包含"GB/T"、"ISO"等标准号的情况，要小心处理
                        if any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                            # 如果包含标准号，只在没有标准号格式的逗号处分割
                            parts = item.split(sep)
                            temp_result = []', cleaned_text, flags=re.IGNORECASE)
                            i = 0
                            while i < len(parts):
                                current = parts[i].strip()
                                # 检查下一部分是否是标准号的一部分d_text)  # 移除分号
                                if (i + 1 < len(parts) and # 标准化空格
                                    any(std in current for std in ['GB', 'ISO', 'ASTM', 'JIS', 'DIN']) and
                                    re.match(r'^\s*[T\s]*\d+', parts[i + 1])):
                                    # 合并标准号部分
                                    current = current + sep + parts[i + 1].strip()
                                    i += 1
                                if current:
                                    temp_result.append(current)
                                i += 1
                            new_items.extend(temp_result)
                        else:([^}]+)\}'
                            new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                    items = new_items
            lt = []
            # 进一步处理其他分隔符（但要更谨慎）
            final_items = []虑特殊情况
            for item in items:
                # 只对不包含标准号的项目使用其他分隔符'；', '、']
                if not any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                    other_separators = ['|', '/', '\\', '&', '+']
                    temp_items = [item]
                    for sep in other_separators:
                        new_temp_items = []
                        for temp_item in temp_items:
                            new_temp_items.extend([i.strip() for i in temp_item.split(sep) if i.strip()])
                        temp_items = new_temp_items
                    final_items.extend(temp_items)
                else:隔符，再尝试次要分隔符
                    final_items.append(item)
                secondary_separators = [',', '，']
            # 过滤掉空值和过短的内容n secondary_separators:
            valid_items = [item for item in final_items if len(item.strip()) > 1]
            result.extend(valid_items)
                        # 对于包含"GB/T"、"ISO"等标准号的情况，要小心处理
        # 去重并保持顺序       if any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
        seen = set()        # 如果包含标准号，只在没有标准号格式的逗号处分割
        unique_result = []  parts = item.split(sep)
        for item in result: temp_result = []
            if item not in seen:0
                seen.add(item)ile i < len(parts):
                unique_result.append(item)parts[i].strip()
                                # 检查下一部分是否是标准号的一部分
        return unique_result    if (i + 1 < len(parts) and 
                                    any(std in current for std in ['GB', 'ISO', 'ASTM', 'JIS', 'DIN']) and
    def process_data_source_field(self, text: str) -> str:d+', parts[i + 1])):
        """处理数据来源字段，提取花括号内容或标明本地搜索来源"""并标准号部分
        if not text or text == 'nan' or pd.isna(text):+ sep + parts[i + 1].strip()
            return "根据通义大模型蒸馏而来"    i += 1
                                if current:
        text = str(text)  # 确保是字符串  temp_result.append(current)
                                i += 1
        # 提取花括号内容           new_items.extend(temp_result)
        braced_content = self.extract_braced_content(text)
                            new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
        if braced_content:= new_items
            # 将提取的内容用分号连接
            return '; '.join(braced_content)
        else:inal_items = []
            # 如果没有花括号内容，说明是本地搜索来源
            return "根据通义大模型蒸馏而来"他分隔符
                if not any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
    def clean_chemical_name_advanced(self, name: str) -> str:'+']
        """高级化学品名称清理"""p_items = [item]
        if not name:for sep in other_separators:
            return ""   new_temp_items = []
                        for temp_item in temp_items:
        # 移除常见的中文填充词汇       new_temp_items.extend([i.strip() for i in temp_item.split(sep) if i.strip()])
        exclude_words = {emp_items = new_temp_items
            '包括', '等', '类', '中', '的', '和', '与', '及', '或', '各种', '多种', 
            '主要', '广泛', '常见', '一般', '通常', '特别', '尤其', '特殊',
            '以及', '还有', '另外', '此外', '同时', '另', '其他', '部分',
            '如', '例如', '比如', '诸如', '像', '譬如', '好比', '如同',
            '等等', '之类', '左右', '上下', '约', '大约', '接近', '差不多',
            '括', '（', '）', '(', ')', '[', ']', '【', '】',if len(item.strip()) > 1]
        }   result.extend(valid_items)
        
        clean_name = name.strip()
        seen = set()
        # 移除括号及其内容ult = []
        clean_name = re.sub(r'[（(][^）)]*[）)]', '', clean_name)
        clean_name = re.sub(r'[【\[][^\】\]]*[\】\]]', '', clean_name)
                seen.add(item)
        # 移除填充词汇unique_result.append(item)
        for word in exclude_words:
            clean_name = clean_name.replace(word, '')
        
        # 移除数字编号data_source_field(self, text: str) -> str:
        clean_name = re.sub(r'^\d+[\.、]\s*', '', clean_name)
        if not text or text == 'nan' or pd.isna(text):
        # 移除多余的空格和标点根据通义大模型蒸馏而来"
        clean_name = re.sub(r'[，,；;：:。\.]+', '', clean_name)
        clean_name = re.sub(r'\s+', '', clean_name)
        
        # 确保是有效的化学品名称
        if len(clean_name) < 2 or len(clean_name) > 50:xt)
            return ""
        if braced_content:
        # 排除明显不是化学品的词汇号连接
        non_chemical_patterns = [ed_content)
            r'^[等类中的和与及或]+$',
            r'^[\d\s\.,，。；;：:]+$',
            r'^[用于作为通过经过]+',馏而来"
        ]
        clean_chemical_name_advanced(self, name: str) -> str:
        for pattern in non_chemical_patterns:
            if re.match(pattern, clean_name):
                return ""
        
        return clean_name
        exclude_words = {
    def clean_chemical_name(self, name: str) -> str: '或', '各种', '多种', 
        """清理和标准化化学品名称"""常见', '一般', '通常', '特别', '尤其', '特殊',
        if not name:有', '另外', '此外', '同时', '另', '其他', '部分',
            return "", '比如', '诸如', '像', '譬如', '好比', '如同',
            '等等', '之类', '左右', '上下', '约', '大约', '接近', '差不多',
        # 移除括号内容和特殊符号 '）', '(', ')', '[', ']', '【', '】',
        clean_name = re.sub(r'[()（）【】\[\]<>《》].*?[()（）【】\[\]<>《》]', '', name)
        clean_name = re.sub(r'[()（）【】\[\]<>《》]', '', clean_name)
        clean_name = name.strip()
        # 移除英文描述
        clean_name = re.sub(r'[A-Za-z\s]+', '', clean_name)
        clean_name = re.sub(r'[（(][^）)]*[）)]', '', clean_name)
        # 移除数字和特殊符号（保留化学品名称中的常见字符）][^\】\]]*[\】\]]', '', clean_name)
        clean_name = re.sub(r'[0-9%％\-\+\=\*\#\@\$\&\^\~\`\|\\\"\']', '', clean_name)
        # 移除填充词汇
        # 只保留中文字符和少数特殊化学符号e_words:
        clean_name = re.sub(r'[^\u4e00-\u9fa5·]', '', clean_name)
        
        # 移除过短或过长的名称
        clean_name = clean_name.strip()\s*', '', clean_name)
        if len(clean_name) < 2 or len(clean_name) > 20:
            return ""
        clean_name = re.sub(r'[，,；;：:。\.]+', '', clean_name)
        # 移除明显不是化学品的词汇e.sub(r'\s+', '', clean_name)
        exclude_words = {
            '等', '类', '中', '上', '下', '前', '后', '左', '右', '内', '外', '高', '低', 
            '大', '小', '多', '少', '好', '坏', '新', '旧', '快', '慢', '热', '冷',
            '工业', '农业', '医药', '食品', '化工', '石油', '煤炭', '天然气',
            '生产', '制造', '加工', '处理', '使用', '应用', '方法', '技术', '工艺'
        } 排除明显不是化学品的词汇
        non_chemical_patterns = [
        if clean_name in exclude_words:
            return "".,，。；;：:]+$',
            r'^[用于作为通过经过]+',
        return clean_name
        
    def validate_chemical_name(self, name: str, known_chemicals: Set[str]) -> bool:
        """验证是否为有效的化学品名称"""tern, clean_name):
        if not name or len(name) < 2:
            return False
        return clean_name
        # 精确匹配已知化学品
        if name in known_chemicals:ame: str) -> str:
            return True""
        if not name:
        # 模糊匹配：检查是否包含在已知化学品中
        for known in known_chemicals:
            if name in known or known in name:
                if abs(len(name) - len(known)) <= 2:  # 长度差异不大《》]', '', name)
                    return True()（）【】\[\]<>《》]', '', clean_name)
        
        # 基于化学品命名规律的验证
        chemical_suffixes = ['酸', '碱', '盐', '醇', '醛', '酮', '酯', '醚', '烷', '烯', '炔', '苯', '酚']
        chemical_prefixes = ['甲', '乙', '丙', '丁', '戊', '己', '庚', '辛', '壬', '癸']
        # 移除数字和特殊符号（保留化学品名称中的常见字符）
        # 检查是否符合化学品命名模式.sub(r'[0-9%％\-\+\=\*\#\@\$\&\^\~\`\|\\\"\']', '', clean_name)
        has_chemical_pattern = (
            any(name.endswith(suffix) for suffix in chemical_suffixes) or
            any(name.startswith(prefix) for prefix in chemical_prefixes) or
            '氢' in name or '氧' in name or '氮' in name or '氯' in name or '硫' in name or
            '钠' in name or '钾' in name or '钙' in name or '镁' in name or '铁' in name or
            '铜' in name or '锌' in name or '铅' in name or '汞' in name or '银' in name
        )f len(clean_name) < 2 or len(clean_name) > 20:
            return ""
        return has_chemical_pattern
        # 移除明显不是化学品的词汇
    def determine_relationship_type(self, text: str, source: str, target: str) -> str:
        """根据上下文确定关系类型""", '上', '下', '前', '后', '左', '右', '内', '外', '高', '低', 
        text_lower = text.lower()好', '坏', '新', '旧', '快', '慢', '热', '冷',
            '工业', '农业', '医药', '食品', '化工', '石油', '煤炭', '天然气',
        if any(keyword in text_lower for keyword in ['上游', '原料', '制备', '制造', '生产']):
            return '工艺'
        elif any(keyword in text_lower for keyword in ['反应', '化合', '分解', '氧化', '还原']):
            return '化学反应'exclude_words:
        elif any(keyword in text_lower for keyword in ['用途', '应用', '制取']):
            return '用途'
        elif any(keyword in text_lower for keyword in ['转化', '变成', '得到']):
            return '转化'
        else:ate_chemical_name(self, name: str, known_chemicals: Set[str]) -> bool:
            return '工艺'  # 默认关系类型
        if not name or len(name) < 2:
    def create_standard_legal_relationships(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """为每个化学品创建标准/法律关系"""
        logger.info("开始创建标准/法律关系...")
        if name in known_chemicals:
        if nodes_df.empty:
            logger.info("没有节点数据，跳过标准/法律关系创建")
            return pd.DataFrame()
        for known in known_chemicals:
        # 尝试导入标准/法律管理器 known or known in name:
        try:    if abs(len(name) - len(known)) <= 2:  # 长度差异不大
            import systurn True
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            ical_suffixes = ['酸', '碱', '盐', '醇', '醛', '酮', '酯', '醚', '烷', '烯', '炔', '苯', '酚']
            # 使用标准/法律管理器s = ['甲', '乙', '丙', '丁', '戊', '己', '庚', '辛', '壬', '癸']
            manager = StandardLegalManager(str(self.base_path))
            _, relationships = manager.update_standard_legal_relationships(nodes_df)
            chemical_pattern = (
            # 检查是否有有效的标准法律数据h(suffix) for suffix in chemical_suffixes) or
            if not relationships.empty and len(relationships) > 0:fixes) or
                # 检查第一行数据是否为占位符in name or '氮' in name or '氯' in name or '硫' in name or
                first_row = relationships.iloc[0]name or '镁' in name or '铁' in name or
                if ':END_ID' in first_row and str(first_row[':END_ID']).startswith('标准法律_'):
                    logger.info("检测到占位符标准法律数据，跳过创建无意义的关系")
                    return pd.DataFrame()
                else:emical_pattern
                    logger.info(f"使用标准/法律管理器创建了 {len(relationships)} 条标准/法律关系")
                    return relationships, text: str, source: str, target: str) -> str:
            else:定关系类型"""
                logger.info("没有有效的标准/法律数据，跳过关系创建")
                return pd.DataFrame()
            ny(keyword in text_lower for keyword in ['上游', '原料', '制备', '制造', '生产']):
        except ImportError:
            logger.warning("无法导入标准/法律管理器，跳过标准/法律关系创建")['反应', '化合', '分解', '氧化', '还原']):
            # 不再创建占位符关系，直接返回空DataFrame
            logger.info("跳过创建占位符标准/法律关系")r keyword in ['用途', '应用', '制取']):
            return pd.DataFrame()
        elif any(keyword in text_lower for keyword in ['转化', '变成', '得到']):
    def create_standard_legal_nodes(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """创建标准/法律节点"""
        logger.info("开始创建标准/法律节点...")
        
        if nodes_df.empty:gal_relationships(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
            logger.info("没有化学品节点数据，跳过标准/法律节点创建")
            return pd.DataFrame()..")
        
        # 尝试使用标准/法律管理器pty:
        try:logger.info("没有节点数据，跳过标准/法律关系创建")
            import sysDataFrame()
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            
            # 使用标准/法律管理器
            manager = StandardLegalManager(str(self.base_path))-processor"))
            nodes, _ = manager.update_standard_legal_relationships(nodes_df)
            
            # 检查是否有有效的标准法律数据
            if not nodes.empty and len(nodes) > 0:f.base_path))
                # 检查第一行数据是否为占位符manager.update_standard_legal_relationships(nodes_df)
                first_row = nodes.iloc[0]
                if 'name:ID' in first_row and str(first_row['name:ID']).startswith('标准法律_'):
                    logger.info("检测到占位符标准法律节点，跳过创建无意义的节点")ps) > 0:
                    return pd.DataFrame()
                else:_row = relationships.iloc[0]
                    logger.info(f"使用标准/法律管理器创建了 {len(nodes)} 个标准/法律节点").startswith('标准法律_'):
                    return nodes"检测到占位符标准法律数据，跳过创建无意义的关系")
            else:   return pd.DataFrame()
                logger.info("没有有效的标准/法律数据，跳过节点创建")
                return pd.DataFrame()准/法律管理器创建了 {len(relationships)} 条标准/法律关系")
                    return relationships
        except ImportError:
            logger.warning("无法导入标准/法律管理器，跳过标准/法律节点创建")
            # 不再创建占位符节点，直接返回空DataFrame
            logger.info("跳过创建占位符标准/法律节点")
            return pd.DataFrame()
            logger.warning("无法导入标准/法律管理器，跳过标准/法律关系创建")
    def save_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame, include_standard_legal: bool = True):
        """保存Neo4j格式的数据"""过创建占位符标准/法律关系")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 创建标准/法律节点和关系d_legal_nodes(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        if include_standard_legal and not nodes_df.empty:
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            odes_df.empty:
            # 合并节点数据nfo("没有化学品节点数据，跳过标准/法律节点创建")
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:法律管理器
                combined_nodes = nodes_df
            import sys
            # 合并关系数据.append(str(self.base_path / "chemical-data-processor"))
            if not standard_legal_relationships.empty:dLegalManager
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:准/法律管理器
                combined_relationships = relationships_dfpath))
        else:odes, _ = manager.update_standard_legal_relationships(nodes_df)
            combined_nodes = nodes_df
            combined_relationships = relationships_df
            if not nodes.empty and len(nodes) > 0:
        # 保存节点数据（分批）第一行数据是否为占位符
        if not combined_nodes.empty:oc[0]
            batch_size = 3000in first_row and str(first_row['name:ID']).startswith('标准法律_'):
            num_batches = (len(combined_nodes) + batch_size - 1) // batch_size
                    return pd.DataFrame()
            for i in range(num_batches):
                start_idx = i * batch_size理器创建了 {len(nodes)} 个标准/法律节点")
                end_idx = min((i + 1) * batch_size, len(combined_nodes))
                batch_data = combined_nodes.iloc[start_idx:end_idx]
                logger.info("没有有效的标准/法律数据，跳过节点创建")
                filename = f"neo4j_ready_chemicals_batch_{i+1}_{timestamp}.csv"
                filepath = self.success_dir / filename
                batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
                logger.info(f"保存Neo4j节点批次 {i+1}: {filename}")
            # 不再创建占位符节点，直接返回空DataFrame
        # 保存关系数据er.info("跳过创建占位符标准/法律节点")
        if not combined_relationships.empty:
            rel_filename = f"neo4j_relationships_{timestamp}.csv"
            rel_filename = f"neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.success_dir / rel_filename
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存Neo4j关系数据: {rel_filename}")
            
        # 统计信息
        chemical_count = len(nodes_df) if not nodes_df.empty else 0
        process_count = len(process_nodes) if not process_nodes.empty else 0
        relationship_count = len(relationships_df) if not relationships_df.empty else 0
        
        logger.info(f"数据保存完成:")
        logger.info(f"  化学品节点: {chemical_count} 个")
        logger.info(f"  工艺节点: {process_count} 个")
        logger.info(f"  关系: {relationship_count} 条")
        
        if include_standard_legal:
            standard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
            standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
            if standard_count > 0 or standard_rel_count > 0:
                logger.info(f"  标准/法律节点: {standard_count} 个")
                logger.info(f"  标准/法律关系: {standard_rel_count} 条")
    def process_files(self, incremental_mode: bool = False):
        """阶段一：数据合并与预处理（支持增量更新）"""ncat(all_nodes, ignore_index=True)
        logger.info("=" * 50)
        if incremental_mode:
            logger.info("开始阶段一：增量数据处理")ionships.empty:
        else:   combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            logger.info("开始阶段一：完整数据合并与预处理")
        logger.info("=" * 50)tionships = relationships_df
        else:
        # 1. 加载危化品目录（化学品节点 + 工艺节点）
        self.load_dangerous_chemicals()
                combined_nodes = pd.concat([nodes_df, process_nodes], ignore_index=True)
        # 2. 根据模式选择文件
        if incremental_mode:es = nodes_df
            new_files = self.get_new_files()nships_df
            if not new_files:
                logger.info("没有新文件需要处理")
                returnd_nodes.empty:
            logger.info(f"增量模式：处理 {len(new_files)} 个新文件")
            num_batches = (len(combined_nodes) + batch_size - 1) // batch_size
            # 按文件处理增量数据
            files_by_batch = {}batches):
            for file_path in new_files:ize
                batch_num = self.extract_batch_number(file_path.name)s))
                if batch_num is not None:es.iloc[start_idx:end_idx]
                    if batch_num not in files_by_batch:
                        files_by_batch[batch_num] = []ch_{i+1}_{timestamp}.csv"
                    files_by_batch[batch_num].append(file_path)
        else:   batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
            # 按序号组织所有文件info(f"保存Neo4j节点批次 {i+1}: {filename}")
            files_by_batch = self.organize_files_by_batch()
            if not files_by_batch:
                logger.error("未找到任何batch文件")
                returnme = f"neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.success_dir / rel_filename
        # 3. 合并重复批次文件relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
        merged_batches = self.merge_duplicate_batches(files_by_batch)
            
        # 4. 检查缺失批次（仅在完整模式下）
        if not incremental_mode:l:
            missing_batches = self.check_missing_batches(merged_batches)
        else:tandard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
            missing_batches = [] len(relationships_df) if not relationships_df.empty else 0
            standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
        # 5. 合并所有数据
        if merged_batches:数据保存完成:")
            all_data = pd.concat(merged_batches.values(), ignore_index=True)
            logger.info(f"合并完成，总计 {len(all_data)} 条记录")")
        else:ogger.info(f"  原有关系: {original_rel_count} 条")
            logger.error("没有成功合并任何数据")standard_rel_count} 条")
            return
    def process_files(self, incremental_mode: bool = False):
        # 6. 数据验证 - 新增严格的数据质量检查"""
        logger.info("开始数据验证...")
        valid_data, validation_failed_data, validation_errors = self.validate_chemical_data(all_data)
            logger.info("开始阶段一：增量数据处理")
        if validation_errors:
            logger.warning("数据验证发现问题:")处理")
            for error in validation_errors:
                logger.warning(f"  - {error}")
        # 1. 加载危化品目录
        # 7. 识别处理失败记录gerous_chemicals()
        success_data, processing_failed_data = self.identify_failed_records(valid_data)
        # 2. 根据模式选择文件
        # 8. 检查缺失的危化品（仅在完整模式下）
        if not incremental_mode:_new_files()
            missing_dangerous = self.find_missing_dangerous_chemicals(success_data)
        else:   logger.info("没有新文件需要处理")
            missing_dangerous = pd.DataFrame()
            logger.info(f"增量模式：处理 {len(new_files)} 个新文件")
        # 9. 合并所有失败数据
        all_failed_data = []
        if len(validation_failed_data) > 0:
            all_failed_data.append(validation_failed_data)
        if len(processing_failed_data) > 0:tch_number(file_path.name)
            all_failed_data.append(processing_failed_data)
                    if batch_num not in files_by_batch:
        # 10. 保存失败数据    files_by_batch[batch_num] = []
        combined_failed = pd.concat(all_failed_data, ignore_index=True) if all_failed_data else pd.DataFrame()
        self.save_failed_data(combined_failed, missing_batches, missing_dangerous)
            # 按序号组织所有文件
        # 11. 保存成功数据_batch = self.organize_files_by_batch()
        if not success_data.empty:
            if incremental_mode:到任何batch文件")
                # 增量模式：保存到专门的增量目录
                self.save_incremental_data(success_data)
            else:批次文件
                # 完整模式：正常保存lf.merge_duplicate_batches(files_by_batch)
                self.save_batch_data(success_data)
             检查缺失批次（仅在完整模式下）
            # 更新已处理文件记录tal_mode:
            if incremental_mode:lf.check_missing_batches(merged_batches)
                self.update_processed_files_record(new_files)
            missing_batches = []
            logger.info(f"阶段一完成，成功处理 {len(success_data)} 条记录")
        else:合并所有数据
            logger.error("阶段一：没有成功处理的数据")
            all_data = pd.concat(merged_batches.values(), ignore_index=True)
    def save_incremental_data(self, data: pd.DataFrame):
        """保存增量更新数据"""
        if len(data) == 0:没有成功合并任何数据")
            logger.warning("没有增量数据需要保存")
            return
        # 6. 数据验证 - 新增严格的数据质量检查
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"incremental_update_{timestamp}.csv"rrors = self.validate_chemical_data(all_data)
        filepath = self.incremental_dir / filename
        if validation_errors:
        data.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"保存增量数据: {filename}, 共{len(data)}条记录")
                logger.warning(f"  - {error}")
    def update_processed_files_record(self, files: List[Path]):
        """更新已处理文件记录"""
        record = self.load_processed_files_record().identify_failed_records(valid_data)
        
        for file_path in files:
            fingerprint = self.calculate_file_fingerprint(file_path)
            file_key = str(file_path.relative_to(self.base_path))cals(success_data)
            record[file_key] = fingerprint
            missing_dangerous = pd.DataFrame()
        self.save_processed_files_record(record)
        logger.info(f"更新了 {len(files)} 个文件的处理记录")
        all_failed_data = []
    def prepare_for_neo4j(self, incremental_mode: bool = False):
        """阶段二：图数据格式化与关系提取（支持增量模式）"""lidation_failed_data)
        logger.info("=" * 50)led_data) > 0:
        if incremental_mode:append(processing_failed_data)
            logger.info("开始阶段二：增量数据Neo4j格式化与关系提取")
        else: 保存失败数据
            logger.info("开始阶段二：图数据格式化与关系提取")ed_data, ignore_index=True) if all_failed_data else pd.DataFrame()
        logger.info("=" * 50)(combined_failed, missing_batches, missing_dangerous)

        # 根据模式选择数据源据
        if incremental_mode:empty:
            # 增量模式：读取增量数据l_mode:
            success_files = list(self.incremental_dir.glob("incremental_update_*.csv"))
            if not success_files:ntal_data(success_data)
                logger.info("没有增量数据需要格式化")
                return：正常保存
        else:   self.save_batch_data(success_data)
            # 完整模式：读取阶段一的成功数据（只加载最新批次）
            success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
            if not success_files:
                logger.error("未找到阶段一的处理结果，请先运行阶段一")new_files)
                return
            logger.info(f"阶段一完成，成功处理 {len(success_data)} 条记录")
            # 按时间戳分组，只取最新的一组
            latest_timestamp = None理的数据")
            latest_files = []
            _incremental_data(self, data: pd.DataFrame):
            for file_path in success_files:
                # 提取时间戳 (例如: processed_chemicals_batch_1_20250719_153149.csv)
                filename = file_path.name
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.csv$', filename)
                if timestamp_match:
                    timestamp = timestamp_match.group(1)%S")
                    if latest_timestamp is None or timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        latest_files = [file_path]
                    elif timestamp == latest_timestamp:f-8-sig')
                        latest_files.append(file_path)条记录")
            
            success_files = latest_fileslf, files: List[Path]):
            logger.info(f"选择最新时间戳 {latest_timestamp} 的 {len(success_files)} 个文件")
        record = self.load_processed_files_record()
        # 合并所有数据
        all_success_data = []s:
        for file_path in success_files:e_file_fingerprint(file_path)
            df = self.load_csv_file(file_path)to(self.base_path))
            if df is not None: fingerprint
                all_success_data.append(df)
        self.save_processed_files_record(record)
        if not all_success_data:iles)} 个文件的处理记录")
            logger.error("无法加载数据")
            returnr_neo4j(self, incremental_mode: bool = False):
        """阶段二：图数据格式化与关系提取（支持增量模式）"""
        combined_data = pd.concat(all_success_data, ignore_index=True)
        logger.info(f"加载数据完成，共 {len(combined_data)} 条记录")
            logger.info("开始阶段二：增量数据Neo4j格式化与关系提取")
        # 格式化为Neo4j格式
        neo4j_nodes = self.format_for_neo4j(combined_data)
        logger.info("=" * 50)
        # 提取关系（使用改进的算法）
        neo4j_relationships = self.extract_relationships(neo4j_nodes)
        if incremental_mode:
        # 保存Neo4j数据读取增量数据
        if incremental_mode:list(self.incremental_dir.glob("incremental_update_*.csv"))
            self.save_incremental_neo4j_data(neo4j_nodes, neo4j_relationships)
        else:   logger.info("没有增量数据需要格式化")
            self.save_neo4j_data(neo4j_nodes, neo4j_relationships)
        else:
        logger.info("阶段二完成！")（只加载最新批次）
            success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
    def save_incremental_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame):
        """保存增量Neo4j格式的数据"""("未找到阶段一的处理结果，请先运行阶段一")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        # 创建标准/法律节点和关系取最新的一组
        if not nodes_df.empty: None
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            for file_path in success_files:
            # 合并节点数据时间戳 (例如: processed_chemicals_batch_1_20250719_153149.csv)
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:f timestamp_match:
                combined_nodes = nodes_df_match.group(1)
                    if latest_timestamp is None or timestamp > latest_timestamp:
            # 合并关系数据    latest_timestamp = timestamp
            if not standard_legal_relationships.empty:
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:       latest_files.append(file_path)
                combined_relationships = relationships_df
        else:uccess_files = latest_files
            combined_nodes = nodes_dftest_timestamp} 的 {len(success_files)} 个文件")
            combined_relationships = relationships_df
        # 合并所有数据
        # 保存增量节点数据s_data = []
        if not combined_nodes.empty:es:
            filename = f"incremental_neo4j_nodes_{timestamp}.csv"
            filepath = self.incremental_dir / filename
            combined_nodes.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j节点数据: {filename}")
        if not all_success_data:
        # 保存增量关系数据.error("无法加载数据")
        if not combined_relationships.empty:
            rel_filename = f"incremental_neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.incremental_dir / rel_filenameex=True)
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j关系数据: {rel_filename}")
            化为Neo4j格式
        # 统计信息nodes = self.format_for_neo4j(combined_data)
        chemical_count = len(nodes_df) if not nodes_df.empty else 0
        standard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
        original_rel_count = len(relationships_df) if not relationships_df.empty else 0
        standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
        # 保存Neo4j数据
        logger.info(f"增量数据保存完成:")
        logger.info(f"  化学品节点: {chemical_count} 个")nodes, neo4j_relationships)
        logger.info(f"  标准/法律节点: {standard_count} 个")
        logger.info(f"  原有关系: {original_rel_count} 条")lationships)
        logger.info(f"  标准/法律关系: {standard_rel_count} 条")
        logger.info("阶段二完成！")
    def run_complete_pipeline(self, incremental_mode: bool = False):
        """运行完整的处理流程（支持增量更新）"""data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame):
        if incremental_mode:
            logger.info("开始运行增量更新的化学品数据处理流程...")m%d_%H%M%S")
        else:
            logger.info("开始运行完整的化学品数据处理流程...")
        if not nodes_df.empty:
        try:standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            # 阶段一：数据合并与预处理_relationships = self.create_standard_legal_relationships(nodes_df)
            self.process_files(incremental_mode=incremental_mode)
            # 合并节点数据
            # 阶段二：图数据格式化与关系提取egal_nodes.empty:
            self.prepare_for_neo4j(incremental_mode=incremental_mode)nodes], ignore_index=True)
            else:
            logger.info("流程执行成功！")odes_df
            
        except Exception as e:
            logger.error(f"流程执行过程中发生错误: {e}")ps.empty:
            raiseombined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:
    def run_incremental_update(self):s = relationships_df
        """运行增量更新"""
        logger.info("执行增量更新...")es_df
        self.run_complete_pipeline(incremental_mode=True)

        # 保存增量节点数据
def main():not combined_nodes.empty:
    """主函数"""ilename = f"incremental_neo4j_nodes_{timestamp}.csv"
    import syslepath = self.incremental_dir / filename
            combined_nodes.to_csv(filepath, index=False, encoding='utf-8-sig')
    # 获取工作目录logger.info(f"保存增量Neo4j节点数据: {filename}")
    base_path = Path(__file__).parent.parent.parent
        # 保存增量关系数据
    # 创建处理器实例t combined_relationships.empty:
    processor = ChemicalProcessor(str(base_path))lationships_{timestamp}.csv"
            rel_filepath = self.incremental_dir / rel_filename
    # 检查命令行参数ombined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
    if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
        # 增量更新模式
        processor.run_incremental_update()
    else:hemical_count = len(nodes_df) if not nodes_df.empty else 0
        # 完整处理模式_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
        processor.run_complete_pipeline()ships_df) if not relationships_df.empty else 0
        standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
        
if __name__ == "__main__":保存完成:")
    main()gger.info(f"  化学品节点: {chemical_count} 个")
        logger.info(f"  标准/法律节点: {standard_count} 个")
        logger.info(f"  原有关系: {original_rel_count} 条")
        logger.info(f"  标准/法律关系: {standard_rel_count} 条")

    def run_complete_pipeline(self, incremental_mode: bool = False):
        """运行完整的处理流程（支持增量更新）"""
        if incremental_mode:
            logger.info("开始运行增量更新的化学品数据处理流程...")
        else:
            logger.info("开始运行完整的化学品数据处理流程...")
        
        try:
            # 阶段一：数据合并与预处理
            self.process_files(incremental_mode=incremental_mode)
            
            # 阶段二：图数据格式化与关系提取
            self.prepare_for_neo4j(incremental_mode=incremental_mode)
            
            logger.info("流程执行成功！")
            
        except Exception as e:
            logger.error(f"流程执行过程中发生错误: {e}")
            raise

    def run_incremental_update(self):
        """运行增量更新"""
        logger.info("执行增量更新...")
        self.run_complete_pipeline(incremental_mode=True)


def main():
    """主函数"""
    import sys
    
    # 获取工作目录
    base_path = Path(__file__).parent.parent.parent
    
    # 创建处理器实例
    processor = ChemicalProcessor(str(base_path))
    
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
        # 增量更新模式
        processor.run_incremental_update()
    else:
        # 完整处理模式
        processor.run_complete_pipeline()


if __name__ == "__main__":
    main()
