#!/usr/bin/env python3
# -*- coding: utf-8)-*-
"""
通用实体数据处理主模块
用于合并、清洗实体数据并为Neo4j图数据库做准备
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
import psutil  # 系统资源监控
import multiprocessing as mp  # 多进程支持

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('entity_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EntityProcessor:
    """通用实体数据处理器 - 实现完整的数据处理流程"""
    
    def __init__(self, base_path: str):
        """
        初始化数据处理器 - 第二代智能优化版本
        
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
        
        # 第二代智能配置系统
        self._init_adaptive_config()
        
        logger.info(f"初始化化学品数据处理器 - 第二代智能优化版本")
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"危化品目录: {self.dangerous_dir}")
        logger.info(f"成功输出目录: {self.success_dir}")
        logger.info(f"失败输出目录: {self.failed_dir}")
        logger.info(f"增量更新目录: {self.incremental_dir}")
        logger.info(f"元数据目录: {self.metadata_dir}")
        logger.info(f"智能配置: 工作进程={self.max_workers}, 分块大小={self.chunk_size}")
    
    def _init_adaptive_config(self):
        """初始化自适应配置系统"""
        try:
            # 系统资源检测
            cpu_cores = mp.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            available_memory = psutil.virtual_memory().available / (1024**3)
            
            # 自适应工作进程配置
            if memory_gb > 16 and available_memory > 8:
                self.max_workers = min(cpu_cores * 2, 24)  # 高内存系统
                self.chunk_size = 3000
            elif memory_gb > 8 and available_memory > 4:
                self.max_workers = min(cpu_cores + 4, 16)  # 中等内存系统
                self.chunk_size = 2000
            else:
                self.max_workers = cpu_cores  # 保守配置
                self.chunk_size = 1000
            
            # 系统负载动态调整
            cpu_percent = psutil.cpu_percent(interval=0.5)
            if cpu_percent < 30 and available_memory > memory_gb * 0.6:
                # 系统空闲时启用激进模式
                self.max_workers = min(self.max_workers + 2, 32)
                self.chunk_size = int(self.chunk_size * 1.2)
                logger.info(f"系统负载较低({cpu_percent:.1f}%)，启用激进处理模式")
            
            # 其他配置
            self.similarity_threshold = 0.8
            self.enable_parallel = True
            self.adaptive_mode = True
            
            logger.info(f"自适应配置完成: CPU={cpu_cores}核, 内存={memory_gb:.1f}GB, 可用={available_memory:.1f}GB")
            
        except Exception as e:
            logger.warning(f"自适应配置失败: {e}")
            # 降级到安全配置
            self.max_workers = mp.cpu_count()
            self.chunk_size = 1000
            self.similarity_threshold = 0.8
            self.enable_parallel = False
            self.adaptive_mode = False

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
        logger.info(f"发现 {len(new_files)} 个新文件或已修改文件需要处理（包含所有csv文件，包括final_processed等）")
        return new_files

    def validate_chemical_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """数据验证规则 - 增加更严格的数据质量检查"""
        if df.empty:
            return df, pd.DataFrame(), []

        logger.info("开始执行数据验证规则...")
        validation_errors = []
        failed_records = []
        
        # 验证规则1: 中文名称必须存在且非空（允许包含数字、英文字符和特殊符号）
        # 化学品名称可能包含英文字符、数字、特殊符号等，只需要不为空即可
        name_invalid = df['中文名称'].isna() | (df['中文名称'].astype(str).str.strip() == '') | (df['中文名称'].astype(str).str.strip() == 'nan')
        
        if name_invalid.any():
            invalid_names = df[name_invalid].copy()
            invalid_names['validation_error'] = '中文名称无效或缺失'
            failed_records.append(invalid_names)
            validation_errors.append(f"中文名称验证失败: {name_invalid.sum()} 条记录")
        
        # 验证规则2: CAS号格式验证（允许为空）
        # CAS号可以为空，这是允许的
        # 不进行CAS号验证，允许所有格式包括空值
        
        # 验证规则3: 分子式格式验证（允许为空）
        # 分子式可以为空，不进行格式验证
        # 很多化学品可能没有明确的分子式
        
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
        all_failed_mask = name_invalid | dangerous_missing_hazard | duplicate_mask
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
        # 也包括final_processed开头的文件
        success_files.extend(list(self.success_dir.glob("final_processed*.csv")))
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
        # 匹配 final_processed_xxx_part_数字 格式
        final_processed_match = re.search(r'final_processed.*?part_(\d+)', filename)
        if final_processed_match:
            return int(final_processed_match.group(1))
        
        # 匹配 batch_数字 格式
        batch_match = re.search(r'batch_(\d+)', filename)
        if batch_match:
            return int(batch_match.group(1))
        
        # 匹配 part_数字 格式  
        part_match = re.search(r'part_(\d+)', filename)
        if part_match:
            return int(part_match.group(1))
        
        return None

    def calculate_data_quality_score(self, df: pd.DataFrame) -> float:
        """计算数据质量得分 - 简化版本，仅用于重复文件比较"""
        if df is None or len(df) == 0:
            return 0.0
        
        # 简单的数据质量评估：主要看关键字段的非空率
        score = 0.0
        total_weight = 0.0
        
        # 关键字段权重
        key_fields = {
            '中文名称': 0.4,      # 最重要
            'CAS号或流水号': 0.3,  # 重要标识
            '分子式': 0.1,
            '英文名称': 0.1,
            '是否为危化品': 0.1
        }
        
        for field, weight in key_fields.items():
            if field in df.columns:
                total_weight += weight
                non_null_ratio = df[field].notna().sum() / len(df)
                score += non_null_ratio * weight
        
        # 归一化得分
        if total_weight > 0:
            final_score = score / total_weight
        else:
            # 如果没有关键字段，至少检查是否有数据
            final_score = 0.1 if len(df) > 0 else 0.0
        
        return final_score
    
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
            else:
                # 没有批次号的csv文件统一分到batch_num=0
                if 0 not in files_by_batch:
                    files_by_batch[0] = []
                files_by_batch[0].append(file_path)
        logger.info(f"找到 {len(csv_files)} 个文件，分组（含无批次号）为 {len(files_by_batch)} 个批次（0为无批次号文件）")
        return files_by_batch

    def merge_duplicate_batches(self, files_by_batch: Dict[int, List[Path]]) -> Dict[int, pd.DataFrame]:
        """合并所有文件并处理重复记录"""
        logger.info("开始合并所有文件...")
        
        merged_batches = {}
        # 先处理有序号的批次（升序），最后处理无序号（batch_num=0）
        batch_nums = sorted([b for b in files_by_batch if b != 0])
        if 0 in files_by_batch:
            batch_nums.append(0)
        for batch_num in batch_nums:
            file_list = files_by_batch[batch_num]
            logger.info(f"处理批次 {batch_num}，共 {len(file_list)} 个文件")
            # 收集所有数据
            all_dataframes = []
            for file_path in file_list:
                try:
                    df = self.load_csv_file(file_path)
                    if df is not None and len(df) > 0:
                        all_dataframes.append(df)
                        logger.info(f"  加载文件 {file_path.name}: {len(df)} 条记录")
                    else:
                        logger.warning(f"  文件 {file_path.name} 无法加载或为空")
                except Exception as e:
                    logger.warning(f"  文件 {file_path.name} 处理时出错: {e}")
                    continue
            # 合并所有数据
            if all_dataframes:
                if len(all_dataframes) == 1:
                    merged_df = all_dataframes[0]
                else:
                    merged_df = pd.concat(all_dataframes, ignore_index=True)
                # 处理重复记录（CAS冲突解决 + 记录合并）
                if len(merged_df) > 0:
                    logger.info(f"  批次 {batch_num} 开始处理重复记录...")
                    original_count = len(merged_df)
                    merged_df = self.merge_duplicate_records(merged_df)
                    final_count = len(merged_df)
                    logger.info(f"  批次 {batch_num} 去重完成: {original_count} -> {final_count} 条记录")
                merged_batches[batch_num] = merged_df
                logger.info(f"  ✅ 批次 {batch_num} 处理完成: {len(all_dataframes)} 个文件，最终 {len(merged_df)} 条记录")
            else:
                logger.warning(f"  ❌ 批次 {batch_num} 没有可用的数据")
        logger.info(f"文件合并和去重完成，成功处理 {len(merged_batches)} 个批次（无序号文件排最后）")
        return merged_batches

    def merge_duplicate_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """合并重复记录，普通化学品去重，危化品保留（按名称+浓度阈值区分）"""
        if df is None or len(df) == 0:
            return df
        
        logger.info("开始合并重复记录...")
        original_count = len(df)
        
        # 分离危化品和普通化学品
        is_dangerous_col = '是否为危化品' if '是否为危化品' in df.columns else None
        
        if is_dangerous_col:
            dangerous_mask = df[is_dangerous_col].astype(str).str.strip() == '是'
            dangerous_chemicals = df[dangerous_mask].copy()
            normal_chemicals = df[~dangerous_mask].copy()
            
            logger.info(f"危化品: {len(dangerous_chemicals)} 条（保留所有记录，按名称+浓度阈值区分）")
            logger.info(f"普通化学品: {len(normal_chemicals)} 条（执行去重）")
        else:
            # 如果没有危化品标记，全部按普通化学品处理
            dangerous_chemicals = pd.DataFrame()
            normal_chemicals = df.copy()
            logger.info(f"未发现危化品标记，全部按普通化学品处理: {len(normal_chemicals)} 条")
        
        # 处理普通化学品去重
        if len(normal_chemicals) > 0:
            normal_chemicals = self._merge_normal_chemicals(normal_chemicals)
        
        # 处理危化品（为危化品创建唯一标识符）
        if len(dangerous_chemicals) > 0:
            dangerous_chemicals = self._process_dangerous_chemicals(dangerous_chemicals)
        
        # 合并结果
        final_data = []
        if len(normal_chemicals) > 0:
            final_data.append(normal_chemicals)
        if len(dangerous_chemicals) > 0:
            final_data.append(dangerous_chemicals)
        
        if final_data:
            result_df = pd.concat(final_data, ignore_index=True)
        else:
            result_df = pd.DataFrame()
        
        removed_count = original_count - len(result_df)
        logger.info(f"记录处理完成：原始记录 {original_count} 条，最终记录 {len(result_df)} 条，处理减少 {removed_count} 条")
        
        return result_df
    
    def _merge_normal_chemicals(self, df: pd.DataFrame) -> pd.DataFrame:
        """合并普通化学品的重复记录 - 确保每个化学品只对应一个CAS号"""
        if len(df) == 0:
            return df
            
        logger.info(f"开始合并普通化学品重复记录，共 {len(df)} 条")
        
        # 第一步：检查并解决同一化学品名称对应多个CAS号的问题
        df_work = df.copy()
        df_work = self._resolve_name_cas_conflicts(df_work)
        
        # 第二步：基于化学品名称+CAS号进行更精确的去重
        has_chinese = '中文名称' in df_work.columns
        has_cas = 'CAS号或流水号' in df_work.columns
        
        if has_chinese and has_cas:
            # 使用中文名称+CAS号作为复合键，避免过度合并
            df_work['merge_key'] = (
                df_work['中文名称'].fillna('').astype(str).str.strip() + '|||' +
                df_work['CAS号或流水号'].fillna('').astype(str).str.strip()
            )
            # 过滤掉无效的键
            valid_mask = (
                (df_work['中文名称'].fillna('').astype(str).str.strip() != '') &
                (df_work['中文名称'].fillna('').astype(str).str.strip() != 'nan')
            )
            df_work = df_work[valid_mask]
        elif has_chinese:
            # 使用中文名称作为主要去重键
            df_work['merge_key'] = df_work['中文名称'].fillna('').astype(str).str.strip()
            df_work = df_work[df_work['merge_key'] != '']
            df_work = df_work[df_work['merge_key'] != 'nan']
        elif has_cas:
            # 如果没有中文名称，使用CAS号
            df_work['merge_key'] = df_work['CAS号或流水号'].fillna('').astype(str).str.strip()
            df_work = df_work[df_work['merge_key'] != '']
            df_work = df_work[df_work['merge_key'] != 'nan']
        else:
            logger.warning("普通化学品没有可用的合并键（中文名称或CAS号）")
            return df
        
        if len(df_work) == 0:
            logger.warning("普通化学品没有有效的合并键")
            return df
        
        # 分组处理重复记录
        grouped = df_work.groupby('merge_key')
        duplicate_groups = {key: group for key, group in grouped if len(group) > 1}
        
        if duplicate_groups:
            logger.info(f"普通化学品发现 {len(duplicate_groups)} 组重复记录，总计 {sum(len(group) for group in duplicate_groups.values())} 条重复记录")
            # 详细记录重复情况
            for key, group in list(duplicate_groups.items())[:5]:  # 只显示前5组
                logger.info(f"  重复组示例: {key} -> {len(group)} 条记录")
        
        # 保留的记录
        final_records = []
        
        # 处理每个分组
        for key, group in grouped:
            if len(group) == 1:
                final_records.append(group.iloc[0])
            else:
                # 处理重复记录，合并信息并确保唯一CAS号
                merged_record = self._select_best_record(group, str(key))
                final_records.append(merged_record)
        
        # 重新构建DataFrame
        result_df = pd.DataFrame(final_records).drop(columns=['merge_key'])
        result_df = result_df.reset_index(drop=True)
        
        removed_count = len(df) - len(result_df)
        logger.info(f"普通化学品去重完成：原始 {len(df)} 条，去重后 {len(result_df)} 条，删除重复 {removed_count} 条")
        
        return result_df
    
    def _resolve_name_cas_conflicts(self, df: pd.DataFrame) -> pd.DataFrame:
        """解决同一化学品名称对应多个CAS号的冲突 - 保留最正常的CAS号"""
        if '中文名称' not in df.columns or 'CAS号或流水号' not in df.columns:
            return df
        
        name_cas_conflicts = {}
        
        # 按中文名称分组，检查CAS号冲突
        for name, group in df.groupby('中文名称'):
            if pd.isna(name) or str(name).strip() == '':
                continue
            
            cas_numbers = group['CAS号或流水号'].dropna().astype(str).str.strip()
            cas_numbers = cas_numbers[cas_numbers != '']
            cas_numbers = cas_numbers[cas_numbers != 'nan']
            unique_cas = cas_numbers.unique()
            
            if len(unique_cas) > 1:
                name_cas_conflicts[name] = unique_cas
                logger.warning(f"化学品 '{name}' 对应多个CAS号: {list(unique_cas)}")
        
        # 解决冲突：为每个冲突的化学品选择最正常的CAS号
        if name_cas_conflicts:
            logger.info(f"发现 {len(name_cas_conflicts)} 个化学品名称与CAS号冲突，开始选择最正常的CAS号...")
            
            for name, cas_list in name_cas_conflicts.items():
                # 选择最正常的CAS号
                best_cas = self._select_best_cas_for_name(df, name, cas_list)
                
                # 更新所有该名称的记录，统一使用最正常的CAS号
                mask = df['中文名称'] == name
                df.loc[mask, 'CAS号或流水号'] = best_cas
                
                logger.info(f"  化学品 '{name}' 选择CAS号: {best_cas} (从 {list(cas_list)} 中选择)")
        
        return df
    
    def _select_best_cas_for_name(self, df: pd.DataFrame, chemical_name: str, cas_list: list) -> str:
        """为化学品名称选择最正常的CAS号"""
        cas_scores = {}
        
        # 为每个CAS号计算正常性得分
        for cas in cas_list:
            records_with_cas = df[(df['中文名称'] == chemical_name) & (df['CAS号或流水号'] == cas)]
            
            score = 0
            
            # 1. CAS号格式检查 (最重要的因素)
            if self._is_valid_cas_format(cas):
                score += 10  # 标准CAS号格式得高分
            elif cas.isdigit() and len(cas) >= 3:
                score += 2   # 纯数字流水号得低分
            
            # 2. 记录数量 (越多记录越可能正确)
            score += len(records_with_cas) * 0.5
            
            # 3. 数据完整性
            for _, record in records_with_cas.iterrows():
                # 有分子式
                if pd.notna(record.get('分子式')) and str(record.get('分子式')).strip():
                    score += 1
                # 有分子量
                if pd.notna(record.get('分子量')) and str(record.get('分子量')).strip():
                    score += 0.5
                # 有英文名称
                if pd.notna(record.get('英文名称')) and str(record.get('英文名称')).strip():
                    score += 1
                # 有详细用途信息
                if pd.notna(record.get('用途')) and len(str(record.get('用途')).strip()) > 5:
                    score += 0.5
                # 有生产来源信息
                if pd.notna(record.get('生产来源')) and str(record.get('生产来源')).strip():
                    score += 0.5
            
            cas_scores[cas] = score
        
        # 选择得分最高的CAS号
        best_cas = max(cas_scores.keys(), key=lambda x: cas_scores[x])
        logger.debug(f"    CAS号选择详情 {chemical_name}:")
        for cas, score in sorted(cas_scores.items(), key=lambda x: x[1], reverse=True):
            logger.debug(f"      {cas}: {score:.1f}分")
        
        return best_cas
    
    def _is_valid_cas_format(self, cas: str) -> bool:
        """检查是否为标准CAS号格式 (如: 67-56-1)"""
        import re
        if not cas or pd.isna(cas):
            return False
        
        cas_str = str(cas).strip()
        # 标准CAS号格式: 数字-数字-数字
        cas_pattern = r'^\d+-\d+-\d+$'
        return bool(re.match(cas_pattern, cas_str))
    
    def _process_dangerous_chemicals(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理危化品记录，按名称+浓度阈值创建唯一标识"""
        if len(df) == 0:
            return df
            
        logger.info(f"处理危化品记录，共 {len(df)} 条（保留所有记录）")
        
        # 为危化品创建复合标识符
        df_work = df.copy()
        
        # 基础名称
        base_name = df_work['中文名称'].fillna('').astype(str).str.strip()
        
        # 浓度阈值
        if '浓度阈值' in df_work.columns:
            threshold = df_work['浓度阈值'].fillna('').astype(str).str.strip()
            # 为每个危化品创建唯一标识：名称_浓度阈值
            df_work['dangerous_id'] = base_name + '_' + threshold
            # 去除末尾的下划线（当浓度阈值为空时）
            df_work['dangerous_id'] = df_work['dangerous_id'].str.rstrip('_')
        else:
            df_work['dangerous_id'] = base_name
        
        # 统计危化品的分布
        unique_combinations = df_work['dangerous_id'].nunique()
        logger.info(f"危化品共有 {unique_combinations} 种不同的名称+浓度组合")
        
        # 移除辅助列
        if 'dangerous_id' in df_work.columns:
            df_work = df_work.drop(columns=['dangerous_id'])
        
        return df_work
        
        # 保留的记录
        final_records = []
        
        # 处理非重复记录
        for key, group in grouped:
            if len(group) == 1:
                final_records.append(group.iloc[0])
            else:
                # 处理重复记录，选择数据质量最好的
                best_record = self._select_best_record(group, str(key))
                final_records.append(best_record)
        
        # 重新构建DataFrame
        result_df = pd.DataFrame(final_records).drop(columns=['merge_key'])
        result_df = result_df.reset_index(drop=True)
        
        removed_count = original_count - len(result_df)
        logger.info(f"去重完成：原始记录 {original_count} 条，最终记录 {len(result_df)} 条，删除重复记录 {removed_count} 条")
        
        return result_df
    
    def _select_best_record(self, group: pd.DataFrame, merge_key: str) -> pd.Series:
        """从重复记录组中合并信息，创建最完整的记录"""
        logger.debug(f"合并重复记录：{merge_key}，共 {len(group)} 条记录")
        
        # 创建合并后的记录
        merged_record = {}
        
        # 对每个字段进行智能合并
        for column in group.columns:
            if column == 'merge_key':
                continue
                
            # 获取所有非空值
            values = group[column].dropna().astype(str).str.strip()
            values = values[values != '']
            values = values[values != 'nan']
            
            if len(values) == 0:
                merged_record[column] = ''
            elif len(values) == 1:
                merged_record[column] = values.iloc[0]
            else:
                # 多个值需要智能合并
                unique_values = values.unique()
                if len(unique_values) == 1:
                    # 所有值都相同
                    merged_record[column] = unique_values[0]
                else:
                    # 值不同，需要合并策略
                    merged_record[column] = self._merge_field_values(column, list(unique_values), group)
        
        # 转换为Series返回
        result = pd.Series(merged_record, name=group.index[0])
        
        logger.debug(f"  合并完成: {result.get('中文名称', merge_key)}")
        return result
    
    def _merge_field_values(self, field_name: str, values: list, group: pd.DataFrame) -> str:
        """智能合并字段值"""
        
        # 特殊字段的合并策略
        if field_name == 'CAS号或流水号':
            # CAS号在冲突解决阶段已经统一，这里应该都相同
            # 选择非空的第一个
            for val in values:
                if val and str(val).strip():
                    return str(val).strip()
            return ''
        
        elif field_name in ['分子式', 'molecular_formula:string', '分子量', 'molecular_weight:string', '英文名称']:
            # 选择最完整的值（非空且长度最长）
            valid_values = [v for v in values if v and str(v).strip()]
            if valid_values:
                return max(valid_values, key=len)
            return ''
        
        elif field_name in ['用途', 'uses:string', '生产来源', 'production_source:string', '自然来源', 'natural_source:string', '备注']:
            # 文本类字段，合并所有不同的值
            unique_values = list(set(str(v).strip() for v in values if v and str(v).strip()))
            if len(unique_values) == 1:
                return unique_values[0]
            elif len(unique_values) > 1:
                # 合并多个值，用分号分隔
                return '; '.join(sorted(unique_values))
            return ''
        
        elif field_name in ['中文别名', 'aliases:string[]', '英文别名', 'english_aliases:string[]', '别名']:
            # 别名字段，合并所有不同的别名
            all_aliases = []
            for val in values:
                val_str = str(val).strip()
                # 分割可能的多个别名（用逗号、分号、中文顿号分隔）
                import re
                aliases = re.split(r'[,;，；、]', val_str)
                for alias in aliases:
                    alias = alias.strip()
                    if alias and alias not in all_aliases:
                        all_aliases.append(alias)
            
            return '; '.join(all_aliases) if all_aliases else ''
        
        elif field_name in ['危险特性', '包装类别', '包装标志']:
            # 危险品相关字段，选择级别最高的
            danger_levels = {'I': 3, 'II': 2, 'III': 1, '1': 3, '2': 2, '3': 1}
            max_danger = ''
            max_level = 0
            
            for val in values:
                val_str = str(val).strip()
                level = danger_levels.get(val_str, 0)
                if level > max_level:
                    max_level = level
                    max_danger = val_str
            
            return max_danger if max_danger else (values[0] if len(values) > 0 else '')
        
        else:
            # 默认策略：选择最长的非空值
            valid_values = [v for v in values if v and str(v).strip()]
            if valid_values:
                return max(valid_values, key=lambda x: len(str(x)))
            return ''
    
    def _calculate_record_quality(self, record: pd.Series) -> float:
        """计算单条记录的数据质量得分"""
        score = 0.0
        
        # 关键字段权重
        field_weights = {
            '中文名称': 0.3,
            'CAS号或流水号': 0.2,
            '分子式': 0.15,
            '英文名称': 0.1,
            '用途': 0.1,
            '危害': 0.05,
            '防范': 0.05,
            '性质': 0.05
        }
        
        for field, weight in field_weights.items():
            if field in record.index and pd.notna(record[field]):
                value = str(record[field]).strip()
                if value and value.lower() not in ['nan', '', 'null', '无', '未知']:
                    # 给有效数据加分
                    field_score = weight
                    # 根据内容长度调整得分（更多内容 = 更高质量）
                    if len(value) > 5:
                        field_score *= 1.2
                    elif len(value) > 2:
                        field_score *= 1.0
                    else:
                        field_score *= 0.8
                    score += field_score
        
        # 总的非空字段数量奖励
        non_empty_fields = sum(1 for val in record.values 
                              if pd.notna(val) and str(val).strip() not in ['', 'nan', 'null', '无', '未知'])
        completeness_bonus = min(non_empty_fields / 20, 0.2)  # 最多20%的完整性奖励
        score += completeness_bonus
        
        return score

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
            '分子量': 'molecular_weight:string',
            '危害': 'hazard:string',
            '防范': 'prevention:string',
            '危害处置': 'hazard_disposal:string',
            '用途': 'uses:string',
            '自然来源': 'natural_source:string',
            '生产来源': 'production_source:string',
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

            is_dangerous = df['是否为危化品'].astype(str).str.strip() == '是'
            formatted_df[':LABEL'] = np.where(is_dangerous, 'DangerousChemical;Chemical', 'Chemical')

            # 只在危化品中保留危害、防范、处置、浓度阈值
            for col in ['hazard:string', 'prevention:string', 'hazard_disposal:string', 'concentration_threshold:string']:
                if col in formatted_df.columns:
                    formatted_df.loc[~is_dangerous, col] = ''
        else:
            formatted_df[':LABEL'] = 'Chemical'
            # 没有危化品标记时，全部清空相关字段
            for col in ['hazard:string', 'prevention:string', 'hazard_disposal:string', 'concentration_threshold:string']:
                if col in formatted_df.columns:
                    formatted_df[col] = ''

        # 过滤非危化品的浓度阈值
        if 'concentration_threshold:string' in formatted_df.columns and '是否为危化品' in df.columns:
            formatted_df.loc[df['是否为危化品'].astype(str).str.strip() != '是', 'concentration_threshold:string'] = ''

        # 处理数组列格式，并清理别名
        array_columns = ['aliases:string[]', 'english_aliases:string[]']
        for col in array_columns:
            if col in formatted_df.columns:
                # 清理别名中的符号，然后格式化
                formatted_df[col] = formatted_df[col].astype(str).apply(
                    lambda x: ';'.join([self.clean_chemical_name_for_neo4j(alias.strip()) 
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

        logger.info("开始提取上游原料关系（双向）...")
        relationships = []

        # 获取已知化学品名称库
        known_chemicals = self.get_known_chemical_names()
        logger.info(f"已知化学品库包含 {len(known_chemicals)} 个化学品名称")


        # 构建所有化学品节点及其别名的映射
        name_to_main = dict()  # 别名/主名 -> 主名
        all_main_names = set()
        for _, row in df.iterrows():
            main_name = self.clean_chemical_name_for_neo4j(row['name:ID'])
            if not main_name:
                continue
            all_main_names.add(main_name)
            name_to_main[main_name] = main_name
            # 处理别名
            for alias_col in ['aliases:string[]', '中文别名']:
                if alias_col in row.index and pd.notna(row[alias_col]):
                    aliases = str(row[alias_col]).split(';')
                    for alias in aliases:
                        alias_clean = self.clean_chemical_name_for_neo4j(alias.strip())
                        if alias_clean:
                            name_to_main[alias_clean] = main_name

        for _, row in df.iterrows():
            chemical_name = self.clean_chemical_name_for_neo4j(row['name:ID'])

            # 提取上游原料关系：生产来源 → 工艺 → 当前化学品
            if 'production_source:string' in row.index and pd.notna(row['production_source:string']):
                source_text = str(row['production_source:string'])
                upstream_materials = self.extract_upstream_materials(source_text, known_chemicals, chemical_name)

                if upstream_materials:
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical_name)
                    if clean_chemical:
                        # 为每个目标化学品创建一个统一的工艺节点
                        process_name = f"{clean_chemical}_生产工艺"

                        # 工艺 → 当前化学品（产生）
                        relationships.append({
                            ':START_ID': process_name,
                            ':END_ID': clean_chemical,
                            ':TYPE': '产生'
                        })
                        # 当前化学品 → 工艺（来源）
                        relationships.append({
                            ':START_ID': clean_chemical,
                            ':END_ID': process_name,
                            ':TYPE': '来源'
                        })

                        # 所有上游原料都指向这一个工艺节点（参与/原料双向）
                        for material in upstream_materials:
                            clean_material = self.clean_chemical_name_for_neo4j(material)
                            if clean_material and clean_material != clean_chemical:
                                # 判断原料是否为已存在化学品节点或其别名
                                node_name = name_to_main.get(clean_material, clean_material)
                                # 原料/别名 → 工艺（参与）
                                relationships.append({
                                    ':START_ID': node_name,
                                    ':END_ID': process_name,
                                    ':TYPE': '参与'
                                })
                                # 工艺 → 原料/别名（原料）
                                relationships.append({
                                    ':START_ID': process_name,
                                    ':END_ID': node_name,
                                    ':TYPE': '原料'
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
            # 处理现在的命名格式：化学品_生产工艺
            if process_name.endswith('_生产工艺'):
                target_chemical = process_name.replace('_生产工艺', '')
                
                process_nodes.append({
                    'name:ID': process_name,
                    ':LABEL': 'Process',
                    'process_type:string': '化学生产工艺',
                    'target_product:string': target_chemical,
                    'description:string': f'{target_chemical}的生产工艺流程',
                    'source:string': 'system_generated',
                    'data_status:string': 'active'
                })
            # 兼容旧的命名格式：原料_制备_产品_工艺
            elif '_制备_' in process_name and process_name.endswith('_工艺'):
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
                        'data_status:string': 'active'
                    })
            # 其他包含"工艺"的节点
            else:
                process_nodes.append({
                    'name:ID': process_name,
                    ':LABEL': 'Process',
                    'process_type:string': '化学工艺',
                    'description:string': f'{process_name}相关的化学工艺流程',
                    'source:string': 'system_generated',
                    'data_status:string': 'active'
                })
        
        logger.info(f"从关系中提取了 {len(process_nodes)} 个工艺节点")
        return pd.DataFrame(process_nodes)

    def clean_chemical_name_for_neo4j(self, name: str) -> str:

        """清理化学品名称用于Neo4j录入，保留-和,，去除其他符号"""
        if not name:
            return name
        
        # 保留中文字符、英文字母、数字、连字符(-)和逗号(,)
        # 去除其他特殊符号如：（）[]{}【】""''：:；;等等·！!？？
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\-,，]', '', name)
        # 去除末尾常见中文关联词，防止识别错误
        # 关联词列表可根据实际需要扩展
        common_connectives = [
            '为', '在', '和', '与', '主要', '包括', '以及', '及', '或', '等', '用', '作', '作为',
            '通过', '经过', '利用', '使用', '含', '含有', '属于', '来源于', '来自', '经过', '等', '等物', '等品'
        ]
        # 按长度从长到短排序，优先去除长词
        common_connectives = sorted(common_connectives, key=len, reverse=True)
        for conn in common_connectives:
            if cleaned.endswith(conn):
                cleaned = cleaned[:-len(conn)]
                break
        return cleaned
        # 清理多余的逗号和连字符
        cleaned = re.sub(r'[-,，]+$', '', cleaned)  # 去除末尾的连字符和逗号
        cleaned = re.sub(r'^[-,，]+', '', cleaned)  # 去除开头的连字符和逗号
        cleaned = re.sub(r'[-]{2,}', '-', cleaned)  # 多个连字符合并为一个
        cleaned = re.sub(r'[,，]{2,}', ',', cleaned)  # 多个逗号合并为一个
        
        return cleaned.strip()

    def smart_split_chemicals(self, text: str) -> list:
        """智能切分化学品名称，保护化学品内部的连接符号"""
        if not text:
            return []
        
        # 预处理：保护化学品中的数字,数字模式和数字-化学基团模式
        protected_text = text
        
        # 保护模式1: 数字,数字- (如: 2,4-二甲基)
        protected_text = re.sub(r'(\d+),(\d+)-', r'\1◆\2◇', protected_text)
        
        # 保护模式2: 数字,数字,数字- (如: 1,2,3-三甲基)  
        protected_text = re.sub(r'(\d+),(\d+),(\d+)-', r'\1◆\2◆\3◇', protected_text)
        
        # 保护模式3: 数字-字母/中文 (如: 3-甲基, N-乙基)
        protected_text = re.sub(r'(\d+|[A-Za-z]+)-([^\s,，、;；和与及或等]+)', r'\1◇\2', protected_text)
        
        # 保护模式4: 字母-数字 (如: α-1)
        protected_text = re.sub(r'([A-Za-z]+)-(\d+)', r'\1◇\2', protected_text)
        
        # 保护模式5: 中文-中文 (如: 聚-氯乙烯)
        protected_text = re.sub(r'([\u4e00-\u9fa5]+)-([\u4e00-\u9fa5]+)', r'\1◇\2', protected_text)
        
        # 保护模式6: 数字,数字(没有连字符的情况，如: 1,2-二氯)
        protected_text = re.sub(r'(\d+),(\d+)(?=[^\d,])', r'\1◆\2', protected_text)
        
        # 现在进行切分，使用真正的分隔符
        # 主要分隔符：、 ； ; 和 与 及 或 等 以及不在保护模式中的逗号
        parts = re.split(r'[、;；和与及或等]+|(?<!◆),(?![^◆]*◇)', protected_text)
        
        # 恢复保护的字符并清理
        cleaned_parts = []
        for part in parts:
            if part.strip():
                # 恢复原始字符
                restored = part.replace('◆', ',').replace('◇', '-').strip()
                
                # 清理前缀和后缀词汇
                cleaned = restored
                # 移除常见前缀
                cleaned = re.sub(r'^(?:由|从|以|通过|经过|利用|使用|主要|含|含有)\s*', '', cleaned)
                # 移除常见后缀
                cleaned = re.sub(r'\s*(?:等|类|制得|制备|制成|生产|合成|反应|裂解|氧化|分解|化学品|原料|物质|材料|中提取|为原料|作为溶剂).*$', '', cleaned)
                
                cleaned = cleaned.strip()
                if cleaned and len(cleaned) >= 2:
                    cleaned_parts.append(cleaned)
        
        return cleaned_parts

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
                # 使用智能切分方法，保护化学品内部的连接符号
                chemicals_in_match = self.smart_split_chemicals(match.strip())
                
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
                            # 验证是否为有效的化学品名称
                            if self.is_valid_chemical_name(clean_name):
                                # 新发现的化学品，添加到数据库
                                self.add_new_chemical_to_database(clean_name)
                                upstream_materials.add(clean_name)
                                logger.info(f"发现并添加新上游化学品: {clean_name}")
        
        # 方法3: 直接扫描已知化学品名称（针对性匹配）
        # 按长度排序，优先匹配长名称
        sorted_chemicals = sorted(all_chemicals, key=len, reverse=True)
        for chemical in sorted_chemicals:
            if chemical != target_chemical and len(chemical) >= 2 and chemical in cleaned_text:
                # 确保匹配到的是完整词汇，不是其他词的一部分
                pattern = r'(?:^|[^a-zA-Z\u4e00-\u9fa5])' + re.escape(chemical) + r'(?:[^a-zA-Z\u4e00-\u9fa5]|$)'
                if re.search(pattern, cleaned_text):
                    # 对匹配到的化学品名称进行清理
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical)
                    if clean_chemical:
                        upstream_materials.add(clean_chemical)
                        logger.debug(f"通过扫描发现上游化学品: {clean_chemical}")
                        
                        # 限制结果数量，避免过度匹配
                        if len(upstream_materials) >= 5:
                            break
                
        logger.info(f"为 {target_chemical} 提取到上游原料: {upstream_materials}")
        return upstream_materials

    def remove_producer_info(self, text: str) -> str:
        """移除生产商信息 - 增强版，包括生厂商和生产商后面的所有文字"""
        if not text or text == 'nan' or pd.isna(text):
            return ""
        
        text = str(text)  # 确保是字符串
        
        # 生产商信息识别模式 - 更全面的匹配
        producer_patterns = [
            # 匹配"生产商"、"生厂商"及其后的所有内容直到句子结束
            r'生产商.*?(?=[。；;，,]|$)',
            r'生厂商.*?(?=[。；;，,]|$)',
            r'主要生产商.*?(?=[。；;，,]|$)',
            r'制造商.*?(?=[。；;，,]|$)', 
            r'生产企业.*?(?=[。；;，,]|$)',
            r'制造企业.*?(?=[。；;，,]|$)',
            # 匹配包含公司、企业、厂、集团等的整个短语
            r'[^。；;，,]*(?:公司|企业|厂|集团|有限责任公司|股份有限公司|工厂|制造厂)[^。；;，,]*(?:[。；;，,]|$)',
            # 匹配"主要生产商包括/有"这种模式后的所有内容
            r'主要生产商[有包括：:].+?(?=[。；;]|$)',
            r'生产商[有包括：:].+?(?=[。；;]|$)',
            r'制造商[有包括：:].+?(?=[。；;]|$)',
        ]
        
        cleaned_text = text
        for pattern in producer_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # 只清理多余的标点符号，保留列表分隔符（逗号）和字段分隔符（冒号）
        # 移除连续的标点符号，但保留单个逗号和冒号
        cleaned_text = re.sub(r'[；;]+', '', cleaned_text)  # 移除分号
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)   # 标准化空格
        
        return cleaned_text.strip()

    def extract_braced_content(self, text: str) -> List[str]:
        """提取花括号{}内的内容并按符号分隔"""
        if not text:
            return []
        
        # 提取所有花括号内的内容
        braced_pattern = r'\{([^}]+)\}'
        matches = re.findall(braced_pattern, text)
        
        result = []
        for match in matches:
            # 按常见分隔符分割内容，但要考虑特殊情况
            # 首先按主要分隔符分割
            main_separators = [';', '；', '、']
            items = [match]
            
            # 使用主要分隔符进行分割
            for sep in main_separators:
                new_items = []
                for item in items:
                    new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                items = new_items
            
            # 如果没有主要分隔符，再尝试次要分隔符
            if len(items) == 1:
                secondary_separators = [',', '，']
                for sep in secondary_separators:
                    new_items = []
                    for item in items:
                        # 对于包含"GB/T"、"ISO"等标准号的情况，要小心处理
                        if any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                            # 如果包含标准号，只在没有标准号格式的逗号处分割
                            parts = item.split(sep)
                            temp_result = []
                            i = 0
                            while i < len(parts):
                                current = parts[i].strip()
                                # 检查下一部分是否是标准号的一部分
                                if (i + 1 < len(parts) and 
                                    any(std in current for std in ['GB', 'ISO', 'ASTM', 'JIS', 'DIN']) and
                                    re.match(r'^\s*[T\s]*\d+', parts[i + 1])):
                                    # 合并标准号部分
                                    current = current + sep + parts[i + 1].strip()
                                    i += 1
                                if current:
                                    temp_result.append(current)
                                i += 1
                            new_items.extend(temp_result)
                        else:
                            new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                    items = new_items
            
            # 进一步处理其他分隔符（但要更谨慎）
            final_items = []
            for item in items:
                # 只对不包含标准号的项目使用其他分隔符
                if not any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                    other_separators = ['|', '/', '\\', '&', '+']
                    temp_items = [item]
                    for sep in other_separators:
                        new_temp_items = []
                        for temp_item in temp_items:
                            new_temp_items.extend([i.strip() for i in temp_item.split(sep) if i.strip()])
                        temp_items = new_temp_items
                    final_items.extend(temp_items)
                else:
                    final_items.append(item)
            
            # 过滤掉空值和过短的内容
            valid_items = [item for item in final_items if len(item.strip()) > 1]
            result.extend(valid_items)
        
        # 去重并保持顺序
        seen = set()
        unique_result = []
        for item in result:
            if item not in seen:
                seen.add(item)
                unique_result.append(item)
        
        return unique_result

    def process_data_source_field(self, text: str) -> str:
        """处理数据来源字段，提取花括号内容或标明本地搜索来源"""
        if not text or text == 'nan' or pd.isna(text):
            return "根据通义大模型蒸馏而来"
        
        text = str(text)  # 确保是字符串
        
        # 提取花括号内容
        braced_content = self.extract_braced_content(text)
        
        if braced_content:
            # 将提取的内容用分号连接
            return '; '.join(braced_content)
        else:
            # 如果没有花括号内容，说明是本地搜索来源
            return "根据通义大模型蒸馏而来"

    def clean_chemical_name_advanced(self, name: str) -> str:
        """高级化学品名称清理"""
        if not name:
            return ""
        
        # 移除常见的中文填充词汇
        exclude_words = {
            '包括', '等', '类', '中', '的', '和', '与', '及', '或', '各种', '多种', 
            '主要', '广泛', '常见', '一般', '通常', '特别', '尤其', '特殊',
            '以及', '还有', '另外', '此外', '同时', '另', '其他', '部分',
            '如', '例如', '比如', '诸如', '像', '譬如', '好比', '如同',
            '等等', '之类', '左右', '上下', '约', '大约', '接近', '差不多',
            '括', '（', '）', '(', ')', '[', ']', '【', '】'
        }
        
        clean_name = name.strip()
        
        # 移除括号及其内容
        clean_name = re.sub(r'[（(][^）)]*[）)]', '', clean_name)
        clean_name = re.sub(r'[【\[][^\】\]]*[\】\]]', '', clean_name)
        
        # 移除填充词汇
        for word in exclude_words:
            clean_name = clean_name.replace(word, '')
        
        # 移除数字编号
        clean_name = re.sub(r'^\d+[\.、]\s*', '', clean_name)
        
        # 移除多余的空格和标点
        clean_name = re.sub(r'[，,；;：:。\.]+', '', clean_name)
        clean_name = re.sub(r'\s+', '', clean_name)
        
        # 确保是有效的化学品名称
        if len(clean_name) < 2 or len(clean_name) > 50:
            return ""
        
        # 排除明显不是化学品的词汇
        non_chemical_patterns = [
            r'^[等类中的和与及或]+$',
            r'^[\d\s\.,，。；;：:]+$',
            r'^[用于作为通过经过]+',
        ]
        
        for pattern in non_chemical_patterns:
            if re.match(pattern, clean_name):
                return ""
        
        return clean_name

    def clean_chemical_name(self, name: str) -> str:
        """清理和标准化化学品名称"""
        if not name:
            return ""
        
        # 移除括号内容和特殊符号
        clean_name = re.sub(r'[()（）【】\[\]<>《》].*?[()（）【】\[\]<>《》]', '', name)
        clean_name = re.sub(r'[()（）【】\[\]<>《》]', '', clean_name)
        
        # 移除英文描述
        clean_name = re.sub(r'[A-Za-z\s]+', '', clean_name)
        
        # 移除数字和特殊符号（保留化学品名称中的常见字符）
        clean_name = re.sub(r'[0-9%％\-\+\=\*\#\@\$\&\^\~\`\|\\\"\']', '', clean_name)
        
        # 只保留中文字符和少数特殊化学符号
        clean_name = re.sub(r'[^\u4e00-\u9fa5·]', '', clean_name)
        
        # 移除过短或过长的名称
        clean_name = clean_name.strip()
        if len(clean_name) < 2 or len(clean_name) > 20:
            return ""
        
        # 移除明显不是化学品的词汇
        exclude_words = {
            '等', '类', '中', '上', '下', '前', '后', '左', '右', '内', '外', '高', '低', 
            '大', '小', '多', '少', '好', '坏', '新', '旧', '快', '慢', '热', '冷',
            '工业', '农业', '医药', '食品', '化工', '石油', '煤炭', '天然气',
            '生产', '制造', '加工', '处理', '使用', '应用', '方法', '技术', '工艺'
        }
        
        if clean_name in exclude_words:
            return ""
        
        return clean_name
        
    def validate_chemical_name(self, name: str, known_chemicals: Set[str]) -> bool:
        """验证是否为有效的化学品名称"""
        if not name or len(name) < 2:
            return False
            
        # 精确匹配已知化学品
        if name in known_chemicals:
            return True
            
        # 模糊匹配：检查是否包含在已知化学品中
        for known in known_chemicals:
            if name in known or known in name:
                if abs(len(name) - len(known)) <= 2:  # 长度差异不大
                    return True
        
        # 基于化学品命名规律的验证
        chemical_suffixes = ['酸', '碱', '盐', '醇', '醛', '酮', '酯', '醚', '烷', '烯', '炔', '苯', '酚']
        chemical_prefixes = ['甲', '乙', '丙', '丁', '戊', '己', '庚', '辛', '壬', '癸']
        
        # 检查是否符合化学品命名模式
        has_chemical_pattern = (
            any(name.endswith(suffix) for suffix in chemical_suffixes) or
            any(name.startswith(prefix) for prefix in chemical_prefixes) or
            '氢' in name or '氧' in name or '氮' in name or '氯' in name or '硫' in name or
            '钠' in name or '钾' in name or '钙' in name or '镁' in name or '铁' in name or
            '铜' in name or '锌' in name or '铅' in name or '汞' in name or '银' in name
        )
        
        return has_chemical_pattern
        
    def determine_relationship_type(self, text: str, source: str, target: str) -> str:
        """根据上下文确定关系类型"""
        text_lower = text.lower()
        
        if any(keyword in text_lower for keyword in ['上游', '原料', '制备', '制造', '生产']):
            return '工艺'
        elif any(keyword in text_lower for keyword in ['反应', '化合', '分解', '氧化', '还原']):
            return '化学反应'
        elif any(keyword in text_lower for keyword in ['用途', '应用', '制取']):
            return '用途'
        elif any(keyword in text_lower for keyword in ['转化', '变成', '得到']):
            return '转化'
        else:
            return '工艺'  # 默认关系类型

    def create_standard_legal_relationships(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """为每个化学品创建标准/法律关系"""
        logger.info("开始创建标准/法律关系...")
        
        if nodes_df.empty:
            logger.info("没有节点数据，跳过标准/法律关系创建")
            return pd.DataFrame()
        
        # 尝试导入标准/法律管理器
        try:
            import sys
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            
            # 使用标准/法律管理器
            manager = StandardLegalManager(str(self.base_path))
            _, relationships = manager.update_standard_legal_relationships(nodes_df)
            
            # 检查是否有有效的标准法律数据
            if not relationships.empty and len(relationships) > 0:
                # 检查第一行数据是否为占位符
                first_row = relationships.iloc[0]
                if ':END_ID' in first_row and str(first_row[':END_ID']).startswith('标准法律_'):
                    logger.info("检测到占位符标准法律数据，跳过创建无意义的关系")
                    return pd.DataFrame()
                else:
                    logger.info(f"使用标准/法律管理器创建了 {len(relationships)} 条标准/法律关系")
                    return relationships
            else:
                logger.info("没有有效的标准/法律数据，跳过关系创建")
                return pd.DataFrame()
                
        except ImportError:
            logger.warning("无法导入标准/法律管理器，跳过标准/法律关系创建")
            # 不再创建占位符关系，直接返回空DataFrame
            logger.info("跳过创建占位符标准/法律关系")
            return pd.DataFrame()

    def create_standard_legal_nodes(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """创建标准/法律节点"""
        logger.info("开始创建标准/法律节点...")
        
        if nodes_df.empty:
            logger.info("没有化学品节点数据，跳过标准/法律节点创建")
            return pd.DataFrame()
        
        # 尝试使用标准/法律管理器
        try:
            import sys
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            
            # 使用标准/法律管理器
            manager = StandardLegalManager(str(self.base_path))
            nodes, _ = manager.update_standard_legal_relationships(nodes_df)
            
            # 检查是否有有效的标准法律数据
            if not nodes.empty and len(nodes) > 0:
                # 检查第一行数据是否为占位符
                first_row = nodes.iloc[0]
                if 'name:ID' in first_row and str(first_row['name:ID']).startswith('标准法律_'):
                    logger.info("检测到占位符标准法律节点，跳过创建无意义的节点")
                    return pd.DataFrame()
                else:
                    logger.info(f"使用标准/法律管理器创建了 {len(nodes)} 个标准/法律节点")
                    return nodes
            else:
                logger.info("没有有效的标准/法律数据，跳过节点创建")
                return pd.DataFrame()
                
        except ImportError:
            logger.warning("无法导入标准/法律管理器，跳过标准/法律节点创建")
            # 不再创建占位符节点，直接返回空DataFrame
            logger.info("跳过创建占位符标准/法律节点")
            return pd.DataFrame()

    def save_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame, include_standard_legal: bool = True):
        """保存Neo4j格式的数据"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 创建标准/法律节点和关系
        if include_standard_legal and not nodes_df.empty:
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            
            # 合并节点数据
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:
                combined_nodes = nodes_df
                
            # 合并关系数据
            if not standard_legal_relationships.empty:
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:
                combined_relationships = relationships_df
        else:
            combined_nodes = nodes_df
            combined_relationships = relationships_df

        # 保存节点数据（分批）
        if not combined_nodes.empty:
            batch_size = 3000
            num_batches = (len(combined_nodes) + batch_size - 1) // batch_size
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(combined_nodes))
                batch_data = combined_nodes.iloc[start_idx:end_idx]
                
                filename = f"neo4j_ready_chemicals_batch_{i+1}_{timestamp}.csv"
                filepath = self.success_dir / filename
                batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
                logger.info(f"保存Neo4j节点批次 {i+1}: {filename}")

        # 保存关系数据
        if not combined_relationships.empty:
            rel_filename = f"neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.success_dir / rel_filename
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存Neo4j关系数据: {rel_filename}")
            
        # 统计信息
        chemical_count = len(nodes_df[nodes_df[':LABEL'].str.contains('Chemical', na=False)]) if not nodes_df.empty else 0
        process_count = len(nodes_df[nodes_df[':LABEL'] == 'Process']) if not nodes_df.empty else 0
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
        """阶段一：数据合并与预处理（支持增量更新）"""
        logger.info("=" * 50)
        if incremental_mode:
            logger.info("开始阶段一：增量数据处理")
        else:
            logger.info("开始阶段一：完整数据合并与预处理")
        
        # 1. 加载危化品目录
        self.load_dangerous_chemicals()
        
        # 2. 根据模式选择文件
        if incremental_mode:
            new_files = self.get_new_files()
            if not new_files:
                logger.info("没有新文件需要处理")
                return
            logger.info(f"增量模式：处理 {len(new_files)} 个新文件")
            
            # 按文件处理增量数据
            files_by_batch = {}
            for file_path in new_files:
                batch_num = self.extract_batch_number(file_path.name)
                if batch_num is not None:
                    if batch_num not in files_by_batch:
                        files_by_batch[batch_num] = []
                    files_by_batch[batch_num].append(file_path)
        else:
            # 按序号组织所有文件
            files_by_batch = self.organize_files_by_batch()
            if not files_by_batch:
                logger.error("未找到任何batch文件")
                return
        
        # 3. 合并重复批次文件
        merged_batches = self.merge_duplicate_batches(files_by_batch)
        
        # 4. 检查缺失批次（仅在完整模式下）
        if not incremental_mode:
            missing_batches = self.check_missing_batches(merged_batches)
        
        # 5. 合并所有数据
        if merged_batches:
            batch_sizes = [len(df) for df in merged_batches.values()]
            total_before_merge = sum(batch_sizes)
            logger.info(f"各批次数据量: {dict(zip(merged_batches.keys(), batch_sizes))}")
            logger.info(f"合并前总计: {total_before_merge} 条记录")
            
            all_data = pd.concat(merged_batches.values(), ignore_index=True)
            logger.info(f"合并完成，总计 {len(all_data)} 条记录")
            
            # 6. 合并重复记录
            logger.info(f"去重前记录数: {len(all_data)}")
            all_data = self.merge_duplicate_records(all_data)
            compression_ratio = (1 - len(all_data) / total_before_merge) * 100
            logger.info(f"去重后记录数: {len(all_data)}")
            logger.info(f"数据压缩率: {compression_ratio:.1f}%")
        else:
            logger.error("没有成功合并任何数据")
            return

        logger.info("开始数据验证...")
        valid_data, validation_failed_data, validation_errors = self.validate_chemical_data(all_data)
        
        if validation_errors:
            logger.warning("数据验证发现问题:")
            for error in validation_errors:
                logger.warning(f"  - {error}")

        # 8. 识别处理失败记录
        success_data, processing_failed_data = self.identify_failed_records(valid_data)

        # 8. 检查缺失的危化品（仅在完整模式下）
        if not incremental_mode:
            missing_dangerous = self.find_missing_dangerous_chemicals(success_data)
        else:
            missing_dangerous = pd.DataFrame()

        # 9. 合并所有失败数据
        all_failed_data = []
        if len(validation_failed_data) > 0:
            all_failed_data.append(validation_failed_data)
        if len(processing_failed_data) > 0:
            all_failed_data.append(processing_failed_data)

        # 10. 保存失败数据
        combined_failed = pd.concat(all_failed_data, ignore_index=True) if all_failed_data else pd.DataFrame()
        self.save_failed_data(combined_failed, missing_batches, missing_dangerous)

        # 11. 保存成功数据
        if not success_data.empty:
            if incremental_mode:
                # 增量模式：保存到专门的增量目录
                self.save_incremental_data(success_data)
            else:
                # 完整模式：正常保存
                self.save_batch_data(success_data)
            
            # 更新已处理文件记录
            if incremental_mode and 'new_files' in locals():
                self.update_processed_files_record(new_files)
            
            logger.info(f"阶段一完成，成功处理 {len(success_data)} 条记录")
        else:
            logger.error("阶段一：没有成功处理的数据")

    def save_incremental_data(self, data: pd.DataFrame):
        """保存增量更新数据"""
        if len(data) == 0:
            logger.warning("没有增量数据需要保存")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"incremental_update_{timestamp}.csv"
        filepath = self.incremental_dir / filename
        data.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"保存增量数据: {filename}, 共{len(data)}条记录")

    def update_processed_files_record(self, files: List[Path]):
        """更新已处理文件记录"""
        record = self.load_processed_files_record()
        
        for file_path in files:
            fingerprint = self.calculate_file_fingerprint(file_path)
            file_key = str(file_path.relative_to(self.base_path))
            record[file_key] = fingerprint
            
        self.save_processed_files_record(record)
        logger.info(f"更新了 {len(files)} 个文件的处理记录")

    def prepare_for_neo4j(self, incremental_mode: bool = False):
        """阶段二：图数据格式化与关系提取（支持增量模式）"""
        logger.info("=" * 50)
        if incremental_mode:
            logger.info("开始阶段二：增量数据Neo4j格式化与关系提取")
        else:
            logger.info("开始阶段二：图数据格式化与关系提取")
        logger.info("=" * 50)

        # 根据模式选择数据源
        if incremental_mode:
            # 增量模式：读取增量数据
            success_files = list(self.incremental_dir.glob("incremental_update_*.csv"))
            if not success_files:
                logger.info("没有增量数据需要格式化")
                return
        else:
            # 完整模式：读取阶段一的所有成功数据
            success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
            # 也包括final_processed开头的文件
            success_files.extend(list(self.success_dir.glob("final_processed*.csv")))
            if not success_files:
                logger.error("未找到阶段一的处理结果，请先运行阶段一")
                return
            
            logger.info(f"找到 {len(success_files)} 个阶段一处理成功的文件，将全部处理")

        # 合并所有数据
        all_success_data = []
        for file_path in success_files:
            df = self.load_csv_file(file_path)
            if df is not None:
                all_success_data.append(df)
        
        # 加载新发现的化学品（如果存在）
        new_chemicals_file = self.metadata_dir / "new_discovered_chemicals.csv"
        if new_chemicals_file.exists():
            logger.info("加载新发现的化学品...")
            new_chemicals_df = self.load_csv_file(new_chemicals_file)
            if new_chemicals_df is not None and not new_chemicals_df.empty:
                # 设置默认值以匹配主数据结构
                for col in ['中文别名', '英文名称', '英文别名', '分子量', '危害处置', '自然来源', '工业生产原料', '性质', 'processing_status']:
                    if col not in new_chemicals_df.columns:
                        new_chemicals_df[col] = ''
                
                all_success_data.append(new_chemicals_df)
                logger.info(f"成功加载 {len(new_chemicals_df)} 个新发现的化学品")
        
        if not all_success_data:
            logger.error("无法加载数据")
            return

        combined_data = pd.concat(all_success_data, ignore_index=True)
        logger.info(f"加载数据完成，共 {len(combined_data)} 条记录")

        # 合并重复记录（阶段二去重）
        logger.info(f"阶段二去重前记录数: {len(combined_data)}")
        combined_data = self.merge_duplicate_records(combined_data)
        logger.info(f"阶段二去重后记录数: {len(combined_data)}")
        logger.info(f"去重后，共 {len(combined_data)} 条记录")

        # 格式化为Neo4j格式
        neo4j_nodes = self.format_for_neo4j(combined_data)
        
        # 提取关系（使用改进的算法）
        neo4j_relationships = self.extract_relationships(neo4j_nodes)
        
        # 从关系中提取工艺节点并合并到节点数据中
        process_nodes = self.extract_process_nodes_from_relationships(neo4j_relationships)
        if not process_nodes.empty:
            neo4j_nodes = pd.concat([neo4j_nodes, process_nodes], ignore_index=True)
            logger.info(f"添加了 {len(process_nodes)} 个工艺节点到节点数据中")
        
        # 保存Neo4j数据
        if incremental_mode:
            self.save_incremental_neo4j_data(neo4j_nodes, neo4j_relationships)
        else:
            self.save_neo4j_data(neo4j_nodes, neo4j_relationships)
        
        logger.info("阶段二完成！")

    def save_incremental_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame):
        """保存增量Neo4j格式的数据"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 从关系中提取工艺节点并合并到节点数据中
        process_nodes = self.extract_process_nodes_from_relationships(relationships_df)
        if not process_nodes.empty:
            nodes_df = pd.concat([nodes_df, process_nodes], ignore_index=True)
            logger.info(f"增量模式：添加了 {len(process_nodes)} 个工艺节点")

        # 创建标准/法律节点和关系
        if not nodes_df.empty:
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            
            # 合并节点数据
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:
                combined_nodes = nodes_df
            
            # 合并关系数据
            if not standard_legal_relationships.empty:
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:
                combined_relationships = relationships_df
        else:
            combined_nodes = nodes_df
            combined_relationships = relationships_df

        # 保存增量节点数据
        if not combined_nodes.empty:
            filename = f"incremental_neo4j_nodes_{timestamp}.csv"
            filepath = self.incremental_dir / filename
            combined_nodes.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j节点数据: {filename}")

        # 保存增量关系数据
        if not combined_relationships.empty:
            rel_filename = f"incremental_neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.incremental_dir / rel_filename
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j关系数据: {rel_filename}")

        # 统计信息
        chemical_count = len(nodes_df) if not nodes_df.empty else 0
        standard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
        original_rel_count = len(relationships_df) if not relationships_df.empty else 0
        standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
        
        logger.info(f"增量数据保存完成:")
        logger.info(f"  化学品节点: {chemical_count} 个")
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
    processor = EntityProcessor(str(base_path))
    
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
        # 增量更新模式
        processor.run_incremental_update()
    else:
        # 完整处理模式
        processor.run_complete_pipeline()


if __name__ == "__main__":
    main()
