#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多格式文件导入器
支持CSV、Excel、JSON等多种格式文件的批量导入和标准化处理
针对化学品数据处理进行优化
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple, Any
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import chardet
from datetime import datetime
import hashlib
import re

logger = logging.getLogger(__name__)


class MultiFormatImporter:
    """多格式文件导入器"""
    
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.xlsx': 'excel',
        '.xls': 'excel', 
        '.json': 'json',
        '.txt': 'text',
        '.tsv': 'tsv'
    }
    
    def __init__(self, base_path: str):
        """
        初始化多格式文件导入器
        
        Args:
            base_path: 工作目录基础路径
        """
        self.base_path = Path(base_path)
        
        # 创建必要的目录结构
        self.input_dirs = {
            'csv': self.base_path / "数据导入" / "csv",
            'excel': self.base_path / "数据导入" / "excel", 
            'json': self.base_path / "数据导入" / "json",
            'text': self.base_path / "数据导入" / "text",
            'other': self.base_path / "数据导入" / "其他格式"
        }
        
        self.output_dir = self.base_path / "处理成功" / "多格式导入"
        self.failed_dir = self.base_path / "处理失败" / "多格式导入"
        self.metadata_dir = self.base_path / "元数据" / "导入记录"
        
        # 创建所有目录
        for dir_path in [*self.input_dirs.values(), self.output_dir, self.failed_dir, self.metadata_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # 处理记录
        self.import_log = self.metadata_dir / "import_log.json"
        self.column_mapping_file = self.metadata_dir / "column_mapping.json"
        
        # 初始化列名映射
        self._init_column_mapping()
        
        logger.info("多格式文件导入器初始化完成")
        logger.info(f"支持格式: {list(self.SUPPORTED_FORMATS.keys())}")
    
    def _init_column_mapping(self):
        """初始化列名映射规则"""
        self.standard_columns = {
            # 中文名称相关
            '中文名称': ['化学品名称', '中文名', '化学名称', '名称', 'name', 'chemical_name', 'chem_name'],
            
            # CAS号相关
            'CAS号': ['cas', 'cas_no', 'cas号', 'cas编号', 'cas_number'],
            
            # 英文名称相关
            '英文名称': ['english_name', 'en_name', 'english', 'eng_name'],
            
            # 分子式相关
            '分子式': ['formula', 'molecular_formula', 'mol_formula'],
            
            # 分子量相关
            '分子量': ['molecular_weight', 'mw', 'mol_weight'],
            
            # 别名相关
            '别名': ['aliases', 'synonyms', 'alias', 'other_names'],
            
            # 生产厂家相关
            '生产厂家': ['manufacturer', 'producer', 'company', 'supplier'],
            
            # 用途相关
            '用途': ['usage', 'application', 'use', 'purpose'],
            
            # 危险性相关
            '危险性': ['hazard', 'danger', 'risk', 'safety'],
            
            # 备注相关
            '备注': ['remark', 'note', 'comment', 'description', 'desc']
        }
        
        # 保存映射规则
        self._save_column_mapping()
    
    def _save_column_mapping(self):
        """保存列名映射规则"""
        try:
            with open(self.column_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(self.standard_columns, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存列名映射失败: {e}")
    
    def _load_column_mapping(self):
        """加载列名映射规则"""
        try:
            if self.column_mapping_file.exists():
                with open(self.column_mapping_file, 'r', encoding='utf-8') as f:
                    self.standard_columns = json.load(f)
        except Exception as e:
            logger.error(f"加载列名映射失败: {e}")
    
    def detect_file_encoding(self, file_path: Path) -> str:
        """智能检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(50000)  # 读取足够的数据
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                
                # 对于低置信度的检测，尝试常见编码
                if confidence < 0.7:
                    common_encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16', 'big5', 'latin1']
                    for enc in common_encodings:
                        try:
                            with open(file_path, 'r', encoding=enc) as test_file:
                                test_file.read(1000)
                                logger.info(f"使用编码 {enc} (检测置信度低)")
                                return enc
                        except:
                            continue
                
                logger.info(f"检测编码: {encoding} (置信度: {confidence:.2f})")
                return encoding or 'utf-8'
                
        except Exception as e:
            logger.warning(f"编码检测失败 {file_path}: {e}, 使用UTF-8")
            return 'utf-8'
    
    def load_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """加载CSV文件"""
        try:
            encoding = self.detect_file_encoding(file_path)
            
            # 尝试不同的分隔符
            separators = [',', ';', '\t', '|']
            
            for sep in separators:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=sep, low_memory=False)
                    # 检查是否正确解析了列
                    if len(df.columns) > 1 and not df.empty:
                        logger.info(f"CSV加载成功: {file_path.name}, 分隔符: '{sep}', {len(df)}行{len(df.columns)}列")
                        return df
                except Exception as e:
                    logger.debug(f"分隔符 '{sep}' 失败: {e}")
                    continue
            
            # 如果所有分隔符都失败，使用默认
            df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
            logger.info(f"CSV加载成功(默认): {file_path.name}, {len(df)}行{len(df.columns)}列")
            return df
            
        except Exception as e:
            logger.error(f"CSV加载失败 {file_path.name}: {e}")
            return None
    
    def load_excel_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """加载Excel文件"""
        try:
            # 检查文件是否存在且可读
            if not file_path.exists():
                logger.error(f"Excel文件不存在: {file_path}")
                return None
            
            # 读取Excel文件
            xl_file = pd.ExcelFile(file_path)
            sheets = xl_file.sheet_names
            
            logger.info(f"Excel文件包含工作表: {sheets}")
            
            if len(sheets) == 1:
                # 只有一个工作表
                df = pd.read_excel(file_path, sheet_name=0)
                logger.info(f"Excel加载成功: {file_path.name}, 工作表: {sheets[0]}, {len(df)}行{len(df.columns)}列")
                return df
            else:
                # 多个工作表，选择最合适的
                main_sheet = self._select_main_sheet(sheets)
                df = pd.read_excel(file_path, sheet_name=main_sheet)
                logger.info(f"Excel加载成功: {file_path.name}, 选择工作表: {main_sheet}, {len(df)}行{len(df.columns)}列")
                return df
                
        except Exception as e:
            logger.error(f"Excel加载失败 {file_path.name}: {e}")
            return None
    
    def _select_main_sheet(self, sheets: List[str]) -> str:
        """选择主要工作表"""
        # 优先选择包含特定关键词的工作表
        priority_keywords = [
            '化学品', '数据', '主表', 'main', 'data', 'chemical', 
            '清单', 'list', 'sheet1', '工作表1'
        ]
        
        for keyword in priority_keywords:
            for sheet in sheets:
                if keyword.lower() in sheet.lower():
                    return sheet
        
        # 如果没有匹配的关键词，选择第一个工作表
        return sheets[0]
    
    def load_json_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """加载JSON文件"""
        try:
            encoding = self.detect_file_encoding(file_path)
            
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
            
            # 处理不同的JSON结构
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # 尝试不同的数据键
                data_keys = ['data', 'records', 'items', 'chemicals', 'results']
                for key in data_keys:
                    if key in data and isinstance(data[key], list):
                        df = pd.DataFrame(data[key])
                        break
                else:
                    # 如果没有找到列表数据，将字典作为单行记录
                    df = pd.DataFrame([data])
            else:
                logger.error(f"不支持的JSON结构类型: {type(data)}")
                return None
            
            logger.info(f"JSON加载成功: {file_path.name}, {len(df)}行{len(df.columns)}列")
            return df
            
        except Exception as e:
            logger.error(f"JSON加载失败 {file_path.name}: {e}")
            return None
    
    def load_text_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """加载文本文件"""
        try:
            encoding = self.detect_file_encoding(file_path)
            
            with open(file_path, 'r', encoding=encoding) as f:
                lines = f.readlines()
            
            # 清理空行
            clean_lines = [line.strip() for line in lines if line.strip()]
            
            if not clean_lines:
                logger.warning(f"文本文件为空: {file_path.name}")
                return None
            
            # 尝试识别分隔符
            first_line = clean_lines[0]
            if '\t' in first_line:
                # TSV格式
                data = []
                for line in clean_lines:
                    data.append(line.split('\t'))
                df = pd.DataFrame(data[1:], columns=data[0] if len(data) > 1 else None)
            elif ',' in first_line and len(first_line.split(',')) > 1:
                # CSV格式
                data = []
                for line in clean_lines:
                    data.append(line.split(','))
                df = pd.DataFrame(data[1:], columns=data[0] if len(data) > 1 else None)
            else:
                # 纯文本，每行作为一个记录
                df = pd.DataFrame({'content': clean_lines})
            
            logger.info(f"文本加载成功: {file_path.name}, {len(df)}行{len(df.columns)}列")
            return df
            
        except Exception as e:
            logger.error(f"文本加载失败 {file_path.name}: {e}")
            return None
    
    def standardize_columns(self, df: pd.DataFrame, source_file: str = "") -> pd.DataFrame:
        """标准化列名"""
        try:
            if df.empty:
                return df
            
            # 创建列名映射
            column_mapping = {}
            df_columns_lower = [col.lower().strip() for col in df.columns]
            
            for standard_col, variants in self.standard_columns.items():
                for variant in variants:
                    variant_lower = variant.lower().strip()
                    
                    # 精确匹配
                    if variant_lower in df_columns_lower:
                        original_col = df.columns[df_columns_lower.index(variant_lower)]
                        column_mapping[original_col] = standard_col
                        break
                    
                    # 部分匹配
                    for i, col in enumerate(df_columns_lower):
                        if variant_lower in col or col in variant_lower:
                            original_col = df.columns[i]
                            if original_col not in column_mapping:
                                column_mapping[original_col] = standard_col
                            break
            
            # 应用映射
            df_renamed = df.rename(columns=column_mapping)
            
            if column_mapping:
                logger.info(f"列名标准化完成 {source_file}: {len(column_mapping)} 个列被重命名")
                for old, new in column_mapping.items():
                    logger.debug(f"  {old} -> {new}")
            
            return df_renamed
            
        except Exception as e:
            logger.error(f"列名标准化失败 {source_file}: {e}")
            return df
    
    def validate_chemical_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """验证化学品数据"""
        errors = []
        
        if df.empty:
            errors.append("数据为空")
            return df, errors
        
        # 检查必要列
        required_columns = ['中文名称']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"缺少必要列: {missing_columns}")
        
        # 数据清洗
        if '中文名称' in df.columns:
            # 移除空的中文名称
            original_count = len(df)
            df = df.dropna(subset=['中文名称'])
            df = df[df['中文名称'].astype(str).str.strip() != '']
            removed_count = original_count - len(df)
            
            if removed_count > 0:
                errors.append(f"移除了 {removed_count} 行空的中文名称记录")
        
        # CAS号格式验证
        if 'CAS号' in df.columns:
            cas_pattern = r'^\d+-\d+-\d+$'
            invalid_cas = df[df['CAS号'].notna() & 
                           ~df['CAS号'].astype(str).str.match(cas_pattern, na=False)]
            if len(invalid_cas) > 0:
                errors.append(f"发现 {len(invalid_cas)} 个无效的CAS号格式")
        
        return df, errors
    
    def save_processed_file(self, df: pd.DataFrame, original_path: Path, format_type: str):
        """保存处理后的文件"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{original_path.stem}_{format_type}_processed_{timestamp}.csv"
            output_path = self.output_dir / filename
            
            # 保存文件
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            # 记录处理信息
            self._log_import_result(original_path, output_path, len(df), True)
            
            logger.info(f"处理文件已保存: {filename}, {len(df)} 条记录")
            
        except Exception as e:
            logger.error(f"保存处理文件失败 {original_path.name}: {e}")
            self._log_import_result(original_path, None, 0, False, str(e))
    
    def save_failed_file(self, original_path: Path, error_msg: str):
        """保存失败文件信息"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_file = self.failed_dir / f"failed_{original_path.stem}_{timestamp}.txt"
            
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"原文件: {original_path}\n")
                f.write(f"失败时间: {datetime.now()}\n")
                f.write(f"错误信息: {error_msg}\n")
            
            self._log_import_result(original_path, None, 0, False, error_msg)
            
        except Exception as e:
            logger.error(f"保存失败记录失败: {e}")
    
    def _log_import_result(self, original_path: Path, output_path: Optional[Path], 
                          record_count: int, success: bool, error_msg: str = ""):
        """记录导入结果"""
        try:
            # 加载现有日志
            log_data = []
            if self.import_log.exists():
                with open(self.import_log, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
            
            # 添加新记录
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'original_file': str(original_path),
                'output_file': str(output_path) if output_path else None,
                'record_count': record_count,
                'success': success,
                'error_message': error_msg,
                'file_size': original_path.stat().st_size if original_path.exists() else 0
            }
            
            log_data.append(log_entry)
            
            # 保存日志（保留最近1000条记录）
            if len(log_data) > 1000:
                log_data = log_data[-1000:]
            
            with open(self.import_log, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"记录导入日志失败: {e}")
    
    def process_file(self, file_path: Path) -> bool:
        """处理单个文件"""
        try:
            file_ext = file_path.suffix.lower()
            
            if file_ext not in self.SUPPORTED_FORMATS:
                logger.warning(f"不支持的文件格式: {file_ext}")
                return False
            
            format_type = self.SUPPORTED_FORMATS[file_ext]
            
            # 根据格式类型加载文件
            if format_type == 'csv':
                df = self.load_csv_file(file_path)
            elif format_type == 'excel':
                df = self.load_excel_file(file_path)
            elif format_type == 'json':
                df = self.load_json_file(file_path)
            elif format_type in ['text', 'tsv']:
                df = self.load_text_file(file_path)
            else:
                logger.error(f"未实现的格式处理器: {format_type}")
                return False
            
            if df is None or df.empty:
                error_msg = f"文件加载失败或为空: {file_path.name}"
                logger.error(error_msg)
                self.save_failed_file(file_path, error_msg)
                return False
            
            # 标准化列名
            df = self.standardize_columns(df, file_path.name)
            
            # 验证数据
            df, validation_errors = self.validate_chemical_data(df)
            
            if validation_errors:
                logger.warning(f"数据验证警告 {file_path.name}: {validation_errors}")
            
            # 保存处理结果
            self.save_processed_file(df, file_path, format_type)
            
            return True
            
        except Exception as e:
            error_msg = f"处理文件异常: {str(e)}"
            logger.error(f"{error_msg} - {file_path.name}")
            self.save_failed_file(file_path, error_msg)
            return False
    
    def scan_and_import_all_files(self, max_workers: int = 4) -> Dict[str, Any]:
        """扫描并导入所有支持的文件"""
        logger.info("开始扫描所有目录中的支持文件...")
        
        # 收集所有文件
        all_files = []
        
        # 从各个输入目录收集文件
        for dir_name, dir_path in self.input_dirs.items():
            if dir_path.exists():
                for ext in self.SUPPORTED_FORMATS.keys():
                    # 直接文件
                    all_files.extend(dir_path.glob(f"*{ext}"))
                    # 子目录中的文件
                    all_files.extend(dir_path.glob(f"**/*{ext}"))
        
        # 也从主工作目录收集
        for ext in self.SUPPORTED_FORMATS.keys():
            all_files.extend(self.base_path.glob(f"*{ext}"))
        
        # 去重
        all_files = list(set(all_files))
        
        logger.info(f"发现 {len(all_files)} 个支持的文件")
        
        if not all_files:
            logger.warning("未发现任何支持的文件")
            return {'total_files': 0, 'success_count': 0, 'failed_count': 0}
        
        # 并行处理文件
        success_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_file = {
                executor.submit(self.process_file, file_path): file_path 
                for file_path in all_files
            }
            
            # 收集结果
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"处理文件异常 {file_path}: {e}")
                    failed_count += 1
        
        # 统计结果
        result = {
            'total_files': len(all_files),
            'success_count': success_count,
            'failed_count': failed_count,
            'success_rate': success_count / len(all_files) if all_files else 0
        }
        
        logger.info(f"批量导入完成: 总文件 {result['total_files']}, "
                   f"成功 {result['success_count']}, 失败 {result['failed_count']}, "
                   f"成功率 {result['success_rate']:.1%}")
        
        return result
    
    def get_import_statistics(self) -> Dict[str, Any]:
        """获取导入统计信息"""
        stats = {
            'supported_formats': list(self.SUPPORTED_FORMATS.keys()),
            'input_directories': {k: str(v) for k, v in self.input_dirs.items()},
            'output_directory': str(self.output_dir),
            'processed_files': 0,
            'total_records': 0,
            'recent_imports': []
        }
        
        # 统计已处理文件
        if self.output_dir.exists():
            processed_files = list(self.output_dir.glob("*_processed_*.csv"))
            stats['processed_files'] = len(processed_files)
            
            # 统计总记录数
            total_records = 0
            for file_path in processed_files:
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                    total_records += len(df)
                except:
                    pass
            stats['total_records'] = total_records
        
        # 获取最近的导入记录
        if self.import_log.exists():
            try:
                with open(self.import_log, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
                stats['recent_imports'] = log_data[-10:]  # 最近10条记录
            except:
                pass
        
        return stats
    
    def export_combined_dataset(self, output_filename: Optional[str] = None) -> Path:
        """导出合并的数据集"""
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"combined_chemical_dataset_{timestamp}.csv"
        
        output_path = self.output_dir / output_filename
        
        # 收集所有处理过的文件
        processed_files = list(self.output_dir.glob("*_processed_*.csv"))
        
        if not processed_files:
            logger.warning("没有找到已处理的文件")
            return output_path
        
        logger.info(f"合并 {len(processed_files)} 个处理过的文件...")
        
        all_dataframes = []
        for file_path in processed_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                if not df.empty:
                    all_dataframes.append(df)
            except Exception as e:
                logger.error(f"读取文件失败 {file_path}: {e}")
        
        if not all_dataframes:
            logger.warning("没有有效的数据可以合并")
            return output_path
        
        # 合并数据
        combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        
        # 去重（基于中文名称）
        original_count = len(combined_df)
        if '中文名称' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['中文名称'], keep='first')
            dedup_count = original_count - len(combined_df)
            if dedup_count > 0:
                logger.info(f"去重 {dedup_count} 条重复记录")
        
        # 保存合并结果
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"合并数据集已导出: {output_filename}")
        logger.info(f"总记录数: {len(combined_df)}, 总列数: {len(combined_df.columns)}")
        
        return output_path


def main():
    """主函数，演示多格式文件导入器"""
    # 获取工作目录
    base_path = Path(__file__).parent.parent
    
    # 创建导入器
    importer = MultiFormatImporter(str(base_path))
    
    print("========================================")
    print("=== 多格式文件导入器演示 ===")
    print("========================================")
    
    # 显示支持的格式
    print(f"\n支持的文件格式: {list(importer.SUPPORTED_FORMATS.keys())}")
    
    # 扫描并导入所有文件
    print("\n1. 扫描并导入所有支持的文件...")
    result = importer.scan_and_import_all_files()
    
    print(f"导入结果:")
    print(f"  总文件数: {result['total_files']}")
    print(f"  成功导入: {result['success_count']}")
    print(f"  导入失败: {result['failed_count']}")
    print(f"  成功率: {result['success_rate']:.1%}")
    
    # 显示统计信息
    print("\n2. 导入统计信息:")
    stats = importer.get_import_statistics()
    print(f"  已处理文件数: {stats['processed_files']}")
    print(f"  总记录数: {stats['total_records']}")
    
    # 导出合并数据集
    if stats['processed_files'] > 0:
        print("\n3. 导出合并数据集...")
        output_path = importer.export_combined_dataset()
        print(f"合并数据集已保存: {output_path}")
    
    print("\n========================================")
    print("=== 多格式文件导入器演示完成 ===")
    print("========================================")


if __name__ == "__main__":
    main()
