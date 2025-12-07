#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强文件处理器 - 支持多格式文件导入与矢量化处理
专为化学品数据处理优化，支持智能文件识别、格式转换和矢量化查询
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
import pickle
from datetime import datetime
import hashlib

# 矢量化相关导入
try:
    from sentence_transformers import SentenceTransformer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import faiss
    VECTORIZATION_AVAILABLE = True
except ImportError:
    VECTORIZATION_AVAILABLE = False
    logging.warning("矢量化相关库未安装，将使用基础文本匹配功能")

# 化学品处理相关导入
try:
    import jieba
    import jieba.posseg as pseg
    CHINESE_NLP_AVAILABLE = True
except ImportError:
    CHINESE_NLP_AVAILABLE = False
    logging.warning("中文NLP库未安装，将使用基础文本处理")

logger = logging.getLogger(__name__)


class EnhancedFileProcessor:
    """增强的文件处理器，支持多格式导入和矢量化查询"""
    
    SUPPORTED_FORMATS = {
        '.csv': 'csv',
        '.xlsx': 'excel',
        '.xls': 'excel', 
        '.json': 'json',
        '.jsonl': 'jsonlines',
        '.parquet': 'parquet',
        '.h5': 'hdf5',
        '.hdf5': 'hdf5',
        '.xml': 'xml',
        '.txt': 'text',
        '.tsv': 'tsv'
    }
    
    def __init__(self, base_path: str, enable_vectorization: bool = True):
        """
        初始化增强文件处理器
        
        Args:
            base_path: 工作目录基础路径
            enable_vectorization: 是否启用矢量化功能
        """
        self.base_path = Path(base_path)
        self.enable_vectorization = enable_vectorization and VECTORIZATION_AVAILABLE
        
        # 创建必要的目录
        self.input_dirs = {
            'csv': self.base_path / "数据导入" / "csv",
            'excel': self.base_path / "数据导入" / "excel", 
            'json': self.base_path / "数据导入" / "json",
            'other': self.base_path / "数据导入" / "其他格式"
        }
        
        self.output_dir = self.base_path / "处理成功" / "多格式导入"
        self.cache_dir = self.base_path / "缓存" / "矢量化缓存"
        
        # 创建目录
        for dir_path in self.input_dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化矢量化组件
        if self.enable_vectorization:
            self._init_vectorization()
        
        # 中文分词器初始化
        if CHINESE_NLP_AVAILABLE:
            jieba.initialize()
        
        logger.info(f"增强文件处理器初始化完成，矢量化: {'启用' if self.enable_vectorization else '禁用'}")
    
    def _init_vectorization(self):
        """初始化矢量化组件"""
        try:
            # 加载中文预训练模型
            model_cache_path = self.cache_dir / "sentence_transformer_model.pkl"
            
            if model_cache_path.exists():
                logger.info("加载缓存的矢量化模型...")
                with open(model_cache_path, 'rb') as f:
                    self.sentence_model = pickle.load(f)
            else:
                logger.info("初始化中文预训练模型...")
                # 使用适合中文的预训练模型
                self.sentence_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                # 缓存模型
                with open(model_cache_path, 'wb') as f:
                    pickle.dump(self.sentence_model, f)
            
            # TF-IDF矢量化器用于快速检索
            self.tfidf_vectorizer = TfidfVectorizer(
                max_features=5000,
                ngram_range=(1, 2),
                stop_words=None,  # 保留中文停用词处理
                analyzer='word'
            )
            
            # FAISS索引用于高效相似度搜索
            self.faiss_index = None
            self.chemical_database = []
            
            logger.info("矢量化组件初始化完成")
            
        except Exception as e:
            logger.error(f"矢量化组件初始化失败: {e}")
            self.enable_vectorization = False
    
    def detect_file_encoding(self, file_path: Path) -> str:
        """智能检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)  # 读取更多数据提高检测准确性
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                
                # 对于低置信度的检测，尝试常见编码
                if confidence < 0.7:
                    common_encodings = ['utf-8', 'gbk', 'gb2312', 'utf-16', 'big5']
                    for enc in common_encodings:
                        try:
                            with open(file_path, 'r', encoding=enc) as test_file:
                                test_file.read(1000)
                                return enc
                        except:
                            continue
                
                return encoding or 'utf-8'
        except Exception as e:
            logger.warning(f"编码检测失败 {file_path}: {e}, 使用UTF-8")
            return 'utf-8'
    
    def load_file_by_format(self, file_path: Path) -> Optional[pd.DataFrame]:
        """根据文件格式智能加载文件"""
        file_ext = file_path.suffix.lower()
        
        if file_ext not in self.SUPPORTED_FORMATS:
            logger.warning(f"不支持的文件格式: {file_ext}")
            return None
        
        format_type = self.SUPPORTED_FORMATS[file_ext]
        
        try:
            if format_type == 'csv':
                return self._load_csv(file_path)
            elif format_type == 'excel':
                return self._load_excel(file_path)
            elif format_type == 'json':
                return self._load_json(file_path)
            elif format_type == 'jsonlines':
                return self._load_jsonlines(file_path)
            elif format_type == 'parquet':
                return self._load_parquet(file_path)
            elif format_type == 'hdf5':
                return self._load_hdf5(file_path)
            elif format_type == 'xml':
                return self._load_xml(file_path)
            elif format_type == 'text':
                return self._load_text(file_path)
            elif format_type == 'tsv':
                return self._load_tsv(file_path)
            else:
                logger.error(f"格式处理器未实现: {format_type}")
                return None
                
        except Exception as e:
            logger.error(f"文件加载失败 {file_path}: {e}")
            return None
    
    def _load_csv(self, file_path: Path) -> pd.DataFrame:
        """加载CSV文件"""
        encoding = self.detect_file_encoding(file_path)
        
        # 尝试不同的分隔符
        separators = [',', ';', '\t', '|']
        
        for sep in separators:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=sep, low_memory=False)
                if len(df.columns) > 1:  # 确保正确解析了列
                    logger.info(f"成功加载CSV文件: {file_path.name}, 分隔符: '{sep}', 共{len(df)}行{len(df.columns)}列")
                    return df
            except:
                continue
        
        # 如果所有分隔符都失败，使用默认逗号分隔符
        df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
        logger.info(f"使用默认分隔符加载CSV: {file_path.name}, 共{len(df)}行{len(df.columns)}列")
        return df
    
    def _load_excel(self, file_path: Path) -> pd.DataFrame:
        """加载Excel文件"""
        # 读取所有工作表
        try:
            xl_file = pd.ExcelFile(file_path)
            sheets = xl_file.sheet_names
            
            if len(sheets) == 1:
                df = pd.read_excel(file_path, sheet_name=0)
                logger.info(f"成功加载Excel文件: {file_path.name}, 工作表: {sheets[0]}, 共{len(df)}行")
                return df
            else:
                # 多个工作表，合并或选择主要工作表
                logger.info(f"Excel文件包含多个工作表: {sheets}")
                
                # 优先选择包含"化学品"、"数据"、"主表"等关键词的工作表
                priority_keywords = ['化学品', '数据', '主表', 'main', 'data', 'chemical']
                main_sheet = None
                
                for sheet in sheets:
                    for keyword in priority_keywords:
                        if keyword.lower() in sheet.lower():
                            main_sheet = sheet
                            break
                    if main_sheet:
                        break
                
                if not main_sheet:
                    main_sheet = sheets[0]  # 默认使用第一个工作表
                
                df = pd.read_excel(file_path, sheet_name=main_sheet)
                logger.info(f"选择工作表 '{main_sheet}' 进行处理, 共{len(df)}行")
                return df
                
        except Exception as e:
            logger.error(f"Excel文件加载失败: {e}")
            raise
    
    def _load_json(self, file_path: Path) -> pd.DataFrame:
        """加载JSON文件"""
        encoding = self.detect_file_encoding(file_path)
        
        with open(file_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        # 处理不同的JSON结构
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            if 'data' in data:
                df = pd.DataFrame(data['data'])
            elif 'records' in data:
                df = pd.DataFrame(data['records'])
            else:
                # 尝试将字典转换为DataFrame
                df = pd.DataFrame([data])
        else:
            raise ValueError("不支持的JSON结构")
        
        logger.info(f"成功加载JSON文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_jsonlines(self, file_path: Path) -> pd.DataFrame:
        """加载JSONL文件"""
        encoding = self.detect_file_encoding(file_path)
        
        records = []
        with open(file_path, 'r', encoding=encoding) as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        df = pd.DataFrame(records)
        logger.info(f"成功加载JSONL文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_parquet(self, file_path: Path) -> pd.DataFrame:
        """加载Parquet文件"""
        df = pd.read_parquet(file_path)
        logger.info(f"成功加载Parquet文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_hdf5(self, file_path: Path) -> pd.DataFrame:
        """加载HDF5文件"""
        # 尝试不同的key
        try:
            df = pd.read_hdf(file_path, key='data')
        except:
            try:
                df = pd.read_hdf(file_path, key='df')
            except:
                # 如果没有指定key，列出所有可用的key
                import tables
                with tables.open_file(file_path, 'r') as h5file:
                    keys = [node._v_name for node in h5file.walk_nodes() if hasattr(node, 'read')]
                    if keys:
                        df = pd.read_hdf(file_path, key=keys[0])
                    else:
                        raise ValueError("HDF5文件中没有找到可读取的数据")
        
        logger.info(f"成功加载HDF5文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_xml(self, file_path: Path) -> pd.DataFrame:
        """加载XML文件"""
        import xml.etree.ElementTree as ET
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # 简单的XML解析，假设数据在相同标签下
        records = []
        
        # 查找重复的子元素作为记录
        child_tags = {}
        for child in root:
            tag = child.tag
            child_tags[tag] = child_tags.get(tag, 0) + 1
        
        # 选择出现次数最多的标签作为记录标签
        if child_tags:
            record_tag = max(child_tags, key=child_tags.get)
            
            for record in root.findall(record_tag):
                record_dict = {}
                for field in record:
                    record_dict[field.tag] = field.text
                records.append(record_dict)
        
        df = pd.DataFrame(records)
        logger.info(f"成功加载XML文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_text(self, file_path: Path) -> pd.DataFrame:
        """加载文本文件"""
        encoding = self.detect_file_encoding(file_path)
        
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        
        # 创建简单的DataFrame，每行作为一个记录
        df = pd.DataFrame({'content': [line.strip() for line in lines if line.strip()]})
        logger.info(f"成功加载文本文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def _load_tsv(self, file_path: Path) -> pd.DataFrame:
        """加载TSV文件"""
        encoding = self.detect_file_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding, sep='\t', low_memory=False)
        logger.info(f"成功加载TSV文件: {file_path.name}, 共{len(df)}行")
        return df
    
    def scan_and_import_all_files(self) -> Dict[str, List[pd.DataFrame]]:
        """扫描并导入所有支持格式的文件"""
        results = {}
        all_files = []
        
        # 收集所有支持的文件
        for input_dir in self.input_dirs.values():
            if input_dir.exists():
                for ext in self.SUPPORTED_FORMATS.keys():
                    all_files.extend(input_dir.glob(f"*{ext}"))
                    all_files.extend(input_dir.glob(f"**/*{ext}"))  # 递归搜索
        
        logger.info(f"发现 {len(all_files)} 个支持的文件")
        
        # 并行处理文件
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_file = {
                executor.submit(self.load_file_by_format, file_path): file_path 
                for file_path in all_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        format_type = self.SUPPORTED_FORMATS[file_path.suffix.lower()]
                        if format_type not in results:
                            results[format_type] = []
                        results[format_type].append(df)
                        
                        # 保存处理结果
                        self._save_processed_file(df, file_path)
                        
                except Exception as e:
                    logger.error(f"处理文件失败 {file_path}: {e}")
        
        return results
    
    def _save_processed_file(self, df: pd.DataFrame, original_path: Path):
        """保存处理后的文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{original_path.stem}_processed_{timestamp}.csv"
        output_path = self.output_dir / output_filename
        
        # 标准化列名
        df = self._standardize_columns(df)
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"保存处理结果: {output_filename}")
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化列名，便于后续处理"""
        # 常见的化学品数据列名映射
        column_mapping = {
            # 中文名称相关
            '化学品名称': '中文名称',
            '中文名': '中文名称', 
            '化学名称': '中文名称',
            '名称': '中文名称',
            'name': '中文名称',
            'chemical_name': '中文名称',
            
            # CAS号相关
            'cas': 'CAS号',
            'cas_no': 'CAS号',
            'cas号': 'CAS号',
            'cas编号': 'CAS号',
            
            # 英文名称相关
            '英文名称': '英文名称',
            'english_name': '英文名称',
            'en_name': '英文名称',
            
            # 分子式相关
            '分子式': '分子式',
            'formula': '分子式',
            'molecular_formula': '分子式',
            
            # 分子量相关
            '分子量': '分子量',
            'molecular_weight': '分子量',
            'mw': '分子量',
        }
        
        # 应用列名映射
        df_renamed = df.rename(columns=column_mapping)
        
        return df_renamed
    
    def build_chemical_vector_database(self, chemical_data: List[pd.DataFrame]):
        """构建化学品矢量数据库"""
        if not self.enable_vectorization:
            logger.warning("矢量化功能未启用")
            return
        
        logger.info("开始构建化学品矢量数据库...")
        
        # 合并所有化学品数据
        all_chemicals = []
        for df in chemical_data:
            if '中文名称' in df.columns:
                chemicals = df['中文名称'].dropna().unique().tolist()
                all_chemicals.extend(chemicals)
        
        if not all_chemicals:
            logger.warning("没有找到化学品名称数据")
            return
        
        all_chemicals = list(set(all_chemicals))  # 去重
        logger.info(f"收集到 {len(all_chemicals)} 个唯一化学品名称")
        
        # 生成文本嵌入
        try:
            # 预处理化学品名称
            processed_chemicals = []
            for chemical in all_chemicals:
                if CHINESE_NLP_AVAILABLE:
                    # 中文分词处理
                    words = jieba.lcut(chemical)
                    processed_text = ' '.join(words)
                else:
                    processed_text = chemical
                processed_chemicals.append(processed_text)
            
            # 生成向量嵌入
            embeddings = self.sentence_model.encode(processed_chemicals, show_progress_bar=True)
            
            # 构建FAISS索引
            dimension = embeddings.shape[1]
            self.faiss_index = faiss.IndexFlatIP(dimension)  # 内积索引，适合余弦相似度
            
            # 标准化向量（用于余弦相似度计算）
            embeddings_normalized = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
            self.faiss_index.add(embeddings_normalized.astype('float32'))
            
            # 保存化学品数据库
            self.chemical_database = all_chemicals
            
            # 缓存矢量数据库
            cache_data = {
                'chemicals': all_chemicals,
                'embeddings': embeddings_normalized,
                'dimension': dimension
            }
            
            cache_file = self.cache_dir / "chemical_vector_db.pkl"
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.info(f"矢量数据库构建完成，包含 {len(all_chemicals)} 个化学品")
            
        except Exception as e:
            logger.error(f"矢量数据库构建失败: {e}")
    
    def search_similar_chemicals(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """基于矢量相似度搜索相似化学品"""
        if not self.enable_vectorization or self.faiss_index is None:
            logger.warning("矢量搜索不可用，使用文本匹配")
            return self._fallback_text_search(query, top_k)
        
        try:
            # 预处理查询文本
            if CHINESE_NLP_AVAILABLE:
                words = jieba.lcut(query)
                processed_query = ' '.join(words)
            else:
                processed_query = query
            
            # 生成查询向量
            query_embedding = self.sentence_model.encode([processed_query])
            query_normalized = query_embedding / np.linalg.norm(query_embedding, axis=1, keepdims=True)
            
            # 搜索最相似的化学品
            scores, indices = self.faiss_index.search(query_normalized.astype('float32'), top_k)
            
            results = []
            for score, idx in zip(scores[0], indices[0]):
                if idx != -1:  # 有效索引
                    chemical_name = self.chemical_database[idx]
                    similarity = float(score)
                    results.append((chemical_name, similarity))
            
            return results
            
        except Exception as e:
            logger.error(f"矢量搜索失败: {e}")
            return self._fallback_text_search(query, top_k)
    
    def _fallback_text_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """备用的文本搜索方法"""
        if not hasattr(self, 'chemical_database') or not self.chemical_database:
            return []
        
        results = []
        query_lower = query.lower()
        
        for chemical in self.chemical_database:
            chemical_lower = chemical.lower()
            
            # 简单的文本相似度计算
            if query_lower == chemical_lower:
                similarity = 1.0
            elif query_lower in chemical_lower:
                similarity = 0.8
            elif chemical_lower in query_lower:
                similarity = 0.7
            else:
                # 基于字符重叠的相似度
                common_chars = set(query_lower) & set(chemical_lower)
                similarity = len(common_chars) / max(len(set(query_lower)), len(set(chemical_lower)))
            
            if similarity > 0.3:  # 阈值过滤
                results.append((chemical, similarity))
        
        # 按相似度排序
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    def export_enhanced_dataset(self, output_path: Optional[Path] = None) -> Path:
        """导出增强的数据集"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = self.output_dir / f"enhanced_chemical_dataset_{timestamp}.csv"
        
        # 合并所有处理过的文件
        all_files = list(self.output_dir.glob("*_processed_*.csv"))
        
        if not all_files:
            logger.warning("没有找到已处理的文件")
            return output_path
        
        all_dataframes = []
        for file_path in all_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                all_dataframes.append(df)
            except Exception as e:
                logger.error(f"读取文件失败 {file_path}: {e}")
        
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # 去重处理
            if '中文名称' in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=['中文名称'], keep='first')
            
            combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"导出增强数据集: {output_path}, 共 {len(combined_df)} 条记录")
        
        return output_path
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        stats = {
            'supported_formats': list(self.SUPPORTED_FORMATS.keys()),
            'vectorization_enabled': self.enable_vectorization,
            'chinese_nlp_available': CHINESE_NLP_AVAILABLE,
            'processed_files': 0,
            'total_records': 0,
            'vector_database_size': 0
        }
        
        # 统计已处理文件
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
        
        # 矢量数据库大小
        if hasattr(self, 'chemical_database'):
            stats['vector_database_size'] = len(self.chemical_database)
        
        return stats


def main():
    """主函数，演示增强文件处理器的功能"""
    # 获取工作目录
    base_path = Path(__file__).parent.parent
    
    # 创建增强文件处理器
    processor = EnhancedFileProcessor(str(base_path), enable_vectorization=True)
    
    print("========================================")
    print("=== 增强文件处理器演示 ===")
    print("========================================")
    
    # 扫描并导入所有文件
    print("\n1. 扫描并导入多格式文件...")
    imported_data = processor.scan_and_import_all_files()
    
    for format_type, dataframes in imported_data.items():
        print(f"  {format_type.upper()} 格式: {len(dataframes)} 个文件")
    
    # 构建矢量数据库
    if imported_data and processor.enable_vectorization:
        print("\n2. 构建化学品矢量数据库...")
        all_dfs = []
        for dfs in imported_data.values():
            all_dfs.extend(dfs)
        processor.build_chemical_vector_database(all_dfs)
    
    # 演示矢量搜索
    if processor.enable_vectorization:
        print("\n3. 演示矢量相似度搜索...")
        test_queries = ["苯", "乙醇", "氯化钠", "硫酸"]
        
        for query in test_queries:
            print(f"\n搜索: '{query}'")
            similar_chemicals = processor.search_similar_chemicals(query, top_k=5)
            for chemical, similarity in similar_chemicals:
                print(f"  {chemical}: {similarity:.3f}")
    
    # 导出增强数据集
    print("\n4. 导出增强数据集...")
    output_path = processor.export_enhanced_dataset()
    print(f"导出完成: {output_path}")
    
    # 显示统计信息
    print("\n5. 处理统计信息:")
    stats = processor.get_processing_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n========================================")
    print("=== 增强文件处理器演示完成 ===")
    print("========================================")


if __name__ == "__main__":
    main()
