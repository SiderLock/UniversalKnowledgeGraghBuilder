#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
矢量化查询优化器
专为化学品数据查询优化，提供高效的相似度搜索和智能匹配功能
"""

import logging
import pickle
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Union
import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import re

# 基础依赖
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)


class ChemicalVectorOptimizer:
    """化学品矢量化查询优化器"""
    
    def __init__(self, base_path: str, cache_dir: Optional[str] = None):
        """
        初始化矢量化优化器
        
        Args:
            base_path: 工作目录基础路径
            cache_dir: 缓存目录路径
        """
        self.base_path = Path(base_path)
        self.cache_dir = Path(cache_dir) if cache_dir else self.base_path / "缓存" / "矢量化"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 化学品数据库
        self.chemical_database = {}  # name -> info
        self.name_variants = {}      # variant -> canonical_name
        self.cas_mapping = {}        # cas -> name
        
        # 矢量化组件
        self.tfidf_vectorizer = None
        self.tfidf_matrix = None
        self.chemical_names = []
        
        # 缓存文件路径
        self.database_cache = self.cache_dir / "chemical_database.pkl"
        self.vector_cache = self.cache_dir / "tfidf_vectors.pkl"
        
        logger.info("化学品矢量化查询优化器初始化完成")
    
    def load_chemical_data(self, data_sources: List[Union[str, Path, pd.DataFrame]]):
        """
        加载化学品数据
        
        Args:
            data_sources: 数据源列表，可以是文件路径或DataFrame
        """
        logger.info("开始加载化学品数据...")
        
        all_chemicals = {}
        
        for source in data_sources:
            try:
                if isinstance(source, pd.DataFrame):
                    df = source
                elif isinstance(source, (str, Path)):
                    source_path = Path(source)
                    if source_path.suffix.lower() == '.csv':
                        df = pd.read_csv(source_path, encoding='utf-8-sig')
                    elif source_path.suffix.lower() in ['.xlsx', '.xls']:
                        df = pd.read_excel(source_path)
                    else:
                        logger.warning(f"不支持的文件格式: {source_path}")
                        continue
                else:
                    logger.warning(f"不支持的数据源类型: {type(source)}")
                    continue
                
                # 提取化学品信息
                chemicals = self._extract_chemicals_from_dataframe(df)
                all_chemicals.update(chemicals)
                
            except Exception as e:
                logger.error(f"加载数据源失败 {source}: {e}")
        
        self.chemical_database = all_chemicals
        self._build_variant_mappings()
        
        logger.info(f"加载完成，共 {len(all_chemicals)} 个化学品")
        
        # 缓存数据库
        self._save_database_cache()
    
    def _extract_chemicals_from_dataframe(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """从DataFrame中提取化学品信息"""
        chemicals = {}
        
        # 标准化列名
        column_mapping = {
            '化学品名称': '中文名称',
            '中文名': '中文名称',
            '化学名称': '中文名称',
            '名称': '中文名称',
            'name': '中文名称',
            'chemical_name': '中文名称',
            'cas': 'CAS号',
            'cas_no': 'CAS号',
            'cas号': 'CAS号',
            'cas编号': 'CAS号',
            '英文名称': '英文名称',
            'english_name': '英文名称',
            'en_name': '英文名称',
            '分子式': '分子式',
            'formula': '分子式',
            'molecular_formula': '分子式',
            '分子量': '分子量',
            'molecular_weight': '分子量',
            'mw': '分子量',
            '别名': '别名',
            'aliases': '别名',
            'synonyms': '别名',
        }
        
        df_renamed = df.rename(columns=column_mapping)
        
        # 必须包含中文名称
        if '中文名称' not in df_renamed.columns:
            logger.warning("DataFrame中缺少中文名称列")
            return chemicals
        
        for _, row in df_renamed.iterrows():
            chinese_name = str(row.get('中文名称', '')).strip()
            if not chinese_name or chinese_name.lower() in ['nan', 'none', '']:
                continue
            
            # 构建化学品信息
            chemical_info = {
                'chinese_name': chinese_name,
                'cas_number': str(row.get('CAS号', '')).strip() if pd.notna(row.get('CAS号')) else '',
                'english_name': str(row.get('英文名称', '')).strip() if pd.notna(row.get('英文名称')) else '',
                'molecular_formula': str(row.get('分子式', '')).strip() if pd.notna(row.get('分子式')) else '',
                'molecular_weight': str(row.get('分子量', '')).strip() if pd.notna(row.get('分子量')) else '',
                'aliases': [],
                'source_data': row.to_dict()
            }
            
            # 处理别名
            aliases_str = str(row.get('别名', '')).strip() if pd.notna(row.get('别名')) else ''
            if aliases_str:
                # 支持多种分隔符
                for sep in ['|', ';', '；', ',', '，', '\n', '\t']:
                    if sep in aliases_str:
                        aliases = [alias.strip() for alias in aliases_str.split(sep) if alias.strip()]
                        chemical_info['aliases'] = aliases
                        break
                else:
                    chemical_info['aliases'] = [aliases_str]
            
            chemicals[chinese_name] = chemical_info
        
        return chemicals
    
    def _build_variant_mappings(self):
        """构建名称变体映射"""
        self.name_variants = {}
        self.cas_mapping = {}
        
        for name, info in self.chemical_database.items():
            # 主名称映射
            self.name_variants[name] = name
            
            # CAS号映射
            cas = info.get('cas_number', '')
            if cas and cas.lower() not in ['nan', 'none', '']:
                self.cas_mapping[cas] = name
            
            # 英文名称映射
            english_name = info.get('english_name', '')
            if english_name and english_name.lower() not in ['nan', 'none', '']:
                self.name_variants[english_name] = name
            
            # 别名映射
            for alias in info.get('aliases', []):
                if alias and alias.strip():
                    self.name_variants[alias.strip()] = name
        
        logger.info(f"构建名称变体映射完成，共 {len(self.name_variants)} 个变体")
    
    def build_tfidf_index(self):
        """构建TF-IDF矢量索引"""
        if not SKLEARN_AVAILABLE:
            logger.warning("scikit-learn不可用，无法构建TF-IDF索引")
            return
        
        logger.info("开始构建TF-IDF矢量索引...")
        
        # 准备文本数据
        texts = []
        self.chemical_names = []
        
        for name, info in self.chemical_database.items():
            # 组合多个文本字段
            text_parts = [name]
            
            if info.get('english_name'):
                text_parts.append(info['english_name'])
            
            if info.get('molecular_formula'):
                text_parts.append(info['molecular_formula'])
            
            # 添加别名
            text_parts.extend(info.get('aliases', []))
            
            # 合并文本
            combined_text = ' '.join(text_parts)
            texts.append(combined_text)
            self.chemical_names.append(name)
        
        if not texts:
            logger.warning("没有文本数据用于构建索引")
            return
        
        # 构建TF-IDF矢量化器
        self.tfidf_vectorizer = TfidfVectorizer(
            analyzer='char',  # 字符级分析，适合中文
            ngram_range=(1, 3),  # 1-3字符的n-gram
            max_features=10000,
            lowercase=True,
            strip_accents='unicode'
        )
        
        # 训练并转换文本
        self.tfidf_matrix = self.tfidf_vectorizer.fit_transform(texts)
        
        logger.info(f"TF-IDF索引构建完成，特征数: {self.tfidf_matrix.shape[1]}")
        
        # 缓存矢量索引
        self._save_vector_cache()
    
    def search_similar_chemicals(self, query: str, top_k: int = 10, threshold: float = 0.1) -> List[Tuple[str, float, Dict]]:
        """
        搜索相似的化学品
        
        Args:
            query: 查询字符串
            top_k: 返回前K个结果
            threshold: 相似度阈值
            
        Returns:
            [(化学品名称, 相似度得分, 化学品信息), ...]
        """
        results = []
        
        # 1. 精确匹配
        exact_matches = self._exact_match_search(query)
        results.extend(exact_matches)
        
        # 2. 变体匹配
        variant_matches = self._variant_match_search(query)
        results.extend(variant_matches)
        
        # 3. CAS号匹配
        cas_matches = self._cas_match_search(query)
        results.extend(cas_matches)
        
        # 4. TF-IDF相似度搜索
        if self.tfidf_vectorizer and self.tfidf_matrix is not None:
            tfidf_matches = self._tfidf_similarity_search(query, top_k, threshold)
            results.extend(tfidf_matches)
        
        # 5. 模糊匹配
        fuzzy_matches = self._fuzzy_match_search(query, top_k, threshold)
        results.extend(fuzzy_matches)
        
        # 去重并排序
        unique_results = {}
        for name, score, info in results:
            if name not in unique_results or score > unique_results[name][0]:
                unique_results[name] = (score, info)
        
        # 转换为列表并排序
        final_results = [(name, score, info) for name, (score, info) in unique_results.items()]
        final_results.sort(key=lambda x: x[1], reverse=True)
        
        return final_results[:top_k]
    
    def _exact_match_search(self, query: str) -> List[Tuple[str, float, Dict]]:
        """精确匹配搜索"""
        results = []
        if query in self.chemical_database:
            results.append((query, 1.0, self.chemical_database[query]))
        return results
    
    def _variant_match_search(self, query: str) -> List[Tuple[str, float, Dict]]:
        """变体匹配搜索"""
        results = []
        if query in self.name_variants:
            canonical_name = self.name_variants[query]
            if canonical_name in self.chemical_database:
                results.append((canonical_name, 0.95, self.chemical_database[canonical_name]))
        return results
    
    def _cas_match_search(self, query: str) -> List[Tuple[str, float, Dict]]:
        """CAS号匹配搜索"""
        results = []
        # 检查是否为CAS号格式
        if re.match(r'^\d+-\d+-\d+$', query.strip()):
            if query in self.cas_mapping:
                name = self.cas_mapping[query]
                if name in self.chemical_database:
                    results.append((name, 0.9, self.chemical_database[name]))
        return results
    
    def _tfidf_similarity_search(self, query: str, top_k: int, threshold: float) -> List[Tuple[str, float, Dict]]:
        """TF-IDF相似度搜索"""
        if not SKLEARN_AVAILABLE or self.tfidf_vectorizer is None:
            return []
        
        try:
            # 转换查询文本
            query_vector = self.tfidf_vectorizer.transform([query])
            
            # 计算相似度
            similarities = cosine_similarity(query_vector, self.tfidf_matrix).flatten()
            
            # 获取最相似的结果
            top_indices = np.argsort(similarities)[::-1][:top_k]
            
            results = []
            for idx in top_indices:
                similarity = similarities[idx]
                if similarity >= threshold:
                    name = self.chemical_names[idx]
                    info = self.chemical_database[name]
                    results.append((name, float(similarity), info))
            
            return results
            
        except Exception as e:
            logger.error(f"TF-IDF搜索失败: {e}")
            return []
    
    def _fuzzy_match_search(self, query: str, top_k: int, threshold: float) -> List[Tuple[str, float, Dict]]:
        """模糊匹配搜索"""
        results = []
        query_lower = query.lower().strip()
        
        for name, info in self.chemical_database.items():
            # 计算字符级相似度
            similarity = self._calculate_char_similarity(query_lower, name.lower())
            
            if similarity >= threshold:
                results.append((name, similarity, info))
        
        # 排序并返回前K个
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
    
    def _calculate_char_similarity(self, s1: str, s2: str) -> float:
        """计算字符级相似度"""
        if not s1 or not s2:
            return 0.0
        
        if s1 == s2:
            return 1.0
        
        # 简单的字符重叠相似度
        set1 = set(s1)
        set2 = set(s2)
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
        
        jaccard_sim = intersection / union
        
        # 考虑子串包含关系
        if s1 in s2 or s2 in s1:
            jaccard_sim += 0.3
        
        return min(jaccard_sim, 1.0)
    
    def add_chemical(self, name: str, info: Dict):
        """添加新的化学品"""
        self.chemical_database[name] = info
        self._build_variant_mappings()
        
        # 重新构建索引
        if self.tfidf_vectorizer:
            self.build_tfidf_index()
        
        logger.info(f"添加新化学品: {name}")
    
    def update_chemical(self, name: str, info: Dict):
        """更新化学品信息"""
        if name in self.chemical_database:
            self.chemical_database[name].update(info)
            self._build_variant_mappings()
            
            # 重新构建索引
            if self.tfidf_vectorizer:
                self.build_tfidf_index()
            
            logger.info(f"更新化学品: {name}")
        else:
            logger.warning(f"化学品不存在: {name}")
    
    def get_chemical_info(self, name: str) -> Optional[Dict]:
        """获取化学品信息"""
        return self.chemical_database.get(name)
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = {
            'total_chemicals': len(self.chemical_database),
            'total_variants': len(self.name_variants),
            'cas_mappings': len(self.cas_mapping),
            'tfidf_index_built': self.tfidf_matrix is not None,
            'sklearn_available': SKLEARN_AVAILABLE
        }
        
        if self.tfidf_matrix is not None:
            stats['tfidf_features'] = self.tfidf_matrix.shape[1]
            stats['tfidf_density'] = self.tfidf_matrix.nnz / (self.tfidf_matrix.shape[0] * self.tfidf_matrix.shape[1])
        
        return stats
    
    def _save_database_cache(self):
        """保存数据库缓存"""
        try:
            cache_data = {
                'chemical_database': self.chemical_database,
                'name_variants': self.name_variants,
                'cas_mapping': self.cas_mapping,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.database_cache, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.info(f"数据库缓存已保存: {self.database_cache}")
            
        except Exception as e:
            logger.error(f"保存数据库缓存失败: {e}")
    
    def _save_vector_cache(self):
        """保存矢量缓存"""
        if not SKLEARN_AVAILABLE or self.tfidf_vectorizer is None:
            return
        
        try:
            cache_data = {
                'tfidf_vectorizer': self.tfidf_vectorizer,
                'tfidf_matrix': self.tfidf_matrix,
                'chemical_names': self.chemical_names,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.vector_cache, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.info(f"矢量缓存已保存: {self.vector_cache}")
            
        except Exception as e:
            logger.error(f"保存矢量缓存失败: {e}")
    
    def load_cache(self) -> bool:
        """加载缓存数据"""
        success = False
        
        # 加载数据库缓存
        if self.database_cache.exists():
            try:
                with open(self.database_cache, 'rb') as f:
                    cache_data = pickle.load(f)
                
                self.chemical_database = cache_data.get('chemical_database', {})
                self.name_variants = cache_data.get('name_variants', {})
                self.cas_mapping = cache_data.get('cas_mapping', {})
                
                logger.info(f"数据库缓存加载成功，共 {len(self.chemical_database)} 个化学品")
                success = True
                
            except Exception as e:
                logger.error(f"加载数据库缓存失败: {e}")
        
        # 加载矢量缓存
        if SKLEARN_AVAILABLE and self.vector_cache.exists():
            try:
                with open(self.vector_cache, 'rb') as f:
                    cache_data = pickle.load(f)
                
                self.tfidf_vectorizer = cache_data.get('tfidf_vectorizer')
                self.tfidf_matrix = cache_data.get('tfidf_matrix')
                self.chemical_names = cache_data.get('chemical_names', [])
                
                logger.info("矢量缓存加载成功")
                
            except Exception as e:
                logger.error(f"加载矢量缓存失败: {e}")
        
        return success
    
    def export_search_api(self, output_file: Optional[Path] = None) -> Path:
        """导出搜索API配置"""
        if output_file is None:
            output_file = self.cache_dir / "search_api_config.json"
        
        config = {
            'api_version': '1.0',
            'total_chemicals': len(self.chemical_database),
            'search_methods': [
                'exact_match',
                'variant_match', 
                'cas_match',
                'tfidf_similarity' if SKLEARN_AVAILABLE else None,
                'fuzzy_match'
            ],
            'supported_features': {
                'chinese_names': True,
                'english_names': True,
                'cas_numbers': True,
                'aliases': True,
                'molecular_formula': True,
                'tfidf_search': SKLEARN_AVAILABLE
            },
            'cache_info': {
                'database_cache': str(self.database_cache),
                'vector_cache': str(self.vector_cache) if SKLEARN_AVAILABLE else None
            }
        }
        
        # 移除None值
        config['search_methods'] = [m for m in config['search_methods'] if m is not None]
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        logger.info(f"搜索API配置已导出: {output_file}")
        return output_file


def main():
    """主函数，演示矢量化查询优化器"""
    # 获取工作目录
    base_path = Path(__file__).parent.parent
    
    # 创建优化器
    optimizer = ChemicalVectorOptimizer(str(base_path))
    
    print("========================================")
    print("=== 矢量化查询优化器演示 ===")
    print("========================================")
    
    # 尝试加载缓存
    print("\n1. 加载缓存数据...")
    if optimizer.load_cache():
        print("缓存加载成功")
    else:
        print("缓存不存在，需要重新构建")
        
        # 查找数据文件
        data_files = []
        success_dir = base_path / "处理成功"
        if success_dir.exists():
            data_files.extend(success_dir.glob("*.csv"))
        
        if data_files:
            print(f"发现 {len(data_files)} 个数据文件")
            optimizer.load_chemical_data(data_files)
            optimizer.build_tfidf_index()
        else:
            print("未找到数据文件")
            return
    
    # 显示统计信息
    print("\n2. 统计信息:")
    stats = optimizer.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 演示搜索功能
    print("\n3. 搜索演示:")
    test_queries = ["苯", "乙醇", "氯化钠", "硫酸", "H2SO4", "7664-93-9"]
    
    for query in test_queries:
        print(f"\n搜索: '{query}'")
        results = optimizer.search_similar_chemicals(query, top_k=3)
        
        if results:
            for i, (name, score, info) in enumerate(results, 1):
                cas = info.get('cas_number', '未知')
                print(f"  {i}. {name} (CAS: {cas}) - 相似度: {score:.3f}")
        else:
            print("  未找到匹配结果")
    
    # 导出API配置
    print("\n4. 导出搜索API配置...")
    config_file = optimizer.export_search_api()
    print(f"配置文件已导出: {config_file}")
    
    print("\n========================================")
    print("=== 矢量化查询优化器演示完成 ===")
    print("========================================")


if __name__ == "__main__":
    main()
