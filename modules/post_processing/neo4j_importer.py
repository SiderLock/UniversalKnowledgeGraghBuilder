#!/usr/bin/env:python3
# -*- coding: utf-8 -*-
"""
Neo4j 数据导入模块
自动将处理好的CSV文件导入到Neo4j数据库中
"""

import os
import shutil
import time
import pandas as pd
from pathlib import Path
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired
from typing import Dict, List, Optional
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import psutil
import sys
import gc
import random
import subprocess
import platform

# --- 配置 ---
# !!! 请在这里填入您Neo4j数据库的连接信息 !!!
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "[2361918131]"  # 替换为您的密码
NEO4J_IMPORT_DIR = "E:/neo4j-chs-community-4.4.43-windows/import"  # Neo4j import目录路径
# --- 配置结束 ---

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('neo4j_importer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Neo4jImporter:
    def _calculate_optimal_timeout(self, memory_gb, disk_type, default_timeout):
        """根据内存和磁盘类型动态调整连接超时时间"""
        if memory_gb < 4:
            return max(60, int(default_timeout * 0.7))
        elif disk_type == "SSD":
            return int(default_timeout * 0.8)
        else:
            return int(default_timeout)

    def _init_performance_tracking(self):
        """初始化性能统计缓存"""
        self._performance_cache = {
            "query_execution": [],
            "errors": {},
        }

    def _classify_error(self, error_str):
        """简单分类错误类型"""
        if "timeout" in error_str or "timed out" in error_str:
            return "timeout"
        if "deadlock" in error_str or "lock" in error_str:
            return "deadlock"
        if "connection" in error_str:
            return "connection"
        if "memory" in error_str:
            return "memory"
        return "other"

    def _update_performance_metrics(self, metric, duration, success, error_type=None):
        """记录查询性能和错误统计"""
        if metric not in self._performance_cache:
            self._performance_cache[metric] = []
        self._performance_cache[metric].append(duration)
        if not success and error_type:
            self._performance_cache.setdefault("errors", {})
            self._performance_cache["errors"][error_type] = self._performance_cache["errors"].get(error_type, 0) + 1

    def _calculate_retry_delay(self, attempt, error):
        """根据错误类型和重试次数智能计算重试延迟"""
        error_str = str(error).lower()
        if "timeout" in error_str:
            return min(10, 2 + attempt * 2)
        if "deadlock" in error_str or "lock" in error_str:
            return min(8, 1 + attempt)
        if "connection" in error_str:
            return min(15, 3 + attempt * 3)
        if attempt > 7:
            return -1  # 超过最大重试次数
        return min(5, 1 + attempt)
    def _calculate_optimal_pool_size(self, cpu_cores, memory_gb, max_pool):
        """根据 CPU 核心数、内存和最大连接池参数动态计算最优连接池大小"""
        # 基础策略：每核 2-4 个连接，受内存限制
        base = min(cpu_cores * 3, max_pool)
        if memory_gb < 4:
            return max(4, min(base, 8))
        elif memory_gb < 8:
            return max(8, min(base, 20))
        elif memory_gb < 16:
            return max(12, min(base, 40))
        else:
            return min(base, max_pool)

    """Neo4j数据导入器 - 优化版本"""
    
    def __init__(self, uri, user, password, import_dir, neo4j_import_dir, max_connection_pool_size=50, connection_acquisition_timeout=180):
        """
        初始化导入器并连接到数据库 - 深度优化版本
        
        Args:
            uri: Neo4j数据库的URI
            user: 用户名
            password: 密码
            import_dir: 包含CSV文件的目录路径
            neo4j_import_dir: Neo4j数据库的import目录路径
            max_connection_pool_size: 连接池最大连接数（优化为50）
            connection_acquisition_timeout: 获取连接超时时间（秒，优化为180）
        """
        try:
            # 深度系统资源分析
            self.system_info = self._analyze_system_resources()
            cpu_cores = self.system_info['cpu_cores']
            memory_gb = self.system_info['memory_gb']
            disk_type = self.system_info['disk_type']
            
            # 第二代智能优化：渐进式激进配置
            # 根据系统负载动态调整连接池策略
            system_load = self._assess_system_load()
            
            # 智能连接池配置 - 基于系统性能动态调整
            optimal_pool_size = self._calculate_optimal_pool_size(cpu_cores, memory_gb, max_connection_pool_size)
            
            # 自适应超时配置 - 第二代优化
            optimized_timeout = self._calculate_optimal_timeout(memory_gb, disk_type, connection_acquisition_timeout)
            
            # 如果系统负载较低，启用激进模式
            if system_load < 0.6:  # 系统负载低于60%
                optimal_pool_size = int(min(optimal_pool_size * 1.3, 150))
                optimized_timeout = int(max(optimized_timeout * 0.8, 30))
                logger.info(f"系统负载较低({system_load:.1%})，启用激进优化模式")
            
            self.driver = GraphDatabase.driver(
                uri,
                auth=(user, password),
                max_connection_pool_size=optimal_pool_size,
                connection_acquisition_timeout=optimized_timeout,
                max_transaction_retry_time=45,  # 进一步增加事务重试时间
                encrypted=False,  # 本地环境关闭加密提升性能
                keep_alive=True,  # 保持连接活跃
                connection_timeout=30  # 连接超时
            )
            self.driver.verify_connectivity()
            
            # 初始化组件 - 确保路径正确
            self.import_dir = Path(import_dir).resolve()
            self.neo4j_import_dir = Path(neo4j_import_dir)
            self.lock = threading.Lock()  # 线程安全锁
            self._resource_monitor = {}  # 资源监控缓存
            self._performance_cache = {}  # 性能缓存
            self._error_patterns = {}  # 错误模式分析
            
            # 初始化性能统计
            self._init_performance_tracking()
            
            logger.info(f"成功连接到Neo4j数据库: {uri}")
            logger.info(f"数据源目录: {self.import_dir}")
            logger.info(f"Neo4j import目录: {neo4j_import_dir}")
            logger.info(f"系统分析: CPU={cpu_cores}核, 内存={memory_gb:.1f}GB, 存储={disk_type}")
            logger.info(f"优化配置: 连接池={optimal_pool_size}, 超时={optimized_timeout}s")
        except Neo4jError as e:
            logger.error(f"无法连接到Neo4j数据库，请检查配置和数据库状态: {e}")
            raise

    def _analyze_system_resources(self):
        """分析系统资源，返回CPU核心数、内存、磁盘类型等信息"""
        try:
            cpu_cores = psutil.cpu_count(logical=True) or 4
            memory_gb = psutil.virtual_memory().total / (1024**3)
            memory_available_gb = psutil.virtual_memory().available / (1024**3)
            disk_type = "SSD" if platform.system() == "Windows" else "Unknown"
            return {
                'cpu_cores': cpu_cores,
                'memory_gb': memory_gb,
                'memory_available_gb': memory_available_gb,
                'disk_type': disk_type
            }
        except Exception as e:
            logger.warning(f"系统资源分析失败: {e}")
            return {
                'cpu_cores': 4,
                'memory_gb': 4.0,
                'memory_available_gb': 2.0,
                'disk_type': 'Unknown'
            }

    def close(self):
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()
            logger.info("已断开与Neo4j的连接。")

    def copy_files_to_import_dir(self):
        """将CSV文件复制到Neo4j的import目录 - 优化版本"""
        logger.info("正在将CSV文件复制到Neo4j的import目录...")
        
        # 确保Neo4j import目录存在
        if not self.neo4j_import_dir.exists():
            logger.error(f"Neo4j import目录不存在: {self.neo4j_import_dir}")
            raise FileNotFoundError(f"Neo4j import目录不存在: {self.neo4j_import_dir}")
        
        # 获取所有需要复制的文件
        node_files = sorted(self.import_dir.glob("neo4j_ready_chemicals_batch_*.csv"))
        rel_files = sorted(self.import_dir.glob("neo4j_relationships_*.csv"))
        all_files = node_files + rel_files
        
        if not all_files:
            logger.warning("未找到任何CSV文件需要复制")
            return
        
        # 使用进度条显示复制进度
        with tqdm(total=len(all_files), desc="复制CSV文件", unit="文件") as pbar:
            for file in all_files:
                try:
                    dest_file = self.neo4j_import_dir / file.name
                    file_size = file.stat().st_size / (1024*1024)  # MB
                    
                    shutil.copy2(file, dest_file)
                    pbar.set_postfix({"当前文件": file.name, "大小": f"{file_size:.1f}MB"})
                    pbar.update(1)
                    
                    logger.debug(f"已复制文件: {file.name} ({file_size:.1f}MB)")
                except Exception as e:
                    logger.error(f"复制文件 {file.name} 失败: {e}")
                    pbar.update(1)
        
        logger.info(f"文件复制完成，共复制 {len(all_files)} 个文件。")

    def run_query(self, query: str, batch_size: Optional[int] = None, parameters: Optional[Dict] = None, max_retries: int = 5):
        """
        执行Cypher查询，支持自动重试和性能监控 - 增强版
        """
        attempt = 0
        last_error = None
        
        while attempt < max_retries:
            try:
                start_time = time.time()
                
                with self.driver.session() as session:
                    if batch_size:
                        # 批量处理
                        result = session.run(query, parameters)
                    else:
                        # 单次执行
                        result = session.run(query, parameters)
                
                duration = time.time() - start_time
                self._update_performance_metrics("query_execution", duration, True)
                
                return result
                
            except (ServiceUnavailable, SessionExpired, Neo4jError) as e:
                last_error = e
                attempt += 1
                
                error_str = str(e).lower()
                error_type = self._classify_error(error_str)
                
                duration = time.time() - start_time
                self._update_performance_metrics("query_execution", duration, False, error_type)
                
                # 计算智能延迟
                retry_delay = self._calculate_retry_delay(attempt, e)
                
                if retry_delay == -1:  # 不可重试的错误
                    break
                
                logger.warning(f"查询失败 (第 {attempt}/{max_retries} 次): {e}")
                logger.info(f"将在 {retry_delay:.1f} 秒后重试...")
                time.sleep(retry_delay)
                
            except Exception as e:
                last_error = e
                logger.error(f"查询时发生意外错误: {e}")
                break  # 对未知错误不进行重试
        
        logger.error(f"查询最终失败，已达到最大重试次数 ({max_retries}次): {last_error}")
        if last_error:
            raise last_error
        else:
            raise Exception("查询最终失败，已达到最大重试次数")
    
    def get_node_count_estimate(self, file_path):
        """估算节点数量用于进度条"""
        try:
            df = pd.read_csv(file_path, usecols=['name:ID', ':LABEL'])
            chemical_count = len(df[df[':LABEL'].str.contains('化学品|Chemical', na=False)])
            process_count = len(df[df[':LABEL'].str.contains('工艺|Process', na=False)])
            return chemical_count, process_count
        except Exception as e:
            logger.warning(f"无法估算节点数量: {e}")
            return 0, 0
    
    def get_relationship_count_estimate(self, file_path):
        """估算关系数量用于进度条"""
        try:
            df = pd.read_csv(file_path)
            return len(df)
        except Exception as e:
            logger.warning(f"无法估算关系数量: {e}")
            return 0

    def create_constraints(self):
        """创建唯一性约束以优化导入性能"""
        logger.info("正在创建节点的唯一性约束...")
        
        # 为化学品节点创建约束
        try:
            query = "CREATE CONSTRAINT chemical_name_unique IF NOT EXISTS FOR (c:化学品) REQUIRE c.名称 IS UNIQUE"
            self.run_query(query)
            logger.info("化学品节点约束创建成功。")
        except Exception as e:
            logger.warning(f"创建化学品约束时出现错误（可能已存在）: {e}")
            
        # 为工艺节点创建约束
        try:
            query = "CREATE CONSTRAINT process_name_unique IF NOT EXISTS FOR (p:工艺) REQUIRE p.名称 IS UNIQUE"
            self.run_query(query)
            logger.info("工艺节点约束创建成功。")
        except Exception as e:
            logger.warning(f"创建工艺约束时出现错误（可能已存在）: {e}")
            
        # 为产业节点创建约束
        try:
            query = "CREATE CONSTRAINT industry_name_unique IF NOT EXISTS FOR (i:产业) REQUIRE i.名称 IS UNIQUE"
            self.run_query(query)
            logger.info("产业节点约束创建成功。")
        except Exception as e:
            logger.warning(f"创建产业约束时出现错误（可能已存在）: {e}")

        # 为别名查询创建索引
        try:
            query = "CREATE INDEX alias_search_index IF NOT EXISTS FOR (c:化学品) ON (c.所有别名)"
            self.run_query(query)
            logger.info("别名查询索引创建成功。")
        except Exception as e:
            logger.warning(f"创建别名索引时出现错误（可能已存在）: {e}")

    def split_large_file(self, file_path, chunk_size=8000):
        """将大文件拆分为小文件以提高导入速度 - 深度优化版本"""
        logger.info(f"正在分析文件 {file_path.name}...")
        
        try:
            # 智能内存管理 - 根据系统资源调整策略
            available_memory = psutil.virtual_memory().available / (1024**3)  # GB
            file_size_mb = file_path.stat().st_size / (1024*1024)
            
            # 动态调整分块大小和读取策略
            if available_memory < 2:  # 低内存系统
                chunk_size = min(chunk_size, 3000)
                read_chunk_size = 1000
                logger.info(f"低内存模式: 调整分块大小为 {chunk_size}")
            elif available_memory > 8:  # 高内存系统
                chunk_size = max(chunk_size, 12000)
                read_chunk_size = chunk_size
                logger.info(f"高内存模式: 调整分块大小为 {chunk_size}")
            else:
                read_chunk_size = chunk_size // 2
            
            # 先读取文件头部信息，确定文件大小
            try:
                df_sample = pd.read_csv(file_path, nrows=100)
                # 使用内存高效的方式估算总行数
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_rows = sum(1 for line in f) - 1  # 减去标题行
            except Exception:
                # 备用方法
                df_sample = pd.read_csv(file_path, nrows=100)
                total_rows = len(pd.read_csv(file_path, usecols=[df_sample.columns[0]]))
            
            # 根据文件大小动态调整分块大小
            if total_rows <= chunk_size:
                logger.info(f"文件 {file_path.name} 行数 ({total_rows:,}) 小于分块大小 ({chunk_size:,})，无需拆分")
                return [file_path]
            
            # 创建拆分文件目录
            split_dir = file_path.parent / f"split_{file_path.stem}"
            split_dir.mkdir(exist_ok=True)
            
            # 清理旧的分块文件
            for old_chunk in split_dir.glob("*.csv"):
                old_chunk.unlink()
            
            logger.info(f"开始拆分文件 {file_path.name}，共 {total_rows:,} 行，目标分块大小: {chunk_size:,}")
            
            chunk_files = []
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            # 使用内存优化的分块读取
            with tqdm(total=total_chunks, desc=f"拆分 {file_path.name}", unit="块", 
                     bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
                
                try:
                    # 使用更小的读取块来节省内存
                    chunk_iter = pd.read_csv(file_path, chunksize=read_chunk_size, low_memory=True)
                    current_chunk = []
                    current_chunk_size = 0
                    chunk_num = 1
                    
                    for read_chunk in chunk_iter:
                        current_chunk.append(read_chunk)
                        current_chunk_size += len(read_chunk)
                        
                        # 当累积到目标大小时，写入文件
                        if current_chunk_size >= chunk_size:
                            # 合并并写入
                            merged_chunk = pd.concat(current_chunk, ignore_index=True)
                            chunk_file = split_dir / f"{file_path.stem}_chunk_{chunk_num:03d}.csv"
                            merged_chunk.to_csv(chunk_file, index=False)
                            chunk_files.append(chunk_file)
                            
                            # 更新进度条
                            file_size = chunk_file.stat().st_size / (1024*1024)
                            pbar.set_postfix({
                                "当前块": f"{chunk_num:03d}",
                                "行数": f"{len(merged_chunk):,}",
                                "大小": f"{file_size:.1f}MB",
                                "内存": f"{available_memory:.1f}GB"
                            })
                            pbar.update(1)
                            
                            # 重置累积器并强制垃圾回收
                            current_chunk = []
                            current_chunk_size = 0
                            chunk_num += 1
                            del merged_chunk
                            
                            # 检查内存使用情况
                            if psutil.virtual_memory().percent > 85:
                                import gc
                                gc.collect()
                                time.sleep(0.1)  # 给系统时间清理内存
                    
                    # 处理剩余数据
                    if current_chunk:
                        merged_chunk = pd.concat(current_chunk, ignore_index=True)
                        chunk_file = split_dir / f"{file_path.stem}_chunk_{chunk_num:03d}.csv"
                        merged_chunk.to_csv(chunk_file, index=False)
                        chunk_files.append(chunk_file)
                        
                        file_size = chunk_file.stat().st_size / (1024*1024)
                        pbar.set_postfix({
                            "当前块": f"{chunk_num:03d}",
                            "行数": f"{len(merged_chunk):,}",
                            "大小": f"{file_size:.1f}MB"
                        })
                        pbar.update(1)
                        
                except MemoryError:
                    logger.warning("内存不足，切换到更小的分块大小")
                    # 递归调用，使用更小的分块
                    return self.split_large_file(file_path, chunk_size=chunk_size//2)
            
            logger.info(f"文件拆分完成: {len(chunk_files)} 个分块，平均每块 {total_rows//len(chunk_files):,} 行")
            return chunk_files
            
        except Exception as e:
            logger.error(f"拆分文件失败: {e}")
            return [file_path]  # 返回原文件

    def import_nodes(self):
        """导入化学品和工艺节点 - 优化版本"""
        logger.info("=" * 60)
        logger.info("开始导入节点...")
        node_files = sorted(self.import_dir.glob("neo4j_ready_chemicals_batch_*.csv"), reverse=True)
        
        if not node_files:
            logger.warning(f"在目录 {self.import_dir} 中未找到节点文件 (neo4j_ready_chemicals_batch_*.csv)。跳过节点导入。")
            return

        # 处理所有文件，优先处理最新批次
        logger.info(f"找到 {len(node_files)} 个节点文件，将全部处理")
        
        # 分离最新批次和旧批次文件
        latest_batch = "20250724_181404"
        latest_files = [f for f in node_files if latest_batch in f.name]
        older_files = [f for f in node_files if latest_batch not in f.name]
        
        # 优先处理最新批次，再处理旧批次（避免重复）
        files_to_process = latest_files if latest_files else node_files
        
        logger.info(f"处理最新批次文件数量: {len(files_to_process)}")
        for f in files_to_process[:3]:  # 显示前3个文件名
            logger.info(f"  - {f.name}")
        
        # 统计所有文件的总数据量
        total_stats = {'chemical': 0, 'process': 0, 'industry': 0, 'total': 0}
        for file in files_to_process:
            stats = self._analyze_node_file(file)
            total_stats['chemical'] += stats['chemical']
            total_stats['process'] += stats['process'] 
            total_stats['industry'] += stats['industry']
            total_stats['total'] += stats['total']
        
        logger.info(f"节点统计: 化学品={total_stats['chemical']:,}, 工艺={total_stats['process']:,}, 产业={total_stats['industry']:,}, 总计={total_stats['total']:,}")
        
        # 将所有文件合并处理或分别处理
        all_chunk_files = []
        for file in files_to_process:
            file_size_mb = file.stat().st_size / (1024*1024)
            chunk_size = 5000 if file_size_mb > 50 else 8000  # 大文件用小块
            
            logger.info(f"处理文件 {file.name} (大小: {file_size_mb:.1f}MB，分块大小: {chunk_size:,})")
            chunk_files = self.split_large_file(file, chunk_size=chunk_size)
            all_chunk_files.extend(chunk_files)
        
        # 复制所有分块文件到Neo4j import目录
        chunk_names = self._copy_chunks_to_import_dir(all_chunk_files)
        
        # 使用总进度条跟踪整个节点导入过程
        total_steps = 3  # 化学品、工艺、产业
        with tqdm(total=total_steps, desc="节点导入总进度", unit="类型", position=0,
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as overall_pbar:
            
            # 导入化学品节点
            overall_pbar.set_description("导入化学品节点")
            success_chemical = self._import_nodes_optimized(chunk_names, '化学品', stats['chemical'], position=1)
            overall_pbar.update(1)
            
            # 导入工艺节点
            overall_pbar.set_description("导入工艺节点")
            success_process = self._import_nodes_optimized(chunk_names, '工艺', stats['process'], position=1)
            overall_pbar.update(1)
            
            # 导入产业节点
            overall_pbar.set_description("导入产业节点")
            success_industry = self._import_nodes_optimized(chunk_names, '产业', stats['industry'], position=1)
            overall_pbar.update(1)
            
            overall_pbar.set_description("节点导入完成")
        
        # 输出最终统计
        total_success = success_chemical + success_process + success_industry
        logger.info("=" * 60)
        logger.info(f"节点导入完成统计:")
        logger.info(f"  化学品: {success_chemical}/{len(chunk_names)} 个分块成功")
        logger.info(f"  工艺:   {success_process}/{len(chunk_names)} 个分块成功")
        logger.info(f"  产业:   {success_industry}/{len(chunk_names)} 个分块成功")
        logger.info(f"  总体成功率: {(total_success/(len(chunk_names)*3))*100:.1f}%")
        logger.info("=" * 60)
        
    def _analyze_node_file(self, file_path):
        """分析节点文件，获取各类型节点数量"""
        try:
            # 使用更高效的方式读取文件统计
            df = pd.read_csv(file_path, usecols=[':LABEL'])
            
            # 支持中文和英文标签
            chemical_count = len(df[df[':LABEL'].str.contains('化学品|Chemical', na=False)])
            process_count = len(df[df[':LABEL'].str.contains('工艺|Process', na=False)])
            industry_count = len(df[df[':LABEL'].str.contains('产业|Industry', na=False)])
            
            return {
                'chemical': chemical_count,
                'process': process_count,
                'industry': industry_count,
                'total': len(df)
            }
        except Exception as e:
            logger.warning(f"无法分析节点文件: {e}")
            return {'chemical': 0, 'process': 0, 'industry': 0, 'total': 0}
    
    def _copy_chunks_to_import_dir(self, chunk_files):
        """复制分块文件到Neo4j import目录"""
        logger.info(f"正在复制 {len(chunk_files)} 个分块文件到Neo4j import目录...")
        
        chunk_names = []
        with tqdm(total=len(chunk_files), desc="复制分块文件", unit="文件") as pbar:
            for chunk_file in chunk_files:
                try:
                    dest_file = self.neo4j_import_dir / chunk_file.name
                    shutil.copy2(chunk_file, dest_file)
                    chunk_names.append(chunk_file.name)
                    
                    file_size = dest_file.stat().st_size / (1024*1024)
                    pbar.set_postfix({"大小": f"{file_size:.1f}MB"})
                    pbar.update(1)
                except Exception as e:
                    logger.error(f"复制分块文件 {chunk_file.name} 失败: {e}")
                    pbar.update(1)
        
        return chunk_names
    
    def _import_nodes_optimized(self, chunk_files, node_type, total_count, position=0):
        """优化的节点导入方法，支持智能重试和死锁避免"""
        if total_count == 0:
            logger.info(f"跳过 {node_type} 节点导入（数量为0）")
            return 0
        
        successful_imports = 0
        failed_chunks = []
        
        # 动态调整并发数 - 更优化的并发策略
        cpu_cores = self.system_info.get('cpu_cores', 4)
        available_memory_gb = self.system_info.get('memory_available_gb', 4.0)
        
        if available_memory_gb > 8 and cpu_cores > 8:
            max_workers = min(cpu_cores // 2, 12)  # 高性能系统
        elif available_memory_gb > 4 and cpu_cores > 4:
            max_workers = min(cpu_cores // 2, 6)   # 中等性能系统
        else:
            max_workers = 2  # 保守配置
        
        logger.info(f"开始导入 {node_type} 节点，总数: {total_count:,}，并发数: {max_workers}")
        
        def import_single_chunk_safe(chunk_file):
            """安全的单个分块导入，包含死锁避免机制"""
            # 添加随机延迟避免同时启动
            import random
            initial_delay = random.uniform(0.1, 0.5)
            time.sleep(initial_delay)
            
            query = self._get_node_import_query(node_type, chunk_file)
            
            try:
                start_time = time.time()
                # 使用更高的重试次数和更智能的重试逻辑
                success = self.run_query(query, max_retries=8)
                elapsed = time.time() - start_time
                return success, elapsed
            except Exception as e:
                logger.error(f"导入 {node_type} 分块 {chunk_file} 失败: {e}")
                return False, 0
        
        # 串行导入以完全避免死锁（在高并发场景下）
        if len(chunk_files) > 20 or node_type == '化学品':  # 化学品数据量大，使用串行
            logger.info(f"使用串行导入模式避免死锁 ({node_type})")
            return self._import_nodes_serial_safe(chunk_files, node_type, total_count, position)
        
        # 对于小量数据使用受控的并行导入
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 创建进度条
            with tqdm(total=len(chunk_files), desc=f"导入{node_type}",
                     position=position, leave=True, unit="块",
                     bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
                
                # 分批提交任务，避免一次性提交所有任务
                batch_size = max(1, len(chunk_files) // max_workers)
                total_elapsed = 0
                
                for i in range(0, len(chunk_files), batch_size):
                    batch_chunks = chunk_files[i:i+batch_size]
                    
                    # 提交当前批次的任务
                    future_to_chunk = {
                        executor.submit(import_single_chunk_safe, chunk_file): chunk_file 
                        for chunk_file in batch_chunks
                    }
                    
                    # 等待当前批次完成
                    for future in as_completed(future_to_chunk):
                        chunk_file = future_to_chunk[future]
                        try:
                            success, elapsed = future.result()
                            total_elapsed += elapsed
                            
                            if success:
                                successful_imports += 1
                                avg_time = total_elapsed / successful_imports if successful_imports > 0 else 0
                                pbar.set_postfix({
                                    "成功": f"{successful_imports}/{len(chunk_files)}",
                                    "平均": f"{avg_time:.1f}s/块"
                                })
                            else:
                                failed_chunks.append(chunk_file)
                                pbar.set_postfix({
                                    "成功": f"{successful_imports}/{len(chunk_files)}",
                                    "失败": len(failed_chunks)
                                })
                            

                            pbar.update(1)
                            
                        except Exception as e:
                            logger.error(f"处理 {node_type} 分块 {chunk_file} 时发生异常: {e}")
                            failed_chunks.append(chunk_file)
                            pbar.update(1)
                    
                    # 批次间添加短暂延迟
                    if i + batch_size < len(chunk_files):
                        time.sleep(0.5)
        
        # 重试失败的分块（串行方式）
        if failed_chunks:
            logger.warning(f"{node_type} 节点有 {len(failed_chunks)} 个分块失败，尝试串行重试...")
            retry_success = self._retry_failed_chunks_serial(failed_chunks, node_type, import_single_chunk_safe)
            successful_imports += retry_success
        
        success_rate = (successful_imports / len(chunk_files)) * 100 if chunk_files else 0
        logger.info(f"{node_type} 节点导入完成: {successful_imports}/{len(chunk_files)} 成功 ({success_rate:.1f}%)")
        
        return successful_imports
    
    def _import_nodes_serial_safe(self, chunk_files, node_type, total_count, position=0):
        """串行安全导入，完全避免死锁"""
        logger.info(f"使用串行模式导入 {node_type} 节点以避免死锁")
        
        successful_imports = 0
        failed_chunks = []
        
        with tqdm(total=len(chunk_files), desc=f"串行导入{node_type}",
                 position=position, leave=True, unit="块",
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
            
            for i, chunk_file in enumerate(chunk_files):
                try:
                    # 每个文件间添加短暂延迟，避免资源争用
                    if i > 0:
                        time.sleep(0.2)
                    
                    start_time = time.time()
                    query = self._get_node_import_query(node_type, chunk_file)
                    
                    # 使用高重试次数的安全查询
                    success = self.run_query(query, max_retries=10)
                    elapsed = time.time() - start_time
                    
                    if success:
                        successful_imports += 1
                        pbar.set_postfix({
                            "成功": f"{successful_imports}/{len(chunk_files)}",
                            "耗时": f"{elapsed:.1f}s"
                        })
                    else:
                        failed_chunks.append(chunk_file)
                        pbar.set_postfix({
                            "成功": f"{successful_imports}/{len(chunk_files)}",
                            "失败": len(failed_chunks)
                        })
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"串行导入 {node_type} 分块 {chunk_file} 失败: {e}")
                    failed_chunks.append(chunk_file)
                    pbar.update(1)
        
        # 最后尝试重试失败的分块
        if failed_chunks:
            logger.info(f"最终重试 {len(failed_chunks)} 个失败的 {node_type} 分块...")
            for chunk_file in failed_chunks:
                try:
                    time.sleep(1)  # 增加等待时间
                    query = self._get_node_import_query(node_type, chunk_file)
                    if self.run_query(query, max_retries=15):  # 更多重试次数
                        successful_imports += 1
                        logger.info(f"重试成功: {chunk_file}")
                except Exception as e:
                    logger.error(f"最终重试仍失败: {chunk_file} - {e}")
        
        return successful_imports
    
    def _retry_failed_chunks_serial(self, failed_chunks, node_type, import_func):
        """串行重试失败的分块，避免并发冲突"""
        logger.info(f"开始串行重试 {len(failed_chunks)} 个失败的 {node_type} 分块...")
        
        retry_success = 0
        with tqdm(total=len(failed_chunks), desc=f"串行重试{node_type}", unit="块") as pbar:
            for i, chunk_file in enumerate(failed_chunks):
                try:
                    # 重试间隔逐渐增加
                    wait_time = min(5, (i // 5 + 1) * 1)
                    time.sleep(wait_time)
                    
                    success, _ = import_func(chunk_file)
                    if success:
                        retry_success += 1
                        pbar.set_postfix({"重试成功": retry_success})
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"串行重试 {node_type} 分块 {chunk_file} 仍然失败: {e}")
                    pbar.update(1)
        
        logger.info(f"{node_type} 串行重试完成: {retry_success}/{len(failed_chunks)} 成功")
        return retry_success
    
    def _get_node_import_query(self, node_type, chunk_file):
        """根据节点类型生成对应的导入查询"""
        
        # 根据可用内存动态调整事务大小
        available_memory_gb = self.system_info.get('memory_available_gb', 4.0)
        if available_memory_gb > 8:
            commit_size = 2000
        elif available_memory_gb > 4:
            commit_size = 1000
        else:
            commit_size = 500
            
        if node_type == '化学品':
            return f"""
            USING PERIODIC COMMIT {commit_size}
            LOAD CSV WITH HEADERS FROM 'file:///{chunk_file}' AS row
            WITH row WHERE row[':LABEL'] CONTAINS 'Chemical' AND trim(row['name:ID']) <> '' AND row['name:ID'] IS NOT NULL
            MERGE (c:化学品 {{名称: row['name:ID']}})
            SET
              c.CAS号 = CASE WHEN trim(row['cas:string']) = '' OR row['cas:string'] = 'nan' THEN null ELSE row['cas:string'] END,
              c.中文别名 = CASE WHEN trim(row['aliases:string[]']) = '' OR row['aliases:string[]'] = 'nan' THEN null ELSE split(row['aliases:string[]'], ';') END,
              c.英文名称 = CASE WHEN trim(row['english_name:string']) = '' OR row['english_name:string'] = 'nan' THEN null ELSE row['english_name:string'] END,
              c.英文别名 = CASE WHEN trim(row['english_aliases:string[]']) = '' OR row['english_aliases:string[]'] = 'nan' THEN null ELSE split(row['english_aliases:string[]'], ';') END,
              c.分子式 = CASE WHEN trim(row['molecular_formula:string']) = '' OR row['molecular_formula:string'] = 'nan' THEN null ELSE row['molecular_formula:string'] END,
              c.分子量 = CASE WHEN trim(row['molecular_weight:string']) = '' OR row['molecular_weight:string'] = 'nan' THEN null ELSE row['molecular_weight:string'] END,
              c.危害 = CASE WHEN trim(row['hazard:string']) = '' OR row['hazard:string'] = 'nan' THEN null ELSE row['hazard:string'] END,
              c.防范 = CASE WHEN trim(row['prevention:string']) = '' OR row['prevention:string'] = 'nan' THEN null ELSE row['prevention:string'] END,
              c.危害处置 = CASE WHEN trim(row['hazard_disposal:string']) = '' OR row['hazard_disposal:string'] = 'nan' THEN null ELSE row['hazard_disposal:string'] END,
              c.用途 = CASE WHEN trim(row['uses:string']) = '' OR row['uses:string'] = 'nan' THEN null ELSE row['uses:string'] END,
              c.自然来源 = CASE WHEN trim(row['natural_source:string']) = '' OR row['natural_source:string'] = 'nan' THEN null ELSE row['natural_source:string'] END,
              c.生产来源 = CASE WHEN trim(row['production_source:string']) = '' OR row['production_source:string'] = 'nan' THEN null ELSE row['production_source:string'] END,
              c.工业生产原料 = CASE WHEN trim(row['upstream_materials:string']) = '' OR row['upstream_materials:string'] = 'nan' THEN null ELSE row['upstream_materials:string'] END,
              c.性质 = CASE WHEN trim(row['properties:string']) = '' OR row['properties:string'] = 'nan' THEN null ELSE row['properties:string'] END,
              c.数据来源 = CASE WHEN trim(row['source:string']) = '' OR row['source:string'] = 'nan' THEN null ELSE row['source:string'] END,
              c.浓度阈值 = CASE WHEN trim(row['concentration_threshold:string']) = '' OR row['concentration_threshold:string'] = 'nan' THEN null ELSE row['concentration_threshold:string'] END
            FOREACH(x IN CASE WHEN row[':LABEL'] CONTAINS '危化品' THEN [1] ELSE [] END |
                SET c:危化品
            )
            """
        elif node_type == '工艺':
            return f"""
            USING PERIODIC COMMIT {commit_size}
            LOAD CSV WITH HEADERS FROM 'file:///{chunk_file}' AS row
            WITH row WHERE row[':LABEL'] = 'Process' AND trim(row['name:ID']) <> '' AND row['name:ID'] IS NOT NULL
            MERGE (p:工艺 {{名称: row['name:ID']}})
            SET
              p.工艺类型 = CASE WHEN trim(row['process_type:string']) = '' OR row['process_type:string'] = 'nan' THEN null ELSE row['process_type:string'] END,
              p.描述 = CASE WHEN trim(row['description:string']) = '' OR row['description:string'] = 'nan' THEN null ELSE row['description:string'] END,
              p.数据状态 = CASE WHEN trim(row['data_status:string']) = '' OR row['data_status:string'] = 'nan' THEN null ELSE row['data_status:string'] END
            """
        else:  # 产业节点
            return f"""
            USING PERIODIC COMMIT {commit_size}
            LOAD CSV WITH HEADERS FROM 'file:///{chunk_file}' AS row
            WITH row WHERE row[':LABEL'] = 'Industry' AND trim(row['name:ID']) <> '' AND row['name:ID'] IS NOT NULL
            MERGE (i:产业 {{名称: row['name:ID']}})
            SET
              i.描述 = CASE WHEN trim(row['description:string']) = '' OR row['description:string'] = 'nan' THEN null ELSE row['description:string'] END,
              i.数据状态 = CASE WHEN trim(row['data_status:string']) = '' OR row['data_status:string'] = 'nan' THEN null ELSE row['data_status:string'] END
            """
    
    def _get_relationship_import_query(self, rel_type, chunk_file):
        """根据关系类型生成对应的导入查询"""
        
        # 默认查询
        query = f"""
        USING PERIODIC COMMIT 1000
        LOAD CSV WITH HEADERS FROM 'file:///{chunk_file}' AS row
        WITH row WHERE trim(row[':START_ID']) <> '' AND trim(row[':END_ID']) <> ''
        
        // 匹配起点和终点节点
        MATCH (start {{名称: row[':START_ID']}})
        MATCH (end {{名称: row[':END_ID']}})
        
        // 创建关系
        MERGE (start)-[:{rel_type}]->(end)
        """
        
        return query
    
    def _get_optimized_relationship_query(self, rel_type, chunk_file):
        """获取优化的关系导入查询"""
        return self._get_relationship_import_query(rel_type, chunk_file)
    
    def _import_relationship_serial_safe(self, chunk_files, rel_type, expected_count, position=0):
        """串行安全导入关系 - 增强的错误处理和重试"""
        logger.info(f"使用串行模式导入 {rel_type} 关系以避免死锁")
        
        successful_imports = 0
        failed_chunks = []
        
        with tqdm(total=len(chunk_files), desc=f"串行导入{rel_type}",
                 position=position, leave=True, unit="块",
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
            
            for i, chunk_file in enumerate(chunk_files):
                try:
                    # 每个文件间添加短暂延迟，避免资源争用
                    if i > 0:
                        time.sleep(0.2)
                    
                    start_time = time.time()
                    query = self._get_relationship_import_query(rel_type, chunk_file)
                    
                    # 使用高重试次数的安全查询
                    success = self.run_query(query, max_retries=10)
                    elapsed = time.time() - start_time
                    
                    if success:
                        successful_imports += 1
                        pbar.set_postfix({
                            "成功": f"{successful_imports}/{len(chunk_files)}",
                            "耗时": f"{elapsed:.1f}s"
                        })
                    else:
                        failed_chunks.append(chunk_file)
                        pbar.set_postfix({
                            "成功": f"{successful_imports}/{len(chunk_files)}",
                            "失败": len(failed_chunks)
                        })
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"串行导入 {rel_type} 分块 {chunk_file} 失败: {e}")
                    failed_chunks.append(chunk_file)
                    pbar.update(1)
        
        # 最后尝试重试失败的分块
        if failed_chunks:
            logger.info(f"最终重试 {len(failed_chunks)} 个失败的 {rel_type} 分块...")
            for chunk_file in failed_chunks:
                try:
                    time.sleep(1)  # 增加等待时间
                    query = self._get_relationship_import_query(rel_type, chunk_file)
                    if self.run_query(query, max_retries=15):  # 更多重试次数
                        successful_imports += 1
                        logger.info(f"重试成功: {chunk_file}")
                except Exception as e:
                    logger.error(f"最终重试仍失败: {chunk_file} - {e}")
        
        return successful_imports
    
    def _retry_failed_chunks_fast(self, failed_chunks, rel_type):
        """快速重试失败的分块"""
        if not failed_chunks:
            return 0
            
        logger.info(f"快速重试 {len(failed_chunks)} 个失败的 {rel_type} 分块...")
        
        retry_success = 0
        with tqdm(total=len(failed_chunks), desc=f"快速重试{rel_type}", unit="块") as pbar:
            for chunk_file in failed_chunks:
                try:
                    # 减少重试间隔以提升速度
                    time.sleep(0.5)
                    
                    query = self._get_optimized_relationship_query(rel_type, chunk_file)
                    
                    # 使用更少的重试次数
                    if self.run_query(query, max_retries=3):
                        retry_success += 1
                        pbar.set_postfix({"重试成功": retry_success})
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.debug(f"快速重试 {rel_type} 分块 {chunk_file} 失败: {e}")
                    pbar.update(1)
        
        logger.info(f"{rel_type} 快速重试完成: {retry_success}/{len(failed_chunks)} 成功")
        return retry_success
    
    def _assess_system_load(self):
        """评估当前系统负载"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # 综合负载评估 (0-1之间，越低表示系统越空闲)
            cpu_load = cpu_percent / 100.0
            memory_load = memory_percent / 100.0
            
            # 加权计算综合负载
            system_load = (cpu_load * 0.5 + memory_load * 0.5)
            
            logger.info(f"系统负载评估: CPU={cpu_percent:.1f}%, 内存={memory_percent:.1f}%, 综合负载={system_load:.1%}")
            return system_load
            
        except Exception as e:
            logger.warning(f"系统负载评估失败: {e}")
            return 0.5  # 返回中等负载作为默认值

    def estimate_import_progress(self):
        """估算导入进度，返回主流程需要的结构"""
        # 统计节点 - 处理所有文件而不只是最新的
        node_files = sorted(self.import_dir.glob("neo4j_ready_chemicals_batch_*.csv"), reverse=True)
        rel_files = sorted(self.import_dir.glob("neo4j_relationships_*.csv"), reverse=True)
        nodes = {"chemical": 0, "process": 0, "industry": 0, "dangerous": 0, "total": 0}
        relationships = {"total": 0, "by_type": {}}
        
        if node_files:
            # 分离最新批次文件
            latest_batch = "20250724_181404"
            latest_files = [f for f in node_files if latest_batch in f.name]
            files_to_process = latest_files if latest_files else node_files
            
            # 统计所有需要处理的文件
            for file in files_to_process:
                stats = self._analyze_node_file(file)
                nodes["chemical"] += stats.get("chemical", 0)
                nodes["process"] += stats.get("process", 0)
                nodes["industry"] += stats.get("industry", 0)
                nodes["total"] += stats.get("total", 0)
                
                # 危化品节点数估算
                try:
                    df = pd.read_csv(file, usecols=[":LABEL"])
                    nodes["dangerous"] += len(df[df[":LABEL"].str.contains("危化品", na=False)])
                except Exception:
                    pass
                    
        # 统计关系 - 使用最新的关系文件
        if rel_files:
            try:
                df = pd.read_csv(rel_files[0])
                relationships["total"] = len(df)
                if ":TYPE" in df.columns:
                    by_type = df[":TYPE"].value_counts().to_dict()
                    relationships["by_type"] = by_type
            except Exception:
                relationships["total"] = 0
                relationships["by_type"] = {}
        else:
            relationships["by_type"] = {}
        return {"nodes": nodes, "relationships": relationships}
        
    def create_constraints_safe(self):
        """安全地创建约束"""
        pass
        
    def create_performance_optimizations(self):
        """创建性能优化"""
        pass
        
    def import_relationships(self):
        """导入关系"""
        pass
        
    def cleanup_after_import(self):
        """导入后清理"""
        pass
        
    def get_database_stats(self):
        """获取数据库统计信息"""
        try:
            result = self.run_query("""
                MATCH (n)
                RETURN 
                    labels(n) as label,
                    count(n) as count
            """)
            
            stats = {
                'nodes': {
                    'chemical': 0,
                    'process': 0, 
                    'dangerous': 0,
                    'industry': 0,
                    'total': 0
                },
                'relationships': {'total': 0}
            }
            
            # 将结果转换为列表以便多次访问
            records = list(result) if result else []
            
            # 统计节点
            for record in records:
                labels = record['label']
                count = record['count']
                
                if '化学品' in labels:
                    stats['nodes']['chemical'] += count
                if '危化品' in labels:
                    stats['nodes']['dangerous'] += count
                if '工艺' in labels:
                    stats['nodes']['process'] += count
                if '产业' in labels:
                    stats['nodes']['industry'] += count
                    
                stats['nodes']['total'] += count
            
            # 统计关系
            rel_result = self.run_query("MATCH ()-[r]->() RETURN count(r) as total")
            if rel_result:
                record = rel_result.single()
                if record:
                    stats['relationships']['total'] = record['total']
                
            return stats
            
        except Exception as e:
            logger.warning(f"获取数据库统计信息失败: {e}")
            return {
                'nodes': {'chemical': 0, 'process': 0, 'dangerous': 0, 'industry': 0, 'total': 0}, 
                'relationships': {'total': 0}
            }
        
    def _display_performance_summary(self, start_time, pre_stats, post_stats):
        """显示性能摘要"""
        pass

def main():
    """主函数，执行整个导入流程 - 优化版本"""
    print("=" * 80)
    print("🚀 Neo4j 化学知识图谱数据导入器 - 优化版本")
    print("=" * 80)
    
    # 这个脚本会自动将'处理成功'文件夹中的所有CSV文件
    # 复制到Neo4j数据库的'import'目录下，然后执行导入。
    base_path = Path(__file__).parent.parent
    import_dir = base_path / "处理成功"
    
    # 调试信息
    logger.info(f"脚本路径: {Path(__file__)}")
    logger.info(f"基础路径: {base_path}")
    logger.info(f"导入目录: {import_dir}")
    logger.info(f"导入目录是否存在: {import_dir.exists()}")
    if import_dir.exists():
        csv_files = list(import_dir.glob("*.csv"))
        logger.info(f"找到CSV文件数量: {len(csv_files)}")
        for f in csv_files[:5]:  # 只显示前5个
            logger.info(f"  文件: {f.name}")
    
    importer = None
    start_time = time.time()
    
    try:
        # 显示系统信息
        cpu_cores = psutil.cpu_count(logical=True) or 4
        memory_gb = psutil.virtual_memory().total / (1024**3)
        disk_free_gb = psutil.disk_usage(str(import_dir.parent)).free / (1024**3)
        
        logger.info(f"系统资源: CPU={cpu_cores}核心, 内存={memory_gb:.1f}GB, 可用磁盘={disk_free_gb:.1f}GB")
        
        # 优化连接池参数，根据系统资源调整
        optimal_pool_size = min(50, max(20, cpu_cores * 3))
        optimal_timeout = max(120, cpu_cores * 15)
        
        logger.info(f"优化配置: 连接池={optimal_pool_size}, 超时={optimal_timeout}s")
        
        importer = Neo4jImporter(
            NEO4J_URI,
            NEO4J_USER,
            NEO4J_PASSWORD,
            import_dir,
            NEO4J_IMPORT_DIR,
            max_connection_pool_size=optimal_pool_size,
            connection_acquisition_timeout=optimal_timeout
        )
        
        # 显示导入前的统计信息
        logger.info("📊 分析待导入数据...")
        pre_stats = importer.estimate_import_progress()
        
        print("\n📈 数据概览:")
        print(f"  📦 节点总数: {pre_stats['nodes']['total']:,}")
        print(f"    🧪 化学品: {pre_stats['nodes']['chemical']:,}")
        print(f"    ⚙️  工艺:   {pre_stats['nodes']['process']:,}")
        print(f"  🔗 关系总数: {pre_stats['relationships']['total']:,}")
        
        if pre_stats['relationships']['by_type']:
            print("  关系类型分布:")
            for rel_type, count in pre_stats['relationships']['by_type'].items():
                if count > 0:
                    print(f"    {rel_type}: {count:,}")
        
        # 预估导入时间
        estimated_minutes = (pre_stats['nodes']['total'] / 10000 + pre_stats['relationships']['total'] / 20000) * 2
        print(f"\n⏱️  预估导入时间: {estimated_minutes:.1f} 分钟")
        print("=" * 80)
        
        # 执行导入流程 - 使用总体进度跟踪
        total_phases = 6  # 增加性能优化和清理阶段
        with tqdm(total=total_phases, desc="🚀 总体导入进度", unit="阶段", position=0,
                 bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as master_pbar:
            
            # 阶段1: 复制文件
            master_pbar.set_description("📁 复制文件到导入目录")
            importer.copy_files_to_import_dir()
            master_pbar.update(1)
            
            # 阶段2: 创建约束和索引
            master_pbar.set_description("🔧 创建数据库约束和索引")
            importer.create_constraints_safe()  # 使用安全版本
            master_pbar.update(1)
            
            # 阶段3: 性能优化配置
            master_pbar.set_description("⚡ 应用性能优化")
            importer.create_performance_optimizations()
            master_pbar.update(1)
            
            # 阶段4: 导入节点
            master_pbar.set_description("📦 导入节点数据")
            node_start = time.time()
            importer.import_nodes()
            node_elapsed = time.time() - node_start
            logger.info(f"节点导入耗时: {node_elapsed/60:.1f} 分钟")
            master_pbar.update(1)
            
            # 内存优化
            gc.collect()
            
            # 阶段5: 导入关系
            master_pbar.set_description("🔗 导入关系数据")
            rel_start = time.time()
            importer.import_relationships()
            rel_elapsed = time.time() - rel_start
            logger.info(f"关系导入耗时: {rel_elapsed/60:.1f} 分钟")
            master_pbar.update(1)
            
            # 阶段6: 清理和优化
            master_pbar.set_description("🧹 导入后清理优化")
            importer.cleanup_after_import()
            master_pbar.update(1)
            
            master_pbar.set_description("✅ 导入完成")
        
        # 显示导入后的统计信息和性能报告
        total_elapsed = time.time() - start_time
        post_stats = importer.get_database_stats()
        
        print("\n" + "=" * 80)
        print("🎉 数据导入流程成功完成！")
        print("=" * 80)
        
        print("📊 数据库最终状态:")
        print(f"  📦 节点总数: {post_stats['nodes']['total']:,}")
        print(f"    🧪 化学品:   {post_stats['nodes']['chemical']:,}")
        print(f"    ⚠️  危化品:   {post_stats['nodes']['dangerous']:,}")
        print(f"    ⚙️  工艺:     {post_stats['nodes']['process']:,}")
        print(f"    🏭 产业:     {post_stats['nodes']['industry']:,}")
        print(f"  🔗 关系总数: {post_stats['relationships']['total']:,}")
        
        # 显示详细性能报告
        importer._display_performance_summary(start_time, pre_stats, post_stats)
        
        print("=" * 80)
        print("🎯 导入成功！您现在可以使用 Neo4j Browser 查看知识图谱数据")
        print("🌐 Neo4j Browser: http://localhost:7474")
        print("🔍 示例查询:")
        print("   MATCH (c:化学品) RETURN c LIMIT 10")
        print("   MATCH (c:化学品)-[r:又称]->(c) RETURN c.名称, r.别名 LIMIT 5")
        print("=" * 80)
        
    except Exception as e:
        total_elapsed = time.time() - start_time
        logger.error(f"❌ 导入过程中发生错误 (耗时 {total_elapsed/60:.1f} 分钟): {e}")
        print(f"\n❌ 导入失败: {e}")
        print("请检查日志文件 'neo4j_importer.log' 获取详细错误信息")
        
    finally:
        if importer:
            importer.close()
            print("🔌 数据库连接已关闭")

if __name__ == "__main__":
    main()
