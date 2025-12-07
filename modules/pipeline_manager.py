#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据流程管理器
提供断点续传、状态持久化、并行处理、错误恢复等功能
"""

import os
import json
import hashlib
import logging
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """流程阶段状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PAUSED = "paused"


@dataclass
class StageResult:
    """阶段执行结果"""
    stage_name: str
    status: StageStatus
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0
    input_file: Optional[str] = None
    output_file: Optional[str] = None
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self):
        d = asdict(self)
        d['status'] = self.status.value
        return d
    
    @classmethod
    def from_dict(cls, d):
        d['status'] = StageStatus(d['status'])
        return cls(**d)


@dataclass
class PipelineState:
    """流程状态"""
    pipeline_id: str
    created_at: str
    updated_at: str
    current_stage: Optional[str] = None
    stages: Dict[str, StageResult] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    checkpoints: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self):
        return {
            'pipeline_id': self.pipeline_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'current_stage': self.current_stage,
            'stages': {k: v.to_dict() for k, v in self.stages.items()},
            'config': self.config,
            'checkpoints': self.checkpoints
        }
    
    @classmethod
    def from_dict(cls, d):
        stages = {k: StageResult.from_dict(v) for k, v in d.get('stages', {}).items()}
        return cls(
            pipeline_id=d['pipeline_id'],
            created_at=d['created_at'],
            updated_at=d['updated_at'],
            current_stage=d.get('current_stage'),
            stages=stages,
            config=d.get('config', {}),
            checkpoints=d.get('checkpoints', {})
        )


class CheckpointManager:
    """断点管理器 - 支持数据处理的断点续传"""
    
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
    
    def save_checkpoint(self, stage_name: str, data: Dict[str, Any]):
        """保存检查点"""
        with self._lock:
            checkpoint_file = self.checkpoint_dir / f"{stage_name}_checkpoint.json"
            data['timestamp'] = datetime.now().isoformat()
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Checkpoint saved for {stage_name}")
    
    def load_checkpoint(self, stage_name: str) -> Optional[Dict[str, Any]]:
        """加载检查点"""
        checkpoint_file = self.checkpoint_dir / f"{stage_name}_checkpoint.json"
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    
    def clear_checkpoint(self, stage_name: str):
        """清除检查点"""
        checkpoint_file = self.checkpoint_dir / f"{stage_name}_checkpoint.json"
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.debug(f"Checkpoint cleared for {stage_name}")
    
    def save_dataframe_checkpoint(self, stage_name: str, df: pd.DataFrame, 
                                   processed_indices: List[int], metadata: Dict = None):
        """保存 DataFrame 处理进度检查点"""
        checkpoint_data = {
            'processed_indices': processed_indices,
            'total_rows': len(df),
            'metadata': metadata or {}
        }
        # 保存已处理的数据
        partial_file = self.checkpoint_dir / f"{stage_name}_partial.csv"
        if len(processed_indices) > 0:
            df.iloc[processed_indices].to_csv(partial_file, index=False, encoding='utf-8-sig')
        
        self.save_checkpoint(stage_name, checkpoint_data)
    
    def load_dataframe_checkpoint(self, stage_name: str) -> Optional[Dict]:
        """加载 DataFrame 处理进度"""
        checkpoint = self.load_checkpoint(stage_name)
        if checkpoint:
            partial_file = self.checkpoint_dir / f"{stage_name}_partial.csv"
            if partial_file.exists():
                checkpoint['partial_df'] = pd.read_csv(partial_file, encoding='utf-8-sig')
            return checkpoint
        return None


class PipelineManager:
    """
    统一流程管理器
    
    功能：
    - 流程状态持久化
    - 断点续传
    - 错误恢复
    - 进度追踪
    - 并行处理
    """
    
    def __init__(self, state_dir: str = "data/.pipeline_state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        self.checkpoint_manager = CheckpointManager(self.state_dir / "checkpoints")
        
        self.stages: Dict[str, Callable] = {}
        self.stage_order: List[str] = []
        self.state: Optional[PipelineState] = None
        
        self._progress_callbacks: List[Callable] = []
        self._status_callbacks: List[Callable] = []
        self._lock = threading.Lock()
        
        logger.info(f"PipelineManager initialized with state dir: {self.state_dir}")
    
    def register_stage(self, name: str, handler: Callable, order: int = None):
        """
        注册流程阶段
        
        Args:
            name: 阶段名称
            handler: 处理函数，签名为 (input_data, config, checkpoint_manager) -> output_data
            order: 执行顺序，默认按注册顺序
        """
        self.stages[name] = handler
        if order is not None:
            self.stage_order.insert(order, name)
        else:
            self.stage_order.append(name)
        logger.info(f"Registered stage: {name}")
    
    def on_progress(self, callback: Callable[[str, int, int], None]):
        """注册进度回调 (stage_name, current, total)"""
        self._progress_callbacks.append(callback)
    
    def on_status_change(self, callback: Callable[[str, StageStatus], None]):
        """注册状态变更回调 (stage_name, status)"""
        self._status_callbacks.append(callback)
    
    def _emit_progress(self, stage_name: str, current: int, total: int):
        for cb in self._progress_callbacks:
            try:
                cb(stage_name, current, total)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")
    
    def _emit_status(self, stage_name: str, status: StageStatus):
        for cb in self._status_callbacks:
            try:
                cb(stage_name, status)
            except Exception as e:
                logger.warning(f"Status callback error: {e}")
    
    def create_pipeline(self, pipeline_id: str = None, config: Dict = None) -> str:
        """创建新的流程实例"""
        if pipeline_id is None:
            pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        now = datetime.now().isoformat()
        self.state = PipelineState(
            pipeline_id=pipeline_id,
            created_at=now,
            updated_at=now,
            config=config or {}
        )
        
        # 初始化所有阶段状态
        for stage_name in self.stage_order:
            self.state.stages[stage_name] = StageResult(
                stage_name=stage_name,
                status=StageStatus.PENDING
            )
        
        self._save_state()
        logger.info(f"Created pipeline: {pipeline_id}")
        return pipeline_id
    
    def load_pipeline(self, pipeline_id: str) -> bool:
        """加载已存在的流程状态"""
        state_file = self.state_dir / f"{pipeline_id}.json"
        if state_file.exists():
            with open(state_file, 'r', encoding='utf-8') as f:
                self.state = PipelineState.from_dict(json.load(f))
            logger.info(f"Loaded pipeline: {pipeline_id}")
            return True
        return False
    
    def _save_state(self):
        """保存流程状态"""
        if self.state:
            with self._lock:
                self.state.updated_at = datetime.now().isoformat()
                state_file = self.state_dir / f"{self.state.pipeline_id}.json"
                with open(state_file, 'w', encoding='utf-8') as f:
                    json.dump(self.state.to_dict(), f, ensure_ascii=False, indent=2)
    
    def run_stage(self, stage_name: str, input_data: Any = None, 
                  resume: bool = True) -> Optional[Any]:
        """
        运行单个阶段
        
        Args:
            stage_name: 阶段名称
            input_data: 输入数据
            resume: 是否从断点恢复
        """
        if stage_name not in self.stages:
            raise ValueError(f"Unknown stage: {stage_name}")
        
        stage_result = self.state.stages[stage_name]
        handler = self.stages[stage_name]
        
        # 检查是否可以从断点恢复
        checkpoint = None
        if resume:
            checkpoint = self.checkpoint_manager.load_checkpoint(stage_name)
            if checkpoint:
                logger.info(f"Resuming {stage_name} from checkpoint")
        
        # 更新状态
        stage_result.status = StageStatus.RUNNING
        stage_result.start_time = datetime.now().isoformat()
        self.state.current_stage = stage_name
        self._save_state()
        self._emit_status(stage_name, StageStatus.RUNNING)
        
        start_time = time.time()
        
        try:
            # 执行处理函数
            output_data = handler(
                input_data, 
                self.state.config,
                self.checkpoint_manager,
                checkpoint,
                lambda cur, total: self._emit_progress(stage_name, cur, total)
            )
            
            # 成功完成
            stage_result.status = StageStatus.COMPLETED
            stage_result.end_time = datetime.now().isoformat()
            stage_result.duration_seconds = time.time() - start_time
            
            # 清除检查点
            self.checkpoint_manager.clear_checkpoint(stage_name)
            
            self._save_state()
            self._emit_status(stage_name, StageStatus.COMPLETED)
            
            logger.info(f"Stage {stage_name} completed in {stage_result.duration_seconds:.2f}s")
            return output_data
            
        except Exception as e:
            stage_result.status = StageStatus.FAILED
            stage_result.end_time = datetime.now().isoformat()
            stage_result.duration_seconds = time.time() - start_time
            stage_result.error_message = str(e)
            
            self._save_state()
            self._emit_status(stage_name, StageStatus.FAILED)
            
            logger.error(f"Stage {stage_name} failed: {e}", exc_info=True)
            raise
    
    def run_all(self, start_from: str = None, stop_at: str = None, 
                skip_completed: bool = True) -> Dict[str, Any]:
        """
        运行所有阶段
        
        Args:
            start_from: 从指定阶段开始
            stop_at: 到指定阶段停止
            skip_completed: 是否跳过已完成的阶段
        """
        results = {}
        started = start_from is None
        input_data = None
        
        for stage_name in self.stage_order:
            if not started:
                if stage_name == start_from:
                    started = True
                else:
                    continue
            
            stage_result = self.state.stages.get(stage_name)
            
            # 检查是否跳过已完成
            if skip_completed and stage_result and stage_result.status == StageStatus.COMPLETED:
                logger.info(f"Skipping completed stage: {stage_name}")
                # 尝试加载上一阶段的输出作为下一阶段的输入
                if stage_result.output_file and Path(stage_result.output_file).exists():
                    input_data = pd.read_csv(stage_result.output_file, encoding='utf-8-sig')
                continue
            
            try:
                output_data = self.run_stage(stage_name, input_data, resume=True)
                results[stage_name] = output_data
                input_data = output_data
            except Exception as e:
                logger.error(f"Pipeline stopped at stage {stage_name}: {e}")
                results[stage_name] = {'error': str(e)}
                break
            
            if stop_at and stage_name == stop_at:
                logger.info(f"Pipeline stopped at requested stage: {stop_at}")
                break
        
        return results
    
    def get_status(self) -> Dict[str, Any]:
        """获取当前流程状态摘要"""
        if not self.state:
            return {'status': 'no_pipeline'}
        
        return {
            'pipeline_id': self.state.pipeline_id,
            'created_at': self.state.created_at,
            'updated_at': self.state.updated_at,
            'current_stage': self.state.current_stage,
            'stages': {
                name: {
                    'status': result.status.value,
                    'duration': result.duration_seconds,
                    'records_processed': result.records_processed,
                    'error': result.error_message
                }
                for name, result in self.state.stages.items()
            }
        }
    
    def list_pipelines(self) -> List[Dict]:
        """列出所有流程"""
        pipelines = []
        for state_file in self.state_dir.glob("pipeline_*.json"):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    pipelines.append({
                        'pipeline_id': data['pipeline_id'],
                        'created_at': data['created_at'],
                        'updated_at': data['updated_at'],
                        'current_stage': data.get('current_stage')
                    })
            except:
                pass
        return sorted(pipelines, key=lambda x: x['created_at'], reverse=True)


class BatchProcessor:
    """
    批量数据处理器
    支持分批处理、断点续传、并行执行
    """
    
    def __init__(self, checkpoint_manager: CheckpointManager, 
                 batch_size: int = 100, max_workers: int = 3):
        self.checkpoint_manager = checkpoint_manager
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._stop_flag = False
    
    def stop(self):
        """请求停止处理"""
        self._stop_flag = True
    
    def process_dataframe(self, df: pd.DataFrame, 
                          process_row: Callable[[pd.Series], Dict],
                          stage_name: str,
                          progress_callback: Callable[[int, int], None] = None,
                          checkpoint: Dict = None) -> pd.DataFrame:
        """
        并行处理 DataFrame
        
        Args:
            df: 输入数据
            process_row: 行处理函数
            stage_name: 阶段名称（用于检查点）
            progress_callback: 进度回调
            checkpoint: 已有的检查点数据
        """
        self._stop_flag = False
        total_rows = len(df)
        
        # 确定已处理的行
        processed_indices = set()
        if checkpoint:
            processed_indices = set(checkpoint.get('processed_indices', []))
            logger.info(f"Resuming from checkpoint: {len(processed_indices)}/{total_rows} rows already processed")
        
        # 确定待处理的行
        pending_indices = [i for i in range(total_rows) if i not in processed_indices]
        
        # 分批处理
        results = {}
        batch_count = 0
        
        for batch_start in range(0, len(pending_indices), self.batch_size):
            if self._stop_flag:
                logger.info("Processing stopped by user request")
                break
            
            batch_indices = pending_indices[batch_start:batch_start + self.batch_size]
            
            # 并行处理批次
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(process_row, df.iloc[idx]): idx 
                    for idx in batch_indices
                }
                
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        result = future.result()
                        if result:
                            results[idx] = result
                        processed_indices.add(idx)
                    except Exception as e:
                        logger.warning(f"Error processing row {idx}: {e}")
                    
                    # 进度回调
                    if progress_callback:
                        progress_callback(len(processed_indices), total_rows)
            
            batch_count += 1
            
            # 每处理几批保存一次检查点
            if batch_count % 5 == 0:
                self.checkpoint_manager.save_checkpoint(stage_name, {
                    'processed_indices': list(processed_indices),
                    'total_rows': total_rows
                })
        
        # 应用结果到 DataFrame
        for idx, row_data in results.items():
            for key, value in row_data.items():
                if key in df.columns:
                    df.at[idx, key] = value
                else:
                    df.loc[idx, key] = value
        
        # 最终保存检查点
        self.checkpoint_manager.save_checkpoint(stage_name, {
            'processed_indices': list(processed_indices),
            'total_rows': total_rows,
            'completed': not self._stop_flag
        })
        
        return df


# ===== 预定义的流程阶段处理器 =====

def data_cleaning_handler(input_data, config, checkpoint_manager, checkpoint, progress_cb):
    """数据清洗阶段处理器"""
    from modules.data_cleaning.data_processor import process_data
    
    input_file = config.get('data_cleaning', {}).get('input_file')
    reference_file = config.get('data_cleaning', {}).get('reference_file')
    output_file = config.get('data_cleaning', {}).get('output_file')
    
    if not input_file or not Path(input_file).exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    success, haz_count, total = process_data(input_file, reference_file, output_file)
    
    if not success:
        raise RuntimeError("Data cleaning failed")
    
    progress_cb(total, total)
    
    return pd.read_csv(output_file, encoding='utf-8-sig')


def data_enrichment_handler(input_data, config, checkpoint_manager, checkpoint, progress_cb):
    """数据补全阶段处理器"""
    from modules.universal_enricher import UniversalEnricher
    import yaml
    
    enrichment_config = config.get('data_enrichment', {})
    api_key = os.environ.get("OPENCHEMKG_API_KEY") or enrichment_config.get('api_key')
    
    if not api_key:
        raise ValueError("API key not configured")
    
    # 加载领域配置
    domain_config_path = Path(config.get('domain_config_path', 'config/domains.yaml'))
    if domain_config_path.exists():
        with open(domain_config_path, 'r', encoding='utf-8') as f:
            domains = yaml.safe_load(f) or {}
    else:
        domains = {}
    
    domain_name = enrichment_config.get('domain', 'chemical')
    domain_config = domains.get(domain_name, {})
    
    # 创建 enricher
    enricher = UniversalEnricher(
        api_key=api_key,
        base_url=enrichment_config.get('base_url'),
        model=enrichment_config.get('model', 'qwen-plus'),
        provider=enrichment_config.get('provider', 'dashscope'),
        options=enrichment_config.get('llm_options')
    )
    
    # 如果 input_data 是 DataFrame，直接使用
    if isinstance(input_data, pd.DataFrame):
        df = input_data
    else:
        input_file = enrichment_config.get('input_file')
        df = pd.read_csv(input_file, encoding='utf-8-sig')
    
    name_col = enrichment_config.get('name_column', '品名')
    if name_col not in df.columns:
        # 尝试猜测
        for col in ['品名', 'Product Name', 'Chemical Name', 'Name', '名称']:
            if col in df.columns:
                name_col = col
                break
    
    # 处理数据
    def progress_wrapper(completed):
        progress_cb(completed, len(df))
    
    result_df = enricher.process_batch(
        df, name_col, domain_config,
        max_workers=enrichment_config.get('max_workers', 3),
        progress_callback=progress_wrapper
    )
    
    # 保存输出
    output_file = enrichment_config.get('output_file')
    if output_file:
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    return result_df


def post_processing_handler(input_data, config, checkpoint_manager, checkpoint, progress_cb):
    """后处理阶段处理器"""
    from modules.post_processing.entity_processor import EntityProcessor
    
    post_config = config.get('post_processing', {})
    base_path = post_config.get('base_path', 'data')
    
    processor = EntityProcessor(base_path)
    
    # 如果有输入数据，保存到临时文件供处理器使用
    if isinstance(input_data, pd.DataFrame):
        input_file = Path(base_path) / "已补全文件" / "temp_input.csv"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_data.to_csv(input_file, index=False, encoding='utf-8-sig')
    
    # 执行处理
    # 这里需要根据 EntityProcessor 的具体实现调整
    # processor.process_all()
    
    output_dir = Path(post_config.get('output_dir', 'data/processed'))
    output_file = output_dir / "final_data.csv"
    
    if output_file.exists():
        return pd.read_csv(output_file, encoding='utf-8-sig')
    return input_data


def graph_construction_handler(input_data, config, checkpoint_manager, checkpoint, progress_cb):
    """图构建阶段处理器"""
    from modules.graph_construction.data_processor import DataProcessor
    from modules.graph_construction.neo4j_exporter import Neo4jExporter
    
    graph_config = config.get('graph_construction', {})
    config_file = graph_config.get('config_file', 'modules/graph_construction/config/config.yaml')
    output_dir = graph_config.get('output_dir', 'data/neo4j')
    
    processor = DataProcessor(config_file)
    
    # 如果有输入数据
    if isinstance(input_data, pd.DataFrame):
        input_file = Path(output_dir) / "temp_input.csv"
        input_file.parent.mkdir(parents=True, exist_ok=True)
        input_data.to_csv(input_file, index=False, encoding='utf-8-sig')
        input_path = str(input_file)
    else:
        input_path = graph_config.get('input_file')
    
    # 执行图构建流程
    result = processor.process_complete_pipeline(input_path, output_dir)
    
    progress_cb(100, 100)
    
    return result


def create_default_pipeline() -> PipelineManager:
    """创建默认的流程管理器"""
    manager = PipelineManager()
    
    # 注册默认阶段
    manager.register_stage('data_cleaning', data_cleaning_handler, 0)
    manager.register_stage('data_enrichment', data_enrichment_handler, 1)
    manager.register_stage('post_processing', post_processing_handler, 2)
    manager.register_stage('graph_construction', graph_construction_handler, 3)
    
    return manager
