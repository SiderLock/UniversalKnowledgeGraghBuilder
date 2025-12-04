# modules_new/core/monitoring_manager.py
"""
智能监控管理器

负责整合状态跟踪、数据质量检测和历史趋势分析，提供全面的系统监控。
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd

from .base import BaseManager
from .status_tracker import StatusTracker
from ..intelligence.smart_detector import SmartDetector
from ..core.exceptions import ConfigurationError

class MonitoringManager(BaseManager):
    """智能监控管理器"""

    def __init__(self, status_tracker: StatusTracker, smart_detector: SmartDetector, log_path: str = "logs/monitoring_log.jsonl"):
        super().__init__("MonitoringManager")
        if not isinstance(status_tracker, StatusTracker):
            raise ConfigurationError("status_tracker 必须是 StatusTracker 的实例。")
        if not isinstance(smart_detector, SmartDetector):
            raise ConfigurationError("smart_detector 必须是 SmartDetector 的实例。")
            
        self.status_tracker = status_tracker
        self.smart_detector = smart_detector
        self.log_path = log_path
        self.logger.info("智能监控管理器已初始化。")

    def create_snapshot(self, df_processed: Optional[pd.DataFrame] = None, sample_size: int = 100) -> Dict[str, Any]:
        """
        创建当前系统状态的快照，包括性能和数据质量。
        """
        self.logger.debug("正在创建监控快照...")
        
        # 1. 获取性能统计
        performance_stats = self.status_tracker.get_stats()
        
        # 2. 获取数据质量统计
        data_quality_stats = {}
        if df_processed is not None and not df_processed.empty:
            try:
                # 只对样本进行分析以提高性能
                sample_df = df_processed.sample(n=min(len(df_processed), sample_size)) if len(df_processed) > sample_size else df_processed
                analysis_result = self.smart_detector.analyze(sample_df)
                # 提取关键质量指标
                data_quality_stats = {
                    'completeness': analysis_result.get('completeness_analysis', {}),
                    'quality_indicators': analysis_result.get('quality_indicators', {})
                }
            except Exception as e:
                self.logger.error(f"数据质量分析失败: {e}", exc_info=True)

        # 3. 组合成一个快照
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'status': self.status_tracker.get_status(),
            'performance': performance_stats,
            'data_quality': data_quality_stats
        }
        
        # 4. 将快照写入日志
        self._log_snapshot(snapshot)
        
        self.logger.info("监控快照创建成功。")
        return snapshot

    def _log_snapshot(self, snapshot: Dict[str, Any]):
        """将快照以JSON Lines格式追加到日志文件。"""
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(snapshot, ensure_ascii=False) + '\\n')
        except Exception as e:
            self.logger.error(f"无法写入监控日志: {e}", exc_info=True)

    def get_historical_data(self, limit: int = 100) -> list[Dict[str, Any]]:
        """从日志中读取最近的历史快照数据。"""
        history = []
        try:
            with open(self.log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    history.append(json.loads(line))
            return history[-limit:]
        except FileNotFoundError:
            self.logger.warning("监控日志文件不存在，无法获取历史数据。")
            return []
        except Exception as e:
            self.logger.error(f"读取监控日志失败: {e}", exc_info=True)
            return []

    def check_alerts(self, snapshot: Optional[Dict[str, Any]] = None) -> list[str]:
        """
        根据预定义的规则检查快照，生成警报。
        (这是一个基础实现，可以根据需求扩展)
        """
        if snapshot is None:
            snapshot = self.create_snapshot()

        alerts = []
        perf = snapshot.get('performance', {})
        quality = snapshot.get('data_quality', {}).get('quality_indicators', {})

        # 规则1: API成功率
        if perf.get('api_success_rate', 100) < 90.0:
            alerts.append(f"API成功率低: {perf['api_success_rate']:.2f}%")

        # 规则2: 解析成功率
        if perf.get('parse_success_rate', 100) < 95.0:
            alerts.append(f"数据解析成功率低: {perf['parse_success_rate']:.2f}%")
            
        # 规则3: 数据质量 - 缺失值
        completeness = snapshot.get('data_quality', {}).get('completeness', {})
        if completeness.get('overall_completeness', 100) < 80.0:
            alerts.append(f"数据整体完整度低: {completeness['overall_completeness']:.2f}%")

        if alerts:
            self.logger.warning(f"触发了 {len(alerts)} 条警报: {'; '.join(alerts)}")

        return alerts
