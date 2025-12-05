# modules_new/core/status_tracker.py
"""
状态跟踪器

负责跟踪和管理应用程序的运行状态和统计信息
"""

import time
from collections import deque
from typing import Dict, Any, Deque
from .base import BaseManager

class StatusTracker(BaseManager):
    """状态跟踪器"""
    
    def __init__(self, name: str = "StatusTracker"):
        super().__init__(name)
        self._status = "stopped"  # stopped, running, paused, completed, error
        self._task_times: Deque[float] = deque(maxlen=100) # 存储最近100个任务的耗时
        self.reset()

    def get_status(self) -> str:
        """获取当前状态"""
        return self._status

    def set_status(self, status: str):
        """设置当前状态"""
        if self._status != status:
            self._status = status
            self.logger.info(f"状态已更新: {status}")
            if status == 'running' and self._stats['start_time'] is None:
                self._stats['start_time'] = time.time()
            elif status in ['completed', 'error', 'stopped']:
                self._stats['end_time'] = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """获取并计算详细统计信息"""
        stats = self._stats.copy()
        
        # 计算已用时间
        if stats['start_time']:
            if stats['end_time']:
                elapsed_time = stats['end_time'] - stats['start_time']
            else:
                elapsed_time = time.time() - stats['start_time']
        else:
            elapsed_time = 0
        
        stats['elapsed_time'] = elapsed_time
        stats['elapsed_time_str'] = time.strftime('%H:%M:%S', time.gmtime(elapsed_time))

        # 计算速度 (移动平均)
        if self._task_times:
            avg_task_time = sum(self._task_times) / len(self._task_times)
            speed = 60 / avg_task_time if avg_task_time > 0 else 0
        else:
            speed = 0
        stats['speed'] = speed

        # 计算预计剩余时间 (ETA)
        remaining_tasks = stats['total'] - stats['processed']
        if speed > 0 and remaining_tasks > 0:
            eta_seconds = (remaining_tasks / speed) * 60
            stats['eta_str'] = time.strftime('%H:%M:%S', time.gmtime(eta_seconds))
        else:
            stats['eta_str'] = '--:--:--'
            
        # 计算成功率
        if stats['api_calls'] > 0:
            stats['api_success_rate'] = (stats['api_calls'] - stats['api_errors']) / stats['api_calls'] * 100
        else:
            stats['api_success_rate'] = 100.0

        if stats['processed'] > 0:
            stats['parse_success_rate'] = stats['parse_success'] / stats['processed'] * 100
        else:
            stats['parse_success_rate'] = 0.0
            
        # 计算平均响应时间
        if stats['api_calls'] > 0:
            stats['avg_response_time'] = stats['total_response_time'] / stats['api_calls']
        else:
            stats['avg_response_time'] = 0.0
            
        # API状态
        stats['api_status'] = '异常' if stats['api_errors'] > 0 else '正常'

        return stats

    def update_stats(self, **kwargs):
        """更新统计信息"""
        # 记录任务耗时
        task_time = kwargs.pop('task_time', None)
        if task_time is not None:
            self._task_times.append(task_time)

        for key, value in kwargs.items():
            if key in self._stats:
                if isinstance(self._stats[key], (int, float)) and isinstance(value, (int, float)):
                    self._stats[key] += value
                else:
                    self._stats[key] = value
        self.logger.debug(f"统计信息已更新: {kwargs}")

    def reset(self):
        """重置状态和统计信息"""
        self._status = "stopped"
        self._task_times.clear()
        self._stats = {
            'total': 0,
            'processed': 0,
            'success': 0,
            'errors': 0, # 重命名 'failed' -> 'errors'
            'api_calls': 0,
            'api_errors': 0,
            'parse_success': 0,
            'parse_failed': 0,
            'total_response_time': 0.0,
            'estimated_tokens': 0,
            'threads': 0,
            'start_time': None,
            'end_time': None
        }
        self.logger.info("状态和统计信息已重置")
