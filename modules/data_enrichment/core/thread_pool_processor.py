# modules_new/core/thread_pool_processor.py
"""
多线程处理器 - 基于API速率限制的智能并发处理

基于Gemini 2.5 Flash API限制优化:
- TPM (每分钟Token数): 1,000,000
- RPD (每日请求数): 10,000
"""

import time
import threading
import queue
import logging
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
import pandas as pd # Moved import to top

from ..core.exceptions import with_error_handling
from ..utils.file_utils import FileUtils # Moved import to top
from ..config.api_config import APIConfig # Corrected import

if TYPE_CHECKING:
    from ..core.status_tracker import StatusTracker
    from ..config.api_config import APIConfig # Added correct TYPE_CHECKING import


@dataclass
class APILimits:
    """API速率限制配置"""
    tokens_per_minute: int = 1_000_000  # TPM: 1,000,000
    requests_per_day: int = 10_000      # RPD: 10,000
    estimated_tokens_per_request: int = 2000  # 估算每个请求的token数
    max_concurrent_threads_override: Optional[int] = None # 允许用户覆盖最大线程数

    def __post_init__(self):
        """验证和修正配置参数"""
        # 确保所有参数都是正数
        if self.tokens_per_minute <= 0:
            self.tokens_per_minute = 1_000_000
        if self.requests_per_day <= 0:
            self.requests_per_day = 10_000
        if self.estimated_tokens_per_request <= 0:
            self.estimated_tokens_per_request = 2000

    @classmethod
    def from_api_config(cls, config: 'APIConfig'):
        """从APIConfig创建APILimits实例"""
        if not config:
            return cls()

        return cls(
            tokens_per_minute=getattr(config, 'tokens_per_minute', 1_000_000),
            requests_per_day=getattr(config, 'requests_per_day', 10_000),
            estimated_tokens_per_request=getattr(config, 'estimated_tokens_per_request', 2000),
            max_concurrent_threads_override=getattr(config, 'max_concurrent_threads', None) if getattr(config, 'auto_optimize', False) else None
        )

    @property
    def max_requests_per_minute(self) -> int:
        """基于Token限制计算每分钟最大请求数"""
        # 防止除零错误
        if self.estimated_tokens_per_request <= 0:
            tokens_based_limit = self.tokens_per_minute // 2000  # 使用默认值2000
        else:
            tokens_based_limit = self.tokens_per_minute // self.estimated_tokens_per_request
        
        return min(
            tokens_based_limit,
            self.requests_per_day // (24 * 60)  # 平均每分钟请求数
        )
    
    @property
    def optimal_delay_seconds(self) -> float:
        """计算最优请求间隔"""
        max_rpm = self.max_requests_per_minute
        if max_rpm <= 0:
            return 1.0  # 默认1秒间隔
        return 60.0 / max_rpm
    
    @property
    def max_concurrent_threads(self) -> int:
        """计算最大并发线程数"""
        if self.max_concurrent_threads_override:
            return self.max_concurrent_threads_override
            
        # 考虑API响应时间（估计5-15秒），计算合理的并发数
        avg_response_time = 10  # 秒
        max_rpm = self.max_requests_per_minute
        
        # 防止除零或负数
        if max_rpm <= 0:
            return 2  # 最小线程数
            
        calculated_threads = int(max_rpm * avg_response_time / 60)
        return min(8, max(2, calculated_threads))


@dataclass
class ProcessingTask:
    """处理任务"""
    index: int
    data: Dict[str, Any]
    chemical_name: str
    retry_count: int = 0


class ThreadPoolProcessor:
    """多线程处理器主类"""
    
    def __init__(self, module_manager):
        self.module_manager = module_manager
        self.api_config_manager = self.module_manager.get_module('api_config_manager')
        self.status_tracker = self.module_manager.get_module('status_tracker')
        self.api_limits = self._get_api_limits_from_config()
        
        self.is_stopped = False
        self.is_paused = False
        self.rate_limit_lock = threading.Lock()
        self.requests_this_minute = 0
        self.minute_start_time = time.time()
        
        self.logger = logging.getLogger(self.__class__.__name__)

        # 用于分批处理和备份
        self.processed_count_since_last_batch = 0
        # Removed self.processed_count_since_last_backup
        self.current_batch_results = []
        self.batch_number = 0

    def _get_api_limits_from_config(self) -> APILimits:
        """从API配置管理器获取APILimits"""
        if self.api_config_manager:
            try:
                # 适配不同的API配置管理器类型
                if hasattr(self.api_config_manager, 'get_current_api_config'):
                    current_config = self.api_config_manager.get_current_api_config()
                elif hasattr(self.api_config_manager, 'get_current_config'):
                    current_config = self.api_config_manager.get_current_config()
                else:
                    current_config = None
                
                if current_config:
                    # 如果配置有to_api_limits方法，使用它
                    if hasattr(current_config, 'to_api_limits'):
                        result = current_config.to_api_limits()
                        # 确保返回的是 APILimits 对象，而不是字典
                        if isinstance(result, dict):
                            return APILimits(**result)
                        elif isinstance(result, APILimits):
                            return result
                    
                    # 否则从配置属性创建APILimits
                    return APILimits.from_api_config(current_config)
            except Exception as e:
                self.logger.warning(f"无法从配置获取API限制: {e}")
        
        # 回退到默认值
        return APILimits()

    def _wait_for_rate_limit(self):
        """等待速率限制"""
        with self.rate_limit_lock:
            current_time = time.time()
            
            # 检查是否需要重置分钟计数器
            if current_time - self.minute_start_time >= 60:
                self.requests_this_minute = 0
                self.minute_start_time = current_time
            
            # 检查是否超过每分钟限制
            if self.requests_this_minute >= self.api_limits.max_requests_per_minute:
                wait_time = 60 - (current_time - self.minute_start_time)
                if wait_time > 0:
                    self.logger.info(f"⏳ 达到每分钟请求限制，等待 {wait_time:.1f} 秒...")
                    time.sleep(wait_time)
                    self.requests_this_minute = 0
                    self.minute_start_time = time.time()
            
            # 记录请求
            self.requests_this_minute += 1
            
            # 基本间隔等待
            time.sleep(self.api_limits.optimal_delay_seconds)

    def _backup_results(self, df_to_backup: pd.DataFrame): # Modified signature to accept DataFrame
        """备份当前处理的结果"""
        backup_manager = self.module_manager.get_module('backup_manager')
        if not backup_manager:
            self.logger.warning("备份管理器未加载，跳过备份")
            return

        try:
            # Removed DataFrame conversion, now accepts DataFrame directly

            if not df_to_backup.empty:
                # Using batch_number for backup filename consistency
                # Modified backup method call from backup_dataframe to create_backup
                backup_manager.create_backup(df_to_backup, f"batch_backup_{self.batch_number}")
                if self.log_callback:
                    self.log_callback(f"📦 已创建备份，包含 {len(df_to_backup)} 条记录", "INFO")
        except Exception as e:
            self.logger.error(f"创建备份失败: {e}")

    def _save_batch_results(self):
        """保存当前批次的结果并创建监控快照"""
        if not self.current_batch_results:
            return

        self.batch_number += 1
        
        try:
            # Removed local imports for pandas and FileUtils

            df_batch = pd.DataFrame([r['data'] for r in self.current_batch_results if r.get('success')])
            
            if not df_batch.empty:
                config_manager = self.module_manager.get_module('config_manager')
                output_folder = config_manager.get_path_config().get('output_folder', 'output_batches')
                
                # 创建批次输出目录
                batch_output_folder = os.path.join(output_folder, 'batches')
                FileUtils.ensure_directory(batch_output_folder) # Corrected method name
                
                output_filename = f"batch_{self.batch_number}.csv"
                output_path = os.path.join(batch_output_folder, output_filename)
                
                csv_processor = self.module_manager.get_module('csv_processor')
                csv_processor.write_csv(df_batch, output_path)
                
                if self.log_callback:
                    self.log_callback(f"📄 已保存批次文件: {output_filename} (包含 {len(df_batch)} 条记录)", "SUCCESS")
                
                # 创建监控快照
                self._create_monitoring_snapshot(df_batch)

                # Call backup after successful save
                self._backup_results(df_batch) # Added call to backup the saved batch

        except Exception as e:
            self.logger.error(f"保存批次文件失败: {e}")
        
        finally:
            # 清空当前批次结果
            self.current_batch_results = []
            self.processed_count_since_last_batch = 0

    def _create_monitoring_snapshot(self, df_batch: pd.DataFrame):
        """创建并记录监控快照"""
        monitoring_manager = self.module_manager.get_module('monitoring_manager')
        if not monitoring_manager:
            self.logger.debug("监控管理器未加载，跳过快照创建")
            return
        
        try:
            monitoring_manager.create_snapshot(df_batch)
            if self.log_callback:
                self.log_callback("📊 已创建新的监控快照。", "DEBUG")
        except Exception as e:
            self.logger.error(f"创建监控快照失败: {e}")

    @with_error_handling()
    def process_batch(self, tasks: List[ProcessingTask], progress_callback=None, log_callback=None) -> List[Dict[str, Any]]:
        """批量处理任务"""
        
        # 存储回调函数
        self.progress_callback = progress_callback
        self.log_callback = log_callback or self.module_manager.get_log_callback()
        
        # 每次批量处理时都重新获取最新的API配置
        self.api_limits = self._get_api_limits_from_config()
        max_workers = self.api_limits.max_concurrent_threads

        # 更新StatusTracker
        if self.status_tracker:
            self.status_tracker.update_stats(
                total=len(tasks),
                threads=max_workers
            )
        
        results = []
        self.is_stopped = False
        self.is_paused = False
        
        # 重置计数器
        self.processed_count_since_last_batch = 0
        # Removed self.processed_count_since_last_backup = 0
        self.current_batch_results = []
        self.batch_number = 0
        
        if self.log_callback:
            self.log_callback(
                f"🚀 启动多线程处理，共 {len(tasks)} 个任务，使用 {max_workers} 个线程", 
                "INFO"
            )
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {
                    executor.submit(self._process_single_task, task): task
                    for task in tasks
                }
                
                for future in as_completed(future_to_task):
                    task = future_to_task.pop(future)
                    
                    while self.is_paused and not self.is_stopped:
                        time.sleep(0.5)
                    
                    if self.is_stopped:
                        if self.log_callback:
                            self.log_callback("⏹️ 用户停止了处理", "WARNING")
                        for f in future_to_task:
                            f.cancel()
                        break
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        # 检查是否需要备份或分批输出
                        if result.get('success'):
                            # Removed self.processed_count_since_last_backup += 1
                            self.processed_count_since_last_batch += 1
                            self.current_batch_results.append(result)

                            # Removed backup check based on processed_count_since_last_backup
                            # if self.processed_count_since_last_backup >= 50:
                            #     self._backup_results(results) # This was incorrect, should use current batch
                            #     self.processed_count_since_last_backup = 0

                       
                            if self.processed_count_since_last_batch >= 100:
                                self._save_batch_results()

                    except Exception as e:
                        error_result = {
                            'index': task.index,
                            'chemical_name': task.chemical_name,
                            'success': False,
                            'error': str(e),
                            'data': task.data
                        }
                        results.append(error_result)
                        if self.status_tracker:
                            self.status_tracker.update_stats(processed=1, errors=1)
                        self.logger.error(f"任务处理失败: {task.chemical_name} - {e}")
                
        except Exception as e:
            self.logger.error(f"批量处理过程中发生错误: {e}")
            if self.log_callback:
                self.log_callback(f"❌ 批量处理错误: {e}", "ERROR")
        
        # 处理最后一批不足100条的结果
        self._save_batch_results()
        
        if self.log_callback:
            self.log_callback("🎉 多线程处理完成", "SUCCESS")
        
        return sorted(results, key=lambda x: x['index'])
    
    def _process_single_task(self, task: ProcessingTask) -> Dict[str, Any]:
        """处理单个任务"""
        task_start_time = time.time()
        
        # 等待速率限制
        self._wait_for_rate_limit()
        
        # 检查停止状态
        if self.is_stopped:
            return {
                'index': task.index,
                'chemical_name': task.chemical_name,
                'success': False,
                'error': 'Processing stopped by user',
                'data': task.data
            }
        
        try:
            core_processor = self.module_manager.get_module('core_processor')
            if not core_processor:
                raise Exception("核心处理器未加载")
            
            # 处理单行数据，现在返回更详细的结果
            processed_result = core_processor.process_single_row(task.data, task.chemical_name)
            
            task_time = time.time() - task_start_time
            
            # 更新统计
            if self.status_tracker:
                stats_update = {
                    'processed': 1,
                    'success': 1 if processed_result.get('success') else 0,
                    'errors': 0 if processed_result.get('success') else 1,
                    'api_calls': processed_result.get('api_calls', 0),
                    'api_errors': processed_result.get('api_errors', 0),
                    'parse_success': 1 if processed_result.get('parse_success') else 0,
                    'total_response_time': processed_result.get('response_time', 0),
                    'estimated_tokens': self.api_limits.estimated_tokens_per_request,
                    'task_time': task_time
                }
                self.status_tracker.update_stats(**stats_update)

            return {
                'index': task.index,
                'chemical_name': task.chemical_name,
                'success': processed_result.get('success', False),
                'data': processed_result.get('data', task.data),
                'error': processed_result.get('error'),
                'thread_id': threading.current_thread().ident
            }
            
        except Exception as e:
            self.logger.error(f"处理任务失败 [{task.chemical_name}]: {e}")
            if self.status_tracker:
                self.status_tracker.update_stats(processed=1, errors=1, task_time=time.time() - task_start_time)
            return {
                'index': task.index,
                'chemical_name': task.chemical_name,
                'success': False,
                'error': str(e),
                'data': task.data
            }
    
    def pause(self):
        """暂停处理"""
        self.is_paused = True
        self.logger.info("⏸️ 多线程处理已暂停")
    
    def resume(self):
        """恢复处理"""
        self.is_paused = False
        self.logger.info("▶️ 多线程处理已恢复")
    
    def stop(self):
        """停止处理"""
        self.is_stopped = True
        self.logger.info("⏹️ 多线程处理已停止")
