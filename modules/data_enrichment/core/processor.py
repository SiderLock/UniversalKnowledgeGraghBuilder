# modules_new/core/processor.py
"""
核心处理器

负责执行主要的数据处理任务
"""

import time
import logging
from typing import Dict, Any, Optional, Tuple
import pandas as pd

from .base import BaseProcessor, ProgressTracker
from .exceptions import ProcessingError, with_error_handling, APIError
from .api_client_wrapper import APIClientWrapper
from ..api import get_chemical_properties_with_grounding
from ..config import APIConfigManager, ConfigManager
from ..config.config_manager import ProcessingConfig
from ..utils import StringUtils

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..main.module_manager import ModuleManager


class CoreProcessor(BaseProcessor):
    """核心处理器"""
    
    def __init__(self, name: str = "CoreProcessor"):
        super().__init__(name)
        self.progress_tracker = ProgressTracker()
        self.string_utils = StringUtils()
        self.api_config_manager: Optional[APIConfigManager] = None
        self.processing_config: Optional[ProcessingConfig] = None
        self.api_client_wrapper: Optional[APIClientWrapper] = None

    def initialize(self, module_manager: 'ModuleManager'):
        """初始化处理器"""
        self.module_manager = module_manager
        self.api_config_manager = self.module_manager.get_module("api_config_manager")
        config_manager: Optional[ConfigManager] = self.module_manager.get_module("config_manager")
        
        if config_manager:
            self.processing_config = config_manager.get_processing_config()
        
        if not self.api_config_manager or not self.processing_config:
            self.logger.error("依赖模块未能正确加载")
            return False
            
        self.api_client_wrapper = APIClientWrapper(self.module_manager)
        self.logger.info("核心处理器初始化成功")
        return True

    def _get_chemical_info(self, row: pd.Series) -> Tuple[str, str]:
        """从数据行中提取化学品名称和CAS号"""
        # 更新的列名映射：中文名称, CAS号或流水号
        chemical_name = (
            row.get('中文名称', '') or 
            row.get('chemical_name', '') or 
            row.get('品名', '') or 
            row.get('name', '') or
            row.get('化学品名称', '') or
            ''
        )
        
        cas_number = (
            row.get('CAS号或流水号', '') or
            row.get('cas_number', '') or 
            row.get('CAS号', '') or 
            row.get('cas', '') or
            row.get('流水号', '') or
            ''
        )
        
        # 清理空值和NaN
        if pd.isna(chemical_name) or str(chemical_name).strip().lower() in ['nan', '', 'null']:
            chemical_name = ''
        if pd.isna(cas_number) or str(cas_number).strip().lower() in ['nan', '', 'null']:
            cas_number = ''
            
        return str(chemical_name).strip(), str(cas_number).strip()

    def _query_cas_number(self, chemical_name: str) -> Optional[str]:
        """如果CAS号缺失，则进行查询"""
        self.module_manager.event_emitter.emit("status_update", "正在查询CAS号", chemical_name)
        try:
            from ..utils.cas_query import get_cas_query_engine
            cas_engine = get_cas_query_engine()
            queried_cas = cas_engine.query_cas_number(chemical_name, use_grounding=True)
            if queried_cas:
                self.logger.info(f"成功查询到CAS号: {chemical_name} -> {queried_cas}")
                self.module_manager.event_emitter.emit("status_update", f"已补充CAS号: {queried_cas}", chemical_name)
                return queried_cas
            else:
                self.logger.info(f"未找到CAS号: {chemical_name}")
                self.module_manager.event_emitter.emit("status_update", "未找到CAS号", chemical_name)
        except Exception as e:
            self.logger.warning(f"CAS号查询失败: {e}")
            self.module_manager.event_emitter.emit("status_update", "CAS号查询失败", chemical_name)
        return None

    def _call_api_with_retry(self, prompt: str, chemical_name: str) -> Tuple[str, int, int, float]:
        """带重试逻辑的API调用"""
        api_calls = 0
        errors = 0
        total_response_time = 0.0

        if not self.processing_config or not self.api_config_manager or not self.api_client_wrapper:
            raise ProcessingError("处理器未正确初始化")

        for attempt in range(self.processing_config.max_retries):
            start_time = time.time()
            api_calls += 1
            
            try:
                self.module_manager.event_emitter.emit("status_update", f"正在进行API调用 ({attempt + 1}/{self.processing_config.max_retries})", chemical_name)
                current_api_config = self.api_config_manager.get_current_api_config()

                if not current_api_config:
                    raise ProcessingError("未找到当前有效的API配置")
                
                response_text = self.api_client_wrapper.call_api(prompt, current_api_config)
                
                response_time = time.time() - start_time
                total_response_time += response_time
                
                return response_text, api_calls, errors, total_response_time

            except APIError as e:
                errors += 1
                self.logger.error(f"处理 '{chemical_name}' 时发生API错误 (尝试 {attempt + 1}): {e}")
                time.sleep(self.processing_config.retry_delay)
        
        raise APIError(f"经过 {self.processing_config.max_retries} 次尝试后，API调用仍然失败。")

    def _parse_and_evaluate_response(self, response_text: str, chemical_name: str) -> Tuple[Dict[str, Any], str, bool]:
        """解析和评估API响应"""
        parse_attempts = 1
        self.module_manager.event_emitter.emit("status_update", "正在解析响应", chemical_name)
        parsed_data, message = self.string_utils.extract_json_data(response_text)
        
        if parsed_data:
            parse_success = True
            quality = self.string_utils.count_valid_fields(parsed_data)
            if self.processing_config and quality >= self.processing_config.quality_threshold:
                self.module_manager.event_emitter.emit("status_update", "处理成功", chemical_name)
                return parsed_data, "成功", parse_success
            else:
                self.module_manager.event_emitter.emit("status_update", "处理成功 (质量较低)", chemical_name)
                return parsed_data, "成功 (质量较低)", parse_success
        
        return {}, "解析失败", False

    @with_error_handling(default_return=(False, "处理失败", {}, 0, 1, 0.0, False))
    def process_single_chemical(self, idx: int, row: pd.Series) -> Tuple[bool, str, Dict[str, Any], int, int, float, bool]:
        """处理单个化学品"""
        chemical_name, cas_number = self._get_chemical_info(row)
        self.logger.info(f"开始处理化学品: {chemical_name}, CAS: {cas_number}")

        if not chemical_name:
            self.logger.warning("化学品名称为空，跳过处理")
            return False, "化学品名称为空", {}, 0, 1, 0.0, False

        if not cas_number or cas_number.strip() == '' or cas_number.lower() == 'nan':
            cas_number = self._query_cas_number(chemical_name) or cas_number

        # 获取当前API配置以生成适配的prompt
        current_api_config = None
        if self.api_config_manager:
            current_api_config = self.api_config_manager.get_current_api_config()
        
        prompt = self.string_utils.generate_prompt(chemical_name, cas_number, current_api_config)
        
        # 检查 prompt 是否成功生成
        if prompt is None:
            error_msg = f"为化学品 {chemical_name} (索引: {idx}) 生成 prompt 失败，跳过处理。"
            self.logger.error(error_msg)
            # 返回一个表示失败的结果
            return False, error_msg, row.to_dict(), 0, 1, 0.0, False

        self.logger.info(f"生成的prompt: {prompt[:100]}...")

        api_calls = 0
        errors = 0
        total_response_time = 0.0
        parse_success = False

        try:
            response_text, api_calls, errors, total_response_time = self._call_api_with_retry(prompt, chemical_name)
            
            best_result, msg, parse_success = self._parse_and_evaluate_response(response_text, chemical_name)

            if best_result:
                return True, msg, best_result, api_calls, errors, total_response_time, parse_success
            else:
                return False, msg, {}, api_calls, errors, total_response_time, parse_success

        except (ProcessingError, APIError) as e:
            self.logger.error(f"处理 '{chemical_name}' 失败: {e}")
            self.module_manager.event_emitter.emit("status_update", "处理失败", chemical_name)
            return False, f"处理失败: {e}", {}, api_calls, errors, total_response_time, parse_success
        
    def process_single_row(self, row_data: Dict[str, Any], chemical_name: str) -> Dict[str, Any]:
        """
        处理单行数据 - 为ThreadPoolProcessor提供的接口
        
        返回一个包含详细统计信息的结果字典
        """
        try:
            row = pd.Series(row_data)
            
            success, msg, data, api_calls, errors, total_response_time, parse_success = \
                self.process_single_chemical(0, row)
            
            final_data = row_data.copy()
            if success:
                # 提取data_source并添加到结果中
                data_source = data.pop('data_source', '模型知识')
                final_data.update(data)
                final_data['data_source'] = data_source
                
            final_data['processing_status'] = msg

            return {
                'success': success,
                'data': final_data,
                'api_calls': api_calls,
                'api_errors': errors,
                'response_time': total_response_time,
                'parse_success': parse_success,
                'error': None if success else msg
            }
                
        except Exception as e:
            self.logger.error(f"处理单行数据失败: {e}")
            error_msg = f"处理错误: {str(e)}"
            final_data = row_data.copy()
            final_data['processing_status'] = error_msg
            return {
                'success': False,
                'data': final_data,
                'api_calls': 0,
                'api_errors': 1, # 记录为API错误
                'response_time': 0.0,
                'parse_success': False,
                'error': error_msg
            }

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        运行处理流程
        
        Args:
            df: 待处理的DataFrame
        """
        self.logger.info(f"开始处理DataFrame，共 {len(df)} 行数据")
        self.progress_tracker.set_total(len(df))
        results = []

        for i, (idx, row) in enumerate(df.iterrows()):
            # 更新获取化学品名称的逻辑
            chemical_name = (
                row.get('中文名称', '') or 
                row.get('chemical_name', '') or 
                row.get('品名', '') or 
                'N/A'
            )
            self.logger.info(f"正在处理第 {i+1} 行: {chemical_name}")
            self.progress_tracker.update(1, f"正在处理: {chemical_name}")
            
            current_idx = 0
            if isinstance(idx, int):
                current_idx = idx
            elif isinstance(idx, str) and idx.isdigit():
                current_idx = int(idx)

            success, msg, data, _, _, _, _ = self.process_single_chemical(current_idx, row)
            self.logger.info(f"处理结果: success={success}, msg={msg}")
            
            result_row = row.to_dict()
            if success:
                result_row.update(data)
            
            result_row['processing_status'] = msg
            results.append(result_row)
            
            self.module_manager.event_emitter.emit("progress", self.progress_tracker.get_progress())

        self.logger.info(f"处理完成，共处理 {len(results)} 行数据")
        return pd.DataFrame(results)
