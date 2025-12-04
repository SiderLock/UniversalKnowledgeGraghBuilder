# modules_new/config/config_manager.py
"""
配置管理器

统一管理应用的各种配置
"""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging

from ..core.base import BaseConfig, Singleton
from ..core.exceptions import ConfigurationError, with_error_handling
from ..core.constants import DEFAULT_CONFIG
from .api_config import APIConfigManager
from .column_mapper import ColumnMapper


@dataclass
class ProcessingConfig:
    """处理配置数据类"""
    batch_size: int = 100
    backup_interval: int = 50
    max_retries: int = 3
    api_delay: float = 1.0
    retry_delay: float = 2.0
    quality_threshold: int = 8
    concurrent_requests: int = 1
    enable_auto_backup: bool = True
    enable_progress_tracking: bool = True
    
    def validate(self):
        """验证配置参数"""
        if self.batch_size <= 0:
            raise ConfigurationError("批次大小必须大于0")
        if self.backup_interval <= 0:
            raise ConfigurationError("备份间隔必须大于0")
        if self.max_retries < 0:
            raise ConfigurationError("最大重试次数不能为负数")
        if self.api_delay < 0:
            raise ConfigurationError("API延迟不能为负数")
        if self.retry_delay < 0:
            raise ConfigurationError("重试延迟不能为负数")
        if self.quality_threshold <= 0 or self.quality_threshold > 10:
            raise ConfigurationError("质量阈值必须在1-10之间")
        if self.concurrent_requests <= 0:
            raise ConfigurationError("并发请求数必须大于0")


class ConfigManager(BaseConfig):
    """配置管理器 - 单例模式"""
    
    def __init__(self):
        super().__init__()
        if hasattr(self, '_config_loaded'):
            return
            
        self._config_loaded = True
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config_file = "app_config.json"
        
        # 初始化子管理器
        self.api_config_manager = APIConfigManager()
        self.column_mapper = ColumnMapper()
        
        # 初始化默认配置
        self._load_default_config()
        
        # 尝试加载用户配置
        self._load_user_config()
    
    def _load_default_config(self):
        """加载默认配置"""
        self._config_data.update(DEFAULT_CONFIG)
        
        # 设置处理配置
        self._processing_config = ProcessingConfig()
        self._config_data['processing'] = asdict(self._processing_config)
        
        self.logger.info("默认配置已加载")
    
    @with_error_handling()
    def _load_user_config(self):
        """加载用户配置文件"""
        if not os.path.exists(self.config_file):
            self.logger.info(f"用户配置文件不存在，使用默认配置: {self.config_file}")
            return
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                user_config = json.load(f)
            
            # 更新主配置
            self._config_data.update(user_config.get('main', {}))
            
            # 更新处理配置
            if 'processing' in user_config:
                processing_data = user_config['processing']
                self._processing_config = ProcessingConfig(**processing_data)
                self._processing_config.validate()
                self._config_data['processing'] = asdict(self._processing_config)
            
            # 加载API配置
            if 'api' in user_config:
                self.api_config_manager.load_from_dict(user_config['api'])

            self.logger.info(f"用户配置已加载: {self.config_file}")
            
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"配置文件格式错误: {e}")
        except Exception as e:
            self.logger.error(f"加载用户配置失败: {e}")
            self.logger.info("使用默认配置")
    
    @with_error_handling()
    def save_config(self, config_file: Optional[str] = None) -> bool:
        """保存配置到文件"""
        file_path = config_file or self.config_file
        
        try:
            # 准备保存的配置数据
            save_data = {
                'main': self._config_data,
                'processing': asdict(self._processing_config),
                'api': self.api_config_manager.to_dict()
            }
            
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True) if os.path.dirname(file_path) else None
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"配置已保存: {file_path}")
            return True
            
        except Exception as e:
            raise ConfigurationError(f"保存配置失败: {e}")
    
    def get_processing_config(self) -> ProcessingConfig:
        """获取处理配置对象"""
        return self._processing_config
    
    def update_processing_config(self, **kwargs):
        """更新处理配置"""
        for key, value in kwargs.items():
            if hasattr(self._processing_config, key):
                setattr(self._processing_config, key, value)
        
        # 验证配置
        self._processing_config.validate()
        
        # 更新内部配置数据
        self._config_data['processing'] = asdict(self._processing_config)
        
        self.logger.info(f"处理配置已更新: {kwargs}")
    
    def set(self, key: str, value: Any):
        """设置顶层配置项"""
        self._config_data[key] = value
        self.logger.info(f"配置项已更新: {key} = {value}")

    def get_path(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """获取单个路径配置"""
        return self.get_path_config().get(key, default)

    def set_user_path(self, key: str, value: str):
        """设置用户路径"""
        self.update_path_config(**{key: value})

    def get_path_config(self) -> Dict[str, str]:
        """获取路径配置"""
        return self._config_data.get('paths', {
            'input_csv': '危化品需补充.csv',
            'output_csv': 'output_enriched.csv',
            'output_folder': 'output_batches',
            'backup_folder': 'backups',
            'log_folder': 'logs',
            'config_file': 'api_config.json'
        })
    
    def update_path_config(self, **kwargs):
        """更新路径配置"""
        if 'paths' not in self._config_data:
            self._config_data['paths'] = {}
        
        self._config_data['paths'].update(kwargs)
        self.logger.info(f"路径配置已更新: {kwargs}")
    
    def get_ui_config(self) -> Dict[str, Any]:
        """获取UI配置"""
        return self._config_data.get('ui', {
            'window_title': '化学数据补全工具',
            'window_size': '1000x800',
            'theme': 'default',
            'show_progress': True,
            'auto_scroll': True,
            'update_interval': 500
        })
    
    def update_ui_config(self, **kwargs):
        """更新UI配置"""
        if 'ui' not in self._config_data:
            self._config_data['ui'] = {}
        
        self._config_data['ui'].update(kwargs)
        self.logger.info(f"UI配置已更新: {kwargs}")
    
    def reset_to_defaults(self):
        """重置为默认配置"""
        self._config_data.clear()
        self._load_default_config()
        self.logger.info("配置已重置为默认值")
    
    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        return {
            'config_file': self.config_file,
            'config_keys': list(self._config_data.keys()),
            'processing_config': asdict(self._processing_config),
            'total_settings': len(self._config_data)
        }
    
    def validate_all_configs(self) -> Dict[str, Any]:
        """验证所有配置"""
        validation_result = {
            'is_valid': True,
            'issues': [],
            'warnings': []
        }
        
        try:
            # 验证处理配置
            self._processing_config.validate()
        except ConfigurationError as e:
            validation_result['is_valid'] = False
            validation_result['issues'].append(f"处理配置错误: {e}")
        
        # 验证路径配置
        path_config = self.get_path_config()
        for key, path in path_config.items():
            if not path:
                validation_result['warnings'].append(f"路径配置为空: {key}")
        
        return validation_result
