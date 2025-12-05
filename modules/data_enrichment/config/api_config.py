# modules_new/config/api_config.py
"""
增强版API配置管理器 - 完整功能版

负责API相关配置的管理，支持GUI配置、高级功能和向后兼容性。
"""

import os
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict, fields, MISSING
import logging
from datetime import datetime
import threading

from ..core.base import BaseConfig
from ..core.exceptions import ConfigurationError, with_error_handling


@dataclass
class APIConfig:
    """API配置数据类 - 完整功能版"""
    # 基本配置
    name: str
    api_key: str
    base_url: str
    model: str
    
    # 核心功能配置
    supports_grounding: bool = False
    use_official_sdk: bool = False
    max_tokens: int = 8000
    temperature: float = 0.1
    timeout: int = 60
    
    # 速率限制配置 (重要：用于线程管理和API限制)
    tokens_per_minute: int = 1_000_000
    requests_per_day: int = 10_000
    requests_per_minute: int = 60
    estimated_tokens_per_request: int = 2000
    
    # 高级配置 (重要：用于自动优化和线程管理)
    max_retries: int = 3
    enable_grounding: bool = False  # 兼容旧版
    custom_headers: Optional[Dict[str, str]] = None
    grounding_config: Optional[Dict[str, Any]] = None  # 支持Grounding搜索配置
    
    # 智能优化配置 (重要：用于性能调优)
    auto_optimize: bool = True
    max_concurrent_threads: int = 8
    adaptive_rate_limit: bool = True
    burst_mode: bool = False
    
    # 元数据
    created_at: str = ""
    last_modified: str = ""
    usage_count: int = 0
    
    def __post_init__(self):
        """初始化后处理"""
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.last_modified:
            self.last_modified = self.created_at
        if self.custom_headers is None:
            self.custom_headers = {}
        if self.grounding_config is None:
            self.grounding_config = {}
    
    # --- 兼容性属性 ---
    @property
    def timeout_seconds(self) -> int:
        return self.timeout
    
    @timeout_seconds.setter
    def timeout_seconds(self, value: int):
        self.timeout = value
        
    @property
    def api_url(self) -> str:
        return self.base_url
        
    @api_url.setter
    def api_url(self, value: str):
        self.base_url = value
        
    @property
    def model_name(self) -> str:
        return self.model
        
    @model_name.setter
    def model_name(self, value: str):
        self.model = value
    # --- 兼容性属性结束 ---

    @property
    def effective_requests_per_minute(self) -> int:
        """计算有效的每分钟请求数（考虑Token限制）"""
        if self.tokens_per_minute == 0 or self.estimated_tokens_per_request == 0:
            return self.requests_per_minute
        token_based_rpm = self.tokens_per_minute // self.estimated_tokens_per_request
        daily_based_rpm = self.requests_per_day // (24 * 60) if self.requests_per_day > 0 else float('inf')
        return min(self.requests_per_minute, token_based_rpm, int(daily_based_rpm))

    @property
    def optimal_delay_seconds(self) -> float:
        """计算最优请求间隔"""
        return 60.0 / max(1, self.effective_requests_per_minute)

    @property
    def calculated_max_threads(self) -> int:
        """计算建议的最大线程数"""
        if not self.auto_optimize:
            return self.max_concurrent_threads
        
        avg_response_time = 10  # 假设平均响应时间为10秒
        optimal_threads = int(self.effective_requests_per_minute * avg_response_time / 60)
        return min(self.max_concurrent_threads, max(2, optimal_threads))

    def validate(self) -> List[str]:
        """验证配置的有效性"""
        errors = []
        if not self.name.strip(): errors.append("配置名称不能为空")
        if not self.base_url.strip(): errors.append("API URL不能为空")
        if not self.api_key.strip(): errors.append("API密钥不能为空")
        if not self.model.strip(): errors.append("模型名称不能为空")
        if self.requests_per_minute <= 0: errors.append("每分钟请求数必须大于0")
        if self.timeout <= 0: errors.append("超时时间必须大于0秒")
        return errors

    def to_api_limits(self) -> Dict[str, Any]:
        """转换为APILimits字典（兼容ThreadPoolProcessor）"""
        return {
            'tokens_per_minute': self.tokens_per_minute,
            'requests_per_day': self.requests_per_day,
            'estimated_tokens_per_request': self.estimated_tokens_per_request
        }


class APIConfigManager(BaseConfig):
    """增强版API配置管理器 - 完整功能版"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 使用modules_new目录下的配置文件
        modules_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.api_config_file = os.path.join(modules_dir, "api_config.json")
        self.current_config_file = os.path.join(modules_dir, "current_config.json")
        
        self._lock = threading.Lock()
        self._api_configs: Dict[str, APIConfig] = {}
        self._current_service: str = 'gemini_grounding'
        self._load_api_configs()

    def _load_api_configs(self):
        """加载API配置"""
        # 定义默认内置配置
        default_configs = {
            'gemini': APIConfig(
                name='Gemini',
                api_key=os.environ.get('GEMINI_API_KEY', 'YOUR_API_KEY_HERE'),
                base_url='https://generativelanguage.googleapis.com/v1beta/models/',
                model='gemini-2.5-flash',
                supports_grounding=True,
                requests_per_minute=60,
            ),
            'gemini_grounding': APIConfig(
                name='Gemini Grounding',
                api_key=os.environ.get('GEMINI_API_KEY', 'YOUR_API_KEY_HERE'),
                base_url='https://generativelanguage.googleapis.com/v1beta/models/',
                model='gemini-1.5-flash',
                supports_grounding=True,
                use_official_sdk=True,
                requests_per_minute=6,
            ),
            'deepseek': APIConfig(
                name='DeepSeek',
                api_key=os.environ.get('DEEPSEEK_API_KEY', ''),
                base_url='https://api.deepseek.com/v1',
                model='deepseek-chat',
                supports_grounding=False,
            )
        }
        self._api_configs = default_configs
        
        # 尝试从文件加载，这会覆盖或添加新配置
        self._load_from_file()
        self._load_current_config()

    @with_error_handling()
    def _load_from_file(self):
        """从文件加载API配置，并与默认配置合并"""
        if not os.path.exists(self.api_config_file):
            self.save_to_file() # 如果文件不存在，创建它
            return
        
        try:
            with open(self.api_config_file, 'r', encoding='utf-8') as f:
                file_configs = json.load(f)
            
            for name, config_data in file_configs.items():
                self._api_configs[name] = APIConfig(**config_data)
            self.logger.info(f"API配置已从文件加载: {self.api_config_file}")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"加载API配置失败，文件格式错误: {e}")

    @with_error_handling()
    def _load_current_config(self):
        """加载当前使用的配置"""
        if not os.path.exists(self.current_config_file):
            return
        try:
            with open(self.current_config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            service_name = data.get('current_service')
            if service_name in self._api_configs:
                self._current_service = service_name
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"加载当前配置失败: {e}")

    @with_error_handling()
    def save_to_file(self, file_path: Optional[str] = None) -> bool:
        """保存所有API配置到文件"""
        with self._lock:
            file_path = file_path or self.api_config_file
            try:
                save_data = {name: asdict(config) for name, config in self._api_configs.items()}
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(save_data, f, ensure_ascii=False, indent=2)
                self._save_current_config()
                self.logger.info(f"API配置已保存到: {file_path}")
                return True
            except Exception as e:
                raise ConfigurationError(f"保存API配置失败: {e}")

    def _save_current_config(self) -> bool:
        """保存当前配置引用"""
        try:
            with open(self.current_config_file, 'w', encoding='utf-8') as f:
                json.dump({'current_service': self._current_service}, f, indent=2)
            return True
        except Exception as e:
            self.logger.error(f"保存当前配置引用失败: {e}")
            return False

    # --- 核心增删改查 ---
    def add_config(self, config: APIConfig) -> bool:
        """添加新配置"""
        if not isinstance(config, APIConfig) or config.name in self._api_configs:
            self.logger.error(f"添加配置失败：无效或已存在的配置名称 '{config.name}'")
            return False
        self._api_configs[config.name] = config
        return self.save_to_file()

    def update_config(self, name: str, config: APIConfig) -> bool:
        """更新配置"""
        if name not in self._api_configs:
            return False
        self._api_configs[name] = config
        return self.save_to_file()

    def delete_config(self, name: str) -> bool:
        """删除配置"""
        if name not in self._api_configs or name in ['gemini', 'gemini_grounding', 'deepseek']:
            self.logger.warning(f"无法删除内置或不存在的配置: {name}")
            return False
        del self._api_configs[name]
        if self._current_service == name:
            self._current_service = 'gemini_grounding'
        return self.save_to_file()

    # --- 兼容性方法 ---
    def get_config(self, name: str) -> Optional[APIConfig]:
        return self._api_configs.get(name)

    def get_api_config(self, name: str) -> Optional[APIConfig]:
        """兼容性方法：get_config的别名"""
        return self.get_config(name)

    def get_current_config(self) -> Optional[APIConfig]:
        return self.get_current_api_config()

    def get_current_api_config(self) -> Optional[APIConfig]:
        return self._api_configs.get(self._current_service)

    def set_current_config(self, name: str) -> bool:
        if name not in self._api_configs:
            return False
        self._current_service = name
        return self._save_current_config()

    def get_config_names(self) -> List[str]:
        return list(self._api_configs.keys())

    def get_all_configs(self) -> Dict[str, APIConfig]:
        return self._api_configs.copy()

    @property
    def default_config_name(self) -> str:
        return 'gemini_grounding'

    # --- 高级功能 ---
    def test_config(self, config: APIConfig) -> Dict[str, Any]:
        """测试API配置的有效性"""
        errors = config.validate()
        if errors:
            return {'success': False, 'message': f"配置验证失败: {'; '.join(errors)}"}
        return {'success': True, 'message': '配置有效'}

    def export_config(self, name: str, file_path: str) -> bool:
        """导出单个配置到文件"""
        config = self.get_config(name)
        if not config:
            return False
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(asdict(config), f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            self.logger.error(f"导出配置 '{name}' 失败: {e}")
            return False

    def import_config(self, file_path: str) -> Optional[APIConfig]:
        """从文件导入单个配置"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 填充缺失的字段
            all_fields = {f.name for f in fields(APIConfig)}
            for f in fields(APIConfig):
                if f.name not in data and f.default != MISSING:
                    data[f.name] = f.default

            config = APIConfig(**data)
            if config.name in self._api_configs:
                config.name = f"{config.name}_imported_{int(datetime.now().timestamp())}"
            
            self.add_config(config)
            return config
        except Exception as e:
            self.logger.error(f"导入配置失败: {e}")
            return None

    def get_optimization_suggestions(self, config: APIConfig) -> List[str]:
        """获取配置优化建议"""
        suggestions = []
        if not isinstance(config, APIConfig):
            return ["无效的配置对象"]

        optimal_threads = config.calculated_max_threads
        if config.max_concurrent_threads > optimal_threads * 1.5:
            suggestions.append(f"建议减少并发线程数到 {optimal_threads} (当前: {config.max_concurrent_threads})")
        
        if config.requests_per_minute > config.effective_requests_per_minute:
            suggestions.append(f"RPM建议调整为 {config.effective_requests_per_minute} (受Token限制)")

        if config.timeout < 15:
            suggestions.append("超时时间较短，建议设置为30秒以上")
        
        if not config.auto_optimize:
            suggestions.append("建议启用自动优化，以动态调整参数")
            
        return suggestions if suggestions else ["配置看起来很不错！"]
