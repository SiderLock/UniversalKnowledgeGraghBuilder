# modules_new/core/base.py
"""
核心基础类模块

定义项目中的基础类、接口和抽象类
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable
import logging

__all__ = [
    'BaseProcessor', 'BaseValidator', 'BaseAnalyzer', 'Singleton', 
    'BaseConfig', 'EventEmitter', 'ProgressTracker', 'BaseManager'
]

class BaseProcessor(ABC):
    """数据处理器基类"""
    
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.name)
        self._initialized = False
    
    @abstractmethod
    def process(self, data: Any) -> Any:
        """处理数据的抽象方法"""
        pass
    
    def initialize(self, module_manager: Optional[Any] = None) -> bool:
        """初始化处理器"""
        try:
            self._setup()
            self._initialized = True
            self.logger.info(f"{self.name} 初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"{self.name} 初始化失败: {e}")
            return False
    
    def _setup(self):
        """子类可重写的设置方法"""
        pass
    
    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化"""
        return self._initialized


class BaseValidator(ABC):
    """验证器基类"""
    
    @abstractmethod
    def validate(self, data: Any) -> Dict[str, Any]:
        """验证数据"""
        pass


class BaseAnalyzer(ABC):
    """分析器基类"""
    
    @abstractmethod
    def analyze(self, data: Any) -> Dict[str, Any]:
        """分析数据"""
        pass


class Singleton:
    """单例模式基类"""
    
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class BaseConfig(Singleton):
    """配置基类"""
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True
        self._config_data = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self._config_data.get(key, default)
    
    def set(self, key: str, value: Any):
        """设置配置值"""
        self._config_data[key] = value
    
    def update(self, config_dict: Dict[str, Any]):
        """批量更新配置"""
        self._config_data.update(config_dict)


class EventEmitter:
    """事件发射器"""
    
    def __init__(self):
        self._listeners = {}
    
    def on(self, event: str, callback: Callable):
        """注册事件监听器"""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(callback)
    
    def emit(self, event: str, *args, **kwargs):
        """触发事件"""
        if event in self._listeners:
            for callback in self._listeners[event]:
                try:
                    callback(*args, **kwargs)
                except Exception as e:
                    logging.error(f"事件回调执行失败: {e}")
    
    def off(self, event: str, callback: Optional[Callable] = None):
        """移除事件监听器"""
        if event in self._listeners:
            if callback:
                if callback in self._listeners[event]:
                    self._listeners[event].remove(callback)
            else:
                self._listeners[event] = []


class ProgressTracker:
    """进度跟踪器"""
    
    def __init__(self, total: int = 0):
        self.total = total
        self.current = 0
        self.status = "waiting"
        self._callbacks = []
    
    def add_callback(self, callback: Callable):
        """添加进度回调"""
        self._callbacks.append(callback)
    
    def update(self, increment: int = 1, status: Optional[str] = None):
        """更新进度"""
        self.current += increment
        if status:
            self.status = status
        
        # 触发回调
        for callback in self._callbacks:
            try:
                callback(self.current, self.total, self.status)
            except Exception as e:
                logging.error(f"进度回调执行失败: {e}")
    
    def set_total(self, total: int):
        """设置总数"""
        self.total = total
    
    def reset(self):
        """重置进度"""
        self.current = 0
        self.status = "waiting"
    
    @property
    def percentage(self) -> float:
        """获取完成百分比"""
        if self.total <= 0:
            return 0.0
        return min(100.0, (self.current / self.total) * 100.0)
    
    @property
    def is_complete(self) -> bool:
        """检查是否完成"""
        return self.current >= self.total if self.total > 0 else False
    
    def get_progress(self) -> Dict[str, Any]:
        """获取进度信息"""
        return {
            'current': self.current,
            'total': self.total,
            'percentage': self.percentage,
            'status': self.status,
            'is_complete': self.is_complete
        }


class BaseManager:
    """管理器基类"""
    
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.name)
        self._components = {}
        self._initialized = False
    
    def register_component(self, name: str, component: Any):
        """注册组件"""
        self._components[name] = component
        self.logger.info(f"组件已注册: {name}")
    
    def get_component(self, name: str) -> Any:
        """获取组件"""
        return self._components.get(name)
    
    def initialize_all(self) -> bool:
        """初始化所有组件"""
        success = True
        for name, component in self._components.items():
            if hasattr(component, 'initialize'):
                if not component.initialize():
                    self.logger.error(f"组件初始化失败: {name}")
                    success = False
        
        self._initialized = success
        return success
    
    @property
    def is_initialized(self) -> bool:
        return self._initialized
