# modules_new/utils/cache_utils.py
"""
缓存工具类

提供简单的内存缓存和文件缓存功能
"""

import time
import pickle
import json
import hashlib
from typing import Any, Optional, Dict, Callable
import logging
from pathlib import Path


class CacheUtils:
    """缓存工具类"""
    
    def __init__(self, cache_dir: str = ".cache", default_ttl: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.default_ttl = default_ttl
        self.memory_cache = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _generate_key(self, key: str) -> str:
        """生成缓存键的哈希值"""
        return hashlib.md5(key.encode()).hexdigest()
    
    def _is_expired(self, timestamp: float, ttl: int) -> bool:
        """检查缓存是否过期"""
        return time.time() - timestamp > ttl
    
    def set_memory(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置内存缓存"""
        ttl = ttl or self.default_ttl
        self.memory_cache[key] = {
            'value': value,
            'timestamp': time.time(),
            'ttl': ttl
        }
        self.logger.debug(f"内存缓存已设置: {key}")
    
    def get_memory(self, key: str) -> Optional[Any]:
        """获取内存缓存"""
        if key not in self.memory_cache:
            return None
        
        cache_item = self.memory_cache[key]
        
        # 检查是否过期
        if self._is_expired(cache_item['timestamp'], cache_item['ttl']):
            del self.memory_cache[key]
            self.logger.debug(f"内存缓存已过期并删除: {key}")
            return None
        
        self.logger.debug(f"内存缓存命中: {key}")
        return cache_item['value']
    
    def set_file(self, key: str, value: Any, ttl: Optional[int] = None, 
                format: str = 'pickle') -> bool:
        """设置文件缓存"""
        try:
            ttl = ttl or self.default_ttl
            cache_key = self._generate_key(key)
            
            cache_data = {
                'value': value,
                'timestamp': time.time(),
                'ttl': ttl,
                'original_key': key
            }
            
            cache_file = self.cache_dir / f"{cache_key}.{format}"
            
            if format == 'pickle':
                with open(cache_file, 'wb') as f:
                    pickle.dump(cache_data, f)
            elif format == 'json':
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
            else:
                raise ValueError(f"不支持的缓存格式: {format}")
            
            self.logger.debug(f"文件缓存已设置: {key} -> {cache_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"设置文件缓存失败: {key}, {e}")
            return False
    
    def get_file(self, key: str, format: str = 'pickle') -> Optional[Any]:
        """获取文件缓存"""
        try:
            cache_key = self._generate_key(key)
            cache_file = self.cache_dir / f"{cache_key}.{format}"
            
            if not cache_file.exists():
                return None
            
            if format == 'pickle':
                with open(cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
            elif format == 'json':
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
            else:
                raise ValueError(f"不支持的缓存格式: {format}")
            
            # 检查是否过期
            if self._is_expired(cache_data['timestamp'], cache_data['ttl']):
                cache_file.unlink()
                self.logger.debug(f"文件缓存已过期并删除: {key}")
                return None
            
            self.logger.debug(f"文件缓存命中: {key}")
            return cache_data['value']
            
        except Exception as e:
            self.logger.error(f"获取文件缓存失败: {key}, {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """删除缓存（内存和文件）"""
        success = True
        
        # 删除内存缓存
        if key in self.memory_cache:
            del self.memory_cache[key]
            self.logger.debug(f"内存缓存已删除: {key}")
        
        # 删除文件缓存
        try:
            cache_key = self._generate_key(key)
            for format in ['pickle', 'json']:
                cache_file = self.cache_dir / f"{cache_key}.{format}"
                if cache_file.exists():
                    cache_file.unlink()
                    self.logger.debug(f"文件缓存已删除: {key}")
        except Exception as e:
            self.logger.error(f"删除文件缓存失败: {key}, {e}")
            success = False
        
        return success
    
    def clear_memory(self) -> None:
        """清空内存缓存"""
        self.memory_cache.clear()
        self.logger.info("内存缓存已清空")
    
    def clear_file(self) -> int:
        """清空文件缓存"""
        count = 0
        try:
            for cache_file in self.cache_dir.glob("*"):
                if cache_file.is_file():
                    cache_file.unlink()
                    count += 1
            
            self.logger.info(f"文件缓存已清空: {count} 个文件")
            return count
            
        except Exception as e:
            self.logger.error(f"清空文件缓存失败: {e}")
            return count
    
    def cleanup_expired(self) -> int:
        """清理过期的缓存"""
        count = 0
        
        # 清理内存缓存
        expired_keys = []
        for key, cache_item in self.memory_cache.items():
            if self._is_expired(cache_item['timestamp'], cache_item['ttl']):
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.memory_cache[key]
            count += 1
        
        # 清理文件缓存
        try:
            for cache_file in self.cache_dir.glob("*.pickle"):
                try:
                    with open(cache_file, 'rb') as f:
                        cache_data = pickle.load(f)
                    
                    if self._is_expired(cache_data['timestamp'], cache_data['ttl']):
                        cache_file.unlink()
                        count += 1
                        
                except Exception:
                    # 如果文件损坏，也删除它
                    cache_file.unlink()
                    count += 1
        
        except Exception as e:
            self.logger.error(f"清理过期文件缓存失败: {e}")
        
        self.logger.info(f"清理了 {count} 个过期缓存项")
        return count
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        memory_count = len(self.memory_cache)
        file_count = len(list(self.cache_dir.glob("*")))
        
        # 计算缓存目录大小
        total_size = 0
        try:
            for cache_file in self.cache_dir.rglob("*"):
                if cache_file.is_file():
                    total_size += cache_file.stat().st_size
        except Exception:
            total_size = 0
        
        return {
            'memory_cache_count': memory_count,
            'file_cache_count': file_count,
            'cache_dir_size_bytes': total_size,
            'cache_dir_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_dir': str(self.cache_dir)
        }
    
    def cache_decorator(self, ttl: Optional[int] = None, use_file: bool = False, 
                       cache_format: str = 'pickle'):
        """缓存装饰器"""
        def decorator(func: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                # 生成缓存键
                cache_key = f"{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
                
                # 尝试获取缓存
                if use_file:
                    cached_result = self.get_file(cache_key, cache_format)
                else:
                    cached_result = self.get_memory(cache_key)
                
                if cached_result is not None:
                    return cached_result
                
                # 执行函数并缓存结果
                result = func(*args, **kwargs)
                
                if use_file:
                    self.set_file(cache_key, result, ttl, cache_format)
                else:
                    self.set_memory(cache_key, result, ttl)
                
                return result
            
            return wrapper
        return decorator


# 全局缓存实例
default_cache = CacheUtils()
