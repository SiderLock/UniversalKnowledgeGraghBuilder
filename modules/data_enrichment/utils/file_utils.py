# modules_new/utils/file_utils.py
"""
文件工具类

提供文件操作相关的实用工具
"""

import os
import shutil
import hashlib
import json
import pickle
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import logging
import tempfile
import uuid


class FileUtils:
    """文件工具类"""
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> bool:
        """确保目录存在"""
        try:
            os.makedirs(path, exist_ok=True)
            return True
        except Exception as e:
            logging.error(f"创建目录失败: {path}, {e}")
            return False
    
    @staticmethod
    def safe_filename(filename: str, max_length: int = 100) -> str:
        """创建安全的文件名"""
        # 移除危险字符
        dangerous_chars = '<>:"/\\|?*'
        safe_name = filename
        
        for char in dangerous_chars:
            safe_name = safe_name.replace(char, '_')
        
        # 限制长度
        if len(safe_name) > max_length:
            name, ext = os.path.splitext(safe_name)
            max_name_length = max_length - len(ext)
            safe_name = name[:max_name_length] + ext
        
        return safe_name or "untitled"
    
    @staticmethod
    def get_file_hash(filepath: str, algorithm: str = 'md5') -> Optional[str]:
        """计算文件哈希值"""
        try:
            hash_func = hashlib.new(algorithm)
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            logging.error(f"计算文件哈希失败: {filepath}, {e}")
            return None
    
    @staticmethod
    def copy_file_safe(source: str, destination: str) -> bool:
        """安全复制文件"""
        try:
            # 确保目标目录存在
            dest_dir = os.path.dirname(destination)
            if dest_dir:
                FileUtils.ensure_directory(dest_dir)
            
            shutil.copy2(source, destination)
            return True
        except Exception as e:
            logging.error(f"复制文件失败: {source} -> {destination}, {e}")
            return False
    
    @staticmethod
    def delete_file_safe(filepath: str) -> bool:
        """安全删除文件"""
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
            return True
        except Exception as e:
            logging.error(f"删除文件失败: {filepath}, {e}")
            return False
    
    @staticmethod
    def get_file_size(filepath: str) -> int:
        """获取文件大小（字节）"""
        try:
            return os.path.getsize(filepath)
        except Exception:
            return 0
    
    @staticmethod
    def get_file_info(filepath: str) -> Dict[str, Any]:
        """获取文件详细信息"""
        try:
            stat = os.stat(filepath)
            return {
                'size': stat.st_size,
                'created': stat.st_ctime,
                'modified': stat.st_mtime,
                'accessed': stat.st_atime,
                'exists': True,
                'is_file': os.path.isfile(filepath),
                'is_dir': os.path.isdir(filepath)
            }
        except Exception:
            return {'exists': False}
    
    @staticmethod
    def find_files(directory: str, pattern: str = "*", recursive: bool = True) -> List[str]:
        """查找文件"""
        try:
            path = Path(directory)
            if recursive:
                files = list(path.rglob(pattern))
            else:
                files = list(path.glob(pattern))
            
            return [str(f) for f in files if f.is_file()]
        except Exception as e:
            logging.error(f"查找文件失败: {directory}, {e}")
            return []
    
    @staticmethod
    def load_json(filepath: str) -> Optional[Dict[str, Any]]:
        """加载JSON文件"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载JSON文件失败: {filepath}, {e}")
            return None
    
    @staticmethod
    def save_json(data: Dict[str, Any], filepath: str) -> bool:
        """保存JSON文件"""
        try:
            FileUtils.ensure_directory(os.path.dirname(filepath))
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logging.error(f"保存JSON文件失败: {filepath}, {e}")
            return False
    
    @staticmethod
    def load_pickle(filepath: str) -> Any:
        """加载pickle文件"""
        try:
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"加载pickle文件失败: {filepath}, {e}")
            return None
    
    @staticmethod
    def save_pickle(data: Any, filepath: str) -> bool:
        """保存pickle文件"""
        try:
            FileUtils.ensure_directory(os.path.dirname(filepath))
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
            return True
        except Exception as e:
            logging.error(f"保存pickle文件失败: {filepath}, {e}")
            return False
    
    @staticmethod
    def get_temp_file(extension: str = ".tmp") -> str:
        """生成临时文件路径"""
        # 确保扩展名以点开头
        if not extension.startswith('.'):
            extension = '.' + extension
        
        # 生成唯一文件名
        temp_name = f"temp_{uuid.uuid4().hex[:8]}{extension}"
        
        # 获取临时目录
        temp_dir = tempfile.gettempdir()
        
        return os.path.join(temp_dir, temp_name)
