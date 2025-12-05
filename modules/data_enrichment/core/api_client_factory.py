# modules_new/core/api_client_factory.py
"""
统一API客户端工厂

支持多种AI服务的统一接口
"""

import logging
import os
from typing import Dict, Any, Optional, Tuple, List, Union, TYPE_CHECKING
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from .exceptions import with_error_handling

if TYPE_CHECKING:
    from .api_config_manager import APIConfig


@dataclass
class APIResponse:
    """统一API响应格式"""
    text: str
    success: bool
    error_message: str = ""
    sources: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    token_usage: Optional[Dict[str, int]] = field(default_factory=dict)
    model_used: str = ""


class BaseAPIClient(ABC):
    """API客户端基类"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = ""):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key
        self.model = model
        self.client = None
    
    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        """测试API连接"""
        pass
    
    @abstractmethod
    def generate_content(self, prompt: str, **kwargs) -> APIResponse:
        """生成内容"""
        pass
    
    @abstractmethod
    def supports_grounding(self) -> bool:
        """是否支持联网搜索"""
        pass


class GeminiClient(BaseAPIClient):
    """Google Gemini客户端"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gemini-2.5-flash"):
        super().__init__(api_key, model)
        self.grounding_available = False
        
        # 检查Google SDK
        try:
            import google.generativeai as genai
            self.genai = genai
            self.sdk_available = True
            self._initialize_client()
        except ImportError:
            self.sdk_available = False
            self.logger.warning("Google SDK未安装，请运行: pip install google-generativeai")
    
    def _initialize_client(self):
        """初始化客户端"""
        try:
            if not self.api_key or self.api_key in ["请在设置中输入您的Google API密钥", ""]:
                return False
            
            # 配置API
            self.genai.configure(api_key=self.api_key)
            self.client = self.genai.GenerativeModel(self.model)
            self.grounding_available = True
            
            self.logger.info("Gemini客户端初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"Gemini客户端初始化失败: {e}")
            return False
    
    @with_error_handling()
    def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        if not self.sdk_available:
            return False, "Google SDK未安装"
        
        if not self.client:
            if not self._initialize_client():
                return False, "客户端初始化失败"
        
        try:
            response = self.client.generate_content("测试连接")
            if response and response.text:
                return True, f"连接成功: {response.text[:50]}..."
            return False, "响应为空"
        except Exception as e:
            return False, f"连接失败: {str(e)}"
    
    @with_error_handling()
    def generate_content(self, prompt: str, enable_grounding: bool = True, **kwargs) -> APIResponse:
        """生成内容"""
        if not self.sdk_available:
            return APIResponse("", False, "Google SDK未安装")
        
        if not self.client:
            if not self._initialize_client():
                return APIResponse("", False, "客户端未初始化")
        
        try:
            # 构建生成配置
            generation_config = {
                "max_output_tokens": kwargs.get("max_tokens", 2048),
                "temperature": kwargs.get("temperature", 0.7),
                "top_p": kwargs.get("top_p", 0.95),
            }
            
            # TODO: 添加联网搜索工具（需要最新SDK版本）
            # 目前使用基本生成功能
            
            response = self.client.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            if response and response.text:
                # 提取token使用信息
                token_usage = {}
                if hasattr(response, 'usage_metadata') and response.usage_metadata:
                    token_usage = {
                        "prompt_tokens": getattr(response.usage_metadata, 'prompt_token_count', 0),
                        "completion_tokens": getattr(response.usage_metadata, 'candidates_token_count', 0),
                        "total_tokens": getattr(response.usage_metadata, 'total_token_count', 0)
                    }
                
                return APIResponse(
                    text=response.text,
                    success=True,
                    token_usage=token_usage,
                    model_used=self.model
                )
            else:
                return APIResponse("", False, "API返回空响应")
                
        except Exception as e:
            return APIResponse("", False, f"生成失败: {str(e)}")
    
    def supports_grounding(self) -> bool:
        """是否支持联网搜索"""
        return self.grounding_available


class OpenAIClient(BaseAPIClient):
    """OpenAI客户端"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        super().__init__(api_key, model)
        
        # 检查OpenAI库
        try:
            import openai
            self.openai = openai
            self.sdk_available = True
            self._initialize_client()
        except ImportError:
            self.sdk_available = False
            self.logger.warning("OpenAI SDK未安装，请运行: pip install openai")
    
    def _initialize_client(self):
        """初始化客户端"""
        try:
            if not self.api_key or self.api_key in ["请在设置中输入您的OpenAI API密钥", ""]:
                return False
            
            self.client = self.openai.OpenAI(api_key=self.api_key)
            self.logger.info("OpenAI客户端初始化成功")
            return True
            
        except Exception as e:
            self.logger.error(f"OpenAI客户端初始化失败: {e}")
            return False
    
    @with_error_handling()
    def test_connection(self) -> Tuple[bool, str]:
        """测试连接"""
        if not self.sdk_available:
            return False, "OpenAI SDK未安装"
        
        if not self.client:
            if not self._initialize_client():
                return False, "客户端初始化失败"
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": "测试连接"}],
                max_tokens=50
            )
            
            if response and response.choices:
                return True, f"连接成功: {response.choices[0].message.content[:50]}..."
            return False, "响应为空"
            
        except Exception as e:
            return False, f"连接失败: {str(e)}"
    
    @with_error_handling()
    def generate_content(self, prompt: str, **kwargs) -> APIResponse:
        """生成内容"""
        if not self.sdk_available:
            return APIResponse("", False, "OpenAI SDK未安装")
        
        if not self.client:
            if not self._initialize_client():
                return APIResponse("", False, "客户端未初始化")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=kwargs.get("max_tokens", 2048),
                temperature=kwargs.get("temperature", 0.7),
                top_p=kwargs.get("top_p", 0.95)
            )
            
            if response and response.choices:
                # 提取token使用信息
                token_usage = {}
                if hasattr(response, 'usage') and response.usage:
                    token_usage = {
                        "prompt_tokens": response.usage.prompt_tokens,
                        "completion_tokens": response.usage.completion_tokens,
                        "total_tokens": response.usage.total_tokens
                    }
                
                return APIResponse(
                    text=response.choices[0].message.content,
                    success=True,
                    token_usage=token_usage,
                    model_used=self.model
                )
            else:
                return APIResponse("", False, "API返回空响应")
                
        except Exception as e:
            return APIResponse("", False, f"生成失败: {str(e)}")
    
    def supports_grounding(self) -> bool:
        """是否支持联网搜索"""
        return False  # OpenAI需要额外配置工具


class APIClientFactory:
    """API客户端工厂"""
    
    @staticmethod
    def create_client(config: 'APIConfig') -> BaseAPIClient:
        """根据配置创建客户端"""
        model_name = config.model_name.lower()
        
        if "gemini" in model_name:
            return GeminiClient(config.api_key, config.model_name)
        elif "gpt" in model_name or "openai" in model_name:
            return OpenAIClient(config.api_key, config.model_name)
        else:
            # 默认尝试Gemini
            return GeminiClient(config.api_key, config.model_name)
    
    @staticmethod
    def get_supported_models() -> Dict[str, List[str]]:
        """获取支持的模型列表"""
        return {
            "Google Gemini": [
                "gemini-2.5-flash",
                "gemini-1.5-pro",
                "gemini-1.5-flash"
            ],
            "OpenAI": [
                "gpt-4",
                "gpt-4-turbo",
                "gpt-3.5-turbo"
            ]
        }
    
    @staticmethod
    def check_dependencies() -> Dict[str, Tuple[bool, str]]:
        """检查依赖安装状态"""
        results = {}
        
        # 检查Google SDK
        try:
            import google.generativeai
            results["Google SDK"] = (True, "已安装")
        except ImportError:
            results["Google SDK"] = (False, "未安装，运行: pip install google-generativeai")
        
        # 检查OpenAI SDK
        try:
            import openai
            results["OpenAI SDK"] = (True, "已安装")
        except ImportError:
            results["OpenAI SDK"] = (False, "未安装，运行: pip install openai")
        
        return results
