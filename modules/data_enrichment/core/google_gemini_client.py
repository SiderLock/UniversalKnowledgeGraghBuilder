# modules_new/core/google_gemini_client.py
"""
Google Gemini API客户端 - 使用Google SDK

支持联网搜索的Gemini API调用
"""

import logging
import os
from typing import Dict, Any, Optional, Tuple, List, Union
from dataclasses import dataclass, field

try:
    from google import genai
    from google.genai import types
    import google.generativeai as genai_config
    GOOGLE_SDK_AVAILABLE = True
except ImportError:
    GOOGLE_SDK_AVAILABLE = False
    genai = None
    types = None
    genai_config = None

from .exceptions import with_error_handling


@dataclass
class GeminiResponse:
    """Gemini API响应"""
    text: str
    success: bool
    error_message: str = ""
    grounding_sources: Optional[List[Dict[str, Any]]] = field(default_factory=list)
    token_usage: Optional[Dict[str, int]] = field(default_factory=dict)


class GoogleGeminiClient:
    """Google Gemini API客户端"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_key = api_key or os.getenv('GOOGLE_API_KEY')
        self.client = None
        self.grounding_tool = None
        
        if not GOOGLE_SDK_AVAILABLE:
            self.logger.error("Google SDK未安装，请运行: pip install google-generativeai")
            return
        
        self._initialize_client()
    
    def _initialize_client(self):
        """初始化Google SDK客户端"""
        try:
            if not self.api_key or self.api_key in ["请在设置中输入您的Google API密钥", ""]:
                self.logger.warning("Google API密钥未设置")
                return False
            
            if GOOGLE_SDK_AVAILABLE and genai:
                # 配置API密钥 - 使用正确的配置方法
                os.environ['GOOGLE_API_KEY'] = self.api_key
                
                # 创建客户端
                self.client = genai.Client(api_key=self.api_key)
                
                # 定义联网搜索工具
                if types:
                    self.grounding_tool = types.Tool(
                        google_search=types.GoogleSearch()
                    )
                
                self.logger.info("Google Gemini客户端初始化成功")
                return True
            else:
                self.logger.error("Google SDK不可用")
                return False
            
        except Exception as e:
            self.logger.error(f"Google Gemini客户端初始化失败: {e}")
            return False
    
    @with_error_handling()
    def test_connection(self) -> Tuple[bool, str]:
        """测试API连接"""
        if not GOOGLE_SDK_AVAILABLE:
            return False, "Google SDK未安装"
        
        if not self.client:
            if not self._initialize_client():
                return False, "客户端初始化失败，请检查API密钥"
        
        try:
            # 发送简单测试请求
            response = self.client.models.generate_content(
                model="gemini-2.5-flash",
                contents="测试连接，请回复'连接成功'",
                config=types.GenerateContentConfig(
                    max_output_tokens=50,
                    temperature=0.1
                )
            )
            
            if response and response.text:
                return True, f"连接成功，响应: {response.text[:50]}..."
            else:
                return False, "API响应为空"
                
        except Exception as e:
            error_msg = str(e)
            if "API_KEY_INVALID" in error_msg:
                return False, "API密钥无效，请检查密钥是否正确"
            elif "QUOTA_EXCEEDED" in error_msg:
                return False, "API配额已用完，请检查账户余额"
            else:
                return False, f"连接测试失败: {error_msg}"
    
    @with_error_handling()
    def generate_content(self, prompt: str, enable_grounding: bool = True, 
                        model: str = "gemini-2.5-flash", **kwargs) -> GeminiResponse:
        """生成内容"""
        if not GOOGLE_SDK_AVAILABLE:
            return GeminiResponse(
                text="",
                success=False,
                error_message="Google SDK未安装"
            )
        
        if not self.client:
            if not self._initialize_client():
                return GeminiResponse(
                    text="",
                    success=False,
                    error_message="客户端未初始化"
                )
        
        try:
            # 构建配置
            config_params = {
                "max_output_tokens": kwargs.get("max_tokens", 2048),
                "temperature": kwargs.get("temperature", 0.7),
                "top_p": kwargs.get("top_p", 0.95),
            }
            
            # 如果启用联网搜索，添加工具
            if enable_grounding and self.grounding_tool:
                config_params["tools"] = [self.grounding_tool]
            
            config = types.GenerateContentConfig(**config_params)
            
            # 发送请求
            response = self.client.models.generate_content(
                model=model,
                contents=prompt,
                config=config
            )
            
            if not response or not response.text:
                return GeminiResponse(
                    text="",
                    success=False,
                    error_message="API返回空响应"
                )
            
            # 提取联网搜索来源（如果有）
            grounding_sources = []
            if hasattr(response, 'grounding_metadata') and response.grounding_metadata:
                try:
                    for source in response.grounding_metadata.search_entry_point.rendered_content:
                        grounding_sources.append({
                            "title": getattr(source, 'title', ''),
                            "url": getattr(source, 'url', ''),
                            "snippet": getattr(source, 'snippet', '')
                        })
                except Exception as e:
                    self.logger.warning(f"提取联网搜索来源失败: {e}")
            
            # 提取token使用信息（如果有）
            token_usage = {}
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                token_usage = {
                    "prompt_tokens": getattr(response.usage_metadata, 'prompt_token_count', 0),
                    "completion_tokens": getattr(response.usage_metadata, 'candidates_token_count', 0),
                    "total_tokens": getattr(response.usage_metadata, 'total_token_count', 0)
                }
            
            return GeminiResponse(
                text=response.text,
                success=True,
                grounding_sources=grounding_sources,
                token_usage=token_usage
            )
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Gemini API调用失败: {error_msg}")
            
            return GeminiResponse(
                text="",
                success=False,
                error_message=error_msg
            )
    
    def update_api_key(self, new_api_key: str):
        """更新API密钥"""
        self.api_key = new_api_key
        self.client = None  # 重置客户端，下次调用时重新初始化
    
    @property
    def is_available(self) -> bool:
        """检查客户端是否可用"""
        return GOOGLE_SDK_AVAILABLE and self.client is not None
    
    @property
    def supports_grounding(self) -> bool:
        """检查是否支持联网搜索"""
        return self.is_available and self.grounding_tool is not None


# 工厂函数，方便其他模块使用
def create_gemini_client(api_key: Optional[str] = None) -> GoogleGeminiClient:
    """创建Gemini客户端实例"""
    return GoogleGeminiClient(api_key)


# 检查依赖
def check_google_sdk_installation() -> Tuple[bool, str]:
    """检查Google SDK安装状态"""
    if GOOGLE_SDK_AVAILABLE:
        try:
            import google.generativeai as genai_check
            return True, f"Google SDK已安装，版本: {getattr(genai_check, '__version__', '未知')}"
        except Exception as e:
            return False, f"Google SDK导入失败: {e}"
    else:
        return False, "Google SDK未安装，请运行: pip install google-generativeai"
