# modules_new/core/api_client_wrapper.py
"""
API客户端包装器

封装API调用逻辑，支持不同的API客户端和配置
"""

import time
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from .exceptions import APIError, ProcessingError
from ..api import get_chemical_properties_with_grounding

if TYPE_CHECKING:
    from ..main.module_manager import ModuleManager
    from ..config.api_config import APIConfig


class APIClientWrapper:
    """API客户端包装器"""

    def __init__(self, module_manager: 'ModuleManager'):
        self.module_manager = module_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def call_api(self, prompt: str, api_config: 'APIConfig') -> str:
        """
        根据配置调用API

        Args:
            prompt: 发送给API的提示
            api_config: API配置

        Returns:
            API返回的文本响应

        Raises:
            APIError: 如果API调用失败
        """
        self.logger.info(f"使用配置 '{api_config.name}' 调用API")

        try:
            if api_config.use_official_sdk:
                # 使用Gemini官方SDK
                response_text = get_chemical_properties_with_grounding(prompt)
            else:
                # 对于OpenAI兼容的API（如通义千问）
                try:
                    from openai import OpenAI
                    
                    client = OpenAI(
                        api_key=api_config.api_key,
                        base_url=api_config.base_url
                    )
                    
                    # 检查是否启用网络搜索（通义千问特有参数）
                    extra_params = {}
                    if hasattr(api_config, 'enable_grounding') and api_config.enable_grounding:
                        extra_params['extra_body'] = {"enable_search": True}
                    
                    # 确保max_tokens不超过通义千问API的限制（16384）
                    max_tokens_value = getattr(api_config, 'max_tokens', 4000)
                    if max_tokens_value > 16384:
                        max_tokens_value = 16384
                    
                    chat_completion = client.chat.completions.create(
                        messages=[{"role": "user", "content": prompt}],
                        model=api_config.model,
                        temperature=getattr(api_config, 'temperature', 0.1),
                        max_tokens=max_tokens_value,
                        **extra_params
                    )
                    response_text = chat_completion.choices[0].message.content
                    
                except ImportError:
                    # 如果OpenAI库不可用，使用原有方式
                    api_client = self.module_manager.get_module("api_client")
                    if not api_client:
                        raise ProcessingError("API客户端未加载")

                    response = api_client.post_sync(
                        url=f"{api_config.base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_config.api_key}",
                            "Content-Type": "application/json"
                        },
                        json_payload={
                            "model": api_config.model,
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": getattr(api_config, 'temperature', 0.1),
                            "max_tokens": getattr(api_config, 'max_tokens', 4000)
                        }
                    )
                    response_text = response.get('data', {}).get('choices', [{}])[0].get('message', {}).get('content', '')

            if not response_text:
                raise APIError("API返回空响应")

            return response_text

        except Exception as e:
            self.logger.error(f"API调用失败: {e}")
            raise APIError(f"API调用失败: {e}") from e
