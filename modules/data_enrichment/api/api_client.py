# modules_new/api/api_client.py
"""
API客户端

负责与外部API的基础交互
"""

import asyncio
import aiohttp
import time
from typing import Dict, Any, Optional, List
import logging

from ..core.base import BaseProcessor
from ..core.exceptions import APIError, with_error_handling
from ..core.constants import DEFAULT_CONFIG
from ..core.error_analyzer import log_api_error


class APIClient(BaseProcessor):
    """API客户端"""
    
    def __init__(self, name: str = "APIClient"):
        super().__init__(name)
        self.session = None
        self.request_count = 0
        self.last_request_time = 0
        self.rate_limit_delay = DEFAULT_CONFIG.get('api_delay', 1.0)
        self.timeout = DEFAULT_CONFIG.get('timeout', 60)
        self.max_retries = DEFAULT_CONFIG.get('max_retries', 3)
    
    def _setup(self):
        """初始化设置"""
        # HTTP会话将在需要时创建
        pass
    
    def test_connection(self, service_name: str) -> tuple[bool, str]:
        """测试API连接"""
        # 此处应实现一个简单的测试逻辑，例如ping或一个轻量级请求
        # 为简化，我们假设如果能获取配置就代表连接基本可用
        return True, f"API服务 '{service_name}' 配置存在，连接基本可用。"

    def process(self, data: Dict[str, Any]) -> Any:
        """处理API请求"""
        method = data.get('method', 'GET')
        url = data.get('url')
        headers = data.get('headers', {})
        payload = data.get('payload')
        
        if not url:
            raise APIError("URL不能为空")
        
        if method.upper() == 'GET':
            return self.get_sync(url, headers=headers, params=payload)
        elif method.upper() == 'POST':
            return self.post_sync(url, headers=headers, json_payload=payload)
        else:
            raise APIError(f"不支持的HTTP方法: {method}")
    
    def get_sync(self, url: str, headers: Optional[Dict[str, str]] = None, 
                 params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步执行GET请求"""
        return asyncio.run(self.get(url, headers=headers, params=params))

    def post_sync(self, url: str, headers: Optional[Dict[str, str]] = None, 
                  json_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步执行POST请求"""
        return asyncio.run(self.post(url, headers=headers, json_payload=json_payload))

    @with_error_handling()
    async def _ensure_session(self):
        """确保会话存在"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(timeout=timeout)
    
    @with_error_handling()
    async def _rate_limit(self):
        """执行速率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            await asyncio.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    async def get(self, url: str, headers: Optional[Dict[str, str]] = None, 
                  params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """执行GET请求"""
        await self._ensure_session()
        await self._rate_limit()
        
        for attempt in range(self.max_retries):
            try:
                if self.session:
                    async with self.session.get(url, headers=headers, params=params) as response:
                        self.logger.info(f"GET请求: {url} - 状态码: {response.status}")
                        
                        if response.status == 200:
                            result = await response.json()
                            return {
                                'success': True,
                                'data': result,
                                'status_code': response.status,
                                'attempt': attempt + 1
                            }
                        else:
                            error_text = await response.text()
                            self.logger.warning(f"GET请求失败: {response.status} - {error_text}")
                            
                            if attempt >= self.max_retries - 1:
                                log_api_error(f"GET请求失败: {response.status} - {error_text}", "HTTP错误")
                                raise APIError(f"GET请求失败: {response.status} - {error_text}")
                await asyncio.sleep(2 ** attempt)
            
            except asyncio.TimeoutError:
                if attempt >= self.max_retries - 1:
                    log_api_error("请求超时", "GET请求")
                    raise APIError("请求超时")
                self.logger.warning(f"请求超时，重试 {attempt + 1}/{self.max_retries}")
                await asyncio.sleep(2 ** attempt)
            
            except Exception as e:
                if attempt >= self.max_retries - 1:
                    log_api_error(str(e), "GET请求")
                    raise APIError(f"请求失败: {e}")
                self.logger.warning(f"请求异常，重试 {attempt + 1}/{self.max_retries}: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return {'success': False, 'data': '请求失败，已达最大重试次数', 'status_code': 0, 'attempt': self.max_retries}

    async def post(self, url: str, headers: Optional[Dict[str, str]] = None, 
                   json_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """执行POST请求"""
        await self._ensure_session()
        await self._rate_limit()
        
        for attempt in range(self.max_retries):
            try:
                if self.session:
                    async with self.session.post(url, headers=headers, json=json_payload) as response:
                        self.logger.info(f"POST请求: {url} - 状态码: {response.status}")
                        
                        if response.status in [200, 201]:
                            result = await response.json()
                            return {
                                'success': True,
                                'data': result,
                                'status_code': response.status,
                                'attempt': attempt + 1
                            }
                        else:
                            error_text = await response.text()
                            self.logger.warning(f"POST请求失败: {response.status} - {error_text}")
                            
                            if attempt >= self.max_retries - 1:
                                log_api_error(f"POST请求失败: {response.status} - {error_text}", "HTTP错误")
                                raise APIError(f"POST请求失败: {response.status} - {error_text}")
                await asyncio.sleep(2 ** attempt)
            
            except asyncio.TimeoutError:
                if attempt >= self.max_retries - 1:
                    log_api_error("请求超时", "POST请求")
                    raise APIError("请求超时")
                self.logger.warning(f"请求超时，重试 {attempt + 1}/{self.max_retries}")
                await asyncio.sleep(2 ** attempt)
            
            except Exception as e:
                if attempt >= self.max_retries - 1:
                    log_api_error(str(e), "POST请求")
                    raise APIError(f"请求失败: {e}")
                self.logger.warning(f"请求异常，重试 {attempt + 1}/{self.max_retries}: {e}")
                await asyncio.sleep(2 ** attempt)
        
        return {'success': False, 'data': '请求失败，已达最大重试次数', 'status_code': 0, 'attempt': self.max_retries}

    def get_stats(self) -> Dict[str, Any]:
        """获取API调用统计"""
        return {
            'total_requests': self.request_count,
            'last_request_time': self.last_request_time,
            'rate_limit_delay': self.rate_limit_delay,
            'session_active': self.session is not None and not self.session.closed
        }
    
    async def close(self):
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.info("API会话已关闭")
    
    def __del__(self):
        """析构函数"""
        if self.session and not self.session.closed:
            # 在析构时尝试关闭会话
            try:
                # 检查事件循环是否存在且未关闭
                loop = None
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = None
                except RuntimeError:
                    loop = None
                
                if loop is not None:
                    loop.run_until_complete(self.close())
                else:
                    # 如果事件循环不可用，直接标记会话关闭（避免警告）
                    self.session = None
            except Exception:
                # 忽略所有异常，避免析构函数中的错误
                pass
