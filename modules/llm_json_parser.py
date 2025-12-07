"""
稳定的LLM JSON解析器
基于多种策略进行JSON解析，确保稳定的结构化输出
"""
import json
import re
import logging
from typing import Any, Dict, List, Optional, Union
from enum import Enum

logger = logging.getLogger(__name__)

class ParseStrategy(Enum):
    """JSON解析策略枚举"""
    DIRECT = "direct"
    MARKDOWN_BLOCK = "markdown_block"
    REGEX_EXTRACT = "regex_extract"
    CLEANUP_AND_PARSE = "cleanup_and_parse"

class RobustLLMJsonParser:
    """
    稳定的LLM JSON解析器
    处理各种格式包括markdown代码块、格式错误的JSON等
    """
    
    def __init__(self):
        # 按优先级排序的解析策略
        self.strategies = [
            self._try_direct_parse,
            self._try_markdown_block_parse,
            self._try_regex_extract,
            self._try_cleanup_and_parse,
        ]
    
    def parse(self, text: str, default_value: Any = None) -> Union[Dict, List, Any]:
        """
        使用多种策略将LLM输出字符串解析为JSON
        
        Args:
            text: LLM的原始字符串输出
            default_value: 解析失败时返回的默认值
        
        Returns:
            解析后的JSON对象（dict、list或其他JSON可序列化类型）
        
        Raises:
            ValueError: 如果所有解析策略都失败且未提供默认值
        """
        logger.info(f"开始解析文本: {text[:100]}...")
        
        if not text or not text.strip():
            if default_value is not None:
                return default_value
            raise ValueError("输入字符串为空")
        
        cleaned_output = text.strip()
        
        # 尝试每种解析策略
        for strategy in self.strategies:
            try:
                result = strategy(cleaned_output)
                if result is not None:
                    logger.debug(f"成功使用策略解析: {strategy.__name__}")
                    return result
            except Exception as e:
                logger.debug(f"策略 {strategy.__name__} 失败: {str(e)}")
                continue
        
        # 如果所有策略都失败
        if default_value is not None:
            logger.warning("所有解析策略都失败，返回默认值")
            return default_value
        
        raise ValueError(f"无法从LLM输出解析JSON: {text[:1000]}...")
    
    def _try_direct_parse(self, text: str) -> Optional[Any]:
        """尝试直接将文本解析为JSON"""
        return json.loads(text)
    
    def _try_markdown_block_parse(self, text: str) -> Optional[Any]:
        """从markdown代码块中提取并解析JSON"""
        patterns = [
            r'```json\s*\n(.*?)\n```',
            r'```\s*\n(.*?)\n```',
            r'```json(.*?)```',
            r'```(.*?)```',
            r'`([^`]*)`',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            for match in matches:
                try:
                    clean_match = match.strip()
                    return json.loads(clean_match)
                except json.JSONDecodeError:
                    continue
        
        return None
    
    def _try_regex_extract(self, text: str) -> Optional[Any]:
        """使用正则表达式模式提取JSON"""
        # 查找看起来像JSON的内容
        json_patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # 简单的JSON对象匹配
            r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]',  # 简单的JSON数组匹配
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            for match in matches:
                try:
                    # 尝试解析找到的JSON片段
                    return json.loads(match.strip())
                except json.JSONDecodeError:
                    continue
        
        return None
    
    def _try_cleanup_and_parse(self, text: str) -> Optional[Any]:
        """清理并尝试解析JSON"""
        # 移除常见的非JSON内容
        cleaned = text
        
        # 移除markdown标记
        cleaned = re.sub(r'```json\s*\n?', '', cleaned)
        cleaned = re.sub(r'\n?```', '', cleaned)
        
        # 移除多余的空白
        cleaned = cleaned.strip()
        
        # 查找第一个 { 或 [ 和最后一个 } 或 ]
        start_chars = ['{', '[']
        end_chars = ['}', ']']
        
        start_idx = -1
        start_char = None
        for char in start_chars:
            idx = cleaned.find(char)
            if idx != -1 and (start_idx == -1 or idx < start_idx):
                start_idx = idx
                start_char = char
        
        if start_idx == -1:
            return None
        
        # 找到对应的结束字符
        end_char = '}' if start_char == '{' else ']'
        end_idx = cleaned.rfind(end_char)
        
        if end_idx == -1:
            return None
        
        # 提取可能的JSON部分
        potential_json = cleaned[start_idx:end_idx + 1]
        
        # 尝试修复常见的JSON格式问题
        fixed_json = self._fix_json_formatting(potential_json)
        
        try:
            return json.loads(fixed_json)
        except json.JSONDecodeError:
            return None
    
    def _fix_json_formatting(self, text: str) -> str:
        """修复常见的JSON格式问题"""
        # 移除尾随逗号
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        
        # 简单修复：将中文引号替换为转义的引号
        text = text.replace('"', '\\"').replace('"', '\\"')
        
        # 修复双重转义的问题
        text = text.replace('\\"\\"', '\\"')
        
        return text


# 全局解析器实例
json_parser = RobustLLMJsonParser()

def parse_llm_json(text: str, default_value: Any = None) -> Union[Dict, List, Any]:
    """
    解析LLM输出的JSON文本的便捷函数
    
    Args:
        text: LLM输出的文本
        default_value: 解析失败时的默认值
    
    Returns:
        解析后的JSON对象
    """
    return json_parser.parse(text, default_value)