"""
稳定的LLM JSON解析器
基于多种策略进行JSON解析，确保稳定的结构化输出
增强版：添加输出验证、属性映射、数据纠偏、严格模式支持
"""
import json
import re
import logging
from typing import Any, Dict, List, Optional, Union, Set
from enum import Enum
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# 无效值集合
INVALID_VALUES = {
    '', 'null', 'none', 'n/a', 'na', 'undefined', 
    '[待补全]', '待补全', '暂无', '无', '未知',
    '...', '<具体值>', '<值>', '占位符',
    'placeholder', 'todo', 'tbd'
}

class ParseStrategy(Enum):
    """JSON解析策略枚举"""
    DIRECT = "direct"
    MARKDOWN_BLOCK = "markdown_block"
    REGEX_EXTRACT = "regex_extract"
    CLEANUP_AND_PARSE = "cleanup_and_parse"
    NESTED_EXTRACT = "nested_extract"
    KEY_VALUE_EXTRACT = "key_value_extract"

class OutputValidator:
    """输出验证和纠偏器"""
    
    @staticmethod
    def is_valid_value(value: Any) -> bool:
        """检查值是否有效"""
        if value is None:
            return False
        str_val = str(value).strip().lower()
        return str_val not in INVALID_VALUES and len(str_val) > 0
    
    @staticmethod
    def clean_value(value: Any) -> Optional[str]:
        """清理值，移除无效内容"""
        if value is None:
            return None
        str_val = str(value).strip()
        
        # 移除常见的引号问题
        str_val = str_val.strip('"\'')
        
        # 检查是否为无效值
        if str_val.lower() in INVALID_VALUES:
            return None
        
        # 检查是否为占位符格式
        if re.match(r'^<.*>$', str_val) or re.match(r'^\[.*\]$', str_val):
            return None
        
        return str_val if str_val else None
    
    @staticmethod
    def validate_and_correct(data: Dict, expected_attributes: List[str] = None) -> Dict:
        """
        验证并纠正输出数据，确保符合预期结构
        
        Args:
            data: 解析后的数据
            expected_attributes: 预期的属性名称列表
        
        Returns:
            验证和纠正后的数据
        """
        if not isinstance(data, dict):
            return data
        
        corrected_data = {}
        
        # 处理嵌套的attributes结构
        if 'attributes' in data and isinstance(data['attributes'], list):
            for attr in data['attributes']:
                if isinstance(attr, dict):
                    name = attr.get('name', attr.get('属性', attr.get('key', '')))
                    value = attr.get('value', attr.get('值', attr.get('val', '')))
                    cleaned_value = OutputValidator.clean_value(value)
                    if name and cleaned_value:
                        corrected_data[name] = cleaned_value
        
        # 直接的key-value对
        for key, value in data.items():
            if key not in ['attributes', 'entity_name', 'data_source', '实体名称']:
                cleaned_value = OutputValidator.clean_value(value)
                if cleaned_value:
                    corrected_data[key] = cleaned_value
        
        # 如果提供了预期属性，进行模糊匹配和纠正
        if expected_attributes:
            corrected_data = OutputValidator._fuzzy_match_attributes(corrected_data, expected_attributes)
        
        return corrected_data
    
    @staticmethod
    def _fuzzy_match_attributes(data: Dict, expected_attributes: List[str]) -> Dict:
        """
        使用模糊匹配将数据属性映射到预期属性
        """
        result = {}
        used_keys = set()
        
        for expected_attr in expected_attributes:
            # 首先尝试精确匹配
            if expected_attr in data:
                result[expected_attr] = data[expected_attr]
                used_keys.add(expected_attr)
                continue
            
            # 然后尝试模糊匹配
            best_match = None
            best_score = 0.0
            
            for key in data.keys():
                if key in used_keys:
                    continue
                
                # 计算相似度
                score = SequenceMatcher(None, expected_attr.lower(), key.lower()).ratio()
                
                # 检查是否包含关键词
                expected_keywords = set(re.findall(r'\w+', expected_attr.lower()))
                key_keywords = set(re.findall(r'\w+', key.lower()))
                keyword_overlap = len(expected_keywords & key_keywords) / max(len(expected_keywords), 1)
                
                combined_score = score * 0.6 + keyword_overlap * 0.4
                
                if combined_score > best_score and combined_score > 0.5:
                    best_score = combined_score
                    best_match = key
            
            if best_match:
                result[expected_attr] = data[best_match]
                used_keys.add(best_match)
                logger.debug(f"模糊匹配: '{best_match}' -> '{expected_attr}' (相似度: {best_score:.2f})")
        
        # 保留未匹配的原始数据
        for key, value in data.items():
            if key not in used_keys and key not in result:
                result[key] = value
        
        return result

class RobustLLMJsonParser:
    """
    稳定的LLM JSON解析器
    处理各种格式包括markdown代码块、格式错误的JSON等
    增强版：支持输出验证、属性映射、数据纠偏
    """
    
    def __init__(self, expected_attributes: List[str] = None):
        """
        初始化解析器
        
        Args:
            expected_attributes: 预期的属性名称列表，用于验证和纠偏
        """
        self.expected_attributes = expected_attributes or []
        self.validator = OutputValidator()
        
        # 按优先级排序的解析策略
        self.strategies = [
            self._try_direct_parse,
            self._try_markdown_block_parse,
            self._try_nested_json_extract,
            self._try_regex_extract,
            self._try_key_value_extract,
            self._try_cleanup_and_parse,
        ]
    
    def set_expected_attributes(self, attributes: List[str]):
        """设置预期属性列表"""
        self.expected_attributes = attributes
    
    def parse(self, text: str, default_value: Any = None, validate: bool = True) -> Union[Dict, List, Any]:
        """
        使用多种策略将LLM输出字符串解析为JSON
        
        Args:
            text: LLM的原始字符串输出
            default_value: 解析失败时返回的默认值
            validate: 是否进行输出验证和纠偏
        
        Returns:
            解析后的JSON对象（dict、list或其他JSON可序列化类型）
        
        Raises:
            ValueError: 如果所有解析策略都失败且未提供默认值
        """
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
                    
                    # 进行输出验证和纠偏
                    if validate and isinstance(result, dict):
                        result = self.validator.validate_and_correct(result, self.expected_attributes)
                    
                    return result
            except Exception as e:
                logger.debug(f"策略 {strategy.__name__} 失败: {str(e)}")
                continue
        
        # 如果所有策略都失败，尝试从文本中提取关键信息
        if self.expected_attributes:
            extracted = self._extract_from_text(cleaned_output)
            if extracted:
                logger.warning("使用文本提取策略")
                return extracted
        
        # 如果所有策略都失败
        if default_value is not None:
            logger.warning("所有解析策略都失败，返回默认值")
            return default_value
        
        raise ValueError(f"无法从LLM输出解析JSON: {text[:500]}...")
    
    def _try_direct_parse(self, text: str) -> Optional[Any]:
        """尝试直接将文本解析为JSON"""
        return json.loads(text)
    
    def _try_markdown_block_parse(self, text: str) -> Optional[Any]:
        """从markdown代码块中提取并解析JSON"""
        patterns = [
            r'```json\s*\n?(.*?)\n?```',
            r'```\s*\n?(.*?)\n?```',
            r'`([^`]+)`',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            for match in matches:
                try:
                    clean_match = match.strip()
                    if clean_match.startswith('{') or clean_match.startswith('['):
                        return json.loads(clean_match)
                except json.JSONDecodeError:
                    continue
        
        return None
    
    def _try_nested_json_extract(self, text: str) -> Optional[Any]:
        """提取嵌套的JSON结构"""
        # 查找最外层的完整JSON对象
        depth = 0
        start_idx = -1
        
        for i, char in enumerate(text):
            if char == '{':
                if depth == 0:
                    start_idx = i
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0 and start_idx >= 0:
                    potential_json = text[start_idx:i+1]
                    try:
                        return json.loads(potential_json)
                    except json.JSONDecodeError:
                        # 继续查找下一个JSON对象
                        start_idx = -1
        
        return None
    
    def _try_regex_extract(self, text: str) -> Optional[Any]:
        """使用正则表达式模式提取JSON"""
        # 更复杂的JSON对象匹配
        json_object_pattern = r'\{(?:[^{}]|(?:\{(?:[^{}]|\{[^{}]*\})*\}))*\}'
        
        matches = re.findall(json_object_pattern, text, re.DOTALL)
        for match in sorted(matches, key=len, reverse=True):  # 优先尝试最长的匹配
            try:
                return json.loads(match.strip())
            except json.JSONDecodeError:
                continue
        
        return None
    
    def _try_key_value_extract(self, text: str) -> Optional[Dict]:
        """从文本中提取key-value对"""
        result = {}
        
        # 匹配各种key-value格式
        patterns = [
            r'"([^"]+)"\s*:\s*"([^"]*)"',  # "key": "value"
            r"'([^']+)'\s*:\s*'([^']*)'",  # 'key': 'value'
            r'"([^"]+)"\s*:\s*(\d+(?:\.\d+)?)',  # "key": number
            r'([^:\n]+?):\s*([^\n,}]+)',  # key: value (简单格式)
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for key, value in matches:
                key = key.strip()
                value = value.strip().strip('"\'')
                if key and value and key not in ['', 'name', 'value']:
                    result[key] = value
        
        return result if result else None
    
    def _try_cleanup_and_parse(self, text: str) -> Optional[Any]:
        """清理并尝试解析JSON"""
        cleaned = text
        
        # 移除markdown标记
        cleaned = re.sub(r'```json\s*\n?', '', cleaned)
        cleaned = re.sub(r'\n?```', '', cleaned)
        
        # 移除常见的前导文字
        prefixes_to_remove = [
            r'^[^{]*?(?=\{)',  # 移除 { 之前的所有内容
            r'以下是.*?[：:]\s*',
            r'返回.*?[：:]\s*',
            r'结果.*?[：:]\s*',
            r'JSON.*?[：:]\s*',
        ]
        for pattern in prefixes_to_remove:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE | re.DOTALL)
        
        # 移除多余的空白
        cleaned = cleaned.strip()
        
        # 查找第一个 { 和最后一个 }
        start_idx = cleaned.find('{')
        end_idx = cleaned.rfind('}')
        
        if start_idx == -1 or end_idx == -1:
            return None
        
        potential_json = cleaned[start_idx:end_idx + 1]
        
        # 尝试修复常见的JSON格式问题
        fixed_json = self._fix_json_formatting(potential_json)
        
        try:
            return json.loads(fixed_json)
        except json.JSONDecodeError:
            # 尝试更aggressive的修复
            aggressive_fix = self._aggressive_json_fix(fixed_json)
            try:
                return json.loads(aggressive_fix)
            except json.JSONDecodeError:
                return None
    
    def _fix_json_formatting(self, text: str) -> str:
        """修复常见的JSON格式问题"""
        # 移除尾随逗号
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        
        # 修复缺少引号的key
        text = re.sub(r'(\{|\,)\s*([a-zA-Z_\u4e00-\u9fa5][a-zA-Z0-9_\u4e00-\u9fa5]*)\s*:', r'\1"\2":', text)
        
        # 修复中文引号
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        
        # 修复单引号为双引号（仅在JSON上下文中）
        # 这个需要小心处理，避免破坏字符串内容
        
        return text
    
    def _aggressive_json_fix(self, text: str) -> str:
        """更激进的JSON修复"""
        # 移除控制字符
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # 修复换行符在字符串中
        text = re.sub(r'("(?:[^"\\]|\\.)*")', lambda m: m.group(1).replace('\n', '\\n'), text)
        
        # 移除注释
        text = re.sub(r'//.*?\n', '\n', text)
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        
        # 修复重复的引号
        text = re.sub(r'""([^"]+)""', r'"\1"', text)
        
        # 修复缺失的逗号
        text = re.sub(r'"\s*\n\s*"', '",\n"', text)
        
        return text
    
    def _extract_from_text(self, text: str) -> Optional[Dict]:
        """从自然语言文本中提取属性值"""
        if not self.expected_attributes:
            return None
        
        result = {}
        
        for attr in self.expected_attributes:
            # 构建匹配模式 - 更严格的匹配
            attr_patterns = [
                rf'{re.escape(attr)}\s*[：:]\s*["\']?([^"\'\n,}}]+)["\']?',
                rf'["\']?{re.escape(attr)}["\']?\s*[：:]\s*["\']?([^"\'\n,}}]+)["\']?',
                rf'{re.escape(attr)}\s*为\s*["\']?([^"\'\n,}}]+)["\']?',
                rf'{re.escape(attr)}\s*是\s*["\']?([^"\'\n,}}]+)["\']?',
            ]
            
            for pattern in attr_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    cleaned = OutputValidator.clean_value(value)
                    if cleaned:
                        result[attr] = cleaned
                    break
        
        return result if result else None


# 全局解析器实例
json_parser = RobustLLMJsonParser()

def parse_llm_json(text: str, default_value: Any = None, expected_attributes: List[str] = None) -> Union[Dict, List, Any]:
    """
    解析LLM输出的JSON文本的便捷函数
    
    Args:
        text: LLM输出的文本
        default_value: 解析失败时的默认值
        expected_attributes: 预期的属性列表，用于验证和纠偏
    
    Returns:
        解析后的JSON对象
    """
    if expected_attributes:
        json_parser.set_expected_attributes(expected_attributes)
    return json_parser.parse(text, default_value)