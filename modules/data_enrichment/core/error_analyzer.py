# modules_new/core/error_analyzer.py
"""
API错误分析工具

提供统一的错误检测和分类功能，支持中英文错误信息。
"""
import logging
from typing import Dict, List, Tuple, Optional, Any


class APIErrorAnalyzer:
    """API错误分析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 错误关键词分类 - 支持中英文
        self.error_categories = {
            'safety_policy': {
                'keywords': [
                    # 英文
                    'safety', 'policy', 'harmful', 'blocked', 'filtered', 'violation', 
                    'restricted', 'prohibited', 'denied', 'unsafe', 'dangerous', 
                    'content_filter', 'harm_category', 'inappropriate',
                    # 中文
                    '安全', '政策', '有害', '阻止', '过滤', '违规', '限制', '禁止', 
                    '拒绝', '不安全', '危险', '内容过滤', '不当内容', '敏感内容'
                ],
                'description': '内容安全政策限制'
            },
            'authentication': {
                'keywords': [
                    # 英文
                    'api_key', 'authentication', 'authorization', 'unauthorized', 
                    'invalid_key', 'access_denied', 'credential', 'token',
                    # 中文
                    '认证', '授权', '密钥', '凭证', '令牌', '身份验证', '访问被拒绝'
                ],
                'description': 'API认证错误'
            },
            'rate_limit': {
                'keywords': [
                    # 英文
                    'quota', 'rate_limit', 'limit_exceeded', 'too_many_requests',
                    'throttle', 'requests_per_minute', 'daily_limit',
                    # 中文
                    '配额', '频率限制', '限制超出', '请求过多', '节流', '每分钟请求数', '日限制'
                ],
                'description': 'API配额或频率限制'
            },
            'network': {
                'keywords': [
                    # 英文
                    'timeout', 'connection', 'network', 'dns', 'unreachable',
                    'socket', 'ssl', 'certificate', 'handshake',
                    # 中文
                    '超时', '连接', '网络', '域名解析', '无法访问', '套接字', '证书', '握手'
                ],
                'description': '网络连接错误'
            },
            'server_error': {
                'keywords': [
                    # 英文
                    'internal_server_error', '500', '502', '503', '504',
                    'service_unavailable', 'gateway_timeout', 'bad_gateway',
                    # 中文
                    '服务器内部错误', '服务不可用', '网关超时', '错误的网关'
                ],
                'description': '服务器错误'
            },
            'request_error': {
                'keywords': [
                    # 英文
                    'bad_request', '400', '404', 'not_found', 'invalid_request',
                    'malformed', 'syntax_error', 'invalid_json',
                    # 中文
                    '错误请求', '未找到', '无效请求', '格式错误', '语法错误', '无效JSON'
                ],
                'description': '请求格式错误'
            }
        }
    
    def analyze_error(self, error_message: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        分析错误信息并分类
        
        Args:
            error_message: 错误信息
            
        Returns:
            Tuple[category, description, details]
        """
        if not error_message:
            return 'unknown', '未知错误', {'raw_message': ''}
        
        error_msg_lower = error_message.lower()
        
        # 按优先级检查错误类别 (更具体的错误类型优先)
        priority_order = ['authentication', 'rate_limit', 'network', 'request_error', 'server_error', 'safety_policy']
        
        for category in priority_order:
            info = self.error_categories[category]
            for keyword in info['keywords']:
                if keyword.lower() in error_msg_lower:
                    details = {
                        'raw_message': error_message,
                        'matched_keyword': keyword,
                        'category': category,
                        'suggested_action': self._get_suggested_action(category)
                    }
                    return category, info['description'], details
        
        # 未匹配到已知错误类别
        return 'unknown', '未知错误类型', {
            'raw_message': error_message,
            'category': 'unknown',
            'suggested_action': '请检查错误详情并联系技术支持'
        }
    
    def _get_suggested_action(self, category: str) -> str:
        """获取针对错误类别的建议操作"""
        suggestions = {
            'safety_policy': '化学品信息可能被视为敏感内容，请尝试调整查询方式或联系API提供商',
            'authentication': '请检查API密钥配置是否正确',
            'rate_limit': '请求频率过高，请稍后重试或升级API配额',
            'network': '网络连接问题，请检查网络状态或稍后重试',
            'server_error': 'API服务器暂时不可用，请稍后重试',
            'request_error': '请求格式有误，请检查请求参数',
            'unknown': '请检查错误详情，必要时联系技术支持'
        }
        return suggestions.get(category, suggestions['unknown'])
    
    def log_analyzed_error(self, error_message: str, context: str = "") -> None:
        """
        分析错误并记录到日志
        
        Args:
            error_message: 错误信息
            context: 上下文信息
        """
        category, description, details = self.analyze_error(error_message)
        
        # 构建日志消息
        log_msg = f"🔍 错误分析 - {description}"
        if context:
            log_msg += f" [{context}]"
        
        # 根据错误类别选择日志级别
        if category in ['safety_policy', 'rate_limit']:
            self.logger.warning(f"{log_msg}: {details['suggested_action']}")
        elif category in ['authentication', 'server_error']:
            self.logger.error(f"{log_msg}: {details['suggested_action']}")
        else:
            self.logger.warning(f"{log_msg}: {error_message}")
        
        # 记录详细信息（调试级别）
        self.logger.debug(f"错误详情: {details}")


# 全局错误分析器实例
_error_analyzer = None


def get_error_analyzer() -> APIErrorAnalyzer:
    """获取错误分析器实例"""
    global _error_analyzer
    if _error_analyzer is None:
        _error_analyzer = APIErrorAnalyzer()
    return _error_analyzer


def analyze_api_error(error_message: str, context: str = "") -> Tuple[str, str, Dict[str, Any]]:
    """
    快速分析API错误的便捷函数
    
    Args:
        error_message: 错误信息
        context: 上下文信息
        
    Returns:
        Tuple[category, description, details]
    """
    analyzer = get_error_analyzer()
    return analyzer.analyze_error(error_message)


def log_api_error(error_message: str, context: str = "") -> None:
    """
    快速记录API错误的便捷函数
    
    Args:
        error_message: 错误信息
        context: 上下文信息
    """
    analyzer = get_error_analyzer()
    analyzer.log_analyzed_error(error_message, context)
