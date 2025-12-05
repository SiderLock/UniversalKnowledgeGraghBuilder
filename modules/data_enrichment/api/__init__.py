# modules_new/api/__init__.py
"""
API处理模块

负责与外部API的交互、请求管理等
"""

from .api_client import APIClient
from .gemini_grounding import GeminiGroundingClient, get_gemini_grounding_client, get_chemical_properties_with_grounding

__all__ = [
    'APIClient',
    'GeminiGroundingClient',
    'get_gemini_grounding_client',
    'get_chemical_properties_with_grounding'
]
