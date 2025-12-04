# modules_new/api/gemini_grounding.py
"""
Google Gemini官方SDK API模块 - 支持联网搜索

使用Google官方的genai SDK来调用Gemini模型，支持Google Search联网功能。
"""
import os
import json
import logging
from typing import Optional, Dict, Any, Tuple

try:
    from google import genai
    from google.genai import types
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False
    logging.warning("Google genai库未安装，无法使用官方SDK联网搜索功能")

from ..core.exceptions import APIError, with_error_handling
from ..core.constants import DEFAULT_CONFIG
from ..core.error_analyzer import log_api_error


class GeminiGroundingClient:
    """Google Gemini官方SDK客户端，支持联网搜索"""
    
    def __init__(self):
        self.client = None
        self.grounding_tool = None
        self.config = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initialize()
    
    def _initialize(self):
        """初始化Gemini客户端和联网搜索工具"""
        if not GENAI_AVAILABLE:
            self.logger.error("Google genai库未安装，请运行: pip install google-genai")
            return False
        
        try:
            # 获取API密钥
            api_key = os.environ.get('GEMINI_API_KEY')
            if not api_key:
                # 从配置管理器获取API密钥
                from ..config.api_config import APIConfigManager
                api_manager = APIConfigManager()
                gemini_config = api_manager.get_config('gemini_grounding')
                if gemini_config:
                    api_key = gemini_config.api_key
            
            if not api_key:
                self.logger.error("GEMINI API密钥未配置")
                return False
            
            # 创建客户端，直接传入API密钥
            self.client = genai.Client(api_key=api_key)
            
            # 定义联网搜索工具
            self.grounding_tool = types.Tool(
                google_search=types.GoogleSearch()
            )
            
            # 配置更宽松的安全设置，以处理化学品查询
            safety_settings = [
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.BLOCK_NONE
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.BLOCK_ONLY_HIGH
                )
            ]
            
            # 配置生成设置
            self.config = types.GenerateContentConfig(
                tools=[self.grounding_tool],
                temperature=0.1,  # 降低温度以获得更一致的输出
                max_output_tokens=800000,  # 增加输出长度以确保完整JSON
                safety_settings=safety_settings  # 应用安全设置
            )
            
            self.logger.info("Gemini官方SDK客户端初始化成功（支持联网搜索，已优化安全设置）")
            return True
            
        except Exception as e:
            self.logger.error(f"Gemini官方SDK客户端初始化失败: {e}")
            return False
    
    def is_available(self) -> bool:
        """检查客户端是否可用"""
        return GENAI_AVAILABLE and self.client is not None
    
    @with_error_handling()
    def generate_content(self, prompt: str, model: str = "gemini-2.5-flash") -> Optional[str]:
        """
        使用联网搜索功能生成内容
        
        Args:
            prompt: 输入提示词
            model: 模型名称
            
        Returns:
            生成的内容，如果失败返回None
        """
        if not self.is_available():
            self.logger.error("Gemini客户端不可用")
            return None
        
        try:
            # 确保客户端存在
            if self.client is None:
                self.logger.error("Gemini客户端未初始化")
                return None
            
            # 发起请求
            response = self.client.models.generate_content(
                model=model,
                contents=prompt,
                config=self.config,
            )
            
            # 返回生成的文本
            if response and hasattr(response, 'text'):
                return response.text
            else:
                self.logger.error("Gemini API返回空响应")
                return None
                
        except Exception as e:
            self.logger.error(f"Gemini API调用失败: {e}")
            return None
    
    def test_connection(self) -> Tuple[bool, str]:
        """测试API连接和联网搜索功能"""
        if not self.is_available():
            return False, "Gemini客户端不可用"
        
        try:
            test_prompt = "请搜索并告诉我今天是几月几号，以及今天有什么重要新闻。"
            response = self.generate_content(test_prompt)
            
            if response:
                return True, f"连接成功！返回内容长度: {len(response)} 字符"
            else:
                return False, "API连接失败，未返回内容"
                
        except Exception as e:
            return False, f"连接测试失败: {e}"


# 全局客户端实例
_gemini_grounding_client = None


def get_gemini_grounding_client() -> GeminiGroundingClient:
    """获取Gemini联网搜索客户端"""
    global _gemini_grounding_client
    if _gemini_grounding_client is None:
        _gemini_grounding_client = GeminiGroundingClient()
    return _gemini_grounding_client


def get_chemical_properties_with_grounding(prompt: str) -> Optional[str]:
    """
    使用Gemini联网搜索功能获取化学品属性
    
    Args:
        prompt: 已经优化的化学品查询提示词（由string_utils.generate_prompt生成）
        
    Returns:
        API响应内容，如果失败返回None
    """
    client = get_gemini_grounding_client()
    
    if not client.is_available():
        logging.error("Gemini联网搜索客户端不可用，请检查配置")
        return None
    
    # 直接使用传入的已优化prompt，添加简体中文指令确保输出语言
    enhanced_prompt = f"""
🌐 **重要指令：请使用简体中文回答，确保所有输出内容均为简体中文。**

{prompt}

**🔍 联网搜索执行指引**：
• 🎯 优先查找官方SDS/MSDS文档和产品数据表
• ✅ 确保数据来自可验证的权威来源，避免非官方或商业推广信息
• 🔄 重点关注工业级化学品的标准安全参数和物理化学性质
• 🛡️ 遵循国际化学品安全标准(GHS全球化学品统一分类和标签制度等)
• 📊 如遇多个数据源，以官方监管机构数据为准
• 🔍 特别注意CAS号的准确性，从PubChem等权威数据库验证
• 📋 针对"CAS号或流水号"字段进行智能处理和验证

**必须补全的核心属性（重点针对中国化学品名录结构）**：
• **CAS号或流水号** (自动查询补充，智能识别类型) 
• **中文名称** • **中文别名** • **英文名称** • **英文别名** 
• **分子式** • **是否为危化品** • **浓度阈值**
• 外观与性状 • 室温状态 • 建议存储方式/条件
• 物理特性 (熔点、沸点、密度、相对密度等)
• 使用Google Search联网功能实时查询权威数据库
• 确保获取最新、最准确的化学品安全数据
• 严格按照已提供的JSON格式输出结果，无需额外解释或markdown格式
• 所有技术术语、描述均使用简体中文表达
• 🔍 **重点**：如需补充CAS号，必须在结果中提供查询到的正确CAS号
• 🏷️ 每个字段后必须用{{}}标明数据来源（如{{PubChem}}、{{ECHA}}等）

**在输出JSON后，请在新的一行用中文说明数据来源：**
✅ 联网查找 - 数据来源于最新的在线权威数据库 (请具体说明主要数据来源，如PubChem、ECHA等)
ℹ️ 模型数据 - 基于训练数据回答，未进行实时联网查找

现在开始联网搜索并提供化学品安全数据："""
    
    try:
        logging.info("正在使用Gemini联网搜索查询化学品数据...")
        
        response = client.generate_content(enhanced_prompt)
        
        if response and response.strip():
            logging.info(f"Gemini联网搜索成功，返回内容长度: {len(response)} 字符")
            return response
        else:
            logging.warning("Gemini联网搜索未返回有效响应，可能触发了内容安全政策")
            return None
            
    except Exception as e:
        error_msg = str(e)
        logging.error(f"Gemini联网搜索API调用失败: {error_msg}")
        
        # 使用增强的错误分析器
        log_api_error(error_msg, "Gemini联网搜索")
        
        return None
