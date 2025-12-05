# modules_new/utils/string_utils.py
"""
字符串工具类

提供字符串处理相关的实用工具
"""

import re
import unicodedata
from typing import List, Dict, Optional, Any, Tuple
import logging
import json
import pandas as pd


class StringUtils:
    """字符串工具类"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """清理文本，移除多余空格"""
        if not isinstance(text, str):
            return str(text)
        
        # 移除前后空格
        text = text.strip()
        
        # 将多个空格替换为单个空格
        text = re.sub(r'\s+', ' ', text)
        
        # 移除控制字符
        text = ''.join(char for char in text if unicodedata.category(char)[0] != 'C')
        
        return text
    
    @staticmethod
    def normalize_chemical_name(name: str) -> str:
        """标准化化学品名称"""
        if not isinstance(name, str):
            return str(name)
        
        # 基础清理
        name = StringUtils.clean_text(name)
        
        # 移除常见的非必要前缀/后缀
        prefixes_to_remove = ['化学纯', '分析纯', 'AR', 'CP', 'GR']
        for prefix in prefixes_to_remove:
            if name.startswith(prefix):
                name = name[len(prefix):].strip()
        
        # 标准化括号
        name = re.sub(r'[（\(]([^）\)]*)[）\)]', r'(\1)', name)
        
        return name
    
    @staticmethod
    def normalize_cas_number(cas: str) -> str:
        """标准化CAS号格式"""
        if not isinstance(cas, str):
            return str(cas)
        
        # 移除所有空格和特殊字符，只保留数字和连字符
        cas = re.sub(r'[^\d\-]', '', cas)
        
        # 验证格式并标准化
        cas_pattern = re.compile(r'^(\d{2,7})-(\d{2})-(\d)$')
        match = cas_pattern.match(cas)
        
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        
        return cas
    
    @staticmethod
    def validate_cas_or_serial_number(identifier: str) -> Dict[str, Any]:
        """
        验证和分析CAS号或流水号
        
        Args:
            identifier: 输入的标识符（CAS号或流水号）
            
        Returns:
            包含验证结果的字典
        """
        if not identifier or str(identifier).strip() == '':
            return {
                'type': '空值',
                'is_valid': False,
                'formatted': '',
                'needs_query': True,
                'suggestion': '需要查询补充标准CAS号'
            }
        
        identifier_str = str(identifier).strip()
        
        # CAS号格式验证
        cas_pattern = r'^\d{2,7}-\d{2}-\d$'
        if re.match(cas_pattern, identifier_str):
            return {
                'type': '标准CAS号',
                'is_valid': True,
                'formatted': identifier_str,
                'needs_query': False,
                'suggestion': 'CAS号格式正确，需验证准确性'
            }
        
        # 流水号判断（纯数字）
        if identifier_str.isdigit() and len(identifier_str) >= 6:
            return {
                'type': '本地流水号',
                'is_valid': True,
                'formatted': identifier_str,
                'needs_query': True,
                'suggestion': '检测到本地流水号，建议查询对应的国际CAS号'
            }
        
        # 格式异常
        return {
            'type': '格式异常',
            'is_valid': False,
            'formatted': identifier_str,
            'needs_query': True,
            'suggestion': f'编号格式不标准：{identifier_str}，需要重新查询验证'
        }

    @staticmethod
    def extract_numbers(text: str) -> List[float]:
        """从文本中提取数字"""
        if not isinstance(text, str):
            return []
        
        # 匹配数字（包括小数和科学计数法）
        number_pattern = r'[-+]?(?:\d*\.\d+|\d+\.?\d*)(?:[eE][-+]?\d+)?'
        matches = re.findall(number_pattern, text)
        
        numbers = []
        for match in matches:
            try:
                numbers.append(float(match))
            except ValueError:
                continue
        
        return numbers

    @staticmethod
    def is_valid_field(value: Any) -> bool:
        """
        检查单个字段值是否有效
        """
        if value is None or pd.isna(value):
            return False

        # 定义一套全面的无效值（优化为集合以提高查找性能）
        invalid_values = {
            'N/A', 'NULL', 'NONE', '未知', '无数据', '无', '不详', 'UNKNOWN',
            'NOT AVAILABLE', 'NOT APPLICABLE', 'NO DATA', '/', '-', '',
            '待补充', '缺失', 'MISSING', 'TBD', 'TO BE DETERMINED', 'NA',
            'NAN', '空', '无效', 'INVALID', 'NO INFO', 'NO INFORMATION'
        }
        
        str_value = str(value).strip()
        
        if not str_value or str_value.upper() in invalid_values:
            return False
            
        # 额外检查：不是纯数字的无意义字符串
        if str_value.isdigit() and len(str_value) < 2:
            return False
            
        return True

    @staticmethod
    def count_valid_fields(properties_data: Dict[str, Any]) -> int:
        """
        计算有效字段数量，优化了性能和准确性
        
        Args:
            properties_data: 属性数据字典
            
        Returns:
            有效字段的数量
        """
        if not properties_data or not isinstance(properties_data, dict):
            return 0
        
        return sum(1 for value in properties_data.values() if StringUtils.is_valid_field(value))

    @staticmethod
    def extract_units(text: str) -> List[str]:
        """从文本中提取单位"""
        if not isinstance(text, str):
            return []
        
        # 常见单位模式
        unit_patterns = [
            r'°C|℃|K|°F',  # 温度单位
            r'Pa|kPa|MPa|bar|atm|mmHg',  # 压力单位
            r'g/mol|Da|u',  # 分子量单位
            r'g/cm³|kg/m³|g/mL',  # 密度单位
            r'mol/L|M|ppm|ppb|%'  # 浓度单位
        ]
        
        units = []
        for pattern in unit_patterns:
            matches = re.findall(pattern, text)
            units.extend(matches)
        
        return list(set(units))  # 去重
    
    @staticmethod
    def is_chinese(text: str) -> bool:
        """判断文本是否包含中文字符"""
        if not isinstance(text, str):
            return False
        
        return bool(re.search(r'[\u4e00-\u9fff]', text))
    
    @staticmethod
    def is_english(text: str) -> bool:
        """判断文本是否为英文"""
        if not isinstance(text, str):
            return False
        
        return bool(re.match(r'^[a-zA-Z\s\-\(\)\[\]0-9,\.]+$', text))
    
    @staticmethod
    def similarity_ratio(text1: str, text2: str) -> float:
        """计算两个字符串的相似度（简单版本）"""
        if not isinstance(text1, str) or not isinstance(text2, str):
            return 0.0
        
        if text1 == text2:
            return 1.0
        
        # 转换为小写
        text1 = text1.lower()
        text2 = text2.lower()
        
        # 计算编辑距离的简化版本
        len1, len2 = len(text1), len(text2)
        if len1 == 0:
            return 0.0 if len2 > 0 else 1.0
        if len2 == 0:
            return 0.0
        
        # 计算共同字符数
        common_chars = 0
        for char in set(text1):
            common_chars += min(text1.count(char), text2.count(char))
        
        # 相似度 = 共同字符数 / 最长字符串长度
        similarity = common_chars / max(len1, len2)
        return similarity
    
    @staticmethod
    def find_best_match(target: str, candidates: List[str], threshold: float = 0.6) -> Optional[str]:
        """在候选列表中找到最佳匹配"""
        if not candidates:
            return None
        
        best_match = None
        best_score = 0.0
        
        for candidate in candidates:
            score = StringUtils.similarity_ratio(target, candidate)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = candidate
        
        return best_match
    
    @staticmethod
    def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
        """截断文本到指定长度"""
        if not isinstance(text, str):
            text = str(text)
        
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
    
    @staticmethod
    def mask_sensitive_info(text: str, mask_char: str = "*") -> str:
        """屏蔽敏感信息"""
        if not isinstance(text, str):
            return str(text)
        
        # 屏蔽可能的API密钥
        text = re.sub(r'([a-zA-Z0-9]{20,})', lambda m: m.group(1)[:4] + mask_char * (len(m.group(1)) - 8) + m.group(1)[-4:], text)
        
        return text
    
    @staticmethod
    def generate_prompt(chemical_name: str, cas_number: Any, api_config=None) -> Optional[str]:
        """
        根据化学品名称和CAS号生成用于知识图谱构建的专业化学品数据查询Prompt。
        包含完整的化学品信息，用于构建化学品知识图谱。
        特别针对"CAS号或流水号"字段进行校验和处理。
        
        Args:
            chemical_name: 化学品名称
            cas_number: CAS号或流水号
            api_config: API配置对象，用于确定是否使用网络搜索优化prompt
        """
        logger = logging.getLogger(StringUtils.__name__)
        logger.debug(f"开始为化学品 '{chemical_name}' (CAS: {cas_number}) 生成 prompt")

        try:
            # 使用新的验证函数分析CAS号或流水号
            validation_result = StringUtils.validate_cas_or_serial_number(cas_number)
            
            cas_status = f"📋 编号类型: {validation_result['type']}"
            if validation_result['is_valid']:
                cas_status += f" ✅ 格式有效: {validation_result['formatted']}"
            else:
                cas_status += f" ❌ 格式无效: {validation_result.get('formatted', '空值')}"
            
            cas_type = validation_result['type']
            validation_note = validation_result['suggestion']
            query_requirement = "🔍 需要查询补充" if validation_result['needs_query'] else "✅ 仅需验证准确性"

            # 检查API是否支持网络搜索（联网功能）
            supports_web_search = False
            web_search_info = ""
            
            if api_config:
                # 检查是否支持grounding（Gemini）或enable_search（通义等）
                supports_grounding = getattr(api_config, 'supports_grounding', False)
                enable_search = getattr(api_config, 'enable_search', False)
                
                if supports_grounding or enable_search:
                    supports_web_search = True
                    web_search_info = f"""
## 🌐 网络搜索增强模式已启用
- **当前API**: {getattr(api_config, 'name', '未知')}
- **搜索能力**: {'支持Grounding搜索' if supports_grounding else '支持联网搜索'}
- **数据源优势**: 可实时获取最新的化学品数据和权威数据库信息
- **查询策略**: 优先使用网络搜索获取最准确的CAS号和化学品属性
"""

            # 根据是否支持网络搜索生成不同的数据源要求和查询策略
            if supports_web_search:
                data_source_section = f"""{web_search_info}

## 📊 数据源要求
⚡ **实时网络搜索策略**：利用联网功能获取最新、最准确的化学品数据
1. 🌐 **PubChem** (美国国家生物技术信息中心) - 最高优先级，CAS号权威来源
2. 🏛️ **ECHA** (欧洲化学品管理局) - 权威监管数据，实时更新  
3. 🔬 **GESTIS国际化学品数据库** - 国际标准数据，联网查询
4. 🏭 **供应商SDS**: Sigma-Aldrich、Fisher Scientific、Merck官方最新数据
5. 🏛️ **政府机构**: NIOSH、OSHA、EPA官方数据库，联网访问
6. 📚 **中国化学品名录2013年版** - 参考本地流水号体系
7. 🌍 **国际化学品数据库联盟** - 利用网络搜索获取全球最新数据

🎯 **网络搜索查询重点**：
- 使用联网功能验证和补充CAS号
- 实时获取最新的安全数据和法规信息
- 查询最新的物理化学性质数据
- 获取最新的用途和应用信息"""
            else:
                data_source_section = """
## 📊 数据源要求 (标准模式 - 严格按优先级)
1. 🌐 **PubChem** (美国国家生物技术信息中心) - 最高优先级，CAS号权威来源
2. 🏛️ **ECHA** (欧洲化学品管理局) - 权威监管数据  
3. 🔬 **GESTIS国际化学品数据库** - 国际标准数据
4. 🏭 **供应商SDS**: Sigma-Aldrich、Fisher Scientific、Merck官方数据
5. 🏛️ **政府机构**: NIOSH、OSHA、EPA官方数据
6. 📚 **中国化学品名录2013年版** - 参考本地流水号体系"""

            prompt: str = f"""


## 🎯 查询目标
- **化学品名称**: {chemical_name}
- **编号状态**: {cas_status}
- **编号类型**: {cas_type}
- **处理要求**: {query_requirement}
- **处理说明**: {validation_note}

## 🔍 CAS号与流水号智能识别指引
### 📋 "CAS号或流水号"字段说明：
- **CAS号**：国际通用化学物质唯一标识编号（格式：XXXX-XX-X，如64-17-5代表乙醇）
- **流水号**：名录编制单位自定义编号，用于无CAS号的新化学品、复合物、特殊材料
- **优先级**：优先使用国际标准CAS号，无CAS号时用本地流水号保证唯一性

### 🎯 核心任务（根据上述编号分析结果）
1. **CAS号验证与补充**：如当前编号为空、格式错误或为流水号，必须查询补充准确的CAS号
2. **编号唯一性检查**：确保每个化学品都有唯一标识符
3. **格式标准化**：CAS号格式必须为"数字-数字-数字"标准格式
4. **数据关联性验证**：确认编号与化学品名称的准确对应关系
5. **源数据兼容性**：兼容《中国化学品名录2013年版》的"CAS号或流水号"字段结构

{data_source_section}

## 📋 知识图谱属性要求 (用于构建化学品知识图谱)
请为上述化学品提供以下详细信息，用于构建完整的化学品知识图谱。所有数据必须以**简体中文**表述，并确保内容的详尽和准确。

### 🔬 基础标识信
- **名称**: 化学品的标准中文名称
- **CAS号或流水号**: 
  - 如为标准CAS号，保持原格式并验证准确性
  - 如为流水号，查询是否存在对应CAS号并优先使用CAS号
  - 如为空值，从权威数据库查询补充标准CAS号
  - 格式要求：CAS号严格为"XXXX-XX-X"，流水号为纯数字
- **别名**: 包含所有常用别名，例如英文名、商品名、俗称、学名等。格式："别名1; 别名2; 别名3; 别名4; 别名5"，至少提供3-5个有价值的别名，用分号分隔
- **分子式**: 准确的化学分子式，例如 "C2H6O", "H2SO4"
- **分子量**: 准确的分子量数值，单位为 g/mol，保留至少两位小数，例如 "46.07"

### ⚠️ 危害与安全信息
- **是否为危化品**: 基于《危险化学品目录》，必须明确回答"是"或"否"
- **浓度阈值**: 参考原名录"浓度阈值"字段，详细说明毒理学数据，例如 "LC50(大鼠吸入): 20000 ppm/10h; LD50(大鼠经口): 7060 mg/kg; LD50(兔子皮肤): >5000 mg/kg"
- **危害**: 详细描述对人体和环境的具体危害，需分类说明：
  - **健康危害**: 急性毒性、皮肤腐蚀/刺激、严重眼损伤/眼刺激、致癌性、生殖毒性等
  - **环境危害**: 对水生生物的危害、持久性、生物累积性等
  - **物理危害**: 易燃性、爆炸性、氧化性等
- **防范**: 具体的防护措施和注意事项，需分类说明：
  - **工程控制**: 通风系统、密闭操作等
  - **个体防护**: 呼吸系统防护、眼睛防护、身体防护、手部防护
  - **操作处置与储存**: 操作注意事项、储存条件
- **危害处置**: 发生事故时的具体应急处置方法和急救措施，需分类说明：
  - **泄漏应急处理**: 环境、人员、处理方法
  - **火灾处置**: 灭火方法、有害燃烧产物
  - **急救措施**: 皮肤接触、眼睛接触、吸入、食入后的急救方法

### 🏭 产业链信息 (知识图谱核心)
- **用途**: 详细说明主要用途和应用领域，至少列举5个具体用途，并描述其在应用中扮演的角色
- **自然来源**: 详细说明该化学品在自然界中的存在形式、分布情况、天然来源（如植物、矿物、微生物等），以及天然提取方法。如果是纯人工合成的化学品，则说明"无天然来源，纯人工合成"
- **生产来源 (上游)**: 详细列出其直接上游原料化学品，以及主要的生产商或供应商信息。这是构建产业链上游关系的关键
- **工业生产原料 (下游)**: 详细列出该化学品作为原料可以用于生产哪些下游产品或化学品。这是构建产业链下游关系的关键

### ⚗️ 物理化学性质
- **性质**: 提供一个综合性的、结构化的物理化学性质描述，至少包括：
  - **外观与性状**: 详细描述常温常压下的颜色、状态、气味等感官特征
  - **熔点**: 数值+单位(°C)，例如 "-114.1°C"
  - **沸点**: 数值+单位(°C)，例如 "78.3°C"
  - **密度**: 数值+单位，并注明温度，例如 "0.789 g/cm³(20°C)"
  - **溶解性**: 在水、乙醇等常见溶剂中的溶解情况，可包含定量数据
  - **闪点**: 数值+单位(°C)，并注明开杯/闭杯，例如 "13°C (闭杯)"
  - **稳定性**: 描述其化学稳定性、需要避免的条件（如光、热）和禁配物质

## 📤 输出格式
**必须严格以JSON对象格式返回，确保所有字段完整填写，不要包含任何额外的解释或Markdown标记。**
**重要：除了 `data_source` 字段，其他字段的数据后不需要标明数据来源。**

```json
{{
    "data_source": "网络搜索/模型知识 {{数据来源}}",
    "名称": "化学品标准中文名称",
    "CAS号或流水号": "优先CAS号格式XXXX-XX-X，无CAS号时为流水号",
    "编号类型说明": "标准CAS号/本地流水号/新分配CAS号",
    "别名": "别名1; 别名2; 别名3",
    "分子式": "化学分子式",
    "分子量": "数值 g/mol",
    "是否为危化品": "是/否",
    "浓度阈值": "毒理学数据详情",
    "危害": "危害描述",
    "防范": "防护措施详情",
    "危害处置": "应急处置详情",
    "用途": "用途详情",
    "自然来源": "天然来源详情",
    "生产来源": "上游原料详情",
    "工业生产原料": "下游产品详情",
    "性质": "物理化学性质详情"
}}
```

## ⚡ 特别要求 (知识图谱专用)
- 🔍 **CAS号/流水号智能处理**：
  - 如输入为流水号，必须查询是否存在对应的标准CAS号
  - 如输入为空值或格式错误，必须从权威数据库查询补充标准CAS号
  - 优先使用国际CAS号，确保全球通用性和数据关联性
  - 保证每个化学品都有唯一且准确的标识符
- 📊 **数据来源权威性**: 优先使用PubChem、ECHA等权威源，确保数据质量
- 🎯 **内容详细具体**: 每个字段都要详细填写，避免使用模糊或笼统的描述，为知识图谱提供高质量的属性信息
- 🔗 **关联性描述**: 特别注意产业链（生产来源、工业生产原料）和危害信息的准确性和完整性，这是构建关系图谱的核心
- 🇨🇳 **简体中文**: 所有描述必须使用简体中文
- 📋 **格式严格统一**: 严格遵守JSON输出格式，便于知识图谱的自动化结构化处理

## 💡 字段填写指导
- **CAS号或流水号字段**: 
  - 标准CAS号示例：64-17-5（乙醇）、7732-18-5（水）
  - 流水号示例：202401001、300015678（纯数字格式）
  - 格式验证：确保CAS号符合"数字-数字-数字"标准
- **别名字段**: 包含学名、俗名、商品名、英文名等，用分号分隔，以提供丰富的检索入口
- **浓度阈值**: 尽量提供多种物种（大鼠、兔子等）和多种途径（经口、吸入、皮肤）的毒理学数据，**参考原名录"浓度阈值"字段进行详细说明**。
- **自然来源**: 详细描述天然存在情况，包括在哪些植物、动物、矿物或微生物中发现，以及天然提取工艺
- **产业链信息**: 要清晰地体现化学品在整个产业链中的上游原料和下游产品关系
- **性质描述**: 尽量提供定量数据，并注明测试条件（如温度、压力）
- **数据来源标注**: **仅在顶层 `data_source` 字段中说明本次查询的主要信息来源，例如 "网络搜索 {{PubChem; ECHA}}" 或 "模型知识 {{模型知识}}"。其他字段无需标注来源。**

现在开始查询并生成用于知识图谱的化学品详细数据："""

            logger.debug(f"成功为化学品 '{chemical_name}' 生成 prompt")
            return prompt

        except Exception as e:
            logger.error(f"为化学品 '{chemical_name}' (CAS: {cas_number}) 生成 prompt 时发生异常: {e}", exc_info=True)
            return None

    @staticmethod
    def extract_json_data(api_response_text: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        从API响应中稳健地提取和解析JSON数据 - 增强版
        同时检测联网查找状态
        """
        if not api_response_text or not api_response_text.strip():
            return None, "API返回空响应"

        # 预处理响应文本，移除常见的markdown标记
        cleaned_text = api_response_text.strip()
        
        # 移除代码块标记
        if cleaned_text.startswith('```json'):
            cleaned_text = cleaned_text[7:]
        elif cleaned_text.startswith('```'):
            cleaned_text = cleaned_text[3:]
        
        if cleaned_text.endswith('```'):
            cleaned_text = cleaned_text[:-3]
            
        cleaned_text = cleaned_text.strip()

        try:
            # 尝试直接解析
            parsed_data = json.loads(cleaned_text)
            return parsed_data, "成功解析JSON"
        except json.JSONDecodeError:
            # 如果直接解析失败，尝试使用正则表达式提取
            json_match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                try:
                    parsed_data = json.loads(json_str)
                    return parsed_data, "成功通过正则提取并解析JSON"
                except json.JSONDecodeError as e:
                    return None, f"提取的JSON内容无效: {e}"
            else:
                return None, "未找到有效的JSON内容"
