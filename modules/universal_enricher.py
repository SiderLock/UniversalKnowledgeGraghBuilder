import os
import json
import pandas as pd
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import deque

# 导入稳定的JSON解析器
from .llm_json_parser import parse_llm_json

logger = logging.getLogger(__name__)

class RateLimiter:
    """速率限制器 - 支持 RPM, TPM, TPD"""
    def __init__(self, requests_per_minute: int = 60, tokens_per_minute: int = 100000, tokens_per_day: int = 1000000):
        self.rpm = requests_per_minute  # Requests Per Minute
        self.tpm = tokens_per_minute    # Tokens Per Minute
        self.tpd = tokens_per_day       # Tokens Per Day
        
        # 跟踪请求时间戳
        self.request_times = deque(maxlen=requests_per_minute * 2)
        self.minute_tokens = deque(maxlen=1000)
        self.daily_tokens = 0
        self.day_start = datetime.now()
        
        self.lock = threading.Lock()
        
    def wait_if_needed(self, estimated_tokens: int = 1000):
        """等待以遵守速率限制"""
        with self.lock:
            now = datetime.now()
            
            # 检查是否需要重置每日计数
            if (now - self.day_start) > timedelta(days=1):
                self.daily_tokens = 0
                self.day_start = now
            
            # 检查每日Token限制
            if self.daily_tokens + estimated_tokens > self.tpd:
                wait_seconds = (self.day_start + timedelta(days=1) - now).total_seconds()
                if wait_seconds > 0:
                    logger.warning(f"达到每日Token限制({self.tpd})，等待 {wait_seconds:.0f} 秒")
                    time.sleep(min(wait_seconds, 60))  # 最多等待60秒
                    return
            
            # 清理1分钟前的请求记录
            one_minute_ago = now - timedelta(minutes=1)
            while self.request_times and self.request_times[0] < one_minute_ago:
                self.request_times.popleft()
            
            # 清理1分钟前的Token记录
            while self.minute_tokens and self.minute_tokens[0][0] < one_minute_ago:
                self.minute_tokens.popleft()
            
            # 检查RPM限制
            if len(self.request_times) >= self.rpm:
                oldest_request = self.request_times[0]
                wait_time = 60 - (now - oldest_request).total_seconds()
                if wait_time > 0:
                    logger.debug(f"达到RPM限制({self.rpm})，等待 {wait_time:.2f} 秒")
                    time.sleep(wait_time + 0.1)
                    now = datetime.now()
            
            # 检查TPM限制
            current_minute_tokens = sum(t[1] for t in self.minute_tokens)
            if current_minute_tokens + estimated_tokens > self.tpm:
                if self.minute_tokens:
                    oldest_token_time = self.minute_tokens[0][0]
                    wait_time = 60 - (now - oldest_token_time).total_seconds()
                    if wait_time > 0:
                        logger.debug(f"达到TPM限制({self.tpm})，等待 {wait_time:.2f} 秒")
                        time.sleep(wait_time + 0.1)
                        now = datetime.now()
            
            # 记录本次请求
            self.request_times.append(now)
            self.minute_tokens.append((now, estimated_tokens))
            self.daily_tokens += estimated_tokens

class UniversalEnricher:
    def __init__(self, api_key: str, base_url: str = None, model: str = "qwen-plus", provider: str = "dashscope", 
                 options: Dict[str, Any] = None, 
                 rpm: int = 60, tpm: int = 100000, tpd: int = 1000000):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.provider = provider
        self.options = options or {}
        
        # 初始化速率限制器
        self.rate_limiter = RateLimiter(requests_per_minute=rpm, tokens_per_minute=tpm, tokens_per_day=tpd)
        logger.info(f"速率限制: RPM={rpm}, TPM={tpm}, TPD={tpd}")
        
        self._setup_client()

    def _setup_client(self):
        if self.provider == "dashscope":
            try:
                import dashscope
                dashscope.api_key = self.api_key
                if self.base_url:
                    dashscope.base_url = self.base_url
            except ImportError:
                raise ImportError("dashscope package not installed")
        elif self.provider == "openai":
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
            except ImportError:
                raise ImportError("openai package not installed")
        elif self.provider == "ollama":
            try:
                from openai import OpenAI
                import httpx
                
                # Ollama使用OpenAI兼容API，默认端口11434
                base_url = self.base_url or "http://localhost:11434/v1"
                
                # Ensure base_url ends with /v1 for OpenAI compatibility
                if base_url and not base_url.endswith("/v1"):
                     base_url = base_url.rstrip("/") + "/v1"
                
                # Use provided API key or default to 'ollama'
                api_key = self.api_key if self.api_key else "ollama"
                
                # Configure timeout for Ollama (local models can be slow)
                timeout_val = self.options.get('timeout', 120)
                http_client = httpx.Client(timeout=httpx.Timeout(timeout_val, connect=30.0))
                
                # Force GPU usage for Ollama
                self.options.setdefault('num_gpu', 1)  # Use 1 GPU
                self.options.setdefault('num_ctx', 4096)  # Reduce context size for better GPU fit
                
                self.client = OpenAI(
                    api_key=api_key, 
                    base_url=base_url,
                    http_client=http_client
                )
                logger.info(f"Ollama client initialized: base_url={base_url}, GPU enabled")
            except ImportError:
                raise ImportError("openai package not installed (required for Ollama)")
        elif self.provider == "deepseek":
            try:
                from openai import OpenAI
                # DeepSeek使用OpenAI兼容API
                base_url = self.base_url or "https://api.deepseek.com/v1"
                self.client = OpenAI(api_key=self.api_key, base_url=base_url)
                logger.info(f"DeepSeek client initialized: base_url={base_url}")
            except ImportError:
                raise ImportError("openai package not installed (required for DeepSeek)")
        elif self.provider == "kimi":
            try:
                from openai import OpenAI
                # Kimi (Moonshot AI) 使用OpenAI兼容API
                base_url = self.base_url or "https://api.moonshot.cn/v1"
                self.client = OpenAI(api_key=self.api_key, base_url=base_url)
                # 设置默认选项支持联网检索
                self.options.setdefault('enable_search', True)
                logger.info(f"Kimi client initialized: base_url={base_url}, search enabled")
            except ImportError:
                raise ImportError("openai package not installed (required for Kimi)")

    def generate_prompts_for_domain(self, domain_name: str, description: str, source_instruction: str = "") -> Dict[str, Any]:
        """
        使用 LLM 自动生成特定领域的 Schema 和 Prompt
        增强版：更强的JSON格式约束和输出验证指令
        """
        source_req = f"\n数据来源要求：{source_instruction}" if source_instruction else ""
        
        meta_prompt = f"""你是一个专家级的数据架构师和提示词工程师。
用户希望构建一个关于"{domain_name}"的知识库。
描述：{description}{source_req}

请你完成以下任务：
1. 定义该领域的核心实体类型（Entity Type），使用英文命名。
2. 列出该实体最重要的5-8个属性（Attributes），属性名使用中英文混合格式（如："名称 (Name)"），并给出简短描述。
3. 编写一个 System Prompt，设定LLM的角色，要求其：
   - 严格按照JSON格式输出
   - 如果某属性信息不确定，使用"未知"而非空值
   - 确保数据的准确性和一致性
4. 编写一个 User Prompt Template，包含明确的JSON输出格式示例。

【关键要求】
- System Prompt 必须强调JSON格式输出
- User Prompt Template 必须包含完整的JSON格式示例
- 模板中必须包含 {{entity_name}} 和 {{attributes}} 占位符

请严格以JSON格式返回，不要添加任何额外文字说明：
{{{{
    "schema": {{{{
        "entity_type": "英文实体类型名",
        "attributes": [
            {{{{"name": "属性名 (English)", "description": "属性说明"}}}},
            {{{{"name": "属性名2 (English2)", "description": "属性说明2"}}}}
        ]
    }}}},
    "prompts": {{{{
        "system": "你是...专家。请严格按照JSON格式输出，确保所有字段都有值。",
        "user_template": "请提供关于\\"{{{{entity_name}}}}\\"的详细信息...返回JSON格式..."
    }}}}
}}}}"""
        
        try:
            response_text = self._call_llm(meta_prompt, json_mode=True)
            # 使用增强的JSON解析器
            from modules.llm_json_parser import parse_llm_json
            result = parse_llm_json(response_text, default_value=None)
            
            if result and isinstance(result, dict) and 'schema' in result:
                # 验证并补充必要字段
                if 'prompts' not in result:
                    result['prompts'] = {{}}
                if 'system' not in result['prompts']:
                    result['prompts']['system'] = f"你是{domain_name}领域的专家，请严格按照JSON格式提供准确的信息。"
                if 'user_template' not in result['prompts']:
                    result['prompts']['user_template'] = f"请提供关于\"{{entity_name}}\"的详细信息，包含以下属性：{{attributes}}。请以JSON格式返回。"
                return result
            else:
                raise ValueError("解析结果格式不正确")
        except Exception as e:
            logger.error(f"Failed to generate prompts: {{e}}")
            # Return a fallback
            return {{
                "schema": {{"entity_type": domain_name, "attributes": []}},
                "prompts": {{"system": "你是一个知识图谱专家，请严格按照JSON格式输出。", "user_template": f"请提供关于\"{{entity_name}}\"的详细信息，以JSON格式返回。"}}
            }}


    def process_batch(self, df: pd.DataFrame, name_col: str, domain_config: Dict[str, Any], 
                       max_workers: int = 3, progress_callback: Callable[[int], None] = None,
                       status_callback: Callable[[str], None] = None) -> pd.DataFrame:
        """
        并发处理一批数据 - 自适应版本
        根据 provider 选择最佳解析策略：Ollama 使用传统解析，其他使用增强解析
        
        Args:
            df: 输入数据框
            name_col: 实体名称列
            domain_config: 领域配置
            max_workers: 最大并发数
            progress_callback: 进度回调函数 (completed_count)
            status_callback: 状态回调函数 (status_message)
        """
        schema = domain_config.get('schema', {})
        prompts = domain_config.get('prompts', {})
        
        attributes = [attr['name'] for attr in schema.get('attributes', [])]
        
        # Ensure columns exist
        for attr in attributes:
            if attr not in df.columns:
                df[attr] = None

        system_prompt = prompts.get('system', '')
        user_template_base = prompts.get('user_template', '')
        source_instruction = domain_config.get('source_instruction', '')
        
        # 根据 provider 选择策略
        use_simple_strategy = self.provider in ["openai", "deepseek", "kimi", "dashscope"]
        
        # 统计信息
        total_count = len(df)
        success_count = 0
        error_count = 0
        
        if status_callback:
            strategy_name = "增强模式" if use_simple_strategy else "兼容模式(Ollama)"
            status_callback(f"开始处理 {total_count} 条记录 [{strategy_name}]...")

        def process_single_entity(idx, row):
            nonlocal success_count, error_count
            entity_name = row.get(name_col)
            if pd.isna(entity_name) or str(entity_name).strip() == '':
                return idx, None, "skipped"
            
            entity_name = str(entity_name).strip()
            
            # 根据 provider 选择不同的提示词和解析策略
            if use_simple_strategy:
                # 商业API使用简洁提示词
                prompt = self._build_simple_prompt(entity_name, attributes, user_template_base, source_instruction)
                sys_prompt = self._build_simple_system_prompt(system_prompt)
            else:
                # Ollama使用更详细的提示词
                prompt = self._build_ollama_prompt(entity_name, attributes, user_template_base, source_instruction)
                sys_prompt = self._build_ollama_system_prompt(system_prompt, attributes)
            
            max_retries = 3
            last_error = None
            last_response = None
            
            for attempt in range(max_retries):
                try:
                    # 应用速率限制
                    self.rate_limiter.wait_if_needed(estimated_tokens=2000)
                    
                    # Call LLM
                    response_text = self._call_llm(prompt, system_prompt=sys_prompt, json_mode=False)
                    last_response = response_text
                    
                    if not response_text or not response_text.strip():
                        last_error = "LLM返回空响应"
                        logger.warning(f"{entity_name}: 空响应 (尝试 {attempt+1})")
                        continue
                    
                    # 解析响应
                    if use_simple_strategy:
                        data = self._simple_parse_json(response_text, attributes)
                    else:
                        # Ollama 使用更宽松的解析
                        data = self._ollama_parse_json(response_text, attributes)
                    
                    if data:
                        valid_count = sum(1 for k, v in data.items() if k in attributes and v and str(v).strip())
                        logger.info(f"✓ {entity_name} (字段: {valid_count}/{len(attributes)})")
                        return idx, data, "success"
                    else:
                        last_error = "解析失败"
                        logger.warning(f"{entity_name}: 解析失败 (尝试 {attempt+1})")
                        
                except Exception as e:
                    last_error = str(e)
                    logger.warning(f"{entity_name}: 异常 (尝试 {attempt+1}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1 * (attempt + 1))
            
            logger.error(f"✗ {entity_name} - {last_error}")
            return idx, None, f"error: {last_error}"

        # Use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_entity, idx, row): idx for idx, row in df.iterrows()}
            
            completed = 0
            for future in as_completed(futures):
                idx, data, status = future.result()
                if data:
                    for key, value in data.items():
                        if key in df.columns and value:
                            df.at[idx, key] = str(value)
                    success_count += 1
                elif status.startswith("error"):
                    error_count += 1
                
                completed += 1
                if progress_callback:
                    progress_callback(completed)
                if status_callback and completed % 3 == 0:
                    status_callback(f"已处理 {completed}/{total_count} | 成功: {success_count} | 失败: {error_count}")
        
        if status_callback:
            status_callback(f"处理完成: 成功 {success_count}/{total_count}，失败 {error_count}")
        
        logger.info(f"批处理完成: 成功 {success_count}/{total_count}, 失败 {error_count}")
        return df
    
    def _build_ollama_system_prompt(self, base_prompt: str, attributes: List[str]) -> str:
        """为 Ollama 构建详细的 System Prompt"""
        attr_list = ", ".join([f'"{a}"' for a in attributes])
        return f"""{base_prompt}

你是数据提取助手。请按以下规则输出：
1. 只输出JSON对象，不要其他文字
2. 必须包含这些字段: {attr_list}
3. 不确定的值填"未知"
4. 不要用```包裹"""

    def _build_ollama_prompt(self, entity_name: str, attributes: List[str], 
                              user_template: str, source_instruction: str) -> str:
        """为 Ollama 构建用户提示词"""
        attr_json = ",\n  ".join([f'"{a}": "值"' for a in attributes])
        return f"""查询实体: {entity_name}

{source_instruction}

请返回JSON格式:
{{
  {attr_json}
}}"""

    def _ollama_parse_json(self, text: str, expected_keys: List[str]) -> Optional[Dict]:
        """
        Ollama 专用的宽松 JSON 解析器
        """
        import json
        import re
        
        if not text:
            return None
        
        text = text.strip()
        
        # 尝试多种解析方式
        # 1. 直接解析
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return self._normalize_data(data, expected_keys)
        except:
            pass
        
        # 2. 移除markdown和常见前缀
        cleaned = text
        cleaned = re.sub(r'```json\s*', '', cleaned)
        cleaned = re.sub(r'```\s*', '', cleaned)
        cleaned = re.sub(r'^[^{]*', '', cleaned)  # 移除 { 之前的内容
        cleaned = re.sub(r'[^}]*$', '', cleaned)  # 移除 } 之后的内容
        
        try:
            data = json.loads(cleaned)
            if isinstance(data, dict):
                return self._normalize_data(data, expected_keys)
        except:
            pass
        
        # 3. 提取第一个 JSON 对象
        match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        if match:
            try:
                # 修复常见问题
                json_str = match.group()
                json_str = json_str.replace('"', '"').replace('"', '"')
                json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
                data = json.loads(json_str)
                if isinstance(data, dict):
                    return self._normalize_data(data, expected_keys)
            except:
                pass
        
        # 4. 从文本提取 key-value
        result = {}
        for key in expected_keys:
            patterns = [
                rf'"{re.escape(key)}"\s*:\s*"([^"]*)"',
                rf"'{re.escape(key)}'\s*:\s*'([^']*)'",
                rf'{re.escape(key)}\s*[：:]\s*([^\n,}}]+)',
            ]
            for pattern in patterns:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    val = m.group(1).strip().strip('"\'')
                    if val and val.lower() not in ['null', 'none', '']:
                        result[key] = val
                        break
        
        return result if result else None

    def _build_simple_system_prompt(self, base_prompt: str) -> str:
        """构建简洁的系统提示词"""
        return f"""{base_prompt}

你是一个数据提取专家。你必须直接输出JSON对象，不要输出任何其他内容。
输出规则：
1. 只输出JSON，不要markdown代码块
2. 不要输出解释、说明或注释
3. 所有字段必须有值，不确定的填"未知"
4. 使用双引号，不要单引号"""

    def _build_simple_prompt(self, entity_name: str, attributes: List[str], 
                             user_template: str, source_instruction: str) -> str:
        """构建简洁有效的用户提示词"""
        # 构建JSON示例
        example_parts = []
        for attr in attributes[:3]:  # 只展示前3个属性作为示例
            example_parts.append(f'  "{attr}": "具体值"')
        example_json = "{\n" + ",\n".join(example_parts) + ",\n  ...\n}"
        
        # 属性列表
        attr_list = ", ".join([f'"{a}"' for a in attributes])
        
        prompt = f"""请为实体"{entity_name}"提供以下属性的信息。

需要填写的属性：{attr_list}

{source_instruction}

直接输出JSON格式：
{example_json}

注意：直接输出JSON，不要```包裹，不要其他文字。"""
        
        return prompt

    def _simple_parse_json(self, text: str, expected_keys: List[str]) -> Optional[Dict]:
        """
        简化的JSON解析器 - 5层解析策略
        """
        import json
        import re
        
        if not text:
            logger.warning("_simple_parse_json: 输入文本为空")
            return None
        
        text = text.strip()
        
        # 策略1: 直接解析
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                logger.debug("✓ 策略1成功: 直接JSON解析")
                return self._normalize_data(data, expected_keys)
        except Exception as e:
            logger.debug(f"策略1失败: {e}")
        
        # 策略2: 移除markdown代码块后解析
        try:
            cleaned = re.sub(r'```json\s*', '', text)
            cleaned = re.sub(r'```\s*', '', cleaned)
            cleaned = cleaned.strip()
            data = json.loads(cleaned)
            if isinstance(data, dict):
                logger.debug("✓ 策略2成功: 移除markdown后解析")
                return self._normalize_data(data, expected_keys)
        except Exception as e:
            logger.debug(f"策略2失败: {e}")
        
        # 策略3: 提取第一个 {...} 块
        try:
            match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
            if match:
                data = json.loads(match.group())
                if isinstance(data, dict):
                    logger.debug("✓ 策略3成功: 提取{}块")
                    return self._normalize_data(data, expected_keys)
        except Exception as e:
            logger.debug(f"策略3失败: {e}")
        
        # 策略4: 修复常见JSON问题后解析
        try:
            fixed = self._fix_json_issues(text)
            data = json.loads(fixed)
            if isinstance(data, dict):
                logger.debug("✓ 策略4成功: 修复JSON问题")
                return self._normalize_data(data, expected_keys)
        except Exception as e:
            logger.debug(f"策略4失败: {e}")
        
        # 策略5: 从文本中提取key-value对
        try:
            extracted = self._extract_key_values(text, expected_keys)
            if extracted:
                logger.debug(f"✓ 策略5成功: 提取键值对 ({len(extracted)}个字段)")
                return extracted
        except Exception as e:
            logger.debug(f"策略5失败: {e}")
        
        logger.warning(f"所有5个策略均失败，文本片段: {text[:200]}...")
        return None

    def _fix_json_issues(self, text: str) -> str:
        """修复常见的JSON格式问题"""
        import re
        
        # 移除markdown
        text = re.sub(r'```json\s*\n?', '', text)
        text = re.sub(r'\n?```', '', text)
        
        # 移除前导文字
        if '{' in text:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                text = text[start:end+1]
        
        # 修复中文引号
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        
        # 移除尾随逗号
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        
        # 修复没有引号的key
        text = re.sub(r'([{,]\s*)([a-zA-Z_\u4e00-\u9fa5][a-zA-Z0-9_\u4e00-\u9fa5]*)\s*:', r'\1"\2":', text)
        
        return text.strip()

    def _extract_key_values(self, text: str, expected_keys: List[str]) -> Optional[Dict]:
        """从文本中提取key-value对"""
        import re
        result = {}
        
        for key in expected_keys:
            # 尝试多种模式匹配
            patterns = [
                rf'"{re.escape(key)}"\s*:\s*"([^"]*)"',
                rf"'{re.escape(key)}'\s*:\s*'([^']*)'",
                rf'"{re.escape(key)}"\s*:\s*([^,}}\n]+)',
                rf'{re.escape(key)}\s*[：:]\s*([^\n,}}]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    value = match.group(1).strip().strip('"\'')
                    if value and value.lower() not in ['null', 'none', '', 'n/a']:
                        result[key] = value
                        break
        
        return result if result else None

    def _normalize_data(self, data: Dict, expected_keys: List[str]) -> Dict:
        """规范化数据，进行模糊匹配"""
        result = {}
        used_keys = set()
        
        # 首先精确匹配
        for key in expected_keys:
            if key in data:
                val = data[key]
                if val is not None and str(val).strip():
                    result[key] = str(val).strip()
                    used_keys.add(key)
        
        # 然后尝试模糊匹配未匹配的key
        for key in expected_keys:
            if key in result:
                continue
            
            # 查找相似的key
            for data_key in data.keys():
                if data_key in used_keys:
                    continue
                
                # 检查是否包含相同的关键词
                key_lower = key.lower()
                data_key_lower = data_key.lower()
                
                if (key_lower in data_key_lower or 
                    data_key_lower in key_lower or
                    self._similar(key_lower, data_key_lower) > 0.6):
                    val = data[data_key]
                    if val is not None and str(val).strip():
                        result[key] = str(val).strip()
                        used_keys.add(data_key)
                        break
        
        return result

    def _similar(self, a: str, b: str) -> float:
        """计算两个字符串的相似度"""
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a, b).ratio()

    def _call_llm(self, prompt: str, system_prompt: str = None, json_mode: bool = False, **kwargs) -> str:
        """
        统一的 LLM 调用接口
        """
        if self.provider == "dashscope":
            import dashscope
            from dashscope import Generation
            
            messages = []
            if system_prompt:
                messages.append({'role': 'system', 'content': system_prompt})
            messages.append({'role': 'user', 'content': prompt})
            
            # Dashscope specific parameters could be mapped here if needed
            response = Generation.call(
                model=self.model,
                messages=messages,
                result_format='message'
            )
            
            if response.status_code == 200:
                return response.output.choices[0].message.content
            else:
                raise Exception(f"DashScope API Error: {response.message}")
                
        elif self.provider in ["openai", "ollama", "deepseek", "kimi"]:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            if not self.model:
                raise ValueError("Model name is required. Please select a model in Settings.")

            api_kwargs = {
                "model": self.model,
                "messages": messages
            }
            
            # Combine instance options with call-specific kwargs
            # kwargs takes precedence
            combined_args = self.options.copy()
            combined_args.update(kwargs)

            # Handle provider-specific parameter filtering
            if self.provider == "ollama":
                # Ollama specific parameters via extra_body
                extra_body = {}
                options = {}
                
                # Top-level parameters for Ollama
                top_level_params = ["keep_alive", "format"]
                for param in top_level_params:
                    if param in combined_args:
                        extra_body[param] = combined_args.pop(param)
                
                # Ollama-specific options parameters
                ollama_options_keys = ["num_ctx", "seed", "top_k", "repeat_penalty", "num_predict", "mirostat", "mirostat_eta", "mirostat_tau", "num_gqa", "num_gpu", "num_thread", "repeat_last_n", "tfs_z"]
                
                for key in ollama_options_keys:
                    if key in combined_args:
                        options[key] = combined_args.pop(key)
                
                # Force GPU usage for compatible models
                if "num_gpu" not in options:
                    options["num_gpu"] = 1
                    
                # Optimize context size for GPU memory
                if "num_ctx" not in options:
                    options["num_ctx"] = min(combined_args.get("num_ctx", 4096), 4096)
                
                if options:
                    extra_body["options"] = options
                
                if extra_body:
                    api_kwargs["extra_body"] = extra_body
            elif self.provider in ["openai", "deepseek", "kimi"]:
                # Filter out Ollama-specific parameters for OpenAI/DeepSeek/Kimi
                ollama_specific_params = ["num_ctx", "keep_alive", "num_gpu", "num_thread", "repeat_penalty", "num_predict", "mirostat", "mirostat_eta", "mirostat_tau", "num_gqa", "repeat_last_n", "tfs_z", "seed", "top_k", "format"]
                for param in ollama_specific_params:
                    combined_args.pop(param, None)
                
                # Kimi特有：添加联网检索支持
                # 参考文档: https://platform.moonshot.cn/docs/guide/use-web-search
                if self.provider == "kimi" and combined_args.get('enable_search', False):
                    # Kimi使用builtin_function类型，function.name为$web_search
                    api_kwargs["tools"] = [
                        {
                            "type": "builtin_function",
                            "function": {
                                "name": "$web_search"
                            }
                        }
                    ]
                    combined_args.pop('enable_search', None)
                    logger.debug("Kimi联网检索已启用 ($web_search)")

            # Merge remaining standard args (temperature, top_p, etc.)
            api_kwargs.update(combined_args)

            # Enable JSON mode for different providers
            if json_mode:
                if self.provider == "ollama":
                    # Ollama may not fully support response_format, use prompt guidance instead
                    if messages:
                        messages[-1]["content"] += "\n\n请务必以有效的JSON格式返回结果。"
                elif self.provider in ["openai", "deepseek", "kimi"]:
                    # OpenAI, DeepSeek and Kimi support response_format
                    api_kwargs["response_format"] = {"type": "json_object"}
            
            try:
                response = self.client.chat.completions.create(**api_kwargs)
                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"LLM call failed: {e}")
                raise
            
        return ""

    def get_models(self) -> List[str]:
        """获取可用模型列表"""
        try:
            if self.provider == "dashscope":
                return ["qwen-plus", "qwen-max", "qwen-turbo", "qwen-long", "qwen2.5-72b-instruct"]
            elif self.provider == "ollama":
                # 直接使用 Ollama 原生 API 获取模型列表
                import requests
                base_url = self.base_url or "http://localhost:11434/v1"
                # 转换为 Ollama 原生 API endpoint
                ollama_base = base_url.replace("/v1", "")
                response = requests.get(f"{ollama_base}/api/tags", timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    models = data.get("models", [])
                    return [m.get("name", "") for m in models if m.get("name")]
                else:
                    logger.warning(f"Ollama API returned status {response.status_code}")
                    return []
            elif self.provider == "openai":
                if not hasattr(self, 'client'):
                    self._setup_client()
                models = self.client.models.list()
                
                # Robust handling for different response structures
                if hasattr(models, 'data') and models.data is not None:
                    return [model.id for model in models.data]
                elif isinstance(models, list):
                    return [m.id if hasattr(m, 'id') else m.get('id') for m in models]
                elif hasattr(models, '__iter__'):
                    return [model.id for model in models]
                else:
                    logger.warning(f"Unexpected models response format: {type(models)}")
                    return []
            elif self.provider == "deepseek":
                return ["deepseek-chat", "deepseek-coder"]
            elif self.provider == "kimi":
                return [
                    "moonshot-v1-8k",
                    "moonshot-v1-32k",
                    "moonshot-v1-128k",
                    "moonshot-v1-auto"
                ]
                    
        except Exception as e:
            logger.error(f"Failed to fetch models: {e}")
            raise e
        return []
