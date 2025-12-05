import os
import json
import pandas as pd
import logging
import time
from typing import Dict, List, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class UniversalEnricher:
    def __init__(self, api_key: str, base_url: str = None, model: str = "qwen-plus", provider: str = "dashscope"):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.provider = provider
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
                # Ollama使用OpenAI兼容API，默认端口11434
                base_url = self.base_url or "http://localhost:11434/v1"
                self.client = OpenAI(api_key="ollama", base_url=base_url)
            except ImportError:
                raise ImportError("openai package not installed (required for Ollama)")

    def generate_prompts_for_domain(self, domain_name: str, description: str, source_instruction: str = "") -> Dict[str, Any]:
        """
        使用 LLM 自动生成特定领域的 Schema 和 Prompt
        """
        source_req = f"\n        数据来源要求：{source_instruction}" if source_instruction else ""
        
        meta_prompt = f"""
        你是一个专家级的数据架构师和提示词工程师。
        用户希望构建一个关于“{domain_name}”的知识库。
        描述：{description}{source_req}
        
        请你完成以下任务：
        1. 定义该领域的核心实体类型（Entity Type）。
        2. 列出该实体最重要的3-5个属性（Attributes），并给出简短描述。
        3. 编写一个 System Prompt，设定LLM的角色。
        4. 编写一个 User Prompt Template，用于让LLM提取这些属性。模板中应包含 {{entity_name}} 和 {{attributes}} 占位符。
        
        特别注意：
        - 如果提供了数据来源要求，请在 User Prompt Template 中明确指示 LLM 优先参考这些来源，并在输出中包含 data_source 字段。

        请严格以 JSON 格式返回，格式如下：
        {{
            "schema": {{
                "entity_type": "...",
                "attributes": [
                    {{"name": "...", "description": "..."}},
                    ...
                ]
            }},
            "prompts": {{
                "system": "...",
                "user_template": "..."
            }}
        }}
        """
        
        try:
            response_text = self._call_llm(meta_prompt, json_mode=True)
            # 清理可能的 markdown 标记
            response_text = response_text.replace("```json", "").replace("```", "").strip()
            return json.loads(response_text)
        except Exception as e:
            logger.error(f"Failed to generate prompts: {e}")
            # Return a fallback
            return {
                "schema": {"entity_type": domain_name, "attributes": []},
                "prompts": {"system": "You are a helpful assistant.", "user_template": f"Tell me about {{entity_name}}"}
            }

    def process_batch(self, df: pd.DataFrame, name_col: str, domain_config: Dict[str, Any], max_workers: int = 3, progress_callback: Callable[[int], None] = None) -> pd.DataFrame:
        """
        并发处理一批数据
        """
        schema = domain_config.get('schema', {})
        prompts = domain_config.get('prompts', {})
        
        attributes = [attr['name'] for attr in schema.get('attributes', [])]
        attr_str = ", ".join(attributes)
        
        # Ensure columns exist
        for attr in attributes:
            if attr not in df.columns:
                df[attr] = None

        system_prompt = prompts.get('system', '')
        user_template_base = prompts.get('user_template', '')
        source_instruction = domain_config.get('source_instruction', '')

        def process_single_entity(idx, row):
            entity_name = row.get(name_col)
            if pd.isna(entity_name):
                return idx, None
            
            # Construct prompt
            user_prompt = user_template_base.replace("{entity_name}", str(entity_name)) \
                                          .replace("{attributes}", attr_str) \
                                          .replace("{source_instruction}", source_instruction)
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Call LLM
                    response_text = self._call_llm(user_prompt, system_prompt=system_prompt, json_mode=True)
                    
                    # Parse JSON
                    try:
                        clean_json = response_text.replace("```json", "").replace("```", "").strip()
                        data = json.loads(clean_json)
                        return idx, data
                                
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON for {entity_name} (Attempt {attempt+1}/{max_retries})")
                        
                except Exception as e:
                    logger.error(f"Error processing {entity_name} (Attempt {attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 * (attempt + 1)) # Exponential backoff
            
            return idx, None

        # Use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_entity, idx, row): idx for idx, row in df.iterrows()}
            
            completed = 0
            for future in as_completed(futures):
                idx, data = future.result()
                if data:
                    for key, value in data.items():
                        if key in df.columns:
                            df.at[idx, key] = str(value)
                
                completed += 1
                if progress_callback:
                    progress_callback(completed)
                    
        return df

    def _call_llm(self, prompt: str, system_prompt: str = None, json_mode: bool = False) -> str:
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
            
            response = Generation.call(
                model=self.model,
                messages=messages,
                result_format='message'
            )
            
            if response.status_code == 200:
                return response.output.choices[0].message.content
            else:
                raise Exception(f"DashScope API Error: {response.message}")
                
        elif self.provider in ["openai", "ollama"]:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            kwargs = {
                "model": self.model,
                "messages": messages
            }
            # Ollama目前不支持json_mode，但OpenAI支持
            if json_mode and self.provider == "openai":
                kwargs["response_format"] = {"type": "json_object"}
                
            response = self.client.chat.completions.create(**kwargs)
            return response.choices[0].message.content
            
        return ""
