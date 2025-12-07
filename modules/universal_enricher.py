import os
import json
import pandas as pd
import logging
import time
from typing import Dict, List, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入稳定的JSON解析器
from .llm_json_parser import parse_llm_json

logger = logging.getLogger(__name__)

class UniversalEnricher:
    def __init__(self, api_key: str, base_url: str = None, model: str = "qwen-plus", provider: str = "dashscope", options: Dict[str, Any] = None):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.provider = provider
        self.options = options or {}
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

    def process_batch(self, df: pd.DataFrame, name_col: str, domain_config: Dict[str, Any], 
                       max_workers: int = 3, progress_callback: Callable[[int], None] = None,
                       status_callback: Callable[[str], None] = None) -> pd.DataFrame:
        """
        并发处理一批数据
        
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
        attr_str = ", ".join(attributes)
        
        # Ensure columns exist
        for attr in attributes:
            if attr not in df.columns:
                df[attr] = None

        system_prompt = prompts.get('system', '')
        user_template_base = prompts.get('user_template', '')
        source_instruction = domain_config.get('source_instruction', '')
        
        # 统计信息
        total_count = len(df)
        success_count = 0
        error_count = 0
        
        if status_callback:
            status_callback(f"开始处理 {total_count} 条记录...")

        def process_single_entity(idx, row):
            nonlocal success_count, error_count
            entity_name = row.get(name_col)
            if pd.isna(entity_name) or str(entity_name).strip() == '':
                return idx, None, "skipped"
            
            entity_name = str(entity_name).strip()
            
            # Construct prompt
            user_prompt = user_template_base.replace("{entity_name}", entity_name) \
                                          .replace("{attributes}", attr_str) \
                                          .replace("{source_instruction}", source_instruction)
            
            max_retries = 3
            last_error = None
            for attempt in range(max_retries):
                try:
                    # Call LLM (关闭json_mode，因为Ollama返回markdown格式)
                    response_text = self._call_llm(user_prompt, system_prompt=system_prompt, json_mode=False)
                    
                    # 使用稳定的JSON解析器
                    try:
                        data = parse_llm_json(response_text, default_value={})
                        if data and isinstance(data, dict):
                            logger.info(f"成功处理: {entity_name}")
                            return idx, data, "success"
                        else:
                            last_error = "解析结果为空或格式不正确"
                            logger.debug(f"解析失败 {entity_name} (尝试 {attempt+1}/{max_retries}): {last_error}")
                    except Exception as parse_error:
                        last_error = f"JSON解析错误: {str(parse_error)[:50]}"
                        logger.debug(f"解析失败 {entity_name} (尝试 {attempt+1}/{max_retries}): {last_error}")
                        
                except Exception as e:
                    last_error = str(e)
                    logger.debug(f"处理失败 {entity_name} (尝试 {attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 * (attempt + 1))  # Exponential backoff
            
            logger.debug(f"处理失败 {entity_name}: {last_error}")
            return idx, None, f"error: {last_error}"

        # Use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_entity, idx, row): idx for idx, row in df.iterrows()}
            
            completed = 0
            for future in as_completed(futures):
                idx, data, status = future.result()
                if data:
                    # 特殊处理中医药数据格式
                    if isinstance(data, dict) and 'attributes' in data:
                        # 处理结构化的attributes数据
                        for attr in data.get('attributes', []):
                            if isinstance(attr, dict) and 'name' in attr and 'value' in attr:
                                attr_name = attr['name']
                                attr_value = attr['value']
                                # 添加到DataFrame中对应的列
                                if attr_name in df.columns:
                                    df.at[idx, attr_name] = str(attr_value) if attr_value is not None else None
                    else:
                        # 标准的key-value映射
                        for key, value in data.items():
                            if key in df.columns:
                                df.at[idx, key] = str(value) if value is not None else None
                    success_count += 1
                elif status.startswith("error"):
                    error_count += 1
                
                completed += 1
                if progress_callback:
                    progress_callback(completed)
                if status_callback and completed % 5 == 0:
                    status_callback(f"已处理 {completed}/{total_count} | 成功: {success_count} | 失败: {error_count}")
        
        if status_callback:
            status_callback(f"处理完成: 成功 {success_count}/{total_count}，失败 {error_count}")
        
        logger.info(f"Batch processing completed: {success_count}/{total_count} success, {error_count} errors")
        return df

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
                
        elif self.provider in ["openai", "ollama", "deepseek"]:
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
            elif self.provider in ["openai", "deepseek"]:
                # Filter out Ollama-specific parameters for OpenAI/DeepSeek
                ollama_specific_params = ["num_ctx", "keep_alive", "num_gpu", "num_thread", "repeat_penalty", "num_predict", "mirostat", "mirostat_eta", "mirostat_tau", "num_gqa", "repeat_last_n", "tfs_z", "seed", "top_k", "format"]
                for param in ollama_specific_params:
                    combined_args.pop(param, None)

            # Merge remaining standard args (temperature, top_p, etc.)
            api_kwargs.update(combined_args)

            # Enable JSON mode for different providers
            if json_mode:
                if self.provider == "ollama":
                    # Ollama may not fully support response_format, use prompt guidance instead
                    if messages:
                        messages[-1]["content"] += "\n\n请务必以有效的JSON格式返回结果。"
                elif self.provider in ["openai", "deepseek"]:
                    # OpenAI and DeepSeek support response_format
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
                    
        except Exception as e:
            logger.error(f"Failed to fetch models: {e}")
            raise e
        return []
