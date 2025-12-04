"""Configuration management for the knowledge graph builder."""

import os
from typing import Optional
from dotenv import load_dotenv


class Config:
    """Configuration settings for the application."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        load_dotenv()
        
        # LLM Configuration
        self.openai_api_key = os.getenv('OPENAI_API_KEY', '')
        self.anthropic_api_key = os.getenv('ANTHROPIC_API_KEY', '')
        self.default_model = os.getenv('DEFAULT_MODEL', 'gpt-3.5-turbo')
        self.llm_provider = os.getenv('LLM_PROVIDER', 'openai')  # 'openai' or 'anthropic'
        
        # Application Settings
        self.default_domain = os.getenv('DEFAULT_DOMAIN', 'general')
        self.output_dir = os.getenv('OUTPUT_DIR', 'output')
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def get_llm_client(self):
        """Get configured LLM client based on settings.
        
        Returns:
            Configured LLM client or None if no API key is set
        """
        if self.llm_provider == 'openai' and self.openai_api_key:
            try:
                import openai
                return openai.OpenAI(api_key=self.openai_api_key)
            except ImportError:
                print("OpenAI package not installed. Please install: pip install openai")
                return None
        elif self.llm_provider == 'anthropic' and self.anthropic_api_key:
            try:
                import anthropic
                return anthropic.Anthropic(api_key=self.anthropic_api_key)
            except ImportError:
                print("Anthropic package not installed. Please install: pip install anthropic")
                return None
        else:
            print("No LLM API key configured. Using fallback extraction.")
            return None

    def is_llm_configured(self) -> bool:
        """Check if LLM is properly configured."""
        if self.llm_provider == 'openai':
            return bool(self.openai_api_key)
        elif self.llm_provider == 'anthropic':
            return bool(self.anthropic_api_key)
        return False
