"""Core knowledge graph extractor using LLM."""

import json
from typing import List, Dict, Any, Optional
import re


class KnowledgeGraphExtractor:
    """Extract entities and relationships from text using LLM."""

    def __init__(self, llm_client=None, model: str = "gpt-3.5-turbo"):
        """Initialize the extractor with an LLM client.
        
        Args:
            llm_client: Optional LLM client (OpenAI, Anthropic, etc.)
            model: Model name to use for extraction
        """
        self.llm_client = llm_client
        self.model = model

    def extract_from_text(self, text: str, domain: str = "general") -> Dict[str, Any]:
        """Extract entities and relationships from text.
        
        Args:
            text: Input text to extract from
            domain: Domain context (e.g., "medical", "finance", "general")
            
        Returns:
            Dictionary with "entities" and "relationships" keys
        """
        if not self.llm_client:
            # Fallback: simple pattern-based extraction
            return self._simple_extraction(text)
        
        prompt = self._build_extraction_prompt(text, domain)
        
        try:
            response = self._call_llm(prompt)
            return self._parse_llm_response(response)
        except Exception as e:
            print(f"LLM extraction failed: {e}. Using fallback.")
            return self._simple_extraction(text)

    def _build_extraction_prompt(self, text: str, domain: str) -> str:
        """Build prompt for LLM extraction."""
        return f"""Extract entities and relationships from the following text in the {domain} domain.

Text: {text}

Please return a JSON object with the following structure:
{{
  "entities": [
    {{"id": "entity1", "label": "Entity Name", "type": "entity_type"}},
    ...
  ],
  "relationships": [
    {{"source": "entity1", "target": "entity2", "relation": "relationship_type"}},
    ...
  ]
}}

Only return the JSON object, no additional text."""

    def _call_llm(self, prompt: str) -> str:
        """Call the LLM with the given prompt."""
        if hasattr(self.llm_client, 'chat'):
            # OpenAI-style API
            response = self.llm_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3
            )
            return response.choices[0].message.content
        elif hasattr(self.llm_client, 'messages'):
            # Anthropic-style API
            response = self.llm_client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        else:
            raise ValueError("Unsupported LLM client")

    def _parse_llm_response(self, response: str) -> Dict[str, Any]:
        """Parse the LLM response into structured format."""
        # Try to extract JSON from response
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        
        # Fallback to empty structure
        return {"entities": [], "relationships": []}

    def _simple_extraction(self, text: str) -> Dict[str, Any]:
        """Simple pattern-based extraction as fallback."""
        # Extract capitalized phrases as potential entities
        entities = []
        entity_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
        
        seen_entities = set()
        for match in re.finditer(entity_pattern, text):
            entity_name = match.group(1)
            if entity_name not in seen_entities and len(entity_name) > 1:
                entity_id = f"entity_{len(entities)}"
                entities.append({
                    "id": entity_id,
                    "label": entity_name,
                    "type": "Entity"
                })
                seen_entities.add(entity_name)
        
        # Simple relationships based on common patterns
        relationships = []
        relation_patterns = [
            (r'(\w+)\s+is\s+a\s+(\w+)', 'is_a'),
            (r'(\w+)\s+has\s+(\w+)', 'has'),
            (r'(\w+)\s+uses\s+(\w+)', 'uses'),
        ]
        
        for pattern, relation_type in relation_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                source, target = match.groups()
                relationships.append({
                    "source": source,
                    "target": target,
                    "relation": relation_type
                })
        
        return {
            "entities": entities,
            "relationships": relationships
        }
