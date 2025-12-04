# Usage Guide - Universal Knowledge Graph Builder

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone https://github.com/SiderLock/UniversalKnowledgeGraghBuilder.git
cd UniversalKnowledgeGraghBuilder

# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv pip install -e .
# or without UV:
pip install networkx matplotlib pyvis python-dotenv
```

### 2. Configuration (Optional)

For LLM-powered extraction, create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and add your API key:

```
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-your-key-here
DEFAULT_MODEL=gpt-3.5-turbo
```

**Note**: The tool works without LLM configuration using pattern-based extraction.

### 3. Running the Application

```bash
python gui.py
```

## Features Overview

### 1. Text Extraction

- **Input Methods**:
  - Type or paste text directly in the input area
  - Load text from a file using "Load from File" button

- **Domain Selection**:
  - Choose appropriate domain: general, medical, finance, technology, science, legal, education
  - Domain context helps LLM extract more relevant entities

### 2. Knowledge Graph Building

- Click "Extract from Text" to analyze the input
- The system will:
  - Identify entities (people, places, concepts, etc.)
  - Detect relationships between entities
  - Build a structured knowledge graph

### 3. Visualization

- Click "Visualize" to create an interactive graph
- Features:
  - Drag nodes to rearrange
  - Zoom in/out
  - Click on nodes and edges for details
  - Color-coded by entity type
  - Opens automatically in your web browser

### 4. Save and Load

- **Save Graph**: Export knowledge graph as JSON
- **Load Graph**: Import previously saved graphs
- **Clear**: Reset the current graph

## Example Workflows

### Workflow 1: Building a Technology Knowledge Graph

1. Open `gui.py`
2. Click "Load from File" and select `examples/python_ecosystem.txt`
3. Select domain: "technology"
4. Click "Extract from Text"
5. Review extracted entities and relationships in the right panel
6. Click "Visualize" to see the interactive graph
7. Click "Save Graph" to export as JSON

### Workflow 2: Medical Knowledge Graph

1. Open `gui.py`
2. Click "Load from File" and select `examples/cardiovascular_system.txt`
3. Select domain: "medical"
4. Click "Extract from Text"
5. Click "Visualize"
6. Explore the medical entity relationships

### Workflow 3: Custom Text Analysis

1. Open `gui.py`
2. Clear the input area and paste your own text
3. Select appropriate domain
4. Click "Extract from Text"
5. Review and visualize results

## LLM vs Fallback Mode

### With LLM (Configured API Key)

- **Advantages**:
  - More accurate entity extraction
  - Better relationship detection
  - Context-aware processing
  - Domain-specific knowledge

- **Models Supported**:
  - OpenAI: GPT-3.5-turbo, GPT-4, GPT-4-turbo
  - Anthropic: Claude-3 (Opus, Sonnet, Haiku)

### Without LLM (Fallback Mode)

- **Advantages**:
  - No API key required
  - No cost
  - Works offline
  - Fast processing

- **Method**:
  - Pattern-based entity extraction
  - Common relationship patterns (is_a, has, uses)
  - Suitable for simple text analysis

## Tips for Best Results

1. **Choose the Right Domain**: Select a domain that matches your text content for better extraction

2. **Text Quality**: 
   - Use well-structured text
   - Clear sentences work best
   - Avoid overly complex or ambiguous text

3. **Incremental Building**:
   - Extract from multiple sources
   - The graph accumulates entities and relationships
   - Use "Clear" when starting a new topic

4. **LLM Configuration**:
   - For best results, use GPT-4 or Claude-3
   - Adjust temperature in config if needed (lower = more focused)

5. **Visualization**:
   - For large graphs, zoom out first
   - Drag nodes to organize layout
   - Use the physics settings in the HTML if needed

## Troubleshooting

### Issue: GUI doesn't start

**Solution**: Ensure all dependencies are installed:
```bash
pip install networkx matplotlib pyvis python-dotenv
```

### Issue: LLM extraction not working

**Solution**: Check your `.env` file:
- Verify API key is correct
- Ensure LLM_PROVIDER matches your key (openai/anthropic)
- Check you have API credits

### Issue: Visualization not opening

**Solution**: 
- Check `output/` directory was created
- Ensure write permissions
- Try opening `output/knowledge_graph.html` manually

### Issue: Empty extraction results

**Solution**:
- Verify input text is not empty
- Try different domain setting
- Check if text contains clear entities (proper nouns, concepts)

## Advanced Usage

### Programmatic Usage

```python
from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, Config

# Initialize
config = Config()
kg = KnowledgeGraph()
extractor = KnowledgeGraphExtractor(
    llm_client=config.get_llm_client(),
    model='gpt-3.5-turbo'
)

# Extract from text
text = "Your text here..."
extraction = extractor.extract_from_text(text, domain='general')

# Build graph
kg.merge_from_extraction(extraction)

# Save to JSON
with open('my_graph.json', 'w') as f:
    f.write(kg.to_json())

# Visualize
from kg_builder import KnowledgeGraphVisualizer
viz = KnowledgeGraphVisualizer(kg)
viz.visualize_interactive('my_graph.html')
```

### Customizing Extraction

Edit `src/kg_builder/extractor.py` to:
- Modify the extraction prompt
- Add custom entity types
- Implement domain-specific patterns

### Customizing Visualization

Edit `src/kg_builder/visualizer.py` to:
- Change colors and layouts
- Adjust physics parameters
- Add custom node/edge styling

## File Formats

### JSON Graph Format

```json
{
  "entities": [
    {
      "id": "entity1",
      "label": "Entity Name",
      "type": "entity_type"
    }
  ],
  "relationships": [
    {
      "source": "entity1",
      "target": "entity2",
      "relation": "relationship_type"
    }
  ]
}
```

## Support

- **Issues**: Open an issue on GitHub
- **Documentation**: See README.md
- **Examples**: Check `examples/` directory
