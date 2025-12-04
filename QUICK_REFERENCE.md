# Quick Reference - Universal Knowledge Graph Builder

## Installation

```bash
# Quick install with pip
pip install networkx matplotlib pyvis python-dotenv

# Or with UV
uv pip install -e .
```

## Quick Start

### GUI Mode
```bash
python gui.py
```

### CLI Mode
```bash
# Extract and visualize
python cli.py -i input.txt -o graph.json --visualize

# From example
python cli.py -i examples/python_ecosystem.txt --visualize -d technology
```

## Common Commands

### Extract Knowledge Graph
```bash
python cli.py -i mytext.txt -o mygraph.json -d general
```

### Visualize Existing Graph
```bash
python cli.py --load mygraph.json --visualize
```

### Show Statistics
```bash
python cli.py -i mytext.txt --stats
```

## Programmatic Usage

```python
import sys
sys.path.insert(0, 'src')
from kg_builder import *

# Initialize
kg = KnowledgeGraph()
extractor = KnowledgeGraphExtractor(llm_client=None)

# Extract
extraction = extractor.extract_from_text(text, domain='general')
kg.merge_from_extraction(extraction)

# Visualize
visualizer = KnowledgeGraphVisualizer(kg)
visualizer.visualize_interactive('output.html')
```

## Configuration (.env)

```bash
# For LLM extraction
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-your-key
DEFAULT_MODEL=gpt-3.5-turbo
```

## Domains

- `general` - General purpose
- `medical` - Healthcare/Medicine
- `finance` - Business/Finance
- `technology` - IT/Software
- `science` - Scientific research
- `legal` - Legal documents
- `education` - Educational content

## File Formats

### Input
- Plain text files (.txt)
- Any UTF-8 text

### Output
- JSON (.json) - For saving graphs
- HTML (.html) - For visualization

## Keyboard Shortcuts (GUI)

- Load file: Click "Load from File"
- Extract: Click "Extract from Text"
- Visualize: Click "Visualize"
- Save: Click "Save Graph"

## Tips

1. Start with example files
2. Use appropriate domain
3. LLM gives better results but works without
4. Visualizations are interactive - drag nodes!
5. Save graphs to JSON for later use

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No GUI | Install python3-tk |
| No entities | Check input text has clear concepts |
| LLM error | Verify API key in .env |
| Import error | Run: `pip install -r requirements.txt` |

## Example Workflow

```bash
# 1. Extract from text
python cli.py -i document.txt -o doc_graph.json -d technology

# 2. Visualize
python cli.py --load doc_graph.json --visualize

# 3. View stats
python cli.py --load doc_graph.json --stats
```

## More Info

- Full docs: `README.md`
- Getting started: `GETTING_STARTED.md`
- Detailed usage: `USAGE.md`
- Examples: `examples/` directory

## Support

GitHub: https://github.com/SiderLock/UniversalKnowledgeGraghBuilder
