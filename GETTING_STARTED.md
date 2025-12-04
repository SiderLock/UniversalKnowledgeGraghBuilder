# Getting Started with Universal Knowledge Graph Builder

This guide will help you get started with the Universal Knowledge Graph Builder in just a few minutes.

## Prerequisites

- Python 3.9 or higher
- pip or UV package manager

## Installation

### Option 1: Using UV (Recommended)

```bash
# Install UV if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/SiderLock/UniversalKnowledgeGraghBuilder.git
cd UniversalKnowledgeGraghBuilder

# Install dependencies
uv pip install -e .
```

### Option 2: Using pip

```bash
# Clone the repository
git clone https://github.com/SiderLock/UniversalKnowledgeGraghBuilder.git
cd UniversalKnowledgeGraghBuilder

# Install dependencies
pip install -r requirements.txt
```

## Your First Knowledge Graph

### Method 1: Using the GUI (Easiest)

1. **Start the application**:
   ```bash
   python gui.py
   ```

2. **Load an example**:
   - Click "Load from File"
   - Select `examples/python_ecosystem.txt`

3. **Extract the knowledge graph**:
   - Select domain: "technology"
   - Click "Extract from Text"
   - Watch as entities and relationships appear in the right panel

4. **Visualize**:
   - Click "Visualize"
   - An interactive graph will open in your browser
   - Try dragging nodes around!

5. **Save your work**:
   - Click "Save Graph"
   - Choose a location to save the JSON file

### Method 2: Using the CLI

```bash
# Extract and visualize in one command
python cli.py -i examples/python_ecosystem.txt -o my_graph.json --visualize -d technology

# View statistics only
python cli.py -i examples/cardiovascular_system.txt --stats -d medical

# Load and visualize existing graph
python cli.py --load my_graph.json --visualize
```

### Method 3: Programmatic Usage

Create a file `my_script.py`:

```python
import sys
sys.path.insert(0, 'src')

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config

# Initialize
config = Config()
kg = KnowledgeGraph()
extractor = KnowledgeGraphExtractor(llm_client=None)  # Use fallback mode

# Extract from text
text = """
Python is a programming language. It was created by Guido van Rossum.
Django is a web framework written in Python.
"""

extraction = extractor.extract_from_text(text, domain='technology')
kg.merge_from_extraction(extraction)

# Show results
print(f"Entities: {kg.graph.number_of_nodes()}")
print(f"Relationships: {kg.graph.number_of_edges()}")

# Visualize
visualizer = KnowledgeGraphVisualizer(kg)
visualizer.visualize_interactive('my_first_graph.html')
print("Visualization saved to my_first_graph.html")
```

Run it:
```bash
python my_script.py
```

## Next Steps

### Enable LLM-Powered Extraction (Optional)

For more accurate extraction, configure an LLM:

1. **Copy the example config**:
   ```bash
   cp .env.example .env
   ```

2. **Add your API key** (edit `.env`):
   ```
   LLM_PROVIDER=openai
   OPENAI_API_KEY=sk-your-api-key-here
   DEFAULT_MODEL=gpt-3.5-turbo
   ```

3. **Install LLM package**:
   ```bash
   pip install openai
   # or for Anthropic:
   pip install anthropic
   ```

4. **Run the application** - it will automatically use the LLM!

### Try Different Domains

The tool works with various domains:

- **Medical**: `examples/cardiovascular_system.txt`
- **Technology**: `examples/python_ecosystem.txt`
- **Custom**: Create your own text files!

### Explore the Examples

```bash
# Medical knowledge graph
python cli.py -i examples/cardiovascular_system.txt --visualize -d medical

# Technology knowledge graph
python cli.py -i examples/python_ecosystem.txt --visualize -d technology
```

## Understanding the Output

### Knowledge Graph Structure

The extracted knowledge graph contains:

- **Entities**: Things, concepts, people, places
  - Each has an ID, label, and type
  - Example: `{"id": "python", "label": "Python", "type": "Language"}`

- **Relationships**: Connections between entities
  - Each has source, target, and relation type
  - Example: `{"source": "django", "target": "python", "relation": "written_in"}`

### Output Formats

1. **JSON** (for saving/loading):
   ```json
   {
     "entities": [...],
     "relationships": [...]
   }
   ```

2. **HTML** (for visualization):
   - Interactive network graph
   - Color-coded by entity type
   - Zoomable and draggable

## Common Use Cases

### 1. Document Analysis

Extract key concepts from documents:

```bash
python cli.py -i my_document.txt -o document_graph.json --visualize
```

### 2. Learning Aid

Create study materials from textbook chapters:

```bash
python cli.py -i textbook_chapter.txt -d education --visualize
```

### 3. Research Organization

Map research papers and their relationships:

```bash
python cli.py -i research_notes.txt -d science -o research_graph.json
```

### 4. Business Intelligence

Extract entities from business reports:

```bash
python cli.py -i quarterly_report.txt -d finance --visualize
```

## Tips for Success

1. **Start Small**: Begin with short, clear text
2. **Choose Right Domain**: Match the domain to your content
3. **Iterate**: Extract multiple times, accumulate knowledge
4. **Use LLM for Complex Text**: Better accuracy for nuanced content
5. **Explore Visualization**: Drag nodes to organize the graph

## Troubleshooting

**Problem**: GUI won't start
- **Solution**: Install tkinter: `sudo apt-get install python3-tk` (Linux)

**Problem**: No entities extracted
- **Solution**: Ensure text has clear proper nouns or concepts

**Problem**: LLM not working
- **Solution**: Check API key in `.env` and verify credits

## Getting Help

- Check `USAGE.md` for detailed documentation
- Review `examples/` for sample inputs
- Open an issue on GitHub for bugs

## What's Next?

- Read the [Usage Guide](USAGE.md) for advanced features
- Explore the codebase to customize extraction
- Try different LLM models for better results
- Share your knowledge graphs!

Happy graph building! 🎉
