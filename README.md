# Universal Knowledge Graph Builder

一个现代化的、可视化的知识图谱构建工具。它不再局限于化学领域，而是提供了一套通用的流水线，帮助用户利用大语言模型 (LLM) 从非结构化或半结构化数据中提取信息，构建任意领域的知识图谱。

A modern, visual knowledge graph construction tool that provides a universal pipeline to help users extract information from unstructured or semi-structured data using Large Language Models (LLMs) to build knowledge graphs in any domain.

## Features

- 🎨 **Visual Interface**: Intuitive GUI for building and visualizing knowledge graphs
- 🤖 **LLM-Powered Extraction**: Leverages OpenAI or Anthropic models for intelligent entity and relationship extraction
- 🌐 **Domain Agnostic**: Works with any domain - medical, finance, technology, science, etc.
- 📊 **Interactive Visualization**: Beautiful, interactive HTML-based graph visualizations
- 💾 **Import/Export**: Save and load knowledge graphs in JSON format
- 🔄 **Fallback Mode**: Works without LLM using pattern-based extraction

## Installation

This project uses Python UV for project management. First, install UV:

```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then, install the project dependencies:

```bash
# Clone the repository
git clone https://github.com/SiderLock/UniversalKnowledgeGraghBuilder.git
cd UniversalKnowledgeGraghBuilder

# Install dependencies using UV
uv pip install -e .
```

## Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and add your API keys:
```
LLM_PROVIDER=openai
OPENAI_API_KEY=your_api_key_here
DEFAULT_MODEL=gpt-3.5-turbo
```

Supported LLM providers:
- **OpenAI**: GPT-3.5, GPT-4, GPT-4-turbo
- **Anthropic**: Claude-3 (Opus, Sonnet, Haiku)

Note: The tool works in fallback mode without an API key, using pattern-based extraction.

## Usage

### Running the GUI

Simply run the GUI application:

```bash
python gui.py
```

Or with UV:

```bash
uv run gui.py
```

### GUI Workflow

1. **Enter or Load Text**: Type text in the input area or load from a file
2. **Select Domain**: Choose the appropriate domain (general, medical, finance, etc.)
3. **Extract**: Click "Extract from Text" to build the knowledge graph
4. **Visualize**: Click "Visualize" to see an interactive graph visualization
5. **Save/Load**: Save your knowledge graph to JSON or load existing graphs

### Example Usage

Try the example files in the `examples/` directory:

```bash
# Open GUI and load examples/python_ecosystem.txt
# or examples/cardiovascular_system.txt
```

## Project Structure

```
UniversalKnowledgeGraghBuilder/
├── gui.py                          # Main GUI application
├── src/
│   └── kg_builder/                 # Core package
│       ├── __init__.py
│       ├── graph.py                # Knowledge graph data structure
│       ├── extractor.py            # LLM-based extraction
│       ├── visualizer.py           # Visualization utilities
│       └── config.py               # Configuration management
├── examples/                       # Example text files
├── pyproject.toml                  # UV project configuration
├── .env.example                    # Example environment configuration
└── README.md

```

## Architecture

### Core Components

1. **KnowledgeGraph**: Data structure for storing entities and relationships
2. **KnowledgeGraphExtractor**: LLM-powered extraction pipeline with fallback
3. **KnowledgeGraphVisualizer**: Creates interactive and static visualizations
4. **Config**: Manages configuration and LLM client setup

### Pipeline

```
Text Input → LLM Extraction → Knowledge Graph → Visualization
                ↓ (fallback)
          Pattern Matching
```

## Development

### Running Tests

```bash
uv run pytest
```

### Code Formatting

```bash
uv run black .
uv run ruff check .
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
