# Project Structure - Universal Knowledge Graph Builder

## Directory Layout

```
UniversalKnowledgeGraghBuilder/
│
├── gui.py                          # Main GUI application (run this!)
├── cli.py                          # Command-line interface
│
├── src/kg_builder/                 # Core package
│   ├── __init__.py                 # Package initialization
│   ├── graph.py                    # KnowledgeGraph class
│   ├── extractor.py                # KnowledgeGraphExtractor class
│   ├── visualizer.py               # KnowledgeGraphVisualizer class
│   └── config.py                   # Configuration management
│
├── examples/                       # Example input files
│   ├── python_ecosystem.txt        # Technology domain example
│   └── cardiovascular_system.txt   # Medical domain example
│
├── tests/                          # Test files
│   ├── test_basic.py               # Basic functionality tests
│   ├── test_complete.py            # End-to-end tests
│   ├── test_gui.py                 # GUI component tests
│   └── test_visualization.py       # Visualization tests
│
├── docs/                           # Documentation
│   ├── README.md                   # Main documentation
│   ├── GETTING_STARTED.md          # Quick start guide
│   ├── USAGE.md                    # Detailed usage guide
│   └── QUICK_REFERENCE.md          # Quick reference card
│
├── config/                         # Configuration files
│   ├── .env.example                # Example environment config
│   ├── pyproject.toml              # UV project configuration
│   └── requirements.txt            # pip requirements
│
└── output/                         # Generated files (gitignored)
    ├── *.json                      # Saved knowledge graphs
    └── *.html                      # Visualizations
```

## Core Components

### 1. Knowledge Graph (`src/kg_builder/graph.py`)
- **KnowledgeGraph**: Main data structure
- Methods: add_entity(), add_relationship(), merge_from_extraction()
- Uses NetworkX for graph management
- JSON serialization/deserialization

### 2. Extractor (`src/kg_builder/extractor.py`)
- **KnowledgeGraphExtractor**: Entity and relationship extraction
- LLM-powered extraction (OpenAI/Anthropic)
- Pattern-based fallback extraction
- Domain-aware processing

### 3. Visualizer (`src/kg_builder/visualizer.py`)
- **KnowledgeGraphVisualizer**: Graph visualization
- Interactive HTML graphs (PyVis)
- Static matplotlib graphs
- Text summaries

### 4. Config (`src/kg_builder/config.py`)
- **Config**: Configuration management
- Environment variable loading
- LLM client initialization
- Default settings

## User Interfaces

### GUI (`gui.py`)
- **KnowledgeGraphBuilderGUI**: Main application window
- Features:
  - Text input area
  - Domain selection
  - Extract/Load/Save/Visualize buttons
  - Graph statistics display
  - Status bar

### CLI (`cli.py`)
- Command-line interface
- Arguments:
  - `-i`: Input file
  - `-o`: Output file
  - `-d`: Domain
  - `--visualize`: Generate visualization
  - `--stats`: Show statistics
  - `--load`: Load existing graph

## Data Flow

```
1. Input Text
   ↓
2. KnowledgeGraphExtractor.extract_from_text()
   ↓ (uses LLM or pattern matching)
3. Extraction Result {entities: [...], relationships: [...]}
   ↓
4. KnowledgeGraph.merge_from_extraction()
   ↓
5. Knowledge Graph (NetworkX DiGraph)
   ↓
6a. KnowledgeGraph.to_json() → Save to file
6b. KnowledgeGraphVisualizer.visualize_interactive() → HTML
6c. Display in GUI or CLI
```

## File Formats

### Input
- **Text files** (.txt): Plain text for extraction
- **JSON files** (.json): Existing knowledge graphs

### Output
- **JSON files** (.json): Serialized knowledge graphs
  ```json
  {
    "entities": [
      {"id": "...", "label": "...", "type": "..."}
    ],
    "relationships": [
      {"source": "...", "target": "...", "relation": "..."}
    ]
  }
  ```
- **HTML files** (.html): Interactive visualizations

## Configuration

### Environment Variables (.env)
- `LLM_PROVIDER`: openai or anthropic
- `OPENAI_API_KEY`: OpenAI API key
- `ANTHROPIC_API_KEY`: Anthropic API key
- `DEFAULT_MODEL`: Model name
- `DEFAULT_DOMAIN`: Default extraction domain
- `OUTPUT_DIR`: Output directory path

### Project Configuration (pyproject.toml)
- Project metadata
- Dependencies
- Build system
- Tool settings (black, ruff)

## Dependencies

### Core
- `networkx`: Graph data structure
- `matplotlib`: Static visualizations
- `pyvis`: Interactive visualizations
- `python-dotenv`: Environment configuration

### Optional
- `openai`: OpenAI LLM integration
- `anthropic`: Anthropic LLM integration

### Development
- `pytest`: Testing framework
- `black`: Code formatting
- `ruff`: Linting

## Entry Points

### For Users
1. **GUI**: `python gui.py`
2. **CLI**: `python cli.py [options]`
3. **Python**: `from kg_builder import *`

### For Developers
1. **Tests**: `python test_complete.py`
2. **Examples**: See `examples/` directory
3. **API**: Import from `kg_builder` package

## Extension Points

### Adding New Extractors
- Extend `KnowledgeGraphExtractor`
- Implement custom extraction logic
- Add new LLM providers

### Adding New Visualizations
- Extend `KnowledgeGraphVisualizer`
- Add new visualization methods
- Customize colors/layouts

### Adding New Domains
- Update domain list in GUI
- Customize extraction prompts
- Add domain-specific patterns

## Best Practices

1. **Use appropriate domains** for better extraction
2. **Save graphs frequently** to JSON
3. **Test with examples** before production use
4. **Configure LLM** for best results
5. **Explore visualizations** interactively

## Support

- **Documentation**: See `README.md`, `GETTING_STARTED.md`, `USAGE.md`
- **Examples**: Check `examples/` directory
- **Tests**: Run `test_complete.py` to validate setup
- **Issues**: Open on GitHub

## Version

Current Version: 0.1.0

## License

MIT License - See LICENSE file
