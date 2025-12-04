#!/usr/bin/env python3
"""
Demo script for Universal Knowledge Graph Builder
Showcases the main features with a simple example.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def demo():
    """Run a demonstration of the knowledge graph builder."""
    print_section("Universal Knowledge Graph Builder - Demo")
    
    # Sample text about artificial intelligence
    demo_text = """
    Artificial Intelligence (AI) is a branch of computer science focused on creating 
    intelligent machines. Machine Learning is a subset of AI that enables systems to 
    learn from data. Deep Learning is a type of Machine Learning based on artificial 
    neural networks.
    
    Python is the most popular programming language for AI development. TensorFlow and 
    PyTorch are leading frameworks for building Deep Learning models. These frameworks 
    are used by researchers and engineers worldwide.
    
    Natural Language Processing (NLP) is an AI field that focuses on understanding 
    human language. GPT and BERT are state-of-the-art NLP models. They are trained on 
    massive text datasets and can perform various language tasks.
    
    Computer Vision is another important AI domain. It enables machines to understand 
    and interpret visual information. Convolutional Neural Networks (CNNs) are 
    commonly used for Computer Vision tasks.
    """
    
    print("\n📝 Sample Text:")
    print("-" * 70)
    print(demo_text.strip()[:200] + "...")
    
    # Initialize components
    print_section("Step 1: Initializing Components")
    config = Config()
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=config.get_llm_client())
    
    llm_status = "✓ LLM Enabled" if config.is_llm_configured() else "⚠ Using Fallback Mode (no LLM)"
    print(f"Status: {llm_status}")
    
    # Extract knowledge graph
    print_section("Step 2: Extracting Knowledge Graph")
    print("Analyzing text and extracting entities and relationships...")
    
    extraction = extractor.extract_from_text(demo_text, domain='technology')
    
    print(f"\n✓ Extracted {len(extraction['entities'])} entities")
    print(f"✓ Extracted {len(extraction['relationships'])} relationships")
    
    # Build the graph
    print_section("Step 3: Building Knowledge Graph")
    kg.merge_from_extraction(extraction)
    
    stats = kg.get_stats()
    print(f"\nGraph Statistics:")
    print(f"  • Total Entities: {stats['num_entities']}")
    print(f"  • Total Relationships: {stats['num_relationships']}")
    print(f"  • Entity Types: {', '.join(stats['entity_types'])}")
    print(f"  • Relation Types: {', '.join(stats['relation_types'])}")
    print(f"  • Average Connections: {stats['avg_degree']:.2f}")
    
    # Display entities
    print_section("Step 4: Displaying Entities")
    entities = kg.get_entities()
    print(f"\nShowing first 10 of {len(entities)} entities:\n")
    
    for i, entity in enumerate(entities[:10], 1):
        label = entity.get('label', entity['id'])
        entity_type = entity.get('type', 'Unknown')
        print(f"  {i:2d}. {label:<30s} [{entity_type}]")
    
    if len(entities) > 10:
        print(f"  ... and {len(entities) - 10} more")
    
    # Display relationships
    print_section("Step 5: Displaying Relationships")
    relationships = kg.get_relationships()
    print(f"\nShowing first 10 of {len(relationships)} relationships:\n")
    
    for i, rel in enumerate(relationships[:10], 1):
        source = rel['source']
        target = rel['target']
        relation = rel.get('relation', 'related_to')
        print(f"  {i:2d}. {source} --[{relation}]--> {target}")
    
    if len(relationships) > 10:
        print(f"  ... and {len(relationships) - 10} more")
    
    # Save to JSON
    print_section("Step 6: Saving Knowledge Graph")
    os.makedirs('output', exist_ok=True)
    output_json = 'output/demo_graph.json'
    
    with open(output_json, 'w') as f:
        f.write(kg.to_json())
    
    file_size = os.path.getsize(output_json)
    print(f"✓ Saved to: {output_json}")
    print(f"  File size: {file_size:,} bytes")
    
    # Generate visualization
    print_section("Step 7: Generating Visualization")
    visualizer = KnowledgeGraphVisualizer(kg)
    output_html = 'output/demo_graph.html'
    
    visualizer.visualize_interactive(output_html)
    
    html_size = os.path.getsize(output_html)
    print(f"✓ Generated: {output_html}")
    print(f"  File size: {html_size:,} bytes")
    print(f"\n  💡 Open in browser: file://{os.path.abspath(output_html)}")
    
    # Display summary
    print_section("Summary")
    summary = visualizer.get_graph_summary()
    print(summary)
    
    # Next steps
    print_section("Next Steps")
    print("""
1. Open the visualization:
   - Navigate to: output/demo_graph.html
   - Or run: python -m webbrowser output/demo_graph.html

2. Try the GUI:
   - Run: python gui.py
   - Load examples from the examples/ directory

3. Try the CLI:
   - Run: python cli.py -i examples/python_ecosystem.txt --visualize

4. Customize:
   - Edit .env to configure LLM (for better extraction)
   - Try different domains (medical, finance, etc.)
   - Process your own text files

For more information:
   - Getting Started: GETTING_STARTED.md
   - Usage Guide: USAGE.md
   - Quick Reference: QUICK_REFERENCE.md
    """)
    
    print("=" * 70)
    print("✓ Demo completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        demo()
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
