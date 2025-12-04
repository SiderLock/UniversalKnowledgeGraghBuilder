#!/usr/bin/env python3
"""Test script for the knowledge graph builder."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


def test_basic_functionality():
    """Test basic knowledge graph functionality."""
    print("Testing Universal Knowledge Graph Builder...")
    print("=" * 50)
    
    # Initialize components
    config = Config()
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=None, model='gpt-3.5-turbo')
    
    # Test text
    test_text = """
    Python is a programming language. It was created by Guido van Rossum.
    Python is used for web development and data science.
    Django is a web framework written in Python.
    """
    
    print("\n1. Extracting from text...")
    extraction = extractor.extract_from_text(test_text, domain='technology')
    print(f"   Extracted {len(extraction['entities'])} entities")
    print(f"   Extracted {len(extraction['relationships'])} relationships")
    
    print("\n2. Building knowledge graph...")
    kg.merge_from_extraction(extraction)
    stats = kg.get_stats()
    print(f"   Graph contains {stats['num_entities']} entities")
    print(f"   Graph contains {stats['num_relationships']} relationships")
    
    print("\n3. Displaying entities:")
    for entity in kg.get_entities()[:5]:  # Show first 5
        print(f"   - {entity.get('label', entity['id'])} ({entity.get('type', 'Entity')})")
    
    print("\n4. Displaying relationships:")
    for rel in kg.get_relationships()[:5]:  # Show first 5
        print(f"   - {rel['source']} --[{rel['relation']}]--> {rel['target']}")
    
    print("\n5. Testing visualization...")
    visualizer = KnowledgeGraphVisualizer(kg)
    summary = visualizer.get_graph_summary()
    print(summary)
    
    print("\n6. Testing JSON serialization...")
    json_str = kg.to_json()
    print(f"   JSON size: {len(json_str)} characters")
    
    # Test loading from JSON
    kg2 = KnowledgeGraph()
    kg2.from_json(json_str)
    print(f"   Loaded graph has {kg2.graph.number_of_nodes()} entities")
    
    print("\n" + "=" * 50)
    print("✓ All tests passed!")
    return True


if __name__ == "__main__":
    try:
        test_basic_functionality()
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
