#!/usr/bin/env python3
"""Test visualization generation."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer


def test_visualization():
    """Test visualization generation."""
    print("Testing visualization generation...")
    
    # Initialize components
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=None)
    
    # Load example text
    with open('examples/python_ecosystem.txt', 'r') as f:
        text = f.read()
    
    print("1. Extracting knowledge graph from example text...")
    extraction = extractor.extract_from_text(text, domain='technology')
    kg.merge_from_extraction(extraction)
    
    print(f"   Extracted {kg.graph.number_of_nodes()} entities")
    print(f"   Extracted {kg.graph.number_of_edges()} relationships")
    
    # Generate visualization
    print("\n2. Generating interactive visualization...")
    visualizer = KnowledgeGraphVisualizer(kg)
    output_path = visualizer.visualize_interactive('output/test_visualization.html')
    
    print(f"   ✓ Visualization saved to: {output_path}")
    
    # Check if file exists
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"   ✓ File size: {file_size} bytes")
        return True
    else:
        print(f"   ✗ File not created!")
        return False


if __name__ == "__main__":
    try:
        success = test_visualization()
        if success:
            print("\n✓ Visualization test passed!")
        else:
            print("\n✗ Visualization test failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
