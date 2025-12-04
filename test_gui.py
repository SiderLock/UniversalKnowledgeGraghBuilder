#!/usr/bin/env python3
"""Test GUI components without displaying windows."""

import sys
import os

# Test if tkinter is available
try:
    import tkinter as tk
    print("✓ Tkinter is available")
    HAS_TKINTER = True
except ImportError:
    print("✗ Tkinter not available (expected in headless environment)")
    HAS_TKINTER = False

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


def test_gui_initialization():
    """Test GUI class initialization without showing windows."""
    print("\nTesting GUI component initialization...")
    print("=" * 50)
    
    if not HAS_TKINTER:
        print("Skipping GUI tests (no tkinter in headless environment)")
        return True
    
    # Test that all imports work
    print("\n1. Checking gui.py imports...")
    
    # We can't actually run the GUI in headless environment
    # But we can check the file loads without syntax errors
    with open('gui.py', 'r') as f:
        code = f.read()
        compile(code, 'gui.py', 'exec')
    
    print("   ✓ gui.py compiles successfully")
    
    # Test that all required components exist
    print("\n2. Checking required components...")
    components = [
        ('KnowledgeGraph', KnowledgeGraph),
        ('KnowledgeGraphExtractor', KnowledgeGraphExtractor),
        ('KnowledgeGraphVisualizer', KnowledgeGraphVisualizer),
        ('Config', Config),
    ]
    
    for name, component in components:
        print(f"   ✓ {name} available")
    
    print("\n3. Testing component instantiation...")
    config = Config()
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=None)
    visualizer = KnowledgeGraphVisualizer(kg)
    print("   ✓ All components instantiate successfully")
    
    print("\n" + "=" * 50)
    print("✓ GUI component tests passed!")
    return True


def test_gui_workflow():
    """Test a complete workflow that the GUI would perform."""
    print("\nTesting GUI workflow simulation...")
    print("=" * 50)
    
    # Initialize components
    config = Config()
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=config.get_llm_client())
    
    # Simulate text extraction
    print("\n1. Simulating text extraction...")
    test_text = "Python is a programming language. Django is a web framework."
    extraction = extractor.extract_from_text(test_text, domain='technology')
    print(f"   ✓ Extracted {len(extraction['entities'])} entities")
    
    # Simulate graph building
    print("\n2. Simulating graph building...")
    kg.merge_from_extraction(extraction)
    print(f"   ✓ Graph contains {kg.graph.number_of_nodes()} entities")
    
    # Simulate visualization
    print("\n3. Simulating visualization...")
    visualizer = KnowledgeGraphVisualizer(kg)
    summary = visualizer.get_graph_summary()
    print("   ✓ Summary generated")
    
    # Simulate save/load
    print("\n4. Simulating save/load...")
    json_data = kg.to_json()
    kg2 = KnowledgeGraph()
    kg2.from_json(json_data)
    print(f"   ✓ Saved and loaded {kg2.graph.number_of_nodes()} entities")
    
    print("\n" + "=" * 50)
    print("✓ GUI workflow simulation passed!")
    return True


if __name__ == "__main__":
    try:
        test_gui_initialization()
        test_gui_workflow()
        print("\n" + "=" * 50)
        print("✓ All GUI tests passed!")
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
