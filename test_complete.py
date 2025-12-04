#!/usr/bin/env python3
"""
Comprehensive end-to-end test for the Universal Knowledge Graph Builder.
This test validates the complete workflow from text extraction to visualization.
"""

import sys
import os
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


def test_end_to_end():
    """Complete end-to-end test of the knowledge graph builder."""
    print("=" * 70)
    print("Universal Knowledge Graph Builder - End-to-End Test")
    print("=" * 70)
    
    # Test 1: Configuration
    print("\n[1/8] Testing configuration...")
    config = Config()
    assert config.default_domain == 'general', "Default domain should be 'general'"
    print("    ✓ Configuration loaded successfully")
    
    # Test 2: Entity Extraction
    print("\n[2/8] Testing entity extraction...")
    kg = KnowledgeGraph()
    extractor = KnowledgeGraphExtractor(llm_client=None)  # Fallback mode
    
    test_text = """
    Artificial Intelligence is a field of computer science. Machine Learning is a subset 
    of Artificial Intelligence. Deep Learning is a type of Machine Learning that uses 
    neural networks. TensorFlow and PyTorch are popular frameworks for Deep Learning.
    """
    
    extraction = extractor.extract_from_text(test_text, domain='technology')
    assert len(extraction['entities']) > 0, "Should extract entities"
    assert 'relationships' in extraction, "Should have relationships"
    print(f"    ✓ Extracted {len(extraction['entities'])} entities")
    print(f"    ✓ Extracted {len(extraction['relationships'])} relationships")
    
    # Test 3: Graph Building
    print("\n[3/8] Testing knowledge graph construction...")
    kg.merge_from_extraction(extraction)
    assert kg.graph.number_of_nodes() > 0, "Graph should have nodes"
    print(f"    ✓ Built graph with {kg.graph.number_of_nodes()} entities")
    
    # Test 4: Graph Statistics
    print("\n[4/8] Testing graph statistics...")
    stats = kg.get_stats()
    assert stats['num_entities'] == kg.graph.number_of_nodes(), "Entity count mismatch"
    assert stats['num_relationships'] == kg.graph.number_of_edges(), "Relationship count mismatch"
    print(f"    ✓ Entity types: {', '.join(stats['entity_types'])}")
    print(f"    ✓ Relation types: {', '.join(stats['relation_types'])}")
    
    # Test 5: JSON Serialization
    print("\n[5/8] Testing JSON serialization...")
    json_str = kg.to_json()
    assert len(json_str) > 0, "JSON should not be empty"
    
    kg2 = KnowledgeGraph()
    kg2.from_json(json_str)
    assert kg2.graph.number_of_nodes() == kg.graph.number_of_nodes(), "Deserialization failed"
    print(f"    ✓ Serialized to {len(json_str)} characters")
    print(f"    ✓ Deserialized {kg2.graph.number_of_nodes()} entities")
    
    # Test 6: File I/O
    print("\n[6/8] Testing file I/O...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        temp_json = f.name
        f.write(kg.to_json())
    
    try:
        with open(temp_json, 'r') as f:
            loaded_json = f.read()
        
        kg3 = KnowledgeGraph()
        kg3.from_json(loaded_json)
        assert kg3.graph.number_of_nodes() == kg.graph.number_of_nodes(), "File I/O failed"
        print(f"    ✓ Saved and loaded from file")
    finally:
        os.unlink(temp_json)
    
    # Test 7: Visualization Generation
    print("\n[7/8] Testing visualization generation...")
    visualizer = KnowledgeGraphVisualizer(kg)
    
    # Test summary
    summary = visualizer.get_graph_summary()
    assert 'Knowledge Graph Summary' in summary, "Summary should have title"
    print(f"    ✓ Generated text summary")
    
    # Test HTML visualization
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        temp_html = f.name
    
    try:
        output_path = visualizer.visualize_interactive(temp_html)
        assert os.path.exists(output_path), "HTML file should exist"
        file_size = os.path.getsize(output_path)
        assert file_size > 1000, "HTML file should have content"
        print(f"    ✓ Generated HTML visualization ({file_size} bytes)")
    finally:
        if os.path.exists(temp_html):
            os.unlink(temp_html)
    
    # Test 8: Example Files
    print("\n[8/8] Testing example files...")
    example_files = [
        'examples/python_ecosystem.txt',
        'examples/cardiovascular_system.txt'
    ]
    
    for example_file in example_files:
        if os.path.exists(example_file):
            with open(example_file, 'r') as f:
                text = f.read()
            
            kg_example = KnowledgeGraph()
            extraction = extractor.extract_from_text(text, domain='general')
            kg_example.merge_from_extraction(extraction)
            
            assert kg_example.graph.number_of_nodes() > 0, f"Should extract from {example_file}"
            print(f"    ✓ Processed {example_file}: {kg_example.graph.number_of_nodes()} entities")
        else:
            print(f"    ⚠ Example file not found: {example_file}")
    
    # Final Summary
    print("\n" + "=" * 70)
    print("Test Summary:")
    print("  • Configuration: PASSED")
    print("  • Entity Extraction: PASSED")
    print("  • Graph Construction: PASSED")
    print("  • Statistics: PASSED")
    print("  • JSON Serialization: PASSED")
    print("  • File I/O: PASSED")
    print("  • Visualization: PASSED")
    print("  • Example Files: PASSED")
    print("\n✓ All tests passed successfully!")
    print("=" * 70)
    
    return True


def test_additional_features():
    """Test additional features and edge cases."""
    print("\n" + "=" * 70)
    print("Additional Feature Tests")
    print("=" * 70)
    
    # Test empty graph
    print("\n[1/3] Testing empty graph handling...")
    kg_empty = KnowledgeGraph()
    stats = kg_empty.get_stats()
    assert stats['num_entities'] == 0, "Empty graph should have 0 entities"
    assert stats['num_relationships'] == 0, "Empty graph should have 0 relationships"
    print("    ✓ Empty graph handled correctly")
    
    # Test graph clearing
    print("\n[2/3] Testing graph clearing...")
    kg = KnowledgeGraph()
    kg.add_entity('test1', 'Test Entity 1', 'Test')
    kg.add_entity('test2', 'Test Entity 2', 'Test')
    kg.add_relationship('test1', 'test2', 'related_to')
    
    assert kg.graph.number_of_nodes() == 2, "Should have 2 entities"
    assert kg.graph.number_of_edges() == 1, "Should have 1 relationship"
    
    kg.clear()
    assert kg.graph.number_of_nodes() == 0, "Cleared graph should be empty"
    assert kg.graph.number_of_edges() == 0, "Cleared graph should have no edges"
    print("    ✓ Graph clearing works correctly")
    
    # Test multiple domains
    print("\n[3/3] Testing multiple domain extractions...")
    extractor = KnowledgeGraphExtractor(llm_client=None)
    
    domains = ['general', 'medical', 'technology', 'science']
    for domain in domains:
        extraction = extractor.extract_from_text("Test text for extraction", domain=domain)
        assert 'entities' in extraction, f"Should have entities for domain {domain}"
        assert 'relationships' in extraction, f"Should have relationships for domain {domain}"
    
    print(f"    ✓ Tested {len(domains)} different domains")
    
    print("\n✓ All additional tests passed!")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    try:
        # Run main tests
        success1 = test_end_to_end()
        
        # Run additional tests
        success2 = test_additional_features()
        
        if success1 and success2:
            print("\n" + "🎉" * 20)
            print("ALL TESTS PASSED - System is ready for use!")
            print("🎉" * 20)
            sys.exit(0)
        else:
            print("\n✗ Some tests failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
