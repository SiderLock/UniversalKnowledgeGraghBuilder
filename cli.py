#!/usr/bin/env python3
"""
Command-line interface for Universal Knowledge Graph Builder.
Alternative to the GUI for users who prefer CLI.
"""

import sys
import os
import argparse
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from kg_builder import KnowledgeGraph, KnowledgeGraphExtractor, KnowledgeGraphVisualizer, Config


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Universal Knowledge Graph Builder - CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract from text file and visualize
  python cli.py -i input.txt -o graph.json --visualize
  
  # Extract from text file with specific domain
  python cli.py -i medical_text.txt -o medical_graph.json -d medical
  
  # Visualize existing graph
  python cli.py --load graph.json --visualize
        """
    )
    
    parser.add_argument('-i', '--input', type=str,
                       help='Input text file to extract knowledge graph from')
    parser.add_argument('-o', '--output', type=str,
                       help='Output JSON file to save knowledge graph')
    parser.add_argument('-d', '--domain', type=str, default='general',
                       choices=['general', 'medical', 'finance', 'technology', 
                               'science', 'legal', 'education'],
                       help='Domain context for extraction (default: general)')
    parser.add_argument('--load', type=str,
                       help='Load existing knowledge graph from JSON file')
    parser.add_argument('--visualize', action='store_true',
                       help='Generate HTML visualization')
    parser.add_argument('--viz-output', type=str, default='output/knowledge_graph.html',
                       help='Output path for visualization HTML (default: output/knowledge_graph.html)')
    parser.add_argument('--stats', action='store_true',
                       help='Show knowledge graph statistics')
    
    args = parser.parse_args()
    
    # Initialize components
    config = Config()
    kg = KnowledgeGraph()
    
    # Load existing graph or extract from input
    if args.load:
        print(f"Loading knowledge graph from {args.load}...")
        with open(args.load, 'r', encoding='utf-8') as f:
            kg.from_json(f.read())
        print(f"✓ Loaded graph with {kg.graph.number_of_nodes()} entities")
    
    elif args.input:
        print(f"Reading input from {args.input}...")
        with open(args.input, 'r', encoding='utf-8') as f:
            text = f.read()
        
        print(f"Extracting knowledge graph (domain: {args.domain})...")
        extractor = KnowledgeGraphExtractor(
            llm_client=config.get_llm_client(),
            model=config.default_model
        )
        
        extraction = extractor.extract_from_text(text, domain=args.domain)
        kg.merge_from_extraction(extraction)
        
        print(f"✓ Extracted {len(extraction['entities'])} entities")
        print(f"✓ Extracted {len(extraction['relationships'])} relationships")
    
    else:
        parser.print_help()
        print("\nError: Please provide either --input or --load")
        sys.exit(1)
    
    # Save graph if output specified
    if args.output:
        print(f"\nSaving knowledge graph to {args.output}...")
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(kg.to_json())
        print(f"✓ Saved to {args.output}")
    
    # Show statistics
    if args.stats or not (args.visualize or args.output):
        print("\n" + "=" * 60)
        visualizer = KnowledgeGraphVisualizer(kg)
        print(visualizer.get_graph_summary())
        
        print("\nEntities:")
        for entity in kg.get_entities()[:10]:
            print(f"  • {entity.get('label', entity['id'])} ({entity.get('type', 'Entity')})")
        
        if kg.graph.number_of_nodes() > 10:
            print(f"  ... and {kg.graph.number_of_nodes() - 10} more")
        
        print("\nRelationships:")
        for rel in kg.get_relationships()[:10]:
            print(f"  • {rel['source']} --[{rel.get('relation', 'related_to')}]--> {rel['target']}")
        
        if kg.graph.number_of_edges() > 10:
            print(f"  ... and {kg.graph.number_of_edges() - 10} more")
        print("=" * 60)
    
    # Generate visualization
    if args.visualize:
        print(f"\nGenerating visualization...")
        visualizer = KnowledgeGraphVisualizer(kg)
        os.makedirs(os.path.dirname(args.viz_output) or '.', exist_ok=True)
        output_path = visualizer.visualize_interactive(args.viz_output)
        print(f"✓ Visualization saved to {output_path}")
        print(f"  Open in browser: file://{os.path.abspath(output_path)}")
    
    print("\n✓ Done!")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
