"""Visualization utilities for knowledge graphs."""

import networkx as nx
from pyvis.network import Network
import matplotlib.pyplot as plt
from typing import Optional
import os


class KnowledgeGraphVisualizer:
    """Visualize knowledge graphs in various formats."""

    def __init__(self, kg):
        """Initialize visualizer with a knowledge graph.
        
        Args:
            kg: KnowledgeGraph instance
        """
        self.kg = kg

    def visualize_interactive(self, output_path: str = "knowledge_graph.html",
                             height: str = "750px", width: str = "100%",
                             notebook: bool = False):
        """Create an interactive HTML visualization using PyVis.
        
        Args:
            output_path: Path to save the HTML file
            height: Height of the visualization
            width: Width of the visualization
            notebook: Whether running in Jupyter notebook
        """
        net = Network(height=height, width=width, notebook=notebook, directed=True)
        
        # Configure physics for better layout
        net.set_options("""
        {
          "physics": {
            "enabled": true,
            "barnesHut": {
              "gravitationalConstant": -8000,
              "centralGravity": 0.3,
              "springLength": 95,
              "springConstant": 0.04
            },
            "minVelocity": 0.75
          }
        }
        """)

        # Add nodes with colors based on entity type
        entity_type_colors = {}
        color_palette = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', 
                        '#FFE66D', '#A8E6CF', '#FF8B94', '#C7CEEA', '#FFDAB9']
        
        for entity in self.kg.get_entities():
            entity_id = entity['id']
            label = entity.get('label', entity_id)
            entity_type = entity.get('type', 'Entity')
            
            # Assign color based on type
            if entity_type not in entity_type_colors:
                color_idx = len(entity_type_colors) % len(color_palette)
                entity_type_colors[entity_type] = color_palette[color_idx]
            
            color = entity_type_colors[entity_type]
            title = f"{label}\nType: {entity_type}"
            
            net.add_node(entity_id, label=label, title=title, color=color)

        # Add edges with labels
        for rel in self.kg.get_relationships():
            source = rel['source']
            target = rel['target']
            relation = rel.get('relation', 'related_to')
            
            net.add_edge(source, target, label=relation, title=relation, arrows='to')

        # Save to file
        net.save_graph(output_path)
        return output_path

    def visualize_static(self, output_path: Optional[str] = None, 
                        figsize: tuple = (12, 8), node_size: int = 2000):
        """Create a static matplotlib visualization.
        
        Args:
            output_path: Optional path to save the figure
            figsize: Figure size (width, height)
            node_size: Size of nodes
        """
        plt.figure(figsize=figsize)
        
        # Get entity type colors
        entity_types = {}
        for entity in self.kg.get_entities():
            entity_id = entity['id']
            entity_type = entity.get('type', 'Entity')
            entity_types[entity_id] = entity_type

        # Create color map
        unique_types = list(set(entity_types.values()))
        color_map = plt.cm.get_cmap('tab10', len(unique_types))
        type_to_color = {t: color_map(i) for i, t in enumerate(unique_types)}
        
        # Node colors
        node_colors = [type_to_color[entity_types.get(node, 'Entity')] 
                      for node in self.kg.graph.nodes()]

        # Draw graph
        pos = nx.spring_layout(self.kg.graph, k=2, iterations=50)
        
        # Draw nodes
        nx.draw_networkx_nodes(self.kg.graph, pos, node_color=node_colors, 
                              node_size=node_size, alpha=0.9)
        
        # Draw edges
        nx.draw_networkx_edges(self.kg.graph, pos, edge_color='gray', 
                              arrows=True, arrowsize=20, alpha=0.5)
        
        # Draw labels
        labels = {entity['id']: entity.get('label', entity['id']) 
                 for entity in self.kg.get_entities()}
        nx.draw_networkx_labels(self.kg.graph, pos, labels, font_size=10)
        
        # Draw edge labels
        edge_labels = {(rel['source'], rel['target']): rel.get('relation', '') 
                      for rel in self.kg.get_relationships()}
        nx.draw_networkx_edge_labels(self.kg.graph, pos, edge_labels, font_size=8)

        plt.title("Knowledge Graph Visualization", fontsize=16, fontweight='bold')
        plt.axis('off')
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        return plt

    def get_graph_summary(self) -> str:
        """Get a text summary of the knowledge graph."""
        stats = self.kg.get_stats()
        
        summary = f"""Knowledge Graph Summary
{'=' * 40}
Entities: {stats['num_entities']}
Relationships: {stats['num_relationships']}
Average Degree: {stats['avg_degree']:.2f}

Entity Types: {', '.join(stats['entity_types']) if stats['entity_types'] else 'None'}
Relation Types: {', '.join(stats['relation_types']) if stats['relation_types'] else 'None'}
"""
        return summary
