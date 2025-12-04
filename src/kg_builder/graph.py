"""Knowledge graph data structure and operations."""

import networkx as nx
from typing import List, Dict, Any, Optional
import json


class KnowledgeGraph:
    """Represents a knowledge graph with entities and relationships."""

    def __init__(self):
        """Initialize an empty knowledge graph."""
        self.graph = nx.DiGraph()
        self.entity_types = set()
        self.relation_types = set()

    def add_entity(self, entity_id: str, label: str, entity_type: str = "Entity", 
                   attributes: Optional[Dict[str, Any]] = None):
        """Add an entity to the knowledge graph.
        
        Args:
            entity_id: Unique identifier for the entity
            label: Display label for the entity
            entity_type: Type/category of the entity
            attributes: Additional attributes for the entity
        """
        attrs = attributes or {}
        attrs.update({
            'label': label,
            'type': entity_type
        })
        self.graph.add_node(entity_id, **attrs)
        self.entity_types.add(entity_type)

    def add_relationship(self, source: str, target: str, relation: str,
                        attributes: Optional[Dict[str, Any]] = None):
        """Add a relationship between entities.
        
        Args:
            source: Source entity ID
            target: Target entity ID
            relation: Type of relationship
            attributes: Additional attributes for the relationship
        """
        attrs = attributes or {}
        attrs['relation'] = relation
        self.graph.add_edge(source, target, **attrs)
        self.relation_types.add(relation)

    def get_entities(self) -> List[Dict[str, Any]]:
        """Get all entities in the graph."""
        entities = []
        for node, attrs in self.graph.nodes(data=True):
            entity = {'id': node}
            entity.update(attrs)
            entities.append(entity)
        return entities

    def get_relationships(self) -> List[Dict[str, Any]]:
        """Get all relationships in the graph."""
        relationships = []
        for source, target, attrs in self.graph.edges(data=True):
            rel = {
                'source': source,
                'target': target
            }
            rel.update(attrs)
            relationships.append(rel)
        return relationships

    def merge_from_extraction(self, extraction: Dict[str, Any]):
        """Merge extracted data into the knowledge graph.
        
        Args:
            extraction: Dictionary with 'entities' and 'relationships' keys
        """
        # Add entities
        for entity in extraction.get('entities', []):
            entity_id = entity.get('id', entity.get('label', ''))
            label = entity.get('label', entity_id)
            entity_type = entity.get('type', 'Entity')
            
            # Get additional attributes
            attrs = {k: v for k, v in entity.items() 
                    if k not in ['id', 'label', 'type']}
            
            self.add_entity(entity_id, label, entity_type, attrs)

        # Add relationships
        for rel in extraction.get('relationships', []):
            source = rel.get('source', '')
            target = rel.get('target', '')
            relation = rel.get('relation', 'related_to')
            
            if source and target:
                # Get additional attributes
                attrs = {k: v for k, v in rel.items() 
                        if k not in ['source', 'target', 'relation']}
                
                self.add_relationship(source, target, relation, attrs)

    def to_dict(self) -> Dict[str, Any]:
        """Convert knowledge graph to dictionary format."""
        return {
            'entities': self.get_entities(),
            'relationships': self.get_relationships(),
            'stats': {
                'num_entities': self.graph.number_of_nodes(),
                'num_relationships': self.graph.number_of_edges(),
                'entity_types': list(self.entity_types),
                'relation_types': list(self.relation_types)
            }
        }

    def to_json(self) -> str:
        """Convert knowledge graph to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def from_dict(self, data: Dict[str, Any]):
        """Load knowledge graph from dictionary format.
        
        Args:
            data: Dictionary with 'entities' and 'relationships' keys
        """
        self.graph.clear()
        self.entity_types.clear()
        self.relation_types.clear()
        
        # Load entities
        for entity in data.get('entities', []):
            entity_id = entity.get('id', '')
            label = entity.get('label', entity_id)
            entity_type = entity.get('type', 'Entity')
            attrs = {k: v for k, v in entity.items() 
                    if k not in ['id', 'label', 'type']}
            self.add_entity(entity_id, label, entity_type, attrs)

        # Load relationships
        for rel in data.get('relationships', []):
            source = rel.get('source', '')
            target = rel.get('target', '')
            relation = rel.get('relation', 'related_to')
            attrs = {k: v for k, v in rel.items() 
                    if k not in ['source', 'target', 'relation']}
            if source and target:
                self.add_relationship(source, target, relation, attrs)

    def from_json(self, json_str: str):
        """Load knowledge graph from JSON string.
        
        Args:
            json_str: JSON string representation of the graph
        """
        data = json.loads(json_str)
        self.from_dict(data)

    def clear(self):
        """Clear all data from the knowledge graph."""
        self.graph.clear()
        self.entity_types.clear()
        self.relation_types.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the knowledge graph."""
        return {
            'num_entities': self.graph.number_of_nodes(),
            'num_relationships': self.graph.number_of_edges(),
            'entity_types': list(self.entity_types),
            'relation_types': list(self.relation_types),
            'avg_degree': sum(dict(self.graph.degree()).values()) / max(self.graph.number_of_nodes(), 1)
        }
