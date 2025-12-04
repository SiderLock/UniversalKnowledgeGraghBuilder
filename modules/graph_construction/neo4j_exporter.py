"""
Neo4j导出工具
将处理后的数据转换为Neo4j导入格式
"""

import pandas as pd
import json
import os
from typing import Dict, List, Any, Optional


class Neo4jExporter:
    def __init__(self, config_path: Optional[str] = None):
        """初始化Neo4j导出器"""
        self.config_path = config_path
        self.node_id_counter = 0
        self.nodes = {}  # 存储所有节点
        self.relationships = []  # 存储所有关系
        
    def generate_node_id(self) -> str:
        """生成唯一的节点ID"""
        self.node_id_counter += 1
        return f"node_{self.node_id_counter}"
    
    def create_chemical_nodes(self, processed_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """创建化学品节点"""
        chemical_nodes = {}
        
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            if not basic_info.get('品名'):
                continue
            
            chemical_name = basic_info['品名']
            node_id = self.generate_node_id()
            
            # 创建化学品节点
            node_data = {
                'id': node_id,
                'label': 'Chemical',
                'properties': {
                    '品名': chemical_name,
                    'CAS号': basic_info.get('CAS号', ''),
                    '外观与性状': basic_info.get('外观与性状', ''),
                    '室温状态': basic_info.get('室温状态', ''),
                    '原始行号': record.get('原始行号', '')
                }
            }
            
            # 清理空值
            node_data['properties'] = {k: v for k, v in node_data['properties'].items() if v}
            
            self.nodes[node_id] = node_data
            chemical_nodes[chemical_name] = node_id
        
        return chemical_nodes
    
    def create_alias_relationships(self, processed_data: List[Dict[str, Any]], chemical_nodes: Dict[str, str]):
        """创建别名关系"""
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            chemical_name = basic_info.get('品名')
            
            if not chemical_name or chemical_name not in chemical_nodes:
                continue
            
            main_chemical_id = chemical_nodes[chemical_name]
            aliases = record.get('aliases', [])
            
            for alias in aliases:
                if not alias or not alias.strip():
                    continue

                alias_id = None
                if alias in chemical_nodes:
                    alias_id = chemical_nodes[alias]
                else:
                    alias_id = self.generate_node_id()
                    alias_node = {
                        'id': alias_id,
                        'label': 'Chemical',
                        'properties': {
                            '品名': alias,
                            '是别名': True
                        }
                    }
                    self.nodes[alias_id] = alias_node
                    chemical_nodes[alias] = alias_id
                
                if alias_id != main_chemical_id:
                    relationship = {
                        'from': main_chemical_id,
                        'to': alias_id,
                        'type': 'HAS_ALIAS',
                        'properties': {}
                    }
                    self.relationships.append(relationship)
    
    def create_property_nodes(self, processed_data: List[Dict[str, Any]], chemical_nodes: Dict[str, str]):
        """创建物理性质节点和关系"""
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            chemical_name = basic_info.get('品名')
            
            if not chemical_name or chemical_name not in chemical_nodes:
                continue
            
            chemical_id = chemical_nodes[chemical_name]
            physical_properties = record.get('physical_properties', {})
            
            for prop_name, prop_values in physical_properties.items():
                if not prop_values:
                    continue

                # prop_values 现在是一个字典列表
                for prop_data in prop_values:
                    prop_id = self.generate_node_id()
                    
                    # 构建节点属性
                    node_props = {'属性名': prop_name}
                    node_props.update(prop_data) # 合并提取的属性

                    prop_node = {
                        'id': prop_id,
                        'label': 'PhysicalProperty',
                        'properties': node_props
                    }
                    self.nodes[prop_id] = prop_node
                    
                    relationship = {
                        'from': chemical_id,
                        'to': prop_id,
                        'type': 'HAS_PROPERTY',
                        'properties': {}
                    }
                    self.relationships.append(relationship)

    def create_storage_nodes(self, processed_data: List[Dict[str, Any]], chemical_nodes: Dict[str, str]):
        """创建存储条件节点和关系"""
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            chemical_name = basic_info.get('品名')
            
            if not chemical_name or chemical_name not in chemical_nodes:
                continue
            
            chemical_id = chemical_nodes[chemical_name]
            storage_conditions = record.get('storage_conditions', {})
            
            for condition_type, conditions in storage_conditions.items():
                for condition in conditions:
                    storage_id = self.generate_node_id()
                    storage_node = {
                        'id': storage_id,
                        'label': 'Storage',
                        'properties': {
                            '存储类型': condition_type,
                            '存储条件': condition
                        }
                    }
                    self.nodes[storage_id] = storage_node
                    
                    relationship = {
                        'from': chemical_id,
                        'to': storage_id,
                        'type': 'REQUIRES_STORAGE',
                        'properties': {}
                    }
                    self.relationships.append(relationship)
    
    def create_solubility_nodes(self, processed_data: List[Dict[str, Any]], chemical_nodes: Dict[str, str]):
        """创建溶解性节点和关系"""
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            chemical_name = basic_info.get('品名')
            
            if not chemical_name or chemical_name not in chemical_nodes:
                continue
            
            chemical_id = chemical_nodes[chemical_name]
            solubility_info = record.get('solubility', {})
            
            # 处理定性溶解性
            for solvent, solubility in solubility_info.get('qualitative', {}).items():
                solubility_id = self.generate_node_id()
                solubility_node = {
                    'id': solubility_id,
                    'label': 'QualitativeSolubility',
                    'properties': {
                        '溶剂': solvent,
                        '溶解性': solubility
                    }
                }
                self.nodes[solubility_id] = solubility_node
                
                relationship = {
                    'from': chemical_id,
                    'to': solubility_id,
                    'type': 'HAS_SOLUBILITY',
                    'properties': {}
                }
                self.relationships.append(relationship)

            # 处理定量溶解性
            for quant_data in solubility_info.get('quantitative', []):
                solubility_id = self.generate_node_id()
                solubility_node = {
                    'id': solubility_id,
                    'label': 'QuantitativeSolubility',
                    'properties': quant_data # 直接使用提取的字典
                }
                self.nodes[solubility_id] = solubility_node
                
                relationship = {
                    'from': chemical_id,
                    'to': solubility_id,
                    'type': 'HAS_SOLUBILITY',
                    'properties': {}
                }
                self.relationships.append(relationship)

    def create_hazard_nodes(self, processed_data: List[Dict[str, Any]], chemical_nodes: Dict[str, str]):
        """创建危害信息节点和关系"""
        for record in processed_data:
            basic_info = record.get('basic_info', {})
            chemical_name = basic_info.get('品名')
            
            if not chemical_name or chemical_name not in chemical_nodes:
                continue
            
            chemical_id = chemical_nodes[chemical_name]
            hazards = record.get('hazards', {})
            
            for hazard_category, hazard_data in hazards.items():
                if isinstance(hazard_data, list):
                    for hazard in hazard_data:
                        hazard_id = self.generate_node_id()
                        hazard_node = {
                            'id': hazard_id,
                            'label': 'SafetyInfo',
                            'properties': {
                                '危害类型': hazard_category,
                                '危害描述': hazard
                            }
                        }
                        self.nodes[hazard_id] = hazard_node
                        
                        relationship = {
                            'from': chemical_id,
                            'to': hazard_id,
                            'type': 'HAS_HAZARD',
                            'properties': {}
                        }
                        self.relationships.append(relationship)
                
                elif isinstance(hazard_data, dict):
                    for sub_category, sub_hazards in hazards.items():
                        for hazard in sub_hazards:
                            hazard_id = self.generate_node_id()
                            hazard_node = {
                                'id': hazard_id,
                                'label': 'SafetyInfo',
                                'properties': {
                                    '危害类型': f"{hazard_category}_{sub_category}",
                                    '危害描述': hazard
                                }
                            }
                            self.nodes[hazard_id] = hazard_node
                            
                            relationship = {
                                'from': chemical_id,
                                'to': hazard_id,
                                'type': 'HAS_HAZARD',
                                'properties': {}
                            }
                            self.relationships.append(relationship)
    
    def export_to_neo4j_format(self, processed_data: List[Dict[str, Any]], output_dir: str):
        """导出为Neo4j格式"""
        print("开始导出Neo4j格式...")
        
        os.makedirs(output_dir, exist_ok=True)
        self.node_id_counter = 0
        self.nodes = {}
        self.relationships = []
        
        chemical_nodes = self.create_chemical_nodes(processed_data)
        print(f"创建了 {len(chemical_nodes)} 个化学品节点")
        
        self.create_alias_relationships(processed_data, chemical_nodes)
        self.create_property_nodes(processed_data, chemical_nodes)
        self.create_storage_nodes(processed_data, chemical_nodes)
        self.create_solubility_nodes(processed_data, chemical_nodes)
        self.create_hazard_nodes(processed_data, chemical_nodes)
        
        print(f"总共创建了 {len(self.nodes)} 个节点")
        print(f"总共创建了 {len(self.relationships)} 个关系")
        
        nodes_file = os.path.join(output_dir, 'nodes.csv')
        self.save_nodes_csv(nodes_file)
        
        relationships_file = os.path.join(output_dir, 'relationships.csv')
        self.save_relationships_csv(relationships_file)
        
        cypher_file = os.path.join(output_dir, 'import_script.cypher')
        self.generate_cypher_script(cypher_file)
        
        json_file = os.path.join(output_dir, 'neo4j_data.json')
        self.save_json_format(json_file)
        
        print(f"Neo4j导出文件已保存到: {output_dir}")
        return {
            'nodes_file': nodes_file,
            'relationships_file': relationships_file,
            'cypher_file': cypher_file,
            'json_file': json_file
        }
    
    def save_nodes_csv(self, file_path: str):
        """保存节点为CSV格式"""
        nodes_data = []
        for node in self.nodes.values():
            row = {
                'id': node['id'],
                'label': node['label']
            }
            for key, value in node['properties'].items():
                row[key] = value
            nodes_data.append(row)
        
        df = pd.DataFrame(nodes_data)
        # 修复Excel兼容性：使用 'utf-8-sig' 编码写入BOM
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    def save_relationships_csv(self, file_path: str):
        """保存关系为CSV格式"""
        relationships_data = []
        for rel in self.relationships:
            row = {
                'from_id': rel['from'],
                'to_id': rel['to'],
                'type': rel['type']
            }
            for key, value in rel['properties'].items():
                row[key] = value
            relationships_data.append(row)
        
        df = pd.DataFrame(relationships_data)
        # 修复Excel兼容性：使用 'utf-8-sig' 编码写入BOM
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    def generate_cypher_script(self, file_path: str):
        """生成Cypher导入脚本"""
        cypher_script = """
// 1. 清空数据库（可选，请谨慎使用）
// MATCH (n) DETACH DELETE n;

// 2. 创建约束
// 为Chemical节点的“品名”属性创建唯一性约束，以确保数据一致性和MERGE操作的性能
CREATE CONSTRAINT chemical_name IF NOT EXISTS FOR (c:Chemical) REQUIRE c.品名 IS UNIQUE;

// 3. 导入Chemical节点 (使用 apoc.periodic.iterate)
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row WITH row WHERE row.label = 'Chemical' AND row.品名 IS NOT NULL AND trim(row.品名) <> '' RETURN row",
  "MERGE (n:Chemical {品名: row.品名}) SET n += apoc.map.clean(row, ['id', 'label', '品名'], [null, '']), n.temp_id = row.id",
  {batchSize: 1000, parallel: false}
);

// 4. 导入其他所有类型的节点 (使用 apoc.periodic.iterate)
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row WITH row WHERE row.label <> 'Chemical' RETURN row",
  "CALL apoc.create.node([row.label], apoc.map.clean(row, ['id', 'label'], [null, ''])) YIELD node SET node.temp_id = row.id",
  {batchSize: 1000, parallel: false}
);

// 5. 导入关系 (使用 apoc.periodic.iterate)
CALL apoc.periodic.iterate(
  "LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row RETURN row",
  "MATCH (from {temp_id: row.from_id}) MATCH (to {temp_id: row.to_id}) CALL apoc.merge.relationship(from, row.type, {}, apoc.map.clean(row, ['from_id', 'to_id', 'type'], [null, '']), to) YIELD rel",
  {batchSize: 1000, parallel: false}
);

// 6. 清理临时ID
// 关系创建完成后，移除所有节点上的临时ID属性，保持图数据库的整洁。
MATCH (n) WHERE n.temp_id IS NOT NULL
REMOVE n.temp_id;

// 7. 创建索引（提高查询性能）
CREATE INDEX chemical_cas IF NOT EXISTS FOR (c:Chemical) ON (c.CAS号);
CREATE INDEX property_name IF NOT EXISTS FOR (p:PhysicalProperty) ON (p.属性名);
CREATE INDEX hazard_type IF NOT EXISTS FOR (h:SafetyInfo) ON (h.危害类型);
"""
        
        # 使用 'utf-8-sig' 编码写入，以添加BOM，帮助Windows程序正确识别UTF-8
        with open(file_path, 'w', encoding='utf-8-sig') as f:
            f.write(cypher_script)
    
    def save_json_format(self, file_path: str):
        """保存为JSON格式（调试用）"""
        export_data = {
            'nodes': list(self.nodes.values()),
            'relationships': self.relationships,
            'statistics': {
                'total_nodes': len(self.nodes),
                'total_relationships': len(self.relationships),
                'node_types': {},
                'relationship_types': {}
            }
        }
        
        for node in self.nodes.values():
            label = node['label']
            export_data['statistics']['node_types'][label] = export_data['statistics']['node_types'].get(label, 0) + 1
        
        for rel in self.relationships:
            rel_type = rel['type']
            export_data['statistics']['relationship_types'][rel_type] = export_data['statistics']['relationship_types'].get(rel_type, 0) + 1
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)