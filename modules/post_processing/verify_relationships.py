#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证工艺关系网络的创建
"""

import logging
from neo4j import GraphDatabase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RelationshipVerifier:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"成功连接到Neo4j数据库: {uri}")

    def close(self):
        self.driver.close()
        logger.info("已断开与Neo4j的连接。")

    def verify_relationship_types(self):
        """验证关系类型"""
        with self.driver.session() as session:
            # 统计每种关系类型的数量
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as relationship_type, count(r) as count
                ORDER BY count DESC
            """)
            
            logger.info("关系类型统计:")
            for record in result:
                logger.info(f"  {record['relationship_type']}: {record['count']} 条")

    def verify_process_nodes(self):
        """验证工艺节点"""
        with self.driver.session() as session:
            # 统计工艺节点数量
            result = session.run("""
                MATCH (p:Process)
                RETURN count(p) as process_count
            """)
            
            count = result.single()['process_count']
            logger.info(f"工艺节点总数: {count}")
            
            # 显示工艺节点示例
            result = session.run("""
                MATCH (p:Process)
                RETURN p.name, p.process_type, p.target_product, p.raw_materials, p.description
                LIMIT 10
            """)
            
            logger.info("工艺节点示例:")
            for record in result:
                logger.info(f"  工艺: {record['p.name']}")
                logger.info(f"    类型: {record['p.process_type']}")
                logger.info(f"    目标产品: {record['p.target_product']}")
                logger.info(f"    原料: {record['p.raw_materials']}")
                logger.info(f"    描述: {record['p.description']}")
                logger.info("")

    def verify_process_relationships(self):
        """验证工艺关系网络"""
        with self.driver.session() as session:
            # 检查"参加"关系 - 原料参加工艺
            result = session.run("""
                MATCH (c:Chemical)-[r:参加]->(p:Process)
                RETURN count(r) as participate_count
            """)
            participate_count = result.single()['participate_count']
            logger.info(f"'参加'关系数量: {participate_count}")
            
            # 检查"原料"关系 - 工艺使用原料
            result = session.run("""
                MATCH (p:Process)-[r:原料]->(c:Chemical)
                RETURN count(r) as material_count
            """)
            material_count = result.single()['material_count']
            logger.info(f"'原料'关系数量: {material_count}")
            
            # 检查"来源"关系 - 化学品来源于工艺
            result = session.run("""
                MATCH (c:Chemical)-[r:来源]->(p:Process)
                RETURN count(r) as source_count
            """)
            source_count = result.single()['source_count']
            logger.info(f"'来源'关系数量: {source_count}")
            
            # 检查"获得"关系 - 工艺获得化学品
            result = session.run("""
                MATCH (p:Process)-[r:获得]->(c:Chemical)
                RETURN count(r) as obtain_count
            """)
            obtain_count = result.single()['obtain_count']
            logger.info(f"'获得'关系数量: {obtain_count}")

    def show_complete_process_example(self):
        """显示完整的工艺关系示例"""
        with self.driver.session() as session:
            # 找一个具有完整关系的工艺节点
            result = session.run("""
                MATCH (p:Process)
                WHERE EXISTS((p)-[:获得]->(:Chemical)) AND EXISTS((:Chemical)-[:参加]->(p))
                WITH p LIMIT 1
                
                // 获取工艺的所有相关信息
                OPTIONAL MATCH (material:Chemical)-[:参加]->(p)
                OPTIONAL MATCH (p)-[:原料]->(material2:Chemical)
                OPTIONAL MATCH (product:Chemical)-[:来源]->(p)
                OPTIONAL MATCH (p)-[:获得]->(product2:Chemical)
                
                RETURN p.name as process_name,
                       p.target_product as target_product,
                       collect(DISTINCT material.name) as materials_participate,
                       collect(DISTINCT material2.name) as materials_used,
                       collect(DISTINCT product.name) as products_source,
                       collect(DISTINCT product2.name) as products_obtained
            """)
            
            logger.info("完整工艺关系示例:")
            for record in result:
                logger.info(f"工艺名称: {record['process_name']}")
                logger.info(f"目标产品: {record['target_product']}")
                logger.info(f"参加工艺的原料: {record['materials_participate']}")
                logger.info(f"工艺使用的原料: {record['materials_used']}")
                logger.info(f"来源于工艺的产品: {record['products_source']}")
                logger.info(f"工艺获得的产品: {record['products_obtained']}")


def main():
    verifier = RelationshipVerifier()
    
    try:
        logger.info("开始验证工艺关系网络...")
        
        # 验证关系类型
        verifier.verify_relationship_types()
        
        # 验证工艺节点
        verifier.verify_process_nodes()
        
        # 验证工艺关系
        verifier.verify_process_relationships()
        
        # 显示完整示例
        verifier.show_complete_process_example()
        
        logger.info("关系网络验证完成！")
        
    except Exception as e:
        logger.error(f"验证过程中发生错误: {e}")
    finally:
        verifier.close()


if __name__ == "__main__":
    main()
