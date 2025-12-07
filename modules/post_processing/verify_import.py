#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证Neo4j数据导入结果
"""

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# --- 配置 ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "[2361918131]"
# --- 配置结束 ---

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class Neo4jVerifier:
    """Neo4j数据验证器"""
    
    def __init__(self, uri, user, password):
        """初始化连接"""
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            logger.info(f"成功连接到Neo4j数据库: {uri}")
        except Neo4jError as e:
            logger.error(f"无法连接到Neo4j数据库: {e}")
            raise

    def close(self):
        """关闭连接"""
        if self.driver:
            self.driver.close()

    def run_query(self, query):
        """执行查询并返回结果"""
        with self.driver.session() as session:
            try:
                result = session.run(query)
                return list(result)
            except Exception as e:
                logger.error(f"查询执行失败: {e}")
                raise

    def verify_data(self):
        """验证导入的数据"""
        logger.info("开始验证导入的数据...")
        
        # 1. 检查化学品节点总数
        result = self.run_query("MATCH (c:Chemical) RETURN count(c) as total_chemicals")
        total_chemicals = result[0]['total_chemicals']
        logger.info(f"总化学品节点数: {total_chemicals}")
        
        # 2. 检查危险化学品节点数
        result = self.run_query("MATCH (c:DangerousChemical) RETURN count(c) as dangerous_chemicals")
        dangerous_chemicals = result[0]['dangerous_chemicals']
        logger.info(f"危险化学品节点数: {dangerous_chemicals}")
        
        # 3. 检查关系总数
        result = self.run_query("MATCH ()-[r:工艺]->() RETURN count(r) as total_relationships")
        total_relationships = result[0]['total_relationships']
        logger.info(f"总关系数: {total_relationships}")
        
        # 4. 检查节点属性完整性
        result = self.run_query("""
            MATCH (c:Chemical) 
            WHERE c.cas IS NOT NULL 
            RETURN count(c) as chemicals_with_cas
        """)
        chemicals_with_cas = result[0]['chemicals_with_cas']
        logger.info(f"有CAS号的化学品数: {chemicals_with_cas}")
        
        # 5. 检查节点属性完整性
        result = self.run_query("""
            MATCH (c:Chemical) 
            WHERE c.molecular_formula IS NOT NULL 
            RETURN count(c) as chemicals_with_formula
        """)
        chemicals_with_formula = result[0]['chemicals_with_formula']
        logger.info(f"有分子式的化学品数: {chemicals_with_formula}")
        
        # 6. 显示一些示例数据
        logger.info("显示部分化学品数据示例:")
        result = self.run_query("""
            MATCH (c:Chemical) 
            WHERE c.cas IS NOT NULL 
            RETURN c.name, c.cas, c.molecular_formula, c.hazard
            LIMIT 5
        """)
        for record in result:
            logger.info(f"  名称: {record['c.name']}, CAS: {record['c.cas']}, "
                       f"分子式: {record['c.molecular_formula']}, 危害: {record['c.hazard']}")
        
        # 7. 检查关系示例
        logger.info("显示部分关系数据示例:")
        result = self.run_query("""
            MATCH (start:Chemical)-[r:工艺]->(end:Chemical) 
            RETURN start.name, end.name
            LIMIT 5
        """)
        for record in result:
            logger.info(f"  {record['start.name']} --工艺--> {record['end.name']}")

def main():
    """主函数"""
    verifier = None
    try:
        verifier = Neo4jVerifier(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        verifier.verify_data()
        logger.info("数据验证完成！")
    except Exception as e:
        logger.error(f"验证过程中发生错误: {e}")
    finally:
        if verifier:
            verifier.close()

if __name__ == "__main__":
    main()
