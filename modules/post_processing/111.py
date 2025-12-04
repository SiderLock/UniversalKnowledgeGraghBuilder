# -*- coding: utf-8 -*-
import time
from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher

# ====== 配置 ======
NEO4J_URI  = "bolt://localhost:7687"      # 或你的远程地址
NEO4J_USER = "neo4j"
NEO4J_PWD  = "[2361918131]"               # ← 修改为你的密码

graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD))

# ====== 数据 ======
start_node_name = "生产工艺_2_吖丙啶的均聚物与氯甲基环氧乙烷的反应产物"
end_node_name   = "吖丙啶的均聚物与氯甲基环氧乙烷的反应产物"
rel_type        = "产出"
label           = "Process"

# ====== 计时开始 ======
t0 = time.perf_counter()

matcher = NodeMatcher(graph)

# 1) 获取或创建起始节点
start_node = matcher.match(label, name=start_node_name).first()
if not start_node:
    start_node = Node(label, name=start_node_name)
    graph.create(start_node)

# 2) 获取或创建终止节点
end_node = matcher.match(label, name=end_node_name).first()
if not end_node:
    end_node = Node(label, name=end_node_name)
    graph.create(end_node)

# 3) 获取或创建关系（避免重复）
rel_matcher = RelationshipMatcher(graph)
rel_exists = rel_matcher.match((start_node, end_node), rel_type).first()
if not rel_exists:
    rel = Relationship(start_node, rel_type, end_node)
    graph.create(rel)

elapsed = time.perf_counter() - t0
print(f"节点与关系已写入，耗时 {elapsed*1000:.2f} ms")