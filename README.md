# 🌐 Universal KG Builder

> **通用知识图谱构建平台 **
> 
> *A Universal, GUI-based Knowledge Graph Construction Platform Powered by LLMs.*

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![GUI](https://img.shields.io/badge/GUI-PyQt5-orange)](https://riverbankcomputing.com/software/pyqt/intro)

**Universal KG Builder** 是一个知识图谱构建工具。它原先是针对于化学品相关的处理，经过Ai的融合与修改开始创始转为通用的流水线，帮助用户利用大语言模型 (LLM) 从非结构化或半结构化数据中提取信息，构建任意领域的知识图谱。
****目前为0.1.0版本尚未确保其可用性，请等待后续测试与更新。****

## ✨ 核心特性 (Features)

*   **🎨 现代化 GUI 界面**: 基于 PyQt5 构建的现代化界面，支持深色模式风格、圆角设计和流畅的交互动画。
*   **🌐 多领域支持**: 内置通用模板，支持自定义领域 Schema（实体与属性），无论是生物、金融还是文学领域皆可适配。
*   **🤖 LLM 智能增强**: 集成 OpenAI / DashScope (通义千问) 等大模型接口，自动补全缺失的实体属性。
*   **📝 可视化 Schema 编辑器**: 通过表格形式直观管理实体属性定义、数据类型和描述。
*   **🚀 一键流水线**: 集成数据清洗、LLM 补全、后处理和图构建（Neo4j 导出）的完整工作流。

## 🛠️ 快速开始 (Quick Start)

### 1. 环境准备

本项目推荐使用 `uv` 进行极速依赖管理（也支持标准的 pip）。

```bash
# 安装 uv (如果尚未安装)
pip install uv

# 同步依赖环境
uv sync
```

或者使用 pip:
```bash
pip install -r requirements.txt
```

### 2. 启动应用

```bash
# 启动图形化界面
uv run python gui.py
```

### 3. 使用流程

1.  **配置领域**: 在 "领域与提示词" 标签页中，选择或新建一个领域（如 "Medicine"）。
2.  **定义 Schema**: 使用可视化编辑器添加该领域实体的属性（如 "适应症", "副作用" 等）。
3.  **生成/加载 Prompt**: 点击 "自动生成提示词" 或 "加载通用模板"。
4.  **数据处理**: 切换到 "数据处理" 标签页，上传包含实体名称的 CSV 文件，点击 "开始补全数据"。
5.  **导出图谱**: 在 "完整流水线" 中运行构建任务，生成 Neo4j 导入文件。

## 📂 项目结构

```
Universal-KG-Builder/
├── config/             # 领域配置与 Schema 定义 (YAML)
├── data/               # 数据存储 (输入/输出)
├── modules/            # 核心功能模块
│   ├── universal_enricher.py  # LLM 交互核心
│   ├── graph_construction/    # 图构建逻辑
│   └── ...
├── gui.py              # PyQt5 图形界面入口
├── main.py             # 命令行流水线入口
└── requirements.txt    # 项目依赖
```

## 📄 许可证 (License)

MIT License




