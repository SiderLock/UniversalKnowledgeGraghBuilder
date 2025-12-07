# 🌐 Universal KG Builder

> **通用知识图谱构建平台 v0.3.0**
> 
> *A Modern, GUI-based Knowledge Graph Construction Platform Powered by Multi-Provider LLMs*

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![PyQt6](https://img.shields.io/badge/GUI-PyQt6-41CD52?logo=qt)](https://www.riverbankcomputing.com/software/pyqt/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![LLM](https://img.shields.io/badge/LLM-Multi--Provider-orange)](https://openai.com/)

**Universal KG Builder** 是一个现代化的、可视化的知识图谱构建工具。它提供了一套完整的流水线，帮助用户利用大语言模型 (LLM) 从非结构化或半结构化数据中提取信息，构建任意领域的知识图谱。

![界面预览](docs/preview.png)

---

## ✨ 核心特性

### 🎨 现代化 GUI 界面
- 基于 **PyQt6** 构建的现代化界面
- 支持 **亮色/暗色主题** 切换
- **Toast 通知系统** - 非阻塞式操作反馈
- 流畅的动画效果和圆角设计

### 🤖 多 LLM 提供商支持
| 提供商 | 模型示例 | 状态 |
|--------|----------|------|
| **DashScope** (阿里通义) | qwen-plus, qwen-max | ✅ 支持 |
| **OpenAI** | gpt-4, gpt-3.5-turbo | ✅ 支持 |
| **Google Gemini** | gemini-pro, gemini-1.5-pro | ✅ 支持 |
| **Ollama** (本地部署) | llama3, qwen2 | ✅ 支持 |

### 📝 可视化 Prompt 构建器
- **变量插入按钮**: 一键插入 `{entity_name}`, `{attributes}` 等动态变量
- **实时预览**: 查看渲染后的 Prompt 效果
- **6种内置模板**: 通用、化学、医药、制造业、学术、科研
- **AI 智能生成**: 描述需求，自动生成完整配置

### 🔄 完整数据流水线
```
CSV数据 → 数据清洗 → LLM数据富集 → 后处理 → Neo4j图构建
            ↓           ↓              ↓          ↓
         [检查点]    [检查点]      [检查点]    [检查点]
```
- **断点续传**: 支持从中断处恢复长时间任务
- **并发处理**: 可配置的多线程并行处理
- **进度追踪**: 可视化阶段状态和进度

### 🌐 多领域支持
内置支持化学品领域模板，同时可自定义任何领域：
- 定义实体类型和属性 Schema
- 配置数据来源要求
- 自定义 System/User Prompt

---

## 🛠️ 快速开始

### 环境要求
- Python 3.10+
- (可选) Neo4j 数据库 (用于图构建)
- (可选) Ollama (用于本地模型)

### 安装

**推荐使用 `uv` (极速依赖管理)**:
```bash
# 安装 uv
pip install uv

# 克隆项目
git clone https://github.com/your-repo/universal-kg-builder.git
cd universal-kg-builder

# 同步依赖
uv sync
```

**或使用 pip**:
```bash
pip install -r requirements.txt
```

### 启动应用

```bash
# 使用 uv
uv run gui.py

# 或直接使用 Python
python gui.py
```

---

## 📖 使用指南

### 1️⃣ 智能向导 (推荐新用户)
1. 进入 **"🚀 智能向导"** 页面
2. 描述您想构建的领域（如："我想构建一个关于中药材的知识图谱..."）
3. 点击 **"AI 分析并推荐"**，AI 将自动推荐实体和属性
4. 确认后点击 **"生成完整数据集"**

### 2️⃣ 领域配置
进入 **"🏷️ 领域配置"** 页面，使用选项卡配置：

| 选项卡 | 功能 |
|--------|------|
| 📋 基础 & Schema | 定义实体类型和属性 |
| 📡 数据来源 | 配置 AI 参考的数据源 |
| 🤖 System Prompt | 设定 AI 的角色 |
| 💬 User Prompt | 构建查询模板（支持变量插入和预览）|
| ✨ AI 生成 | 一键生成全部配置 |

### 3️⃣ 数据处理
1. 进入 **"📂 数据处理"** 页面
2. 上传 CSV 文件（或点击 "加载示例" 使用内置数据）
3. 选择实体名称列和领域配置
4. 点击 **"开始补全数据"**

### 4️⃣ 流水线运行
进入 **"⚙️ 流水线"** 页面，运行完整的数据处理流程：
- 可视化阶段状态卡片
- 支持暂停/继续/停止
- 断点恢复功能

---

## ⚙️ 配置说明

### API 配置
在 **"🔧 设置"** 页面配置 LLM 提供商：

**DashScope (阿里通义)**:
```yaml
Provider: dashscope
API Key: sk-your-dashscope-key
Model: qwen-plus
```

**OpenAI**:
```yaml
Provider: openai
API Key: sk-your-openai-key
Base URL: https://api.openai.com/v1 (或代理地址)
Model: gpt-4
```

**Ollama (本地)**:
```yaml
Provider: ollama
Base URL: http://localhost:11434/v1
Model: qwen2:7b
```

### Neo4j 配置
```yaml
URI: bolt://localhost:7687
User: neo4j
Password: your-password
```

---

## 📂 项目结构

```
Universal-KG-Builder/
├── gui.py                  # 🖥️ PyQt6 图形界面入口
├── main.py                 # 📟 命令行入口
├── config/
│   ├── config.yaml         # 全局配置
│   └── domains.yaml        # 领域配置存储
├── data/
│   ├── demo/               # 示例数据
│   └── reference_data/     # 参考数据
├── modules/
│   ├── universal_enricher.py      # LLM 交互核心
│   ├── pipeline_manager.py        # 流水线管理器
│   ├── data_cleaning/             # 数据清洗模块
│   ├── data_enrichment/           # 数据富集模块
│   │   ├── core/                  # 核心处理逻辑
│   │   ├── utils/                 # 工具函数
│   │   └── config/                # 配置管理
│   ├── post_processing/           # 后处理模块
│   └── graph_construction/        # Neo4j 图构建
├── pyproject.toml          # 项目配置
└── requirements.txt        # 依赖列表
```

---

## 🔧 高级功能

### 并发处理配置
在设置页面调整 `最大并发数` 参数，控制同时处理的实体数量。

### 断点续传
流水线支持从检查点恢复：
1. 任务中断后，重新打开应用
2. 在流水线页面选择之前的管道实例
3. 勾选 "跳过已完成阶段" 后继续运行

### 自定义 Prompt 变量
在 User Prompt 中可使用以下变量：
- `{entity_name}` - 当前实体名称
- `{attributes}` - Schema 中定义的属性列表
- `{source_instruction}` - 数据来源要求

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发环境设置
```bash
# 安装开发依赖
uv sync --group dev

# 运行测试
uv run pytest

# 代码格式化
uv run black .
uv run isort .
```

---

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

---

## 📮 联系方式

- **Author**: FlyWill
- **Email**: WillingXun@outlook.com

---

<div align="center">

**🌟 如果这个项目对您有帮助，欢迎 Star！🌟**

</div>
