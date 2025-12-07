# 🌐 Universal KG Builder

> **通用知识图谱构建平台 v0.5.0**
> 
> *A Modern, GUI-based Knowledge Graph Construction Platform Powered by Multi-Provider LLMs*

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![PyQt6](https://img.shields.io/badge/GUI-PyQt6-41CD52?logo=qt)](https://www.riverbankcomputing.com/software/pyqt/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![LLM](https://img.shields.io/badge/LLM-Multi--Provider-orange)](https://openai.com/)

**Universal KG Builder** 是一个现代化的、可视化的知识图谱构建工具。它提供了一套完整的流水线，帮助用户利用大语言模型 (LLM) 从非结构化或半结构化数据中提取信息，构建任意领域的知识图谱。

## 特别提醒
**本项目仍处于测试阶段，目前保证流程跑通，但是在结构化输出方面罄待优化，会导致补全的数据无法填入最后输出文件中，请等待后续优化。**

---

## ✨ 核心特性

### 🎨 现代化 GUI 界面
- 基于 **PyQt6** 构建的现代化界面，采用温暖的**金山橙**主题
- 支持 **亮色/暗色主题** 切换
- **Toast 通知系统** - 非阻塞式操作反馈
- 流畅的动画效果和圆角设计

### 🤖 多 LLM 提供商支持
| 提供商 | 模型示例 | 状态 | 特点 |
|--------|----------|------|------|
| **DeepSeek** | deepseek-chat, deepseek-coder | ✅ 新增 | 高性价比，编程能力强 |
| **DashScope** (阿里通义) | qwen-plus, qwen-max | ✅ 支持 | 中文理解能力优秀 |
| **OpenAI** | gpt-4, gpt-3.5-turbo | ✅ 支持 | 行业标准，性能稳定 |
| **Ollama** (本地部署) | llama3, qwen2, gemma | ✅ 增强 | 隐私安全，GPU加速 |
| **Google Gemini** | gemini-pro | ✅ 支持 | 长上下文支持 |

### 🧙‍♂️ 智能向导 (Smart Wizard)
- **自动领域分析**: 输入一句话描述，自动生成 Schema 和 Prompt
- **智能实体生成**: 支持生成任意数量(1-100+)的领域实体，自动去重和多样化
- **一键配置**: 自动配置所有参数，直接开始数据生产

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
内置多种领域模板，支持一键切换：
- **PC硬件**: CPU、GPU、主板等硬件参数提取
- **中医药**: 药材性味、功效、配伍等信息
- **化学品**: 物理性质、危险性、用途等
- **编程技术**: 语言、框架、工具特性分析

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
2. 输入领域描述（例如："我想构建一个关于PC硬件的知识库"）
3. 点击分析，系统自动生成 Schema
4. 设置需要的实体数量，点击生成数据集
5. 导出数据并开始构建

### 2️⃣ 高级配置
1. 在 **"⚙️ 设置"** 中配置 API Key 和模型参数
2. 在 **"📝 Prompt 构建"** 中微调提示词
3. 在 **"📊 任务管理"** 中监控处理进度

---

## 📄 更新日志

查看 [CHANGELOG.md](CHANGELOG.md) 了解最新变化。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。
