# 更新日志

本项目的所有重要更改都将记录在此文件中。

## [v0.5.0] - 2025-12-07

### 🚀 新特性
- **DeepSeek API 支持**: 全面支持 DeepSeek 模型（`deepseek-chat`, `deepseek-coder`）作为一级模型提供商。
- **智能实体生成**: 智能向导现在支持生成任意数量的实体（1-100+），通过智能调用 LLM 扩展初始推荐列表。
- **金山橙主题**: 引入了全新的、温暖的“金山橙”UI 主题，提供更好的视觉体验。
- **PC 硬件领域**: 添加了用于 PC 硬件知识图谱构建的综合预设。

### ⚡ 改进
- **Ollama 集成**: 
  - 优化了参数处理（GPU 层数，上下文窗口）。
  - 添加了自动参数过滤，以确保与 OpenAI 风格端点的兼容性。
- **JSON 解析**: 增强了 `RobustLLMJsonParser`，采用多策略解析以处理各种 LLM 输出格式（Markdown 代码块，格式错误的 JSON）。
- **提示词工程**: 简化并优化了 PC 硬件和中医药领域的提示词，以提高提取准确性。

### 🐛 Bug 修复
- 修复了 DeepSeek/OpenAI 提供商的 `Completions.create() got an unexpected keyword argument` 错误，通过过滤 Ollama 特有参数解决。
- 修复了智能向导实体生成限制，此前输出被限制为初始推荐列表的大小。
- 解决了 GUI 光标安全问题并减少了调试日志噪音。

## [v0.3.0] - 上一个版本
- 首次发布 GUI 版本。
- 支持 DashScope, OpenAI, Gemini, 和 Ollama。
- 可视化 Prompt 构建器和智能向导。
- 完整的 CSV 到 Neo4j 流水线。
