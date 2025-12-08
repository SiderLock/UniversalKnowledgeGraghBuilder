# 📋 更新日志 (Changelog)

本文档记录了 OpenChemKG 项目的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

---

## [1.0.0] - 2025-12-08

### 🎉 首个正式发布版本

这是 OpenChemKG 的第一个正式版本，包含完整的知识图谱构建功能。

### ✨ 新增功能

#### 多 LLM 提供商支持
- **DashScope (阿里通义)**: 支持 qwen-plus, qwen-max, qwen-turbo, qwen-long 等模型
- **OpenAI**: 支持 gpt-4, gpt-4o, gpt-3.5-turbo 等模型
- **DeepSeek**: 支持 deepseek-chat, deepseek-coder 模型
- **Kimi (月之暗面)**: 支持 moonshot-v1-8k/32k/128k/auto 模型，包含联网搜索功能
- **Ollama**: 支持本地部署的所有兼容模型

#### 智能 API 调度
- 新增 **RateLimiter** 类，支持 RPM (每分钟请求数)、TPM (每分钟Token数)、TPD (每天Token数) 限制
- 自适应解析策略：
  - 商业 API 使用增强模式（简洁提示词）
  - Ollama 使用兼容模式（详细提示词，宽松解析）
- 指数退避重试机制

#### GUI 界面
- 全新的 **现代蓝主题** 设计 (Accent Color: #0d6efd)
- **深色模式支持**：侧边栏新增一键切换深色/浅色模式
- **实时仪表盘**：视觉升级，采用清新渐变背景，优化卡片阴影与边框
- **侧边栏重构**：优化 Logo 区域与用户信息展示，新增底部功能区
- **系统状态面板**：显示 LLM 服务、速率限制、工作目录
- **Toast 通知系统**：非阻塞式操作反馈，支持成功/错误/警告/信息四种类型
- 设置页面新增速率限制配置 (RPM/TPM/TPD)

#### 智能领域向导
- AI 驱动的领域分析功能
- 自动生成推荐实体列表 (10-15个)
- 自动生成推荐属性 Schema (5-8个)
- 实体生成数量限制提升至 **1-1000**

#### JSON 解析器增强
- 5层解析策略：
  1. 直接 JSON 解析
  2. Markdown 代码块清理
  3. JSON 块正则提取
  4. 常见格式问题修复
  5. Key-Value 文本提取
- 模糊属性名匹配
- 中文引号自动修复
- 无效值过滤（null, N/A, 占位符等）

#### 数据补全
- 并发处理支持（可配置 max_workers）
- 实时进度显示
- 成功/失败统计
- 详细日志输出

### 🔧 技术改进

- 重构 `UniversalEnricher.process_batch()` 方法
- 新增 `_build_ollama_system_prompt()` 和 `_build_ollama_prompt()` 方法
- 新增 `_ollama_parse_json()` 专用解析器
- 新增 `_simple_parse_json()` 通用解析器
- 优化 Kimi 联网搜索配置（使用 builtin_function 类型）

### 🐛 修复

- 修复 Kimi web_search 工具类型错误
- 修复 `on_dataset_complete` 空结果导致的 TypeError
- 修复领域分析结果解析失败的问题
- 修复 QGridLayout 未导入的错误

### 📝 文档

- 全新的 README.md（v1.0.0）
- 新增 CHANGELOG.md 更新日志
- 新增 OLLAMA_SETUP.md Ollama 配置指南
- 新增 MIGRATION_PYQT6.md PyQt6 迁移说明

---

## [0.3.0] - 2025-11-xx (开发版本)

### 新增
- 初始 GUI 框架
- 基础的 LLM 调用功能
- CSV 数据处理

### 变更
- 从 PyQt5 迁移到 PyQt6

---

## 路线图

### v1.1.0 (计划中)
- [ ] 支持 Google Gemini API
- [ ] 批量导入/导出领域配置
- [ ] 数据可视化图表
- [ ] Neo4j 图谱预览

### v1.2.0 (计划中)
- [ ] 多语言界面支持
- [ ] 插件系统
- [ ] API 成本估算器
- [ ] 云端配置同步

---

## 贡献者

感谢所有为本项目做出贡献的开发者！

---

[1.0.0]: https://github.com/your-repo/OpenChemKG/releases/tag/v1.0.0
[0.3.0]: https://github.com/your-repo/OpenChemKG/releases/tag/v0.3.0

