import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QTabWidget, QLabel, QLineEdit, 
                             QComboBox, QTextEdit, QPushButton, QFileDialog, 
                             QMessageBox, QProgressBar, QSplitter, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QGroupBox, QFormLayout,
                             QFrame, QSizePolicy, QStyleFactory, QGraphicsDropShadowEffect)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSize
from PyQt5.QtGui import QFont, QIcon, QPalette, QColor

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.universal_enricher import UniversalEnricher

# Constants
CONFIG_DIR = Path("config")
DOMAINS_FILE = CONFIG_DIR / "domains.yaml"
DATA_DIR = Path("data")

DEFAULT_UNIVERSAL_PROMPT = """
# 🌐 通用知识图谱数据构建指令

## 🎯 核心目标
- **实体名称**: {entity_name}
- **任务**: 为该实体构建详细的结构化属性数据

## 📋 数据要求
1. **准确性**: 确保所有信息基于事实，优先参考权威来源。
2. **完整性**: 尽可能完善地填写所有定义的属性字段。
3. **语言**: 除非专有名词，否则请使用**简体中文**。

## 🏗️ 属性定义
请基于以下维度提取信息（请根据实际Schema调整）：
- **基础信息**: 定义、分类、别名等
- **核心特征**: 关键参数、规格、性质
- **关联关系**: 上游来源、下游应用、相关实体
- **描述信息**: 功能描述、背景介绍

## 📤 输出规范
请返回严格的 JSON 格式数据：

```json
{{
    "data_source": "模型知识",
    "名称": "{entity_name}",
    "别名": "别名1; 别名2",
    "类型": "实体类型",
    "描述": "详细描述...",
    "关键属性": "属性值..."
}}
```
"""

DEFAULT_CHEMICAL_PROMPT = """
# 🧪 化学品知识图谱数据查询指令 v4.1 - API智能适配版

## 🎯 查询目标
- **化学品名称**: {entity_name}

## 🔍 CAS号与流水号智能识别指引
### 📋 "CAS号或流水号"字段说明：
- **CAS号**：国际通用化学物质唯一标识编号（格式：XXXX-XX-X，如64-17-5代表乙醇）
- **流水号**：名录编制单位自定义编号，用于无CAS号的新化学品、复合物、特殊材料
- **优先级**：优先使用国际标准CAS号，无CAS号时用本地流水号保证唯一性

### 🎯 核心任务
1. **CAS号验证与补充**：如当前编号为空、格式错误或为流水号，必须查询补充准确的CAS号
2. **编号唯一性检查**：确保每个化学品都有唯一标识符
3. **格式标准化**：CAS号格式必须为"数字-数字-数字"标准格式
4. **数据关联性验证**：确认编号与化学品名称的准确对应关系
5. **源数据兼容性**：兼容《中国化学品名录2013年版》的"CAS号或流水号"字段结构

## 📋 知识图谱属性要求 (用于构建化学品知识图谱)
请为上述化学品提供以下详细信息，用于构建完整的化学品知识图谱。所有数据必须以**简体中文**表述，并确保内容的详尽和准确。

### 🔬 基础标识信息（重点优化）
- **名称**: 化学品的标准中文名称
- **CAS号或流水号**: 
  - 如为标准CAS号，保持原格式并验证准确性
  - 如为流水号，查询是否存在对应CAS号并优先使用CAS号
  - 如为空值，从权威数据库查询补充标准CAS号
  - 格式要求：CAS号严格为"XXXX-XX-X"，流水号为纯数字
- **别名**: 包含所有常用别名，例如英文名、商品名、俗称、学名等。格式："别名1; 别名2; 别名3; 别名4; 别名5"，至少提供3-5个有价值的别名，用分号分隔
- **分子式**: 准确的化学分子式，例如 "C2H6O", "H2SO4"
- **分子量**: 准确的分子量数值，单位为 g/mol，保留至少两位小数，例如 "46.07"

### ⚠️ 危害与安全信息
- **是否为危化品**: 基于《危险化学品目录》，必须明确回答"是"或"否"
- **浓度阈值**: 参考原名录"浓度阈值"字段，详细说明毒理学数据，例如 "LC50(大鼠吸入): 20000 ppm/10h; LD50(大鼠经口): 7060 mg/kg; LD50(兔子皮肤): >5000 mg/kg"
- **危害**: 详细描述对人体和环境的具体危害，需分类说明：
  - **健康危害**: 急性毒性、皮肤腐蚀/刺激、严重眼损伤/眼刺激、致癌性、生殖毒性等
  - **环境危害**: 对水生生物的危害、持久性、生物累积性等
  - **物理危害**: 易燃性、爆炸性、氧化性等
- **防范**: 具体的防护措施和注意事项，需分类说明：
  - **工程控制**: 通风系统、密闭操作等
  - **个体防护**: 呼吸系统防护、眼睛防护、身体防护、手部防护
  - **操作处置与储存**: 操作注意事项、储存条件
- **危害处置**: 发生事故时的具体应急处置方法和急救措施，需分类说明：
  - **泄漏应急处理**: 环境、人员、处理方法
  - **火灾处置**: 灭火方法、有害燃烧产物
  - **急救措施**: 皮肤接触、眼睛接触、吸入、食入后的急救方法

### 🏭 产业链信息 (知识图谱核心)
- **用途**: 详细说明主要用途和应用领域，至少列举5个具体用途，并描述其在应用中扮演的角色
- **自然来源**: 详细说明该化学品在自然界中的存在形式、分布情况、天然来源（如植物、矿物、微生物等），以及天然提取方法。如果是纯人工合成的化学品，则说明"无天然来源，纯人工合成"
- **生产来源 (上游)**: 详细列出其直接上游原料化学品，以及主要的生产商或供应商信息。这是构建产业链上游关系的关键
- **工业生产原料 (下游)**: 详细列出该化学品作为原料可以用于生产哪些下游产品或化学品。这是构建产业链下游关系的关键

### ⚗️ 物理化学性质
- **性质**: 提供一个综合性的、结构化的物理化学性质描述，至少包括：
  - **外观与性状**: 详细描述常温常压下的颜色、状态、气味等感官特征
  - **熔点**: 数值+单位(°C)，例如 "-114.1°C"
  - **沸点**: 数值+单位(°C)，例如 "78.3°C"
  - **密度**: 数值+单位，并注明温度，例如 "0.789 g/cm³(20°C)"
  - **溶解性**: 在水、乙醇等常见溶剂中的溶解情况，可包含定量数据
  - **闪点**: 数值+单位(°C)，并注明开杯/闭杯，例如 "13°C (闭杯)"
  - **稳定性**: 描述其化学稳定性、需要避免的条件（如光、热）和禁配物质

## 📤 输出格式
**必须严格以JSON对象格式返回，确保所有字段完整填写，不要包含任何额外的解释或Markdown标记。**
**重要：除了 `data_source` 字段，其他字段的数据后不需要标明数据来源。**

```json
{{
    "data_source": "网络搜索/模型知识 {{数据来源}}",
    "名称": "化学品标准中文名称",
    "CAS号或流水号": "优先CAS号格式XXXX-XX-X，无CAS号时为流水号",
    "编号类型说明": "标准CAS号/本地流水号/新分配CAS号",
    "别名": "别名1; 别名2; 别名3",
    "分子式": "化学分子式",
    "分子量": "数值 g/mol",
    "是否为危化品": "是/否",
    "浓度阈值": "毒理学数据详情",
    "危害": "危害描述",
    "防范": "防护措施详情",
    "危害处置": "应急处置详情",
    "用途": "用途详情",
    "自然来源": "天然来源详情",
    "生产来源": "上游原料详情",
    "工业生产原料": "下游产品详情",
    "性质": "物理化学性质详情"
}}
```

## ⚡ 特别要求 (知识图谱专用)
- 🔍 **CAS号/流水号智能处理**：
  - 如输入为流水号，必须查询是否存在对应的标准CAS号
  - 如输入为空值或格式错误，必须从权威数据库查询补充标准CAS号
  - 优先使用国际CAS号，确保全球通用性和数据关联性
  - 保证每个化学品都有唯一且准确的标识符
- 📊 **数据来源权威性**: 优先使用PubChem、ECHA等权威源，确保数据质量
- 🎯 **内容详细具体**: 每个字段都要详细填写，避免使用模糊或笼统的描述，为知识图谱提供高质量的属性信息
- 🔗 **关联性描述**: 特别注意产业链（生产来源、工业生产原料）和危害信息的准确性和完整性，这是构建关系图谱的核心
- 📋 **格式严格统一**: 严格遵守JSON输出格式，便于知识图谱的自动化结构化处理

## 💡 字段填写指导
- **CAS号或流水号字段**: 
  - 标准CAS号示例：64-17-5（乙醇）、7732-18-5（水）
  - 流水号示例：202401001、300015678（纯数字格式）
  - 格式验证：确保CAS号符合"数字-数字-数字"标准
- **别名字段**: 包含学名、俗名、商品名、英文名等，用分号分隔，以提供丰富的检索入口
- **浓度阈值**: 尽量提供多种物种（大鼠、兔子等）和多种途径（经口、吸入、皮肤）的毒理学数据，**参考原名录"浓度阈值"字段进行详细说明**。
- **自然来源**: 详细描述天然存在情况，包括在哪些植物、动物、矿物或微生物中发现，以及天然提取工艺
- **产业链信息**: 要清晰地体现化学品在整个产业链中的上游原料和下游产品关系
- **性质描述**: 尽量提供定量数据，并注明测试条件（如温度、压力）
- **数据来源标注**: **仅在顶层 `data_source` 字段中说明本次查询的主要信息来源，例如 "网络搜索 {{PubChem; ECHA}}" 或 "模型知识 {{模型知识}}"。其他字段无需标注来源。**

现在开始查询并生成用于知识图谱的化学品详细数据：
"""

class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)
    status = pyqtSignal(str)

    def __init__(self, task_func, *args, **kwargs):
        super().__init__()
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.task_func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))

class SchemaEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Entity Type
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("实体类型:"))
        self.entity_type_input = QLineEdit()
        self.entity_type_input.setPlaceholderText("例如: Chemical, Protein... (建议使用英文)")
        type_layout.addWidget(self.entity_type_input)
        layout.addLayout(type_layout)

        # Attributes Table
        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["属性名称", "数据类型", "属性描述"])
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_add = QPushButton("➕ 添加属性")
        self.btn_add.setStyleSheet("background-color: #2ecc71; color: white;")
        self.btn_add.clicked.connect(lambda: self.add_row())
        
        self.btn_remove = QPushButton("➖ 删除选中")
        self.btn_remove.setStyleSheet("background-color: #e74c3c; color: white;")
        self.btn_remove.clicked.connect(self.remove_row)
        
        btn_layout.addWidget(self.btn_add)
        btn_layout.addWidget(self.btn_remove)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

    def set_data(self, schema_data):
        self.entity_type_input.setText(schema_data.get('entity_type', ''))
        attributes = schema_data.get('attributes', [])
        self.table.setRowCount(0)
        for attr in attributes:
            self.add_row(attr.get('name', ''), attr.get('type', 'String'), attr.get('description', ''))

    def get_data(self):
        attributes = []
        for i in range(self.table.rowCount()):
            name_item = self.table.item(i, 0)
            type_widget = self.table.cellWidget(i, 1)
            desc_item = self.table.item(i, 2)
            
            if name_item and name_item.text().strip():
                attributes.append({
                    "name": name_item.text().strip(),
                    "type": type_widget.currentText() if type_widget else "String",
                    "description": desc_item.text().strip() if desc_item else ""
                })
        
        return {
            "entity_type": self.entity_type_input.text().strip(),
            "attributes": attributes
        }

    def add_row(self, name="", type_val="String", desc=""):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(str(name)))
        
        combo = QComboBox()
        combo.addItems(["String", "Integer", "Float", "Boolean", "List", "Date"])
        combo.setCurrentText(type_val if type_val else "String")
        self.table.setCellWidget(row, 1, combo)
        
        self.table.setItem(row, 2, QTableWidgetItem(str(desc)))

    def remove_row(self):
        rows = set(index.row() for index in self.table.selectedIndexes())
        for row in sorted(rows, reverse=True):
            self.table.removeRow(row)

class ModernStyle:
    QSS = """
    /* 全局设置 */
    QMainWindow {
        background-color: #f0f2f5;
    }
    QWidget {
        font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif;
        font-size: 14px;
        color: #2c3e50;
    }
    
    /* 分组框 */
    QGroupBox {
        background-color: white;
        border: 1px solid #e1e4e8;
        border-radius: 12px;
        margin-top: 16px;
        padding: 24px 16px 16px 16px;
        font-weight: 600;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 8px;
        color: #34495e;
        background-color: transparent;
    }

    /* 按钮通用 */
    QPushButton {
        background-color: #3498db;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 10px 20px;
        font-weight: 600;
        font-size: 14px;
    }
    QPushButton:hover {
        background-color: #2980b9;
        margin-top: -1px; /* 简单的悬浮位移效果 */
        margin-bottom: 1px;
    }
    QPushButton:pressed {
        background-color: #2573a7;
        margin-top: 1px;
        margin-bottom: -1px;
    }
    QPushButton:disabled {
        background-color: #bdc3c7;
        color: #ecf0f1;
    }

    /* 输入框 */
    QLineEdit, QTextEdit, QComboBox {
        border: 1px solid #dfe6e9;
        border-radius: 8px;
        padding: 8px 12px;
        background-color: #ffffff;
        selection-background-color: #3498db;
        selection-color: white;
    }
    QLineEdit:focus, QTextEdit:focus, QComboBox:focus {
        border: 2px solid #3498db;
        background-color: #ffffff;
    }
    QLineEdit:hover, QTextEdit:hover, QComboBox:hover {
        border: 1px solid #b2bec3;
    }

    /* 下拉框 */
    QComboBox::drop-down {
        subcontrol-origin: padding;
        subcontrol-position: top right;
        width: 20px;
        border-left-width: 0px;
        border-top-right-radius: 8px;
        border-bottom-right-radius: 8px;
    }
    
    /* 标签页 */
    QTabWidget::pane {
        border: 1px solid #e1e4e8;
        background-color: white;
        border-radius: 12px;
        /* 移除顶部圆角以连接标签 */
        border-top-left-radius: 0px; 
    }
    QTabBar::tab {
        background-color: #dfe6e9;
        color: #636e72;
        padding: 12px 24px;
        border-top-left-radius: 10px;
        border-top-right-radius: 10px;
        margin-right: 4px;
        font-weight: 600;
    }
    QTabBar::tab:selected {
        background-color: white;
        color: #3498db;
        border-bottom: 2px solid #3498db; /* 底部高亮条 */
    }
    QTabBar::tab:hover {
        background-color: #ecf0f1;
        color: #2980b9;
    }

    /* 进度条 */
    QProgressBar {
        border: none;
        background-color: #dfe6e9;
        border-radius: 10px;
        text-align: center;
        color: #2c3e50;
        font-weight: bold;
    }
    QProgressBar::chunk {
        background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #3498db, stop:1 #2ecc71);
        border-radius: 10px;
    }

    /* 表格 */
    QTableWidget {
        border: 1px solid #e1e4e8;
        border-radius: 8px;
        gridline-color: #f0f2f5;
        background-color: white;
        selection-background-color: #e8f4fc; /* 浅蓝色选中背景 */
        selection-color: #2c3e50;
    }
    QHeaderView::section {
        background-color: #f8f9fa;
        padding: 10px;
        border: none;
        border-bottom: 2px solid #e1e4e8;
        font-weight: bold;
        color: #2c3e50;
    }
    QTableWidget::item {
        padding: 5px;
    }
    QTableWidget::item:hover {
        background-color: #f1f2f6;
    }

    /* 滚动条美化 */
    QScrollBar:vertical {
        border: none;
        background: #f1f2f6;
        width: 10px;
        margin: 0px 0px 0px 0px;
        border-radius: 5px;
    }
    QScrollBar::handle:vertical {
        background: #bdc3c7;
        min-height: 20px;
        border-radius: 5px;
    }
    QScrollBar::handle:vertical:hover {
        background: #95a5a6;
    }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
        height: 0px;
    }
    
    /* 分割器 */
    QSplitter::handle {
        background-color: #dfe6e9;
    }
    """

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Universal KG Builder - 通用知识图谱构建平台")
        self.resize(1280, 850)
        
        # Set Global Font
        font = QFont("Segoe UI", 10)
        font.setStyleStrategy(QFont.PreferAntialias)
        QApplication.setFont(font)
        
        # Apply Style
        self.setStyleSheet(ModernStyle.QSS)
        
        # State
        self.domains = self.load_domains()
        self.api_key = os.environ.get("OPENCHEMKG_API_KEY", "")
        self.current_df = None
        
        # UI Setup
        self.init_ui()
        
    def load_domains(self):
        if DOMAINS_FILE.exists():
            with open(DOMAINS_FILE, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}

    def save_domains(self):
        with open(DOMAINS_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(self.domains, f, allow_unicode=True)

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(25, 25, 25, 25)
        main_layout.setSpacing(25)
        
        # Sidebar
        sidebar = self.create_sidebar()
        main_layout.addWidget(sidebar, 1)
        
        # Main Content (Tabs)
        self.tabs = QTabWidget()
        
        # Add Shadow to Tabs
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(20)
        shadow.setXOffset(0)
        shadow.setYOffset(4)
        shadow.setColor(QColor(0, 0, 0, 30))
        self.tabs.setGraphicsEffect(shadow)
        
        self.setup_tabs()
        main_layout.addWidget(self.tabs, 4)

    def create_sidebar(self):
        container = QWidget()
        container.setStyleSheet(".QWidget { background-color: white; border-radius: 16px; }")
        
        # Add Shadow
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(20)
        shadow.setXOffset(0)
        shadow.setYOffset(4)
        shadow.setColor(QColor(0, 0, 0, 30))
        container.setGraphicsEffect(shadow)

        layout = QVBoxLayout(container)
        layout.setContentsMargins(20, 25, 20, 25)
        
        # Title/Logo Area
        title_label = QLabel("🌐 Universal KG")
        title_label.setStyleSheet("font-size: 26px; font-weight: 800; color: #2c3e50; margin-bottom: 20px; font-family: 'Segoe UI Black';")
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)
        
        # Settings Group
        group = QGroupBox("⚙️ 全局设置")
        form_layout = QFormLayout()
        form_layout.setSpacing(15)
        
        self.provider_combo = QComboBox()
        self.provider_combo.addItems(["dashscope", "openai"])
        form_layout.addRow("模型提供商:", self.provider_combo)
        
        self.api_key_input = QLineEdit()
        self.api_key_input.setEchoMode(QLineEdit.Password)
        self.api_key_input.setPlaceholderText("输入 API 密钥")
        self.api_key_input.setText(self.api_key)
        self.api_key_input.textChanged.connect(self.update_api_key)
        form_layout.addRow("API 密钥:", self.api_key_input)
        
        self.base_url_input = QLineEdit()
        self.base_url_input.setPlaceholderText("可选")
        form_layout.addRow("基础 URL:", self.base_url_input)
        
        self.model_input = QLineEdit("qwen-plus")
        form_layout.addRow("模型名称:", self.model_input)
        
        self.provider_combo.currentTextChanged.connect(self.update_model_default)
        
        group.setLayout(form_layout)
        layout.addWidget(group)
        
        layout.addStretch()
        
        # Footer
        version_label = QLabel("v0.1.0 | Powered by PyQt5")
        version_label.setStyleSheet("color: #95a5a6; font-size: 12px; font-weight: 600;")
        version_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(version_label)
        
        return container

    def update_model_default(self, provider):
        if provider == "dashscope":
            self.model_input.setText("qwen-plus")
        else:
            self.model_input.setText("gpt-4")

    def update_api_key(self, text):
        self.api_key = text
        os.environ["OPENCHEMKG_API_KEY"] = text

    def setup_tabs(self):
        self.tab1 = QWidget()
        self.setup_domain_tab()
        self.tabs.addTab(self.tab1, "🏷️ 领域与提示词")
        
        self.tab2 = QWidget()
        self.setup_data_tab()
        self.tabs.addTab(self.tab2, "📂 数据处理")
        
        self.tab3 = QWidget()
        self.setup_preview_tab()
        self.tabs.addTab(self.tab3, "📊 结果预览")
        
        self.tab4 = QWidget()
        self.setup_pipeline_tab()
        self.tabs.addTab(self.tab4, "⚙️ 完整流水线")

    def setup_domain_tab(self):
        layout = QHBoxLayout(self.tab1)
        layout.setContentsMargins(20, 20, 20, 20)
        
        splitter = QSplitter(Qt.Horizontal)
        
        # Left Panel: Domain Management
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 10, 0)
        
        # Selection Group
        sel_group = QGroupBox("选择领域")
        sel_layout = QVBoxLayout()
        self.domain_combo = QComboBox()
        self.update_domain_combo()
        self.domain_combo.currentTextChanged.connect(self.on_domain_changed)
        sel_layout.addWidget(self.domain_combo)
        sel_group.setLayout(sel_layout)
        left_layout.addWidget(sel_group)
        
        # Creation Group
        create_group = QGroupBox("新建领域")
        create_layout = QVBoxLayout()
        create_layout.addWidget(QLabel("领域 ID (英文):"))
        self.new_domain_name = QLineEdit()
        self.new_domain_name.setPlaceholderText("e.g., biology")
        create_layout.addWidget(self.new_domain_name)
        
        create_layout.addWidget(QLabel("领域描述:"))
        self.new_domain_desc = QTextEdit()
        self.new_domain_desc.setPlaceholderText("描述该领域，例如：生物学，关注蛋白质结构和功能...")
        self.new_domain_desc.setMaximumHeight(100)
        create_layout.addWidget(self.new_domain_desc)
        
        self.btn_generate_prompts = QPushButton("✨ 自动生成提示词")
        self.btn_generate_prompts.clicked.connect(self.generate_prompts)
        create_layout.addWidget(self.btn_generate_prompts)

        # Templates
        template_layout = QHBoxLayout()
        self.btn_load_universal = QPushButton("🌐 加载通用模板")
        self.btn_load_universal.setStyleSheet("background-color: #3498db; color: white;")
        self.btn_load_universal.clicked.connect(self.load_universal_defaults)
        
        self.btn_load_defaults = QPushButton("🧪 加载化学模板")
        self.btn_load_defaults.setStyleSheet("background-color: #2ecc71; color: white;")
        self.btn_load_defaults.clicked.connect(self.load_chemical_defaults)
        
        template_layout.addWidget(self.btn_load_universal)
        template_layout.addWidget(self.btn_load_defaults)
        create_layout.addLayout(template_layout)
        
        create_group.setLayout(create_layout)
        left_layout.addWidget(create_group)
        left_layout.addStretch()
        
        # Right Panel: Configuration Editor
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(10, 0, 0, 0)
        
        header_layout = QHBoxLayout()
        self.config_label = QLabel("配置: None")
        self.config_label.setStyleSheet("font-size: 18px; font-weight: bold; color: #2f3640;")
        header_layout.addWidget(self.config_label)
        header_layout.addStretch()
        self.btn_save_config = QPushButton("💾 保存修改")
        self.btn_save_config.setFixedWidth(120)
        self.btn_save_config.clicked.connect(self.save_current_config)
        header_layout.addWidget(self.btn_save_config)
        right_layout.addLayout(header_layout)
        
        self.config_desc_label = QLabel()
        self.config_desc_label.setStyleSheet("color: #7f8c8d; font-style: italic;")
        right_layout.addWidget(self.config_desc_label)
        
        # Editors
        right_layout.addWidget(QLabel("Schema 定义 (属性):"))
        self.schema_editor = SchemaEditor()
        right_layout.addWidget(self.schema_editor)
        
        right_layout.addWidget(QLabel("系统提示词 (System Prompt):"))
        self.system_prompt_editor = QTextEdit()
        self.system_prompt_editor.setMaximumHeight(100)
        right_layout.addWidget(self.system_prompt_editor)
        
        right_layout.addWidget(QLabel("用户提示词模板 (User Prompt Template):"))
        self.user_template_editor = QTextEdit()
        self.user_template_editor.setFont(QFont("Consolas", 10))
        right_layout.addWidget(self.user_template_editor)
        
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setStretchFactor(1, 2)
        
        layout.addWidget(splitter)

    def update_domain_combo(self):
        self.domain_combo.clear()
        self.domain_combo.addItems(list(self.domains.keys()))
        self.domain_combo.addItem("➕ 新建领域...")

    def on_domain_changed(self, text):
        if text == "➕ 新建领域..." or not text:
            self.config_label.setText("配置: 新建")
            self.config_desc_label.setText("")
            self.schema_editor.set_data({})
            self.system_prompt_editor.clear()
            self.user_template_editor.clear()
            return

        config = self.domains.get(text, {})
        self.config_label.setText(f"配置: {text}")
        self.config_desc_label.setText(config.get('description', ''))
        self.schema_editor.set_data(config.get('schema', {}))
        self.system_prompt_editor.setText(config.get('prompts', {}).get('system', ''))
        self.user_template_editor.setText(config.get('prompts', {}).get('user_template', ''))

    def generate_prompts(self):
        name = self.new_domain_name.text()
        desc = self.new_domain_desc.toPlainText()
        
        if not self.api_key:
            QMessageBox.warning(self, "错误", "请先配置 API 密钥")
            return
        if not name:
            QMessageBox.warning(self, "错误", "请输入领域名称")
            return

        self.btn_generate_prompts.setEnabled(False)
        self.btn_generate_prompts.setText("正在生成...")
        
        def task():
            enricher = UniversalEnricher(self.api_key, self.base_url_input.text(), 
                                       self.model_input.text(), self.provider_combo.currentText())
            return enricher.generate_prompts_for_domain(name, desc)

        self.worker = WorkerThread(task)
        self.worker.finished.connect(lambda res: self.on_prompts_generated(name, desc, res))
        self.worker.error.connect(self.on_worker_error)
        self.worker.start()

    def on_prompts_generated(self, name, desc, result):
        self.domains[name] = {
            "description": desc,
            **result
        }
        self.save_domains()
        self.update_domain_combo()
        self.domain_combo.setCurrentText(name)
        self.btn_generate_prompts.setEnabled(True)
        self.btn_generate_prompts.setText("✨ 自动生成提示词")
        QMessageBox.information(self, "成功", f"领域 '{name}' 创建成功！")

    def load_universal_defaults(self):
        self.user_template_editor.setText(DEFAULT_UNIVERSAL_PROMPT)
        self.system_prompt_editor.setText("你是一个知识图谱构建专家，擅长从非结构化文本或知识库中提取结构化实体属性。")
        QMessageBox.information(self, "成功", "已加载通用领域模板")

    def load_chemical_defaults(self):
        self.user_template_editor.setText(DEFAULT_CHEMICAL_PROMPT)
        self.system_prompt_editor.setText("你是一个化学领域的专家，精通化学品知识图谱的构建。")
        QMessageBox.information(self, "成功", "已加载默认化学品提示词")

    def save_current_config(self):
        domain = self.domain_combo.currentText()
        if domain == "➕ 新建领域..." or not domain:
            return
            
        try:
            new_schema = self.schema_editor.get_data()
            self.domains[domain]['schema'] = new_schema
            self.domains[domain]['prompts']['system'] = self.system_prompt_editor.toPlainText()
            self.domains[domain]['prompts']['user_template'] = self.user_template_editor.toPlainText()
            self.save_domains()
            QMessageBox.information(self, "成功", "配置已保存")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {e}")

    def setup_data_tab(self):
        layout = QVBoxLayout(self.tab2)
        layout.setContentsMargins(40, 40, 40, 40)
        layout.setSpacing(20)
        
        # Header
        header = QLabel("数据处理流水线")
        header.setStyleSheet("font-size: 20px; font-weight: bold; color: #2f3640;")
        layout.addWidget(header)
        
        # Form Container
        form_container = QGroupBox("任务配置")
        form_layout = QFormLayout()
        form_layout.setSpacing(15)
        form_layout.setContentsMargins(20, 30, 20, 30)
        
        # File Input
        file_widget = QWidget()
        file_layout = QHBoxLayout(file_widget)
        file_layout.setContentsMargins(0, 0, 0, 0)
        self.file_path_input = QLineEdit()
        self.file_path_input.setPlaceholderText("选择 CSV 文件...")
        self.btn_browse = QPushButton("浏览...")
        self.btn_browse.setFixedWidth(100)
        self.btn_browse.clicked.connect(self.browse_file)
        file_layout.addWidget(self.file_path_input)
        file_layout.addWidget(self.btn_browse)
        form_layout.addRow("输入文件:", file_widget)
        
        self.col_combo = QComboBox()
        form_layout.addRow("实体名称列:", self.col_combo)
        
        self.output_name_input = QLineEdit()
        form_layout.addRow("输出文件名:", self.output_name_input)
        
        form_container.setLayout(form_layout)
        layout.addWidget(form_container)
        
        # Action Area
        action_layout = QVBoxLayout()
        self.btn_process = QPushButton("🚀 开始补全数据")
        self.btn_process.setMinimumHeight(50)
        self.btn_process.setStyleSheet("""
            QPushButton {
                background-color: #00a8ff;
                font-size: 16px;
                border-radius: 8px;
            }
            QPushButton:hover { background-color: #0097e6; }
        """)
        self.btn_process.clicked.connect(self.process_data)
        action_layout.addWidget(self.btn_process)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(20)
        self.progress_bar.setTextVisible(True)
        action_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("准备就绪")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: #7f8c8d;")
        action_layout.addWidget(self.status_label)
        
        layout.addLayout(action_layout)
        layout.addStretch()

    def browse_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, '打开 CSV 文件', '', 'CSV 文件 (*.csv)')
        if fname:
            self.file_path_input.setText(fname)
            try:
                df = pd.read_csv(fname)
                self.col_combo.clear()
                self.col_combo.addItems(df.columns.tolist())
                domain = self.domain_combo.currentText()
                if domain and domain != "➕ 新建领域...":
                    self.output_name_input.setText(f"enriched_{domain}.csv")
            except Exception as e:
                QMessageBox.warning(self, "错误", f"无法读取文件: {e}")

    def process_data(self):
        if not self.api_key:
            QMessageBox.warning(self, "错误", "请先配置 API 密钥")
            return
        
        domain = self.domain_combo.currentText()
        if not domain or domain == "➕ 新建领域...":
            QMessageBox.warning(self, "错误", "请先选择一个有效领域")
            return
            
        fname = self.file_path_input.text()
        if not fname:
            return

        try:
            df = pd.read_csv(fname)
            name_col = self.col_combo.currentText()
            output_filename = self.output_name_input.text()
            
            self.btn_process.setEnabled(False)
            self.progress_bar.setValue(0)
            self.status_label.setText("正在处理数据...")
            
            def task():
                enricher = UniversalEnricher(self.api_key, self.base_url_input.text(), 
                                           self.model_input.text(), self.provider_combo.currentText())
                domain_config = self.domains[domain]
                return enricher.process_batch(df, name_col, domain_config)

            self.worker = WorkerThread(task)
            self.worker.finished.connect(lambda res: self.on_process_finished(res, output_filename))
            self.worker.error.connect(self.on_worker_error)
            self.worker.start()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", str(e))

    def on_process_finished(self, result_df, output_filename):
        output_path = DATA_DIR / "processed" / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        self.current_df = result_df
        self.update_preview_table(result_df)
        
        self.progress_bar.setValue(100)
        self.status_label.setText(f"处理完成！文件已保存至 {output_path}")
        self.btn_process.setEnabled(True)
        QMessageBox.information(self, "成功", "处理完成")

    def on_worker_error(self, error_msg):
        self.btn_process.setEnabled(True)
        self.btn_generate_prompts.setEnabled(True)
        self.btn_generate_prompts.setText("✨ 自动生成提示词")
        self.status_label.setText("发生错误")
        QMessageBox.critical(self, "错误", error_msg)

    def setup_preview_tab(self):
        layout = QVBoxLayout(self.tab3)
        layout.setContentsMargins(20, 20, 20, 20)
        
        self.preview_table = QTableWidget()
        self.preview_table.setAlternatingRowColors(True)
        self.preview_table.setStyleSheet("""
            QTableWidget {
                gridline-color: #ecf0f1;
            }
            QHeaderView::section {
                background-color: #f5f6fa;
                padding: 8px;
                border: none;
                font-weight: bold;
            }
        """)
        layout.addWidget(self.preview_table)

    def update_preview_table(self, df):
        self.preview_table.setRowCount(df.shape[0])
        self.preview_table.setColumnCount(df.shape[1])
        self.preview_table.setHorizontalHeaderLabels(df.columns)
        
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                self.preview_table.setItem(i, j, QTableWidgetItem(str(df.iat[i, j])))

    def setup_pipeline_tab(self):
        layout = QVBoxLayout(self.tab4)
        layout.setContentsMargins(40, 40, 40, 40)
        
        header = QLabel("完整流水线控制")
        header.setStyleSheet("font-size: 20px; font-weight: bold; color: #2f3640;")
        layout.addWidget(header)
        
        desc = QLabel("在此处可以运行完整的 OpenChemKG 流水线，包括数据清洗、补全、后处理和图构建。")
        desc.setStyleSheet("color: #7f8c8d; margin-bottom: 20px;")
        layout.addWidget(desc)
        
        self.btn_run_pipeline = QPushButton("▶️ 运行完整流水线")
        self.btn_run_pipeline.setMinimumHeight(60)
        self.btn_run_pipeline.setStyleSheet("""
            QPushButton {
                background-color: #e84118;
                font-size: 18px;
                border-radius: 8px;
            }
            QPushButton:hover { background-color: #c23616; }
        """)
        self.btn_run_pipeline.clicked.connect(self.run_pipeline)
        layout.addWidget(self.btn_run_pipeline)
        
        layout.addWidget(QLabel("运行日志:"))
        self.pipeline_log = QTextEdit()
        self.pipeline_log.setReadOnly(True)
        self.pipeline_log.setStyleSheet("""
            QTextEdit {
                background-color: #2f3640;
                color: #f5f6fa;
                font-family: Consolas, monospace;
                border-radius: 8px;
                padding: 10px;
            }
        """)
        layout.addWidget(self.pipeline_log)

    def run_pipeline(self):
        if not self.api_key:
            QMessageBox.warning(self, "错误", "请先配置 API 密钥")
            return
            
        self.btn_run_pipeline.setEnabled(False)
        self.pipeline_log.append("正在启动流水线...")
        
        def task():
            # Import Pipeline here
            from main import Pipeline
            pipeline = Pipeline()
            pipeline.run()
            return "完成"

        self.worker = WorkerThread(task)
        self.worker.finished.connect(self.on_pipeline_finished)
        self.worker.error.connect(self.on_worker_error)
        self.worker.start()

    def on_pipeline_finished(self, res):
        self.pipeline_log.append("流水线运行完成！")
        self.btn_run_pipeline.setEnabled(True)
        QMessageBox.information(self, "成功", "流水线运行完成")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion")) # Use Fusion as base
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
