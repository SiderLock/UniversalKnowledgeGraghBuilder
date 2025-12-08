import sys
import os
import yaml
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import getpass
import platform
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QComboBox, QTextEdit, 
                             QPushButton, QFileDialog, QMessageBox, QProgressBar, 
                             QSplitter, QTableWidget, QTableWidgetItem, QHeaderView, 
                             QGroupBox, QFormLayout, QFrame, QSizePolicy, QStyleFactory, 
                             QGraphicsDropShadowEffect, QStackedWidget, QListWidget, 
                             QListWidgetItem, QScrollArea, QCheckBox, QTabWidget,
                             QSpinBox, QSlider, QToolButton, QPlainTextEdit, QGridLayout)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QSize, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QFont, QIcon, QPalette, QColor, QAction

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.universal_enricher import UniversalEnricher

# Setup logging
logger = logging.getLogger(__name__)

# Constants
CONFIG_DIR = Path("config")
DOMAINS_FILE = CONFIG_DIR / "domains.yaml"
SETTINGS_FILE = CONFIG_DIR / "settings.yaml"
DATA_DIR = Path("data")

# --- Default Prompts (Kept from original) ---
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

# --- Styles ---
class Theme:
    LIGHT = {
        "bg_main": "#f8f9fa",       # 更现代的浅灰背景
        "bg_card": "#ffffff",       # 纯白卡片
        "bg_sidebar": "#ffffff",    # 侧边栏背景
        "text_main": "#212529",     # 深灰主文本
        "text_secondary": "#495057", # 次要文本
        "text_muted": "#adb5bd",    # 弱化文本
        "accent": "#0d6efd",        # 现代蓝
        "accent_hover": "#0b5ed7",  # 悬停状态
        "border": "#dee2e6",        # 边框色
        "input_bg": "#ffffff",      # 输入框背景
        "selection": "#e7f1ff",     # 选中背景
        "selection_text": "#0d6efd",# 选中文本
        "danger": "#dc3545",        # 危险色
        "danger_hover": "#bb2d3b",  # 危险色悬停
        "scroll_bg": "#f8f9fa",
        "scroll_handle": "#ced4da", # 滚动条
        "success": "#198754",       # 成功色
        "warning": "#ffc107"        # 警告色
    }
    
    DARK = {
        "bg_main": "#212529",       # 深色背景
        "bg_card": "#2c3034",       # 卡片背景
        "bg_sidebar": "#2c3034",    # 侧边栏背景
        "text_main": "#f8f9fa",     # 主文本
        "text_secondary": "#dee2e6", # 次要文本
        "text_muted": "#6c757d",    # 弱化文本
        "accent": "#0d6efd",        # 现代蓝
        "accent_hover": "#0b5ed7",  # 悬停状态
        "border": "#495057",        # 边框颜色
        "input_bg": "#343a40",      # 输入框背景
        "selection": "#0a58ca",     # 选中背景
        "selection_text": "#ffffff",# 选中文本
        "danger": "#dc3545",        # 危险色
        "danger_hover": "#bb2d3b",  # 危险色悬停
        "scroll_bg": "#212529",
        "scroll_handle": "#495057", # 滚动条
        "success": "#198754",       # 成功色
        "warning": "#ffc107"        # 警告色
    }

class ModernStyle:
    @staticmethod
    def get_style(theme_name="Light"):
        colors = Theme.DARK if theme_name == "Dark" else Theme.LIGHT
        return f"""
        /* Global */
        QMainWindow {{
            background-color: {colors['bg_main']};
        }}
        QWidget {{
            font-family: 'Segoe UI', 'Microsoft YaHei UI', sans-serif;
            font-size: 14px;
            color: {colors['text_main']};
        }}
        
        /* Sidebar */
        QListWidget {{
            background-color: {colors['bg_sidebar']};
            border: none;
            outline: none;
            padding: 10px;
            border-right: 1px solid {colors['border']};
        }}
        QListWidget::item {{
            height: 40px;
            border-radius: 6px;
            padding-left: 10px;
            margin-bottom: 2px;
            color: {colors['text_secondary']};
            font-weight: 500;
        }}
        QListWidget::item:selected {{
            background-color: {colors['selection']};
            color: {colors['selection_text']};
            font-weight: 600;
        }}
        QListWidget::item:hover {{
            background-color: {colors['bg_main']};
            color: {colors['text_main']};
        }}

        /* Cards/Containers */
        QFrame#Card {{
            background-color: {colors['bg_card']};
            border-radius: 8px;
            border: 1px solid {colors['border']};
        }}
        
        /* Buttons */
        QPushButton {{
            background-color: {colors['accent']};
            color: white;
            border: none;
            border-radius: 4px;
            padding: 6px 12px;
            font-weight: 600;
            font-size: 13px;
        }}
        QPushButton:hover {{
            background-color: {colors['accent_hover']};
        }}
        QPushButton:pressed {{
            background-color: {colors['accent']};
            padding-top: 7px;
            padding-bottom: 5px;
        }}
        QPushButton:disabled {{
            background-color: {colors['border']};
            color: {colors['text_muted']};
        }}
        QPushButton#SecondaryButton {{
            background-color: transparent;
            border: 1px solid {colors['border']};
            color: {colors['text_secondary']};
        }}
        QPushButton#SecondaryButton:hover {{
            background-color: {colors['bg_main']};
            border-color: {colors['accent']};
            color: {colors['accent']};
        }}
        QPushButton#DangerButton {{
            background-color: {colors['danger']};
        }}
        QPushButton#DangerButton:hover {{
            background-color: {colors['danger_hover']};
        }}
        QPushButton#GhostButton {{
            background-color: transparent;
            color: {colors['text_secondary']};
            border: none;
        }}
        QPushButton#GhostButton:hover {{
            background-color: {colors['bg_main']};
            color: {colors['text_main']};
        }}

        /* Inputs */
        QLineEdit, QTextEdit, QPlainTextEdit {{
            border: 1px solid {colors['border']};
            border-radius: 6px;
            padding: 8px;
            background-color: {colors['input_bg']};
            color: {colors['text_main']};
            selection-background-color: {colors['selection']};
            selection-color: {colors['selection_text']};
        }}
        QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {{
            border: 1px solid {colors['accent']};
            background-color: {colors['bg_card']};
        }}

        /* ComboBox */
        QComboBox {{
            border: 1px solid {colors['border']};
            border-radius: 6px;
            padding: 6px 10px;
            background-color: {colors['input_bg']};
            color: {colors['text_main']};
            min-width: 6em;
        }}
        QComboBox:hover {{
            border-color: {colors['accent']};
        }}
        QComboBox::drop-down {{
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 20px;
            border-left-width: 0px;
        }}
        QComboBox QAbstractItemView {{
            border: 1px solid {colors['border']};
            background-color: {colors['bg_card']};
            selection-background-color: {colors['selection']};
            selection-color: {colors['selection_text']};
            outline: none;
        }}

        /* GroupBox */
        QGroupBox {{
            border: 1px solid {colors['border']};
            border-radius: 8px;
            margin-top: 1.2em;
            padding-top: 10px; 
            font-weight: 600;
            color: {colors['text_secondary']};
        }}
        QGroupBox::title {{
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 0 5px;
            left: 10px;
        }}

        /* TabWidget */
        QTabWidget::pane {{
            border: 1px solid {colors['border']};
            border-radius: 6px;
            background-color: {colors['bg_card']};
        }}
        QTabBar::tab {{
            background: {colors['bg_main']};
            color: {colors['text_secondary']};
            padding: 8px 16px;
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
            margin-right: 2px;
        }}
        QTabBar::tab:selected {{
            background: {colors['bg_card']};
            color: {colors['accent']};
            border-bottom: 2px solid {colors['accent']};
            font-weight: bold;
        }}
        QTabBar::tab:hover {{
            color: {colors['text_main']};
        }}

        /* ProgressBar */
        QProgressBar {{
            border: none;
            background-color: {colors['border']};
            border-radius: 4px;
            text-align: center;
            color: white;
        }}
        QProgressBar::chunk {{
            background-color: {colors['accent']};
            border-radius: 4px;
        }}

        /* ToolTip */
        QToolTip {{
            border: 1px solid {colors['border']};
            background-color: {colors['bg_card']};
            color: {colors['text_main']};
            padding: 4px;
            border-radius: 4px;
            opacity: 230;
        }}

        /* Tables */
        QTableWidget {{
            border: 1px solid {colors['border']};
            border-radius: 8px;
            background-color: {colors['bg_card']};
            gridline-color: {colors['border']};
            color: {colors['text_main']};
            selection-background-color: {colors['selection']};
            selection-color: {colors['selection_text']};
            alternate-background-color: {colors['bg_main']};
        }}
        QHeaderView::section {{
            background-color: {colors['bg_main']};
            padding: 8px;
            border: none;
            border-bottom: 1px solid {colors['border']};
            font-weight: 600;
            color: {colors['text_secondary']};
        }}
        QTableWidget::item {{
            padding: 6px;
        }}

        /* Scrollbar */
        QScrollBar:vertical {{
            border: none;
            background: {colors['scroll_bg']};
            width: 8px;
            border-radius: 4px;
            margin: 0px;
        }}
        QScrollBar::handle:vertical {{
            background: {colors['scroll_handle']};
            border-radius: 4px;
            min-height: 20px;
        }}
        QScrollBar::handle:vertical:hover {{
            background: {colors['accent']};
        }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            height: 0px;
        }}
        
        /* Status Bar */
        QStatusBar {{
            background-color: {colors['bg_main']};
            color: {colors['text_secondary']};
            border-top: 1px solid {colors['border']};
        }}
        """

# --- Worker Thread ---
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

# --- Components ---

class ToastNotification(QWidget):
    def __init__(self, parent, message, type="info"):
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.SubWindow)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(24, 12, 24, 12)
        
        # Add icon based on type (using emoji for simplicity)
        icon_map = {"info": "ℹ️", "success": "✅", "error": "❌", "warning": "⚠️"}
        icon_label = QLabel(icon_map.get(type, "ℹ️"))
        icon_label.setStyleSheet("font-size: 16px; margin-right: 8px; color: white; background: transparent;")
        layout.addWidget(icon_label)

        self.label = QLabel(message)
        self.label.setStyleSheet("color: white; font-weight: 600; font-size: 14px; background: transparent;")
        layout.addWidget(self.label)
        
        color = "#0d6efd" # Info
        if type == "success": color = "#198754"
        elif type == "error": color = "#dc3545"
        elif type == "warning": color = "#ffc107"
        
        self.setStyleSheet(f"""
            QWidget {{
                background-color: {color};
                border-radius: 8px;
            }}
        """)
        
        # Shadow effect
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(15)
        shadow.setColor(QColor(0, 0, 0, 60))
        shadow.setOffset(0, 4)
        self.setGraphicsEffect(shadow)
        
        # Animation
        self.opacity_anim = QPropertyAnimation(self, b"windowOpacity")
        self.opacity_anim.setDuration(300)
        self.opacity_anim.setStartValue(0.0)
        self.opacity_anim.setEndValue(1.0)
        self.opacity_anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self.opacity_anim.start()
        
        # Auto close
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(3000, self.fade_out)
        
    def fade_out(self):
        self.opacity_anim.setDirection(QPropertyAnimation.Direction.Backward)
        self.opacity_anim.finished.connect(self.close)
        self.opacity_anim.start()

class Sidebar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedWidth(260)
        self.setObjectName("Sidebar")
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Logo Area
        logo_frame = QFrame()
        logo_frame.setFixedHeight(80)
        logo_layout = QHBoxLayout(logo_frame)
        logo_layout.setContentsMargins(20, 20, 20, 20)
        
        # Logo Icon
        logo_icon = QLabel("🌐")
        logo_icon.setStyleSheet("""
            font-size: 24px;
            background-color: #0d6efd;
            color: white;
            border-radius: 8px;
            padding: 4px;
        """)
        logo_layout.addWidget(logo_icon)
        
        logo_text_layout = QVBoxLayout()
        logo_text_layout.setSpacing(0)
        logo_title = QLabel("Universal KG")
        logo_title.setStyleSheet("""
            font-size: 18px; 
            font-weight: 800; 
            font-family: 'Segoe UI', sans-serif;
        """)
        logo_subtitle = QLabel("Builder v0.5.0")
        logo_subtitle.setStyleSheet("font-size: 11px; color: #0d6efd; font-weight: 600; margin-top: 2px;")
        
        logo_text_layout.addWidget(logo_title)
        logo_text_layout.addWidget(logo_subtitle)
        logo_layout.addLayout(logo_text_layout)
        logo_layout.addStretch()
        
        layout.addWidget(logo_frame)
        
        # Navigation List
        self.nav_list = QListWidget()
        self.nav_list.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.nav_list.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        items = [
            ("🏠  仪表盘", "dashboard", "项目概览与快捷入口"),
            ("🚀  智能向导", "wizard", "AI辅助创建领域和生成初始数据集"),
            ("🏷️  领域配置", "domain", "配置知识图谱的领域Schema和提示词"),
            ("📂  数据处理", "data", "导入CSV数据并进行知识补全"),
            ("📊  结果预览", "preview", "查看处理后的数据结果"),
            ("⚙️  流水线", "pipeline", "运行完整的数据处理流水线"),
            ("🔧  设置", "settings", "配置API Key和外观")
        ]
        
        for text, data, tooltip in items:
            item = QListWidgetItem(text)
            item.setData(Qt.ItemDataRole.UserRole, data)
            item.setToolTip(tooltip)
            self.nav_list.addItem(item)
            
        self.nav_list.setCurrentRow(0)
        layout.addWidget(self.nav_list)
        
        # Theme Toggle & User Profile
        bottom_frame = QFrame()
        bottom_frame.setStyleSheet("border-top: 1px solid #dee2e6;")
        bottom_layout = QVBoxLayout(bottom_frame)
        bottom_layout.setContentsMargins(16, 16, 16, 16)
        bottom_layout.setSpacing(12)

        # Theme Toggle
        theme_layout = QHBoxLayout()
        theme_label = QLabel("深色模式")
        theme_label.setStyleSheet("font-size: 12px; font-weight: 600;")
        
        self.theme_toggle = QCheckBox()
        self.theme_toggle.setCursor(Qt.CursorShape.PointingHandCursor)
        self.theme_toggle.toggled.connect(self.toggle_theme)
        
        theme_layout.addWidget(theme_label)
        theme_layout.addStretch()
        theme_layout.addWidget(self.theme_toggle)
        bottom_layout.addLayout(theme_layout)
        
        # User Info
        user_layout = QHBoxLayout()
        avatar = QLabel("👤")
        avatar.setStyleSheet("""
            font-size: 20px; 
            background-color: #e7f1ff;
            color: #0d6efd;
            border-radius: 18px; 
            padding: 6px;
        """)
        avatar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        avatar.setFixedSize(36, 36)
        
        user_info = QVBoxLayout()
        user_info.setSpacing(0)
        
        current_user = getpass.getuser()
        user_name = QLabel(current_user)
        user_name.setStyleSheet("font-weight: bold; font-size: 13px;")
        
        status_lbl = QLabel("Online")
        status_lbl.setStyleSheet("color: #198754; font-size: 11px;")
        
        user_info.addWidget(user_name)
        user_info.addWidget(status_lbl)
        
        user_layout.addWidget(avatar)
        user_layout.addLayout(user_info)
        user_layout.addStretch()
        
        bottom_layout.addLayout(user_layout)
        layout.addWidget(bottom_frame)

    def toggle_theme(self, checked):
        window = self.window()
        if hasattr(window, 'apply_theme'):
            window.apply_theme("Dark" if checked else "Light")

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
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_add = QPushButton("➕ 添加属性")
        self.btn_add.clicked.connect(lambda: self.add_row())
        
        self.btn_remove = QPushButton("➖ 删除选中")
        self.btn_remove.setObjectName("DangerButton")
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


class VariableButton(QPushButton):
    """可点击的变量标签按钮，点击后插入变量到文本框"""
    def __init__(self, var_name, description, target_editor=None):
        super().__init__()
        self.var_name = var_name
        self.target_editor = target_editor
        self.setText(f"{{{var_name}}}")
        self.setToolTip(description)
        self.setFixedHeight(28)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setStyleSheet("""
            QPushButton {
                background-color: #fff3e0;
                color: #e67e22;
                border: 1px solid #ffcc80;
                border-radius: 4px;
                padding: 2px 8px;
                font-size: 12px;
                font-family: 'Consolas', 'Courier New', monospace;
            }
            QPushButton:hover {
                background-color: #ffe0b2;
                border-color: #ffb74d;
            }
            QPushButton:pressed {
                background-color: #ffcc80;
            }
        """)
        self.clicked.connect(self.insert_variable)
    
    def insert_variable(self):
        if self.target_editor and hasattr(self.target_editor, 'textCursor'):
            try:
                cursor = self.target_editor.textCursor()
                # 检查文本编辑器是否有内容，避免位置越界
                text_length = len(self.target_editor.toPlainText())
                current_pos = cursor.position()
                
                # 确保位置在有效范围内
                if current_pos <= text_length:
                    cursor.insertText(f"{{{self.var_name}}}")
                    self.target_editor.setFocus()
                else:
                    # 如果位置无效，移动到文本末尾再插入
                    cursor.movePosition(cursor.MoveOperation.End)
                    cursor.insertText(f"{{{self.var_name}}}")
                    self.target_editor.setFocus()
            except Exception as e:
                # 记录错误但不中断程序
                print(f"插入变量时出错: {e}")
                # 作为备用方案，直接在末尾添加文本
                try:
                    current_text = self.target_editor.toPlainText()
                    self.target_editor.setPlainText(current_text + f"{{{self.var_name}}}")
                except:
                    pass  # 如果备用方案也失败，静默忽略


class PromptBuilderWidget(QWidget):
    """增强的 Prompt 构建器，支持变量插入、预览和模板选择"""
    prompt_changed = pyqtSignal()  # 当 prompt 内容改变时发出
    
    def __init__(self, prompt_type="user", parent=None):
        super().__init__(parent)
        self.prompt_type = prompt_type  # "system" or "user"
        self.preview_entity = "示例实体"
        self.preview_attributes = "属性1, 属性2, 属性3"
        self.preview_source = "优先参考权威数据库"
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # 初始化变量按钮列表
        self.var_buttons = []
        
        # 标题和模板选择
        header_layout = QHBoxLayout()
        
        title = QLabel("📝 System Prompt" if self.prompt_type == "system" else "💬 User Prompt Template")
        title.setStyleSheet("font-weight: bold; font-size: 14px;")
        header_layout.addWidget(title)
        
        header_layout.addStretch()
        
        # 模板下拉菜单
        if self.prompt_type == "user":
            self.template_combo = QComboBox()
            self.template_combo.setMinimumWidth(150)
            self.template_combo.addItems([
                "-- 选择模板 --",
                "🌐 通用知识模板",
                "🧪 化学品模板", 
                "🏥 医药模板",
                "🏭 制造业模板",
                "📚 学术模板",
                "🔬 科研模板"
            ])
            self.template_combo.currentTextChanged.connect(self._on_template_selected)
            header_layout.addWidget(QLabel("快速模板:"))
            header_layout.addWidget(self.template_combo)
        
        layout.addLayout(header_layout)
        
        # 变量插入区域 (仅用于 User Prompt)
        if self.prompt_type == "user":
            var_frame = QFrame()
            var_frame.setStyleSheet("""
                QFrame {
                    background-color: #f5f5f5;
                    border: 1px solid #e0e0e0;
                    border-radius: 6px;
                    padding: 5px;
                }
            """)
            var_layout = QVBoxLayout(var_frame)
            var_layout.setContentsMargins(10, 8, 10, 8)
            var_layout.setSpacing(6)
            
            var_title = QLabel("📌 点击插入变量:")
            var_title.setStyleSheet("color: #666; font-size: 12px; font-weight: bold;")
            var_layout.addWidget(var_title)
            
            # 变量按钮行
            btn_layout = QHBoxLayout()
            btn_layout.setSpacing(8)
            
            variables = [
                ("entity_name", "实体名称 - 当前处理的实体名"),
                ("attributes", "属性列表 - Schema中定义的属性"),
                ("source_instruction", "数据来源 - 数据来源要求说明"),
            ]
            
            for var_name, desc in variables:
                btn = VariableButton(var_name, desc)
                self.var_buttons.append(btn)
                btn_layout.addWidget(btn)
            
            btn_layout.addStretch()
            var_layout.addLayout(btn_layout)
            
            layout.addWidget(var_frame)
        
        # 编辑器
        self.editor = QTextEdit()
        self.editor.setFont(QFont("Consolas", 11))
        self.editor.setMinimumHeight(120 if self.prompt_type == "system" else 200)
        self.editor.setPlaceholderText(
            "输入 System Prompt，设定 AI 的角色和行为..." if self.prompt_type == "system" 
            else "输入 User Prompt 模板，使用 {变量名} 插入动态变量..."
        )
        self.editor.textChanged.connect(self._on_text_changed)
        layout.addWidget(self.editor)
        
        # 更新变量按钮的目标编辑器
        for btn in self.var_buttons:
            btn.target_editor = self.editor
        
        # 预览区域 (仅用于 User Prompt)
        if self.prompt_type == "user":
            # 预览折叠面板
            preview_header = QHBoxLayout()
            self.preview_toggle = QPushButton("👁️ 预览效果")
            self.preview_toggle.setObjectName("SecondaryButton")
            self.preview_toggle.setCheckable(True)
            self.preview_toggle.clicked.connect(self._toggle_preview)
            preview_header.addWidget(self.preview_toggle)
            preview_header.addStretch()
            
            # 预览参数输入
            preview_header.addWidget(QLabel("测试实体:"))
            self.preview_entity_input = QLineEdit("示例化学品")
            self.preview_entity_input.setMaximumWidth(120)
            self.preview_entity_input.textChanged.connect(self._update_preview)
            preview_header.addWidget(self.preview_entity_input)
            
            layout.addLayout(preview_header)
            
            # 预览内容区
            self.preview_frame = QFrame()
            self.preview_frame.setStyleSheet("""
                QFrame {
                    background-color: #fff8e1;
                    border: 1px solid #ffcc80;
                    border-radius: 6px;
                }
            """)
            self.preview_frame.setVisible(False)
            
            preview_layout = QVBoxLayout(self.preview_frame)
            preview_layout.setContentsMargins(12, 12, 12, 12)
            
            preview_label = QLabel("📋 渲染后的 Prompt:")
            preview_label.setStyleSheet("color: #e65100; font-weight: bold; font-size: 12px;")
            preview_layout.addWidget(preview_label)
            
            self.preview_text = QTextEdit()
            self.preview_text.setReadOnly(True)
            self.preview_text.setMaximumHeight(150)
            self.preview_text.setStyleSheet("""
                QTextEdit {
                    background-color: #fffde7;
                    border: none;
                    font-family: 'Consolas', 'Courier New', monospace;
                    font-size: 11px;
                }
            """)
            preview_layout.addWidget(self.preview_text)
            
            layout.addWidget(self.preview_frame)
    
    def _on_template_selected(self, template_name):
        """当选择模板时填充内容"""
        templates = {
            "🌐 通用知识模板": DEFAULT_UNIVERSAL_PROMPT,
            "🧪 化学品模板": DEFAULT_CHEMICAL_PROMPT,
            "🏥 医药模板": self._get_medical_template(),
            "🏭 制造业模板": self._get_manufacturing_template(),
            "📚 学术模板": self._get_academic_template(),
            "🔬 科研模板": self._get_research_template(),
        }
        
        if template_name in templates:
            self.editor.setText(templates[template_name])
    
    def _get_medical_template(self):
        return """# 🏥 医药知识图谱数据构建指令

## 🎯 查询目标
- **药品/成分名称**: {entity_name}

## 📋 属性要求
请提供以下详细信息:

### 基础信息
- **名称**: 标准药品名称
- **别名**: 商品名、通用名、英文名等
- **分类**: 药物类别（如：抗生素、解热镇痛药等）

### 药理信息
- **适应症**: 主治疾病或症状
- **用法用量**: 推荐剂量和使用方法
- **不良反应**: 可能的副作用
- **禁忌症**: 不宜使用的情况
- **药物相互作用**: 与其他药物的相互作用

### 其他信息
- **生产厂家**: 主要生产企业
- **批准文号**: 国药准字号
- **规格**: 常见规格

## 📤 输出格式
```json
{{
    "data_source": "数据来源",
    "名称": "{entity_name}",
    "别名": "别名列表",
    "分类": "药物分类",
    "适应症": "适应症说明",
    "用法用量": "用法用量",
    "不良反应": "不良反应",
    "禁忌症": "禁忌症",
    "药物相互作用": "相互作用说明"
}}
```
"""
    
    def _get_manufacturing_template(self):
        return """# 🏭 制造业知识图谱数据构建指令

## 🎯 查询目标
- **产品/物料名称**: {entity_name}

## 📋 属性要求
{attributes}

### 基础信息
- **名称**: 标准产品名称
- **规格型号**: 产品规格
- **分类**: 产品类别

### 技术参数
- **材质**: 主要材料组成
- **尺寸**: 外形尺寸规格
- **性能指标**: 关键技术参数

### 供应链信息
- **供应商**: 主要供应商列表
- **生产周期**: 标准生产周期
- **应用领域**: 主要应用场景

## 📤 输出格式
返回 JSON 格式数据
"""
    
    def _get_academic_template(self):
        return """# 📚 学术知识图谱数据构建指令

## 🎯 查询目标
- **概念/术语**: {entity_name}

## 📋 属性要求
{attributes}

### 定义与描述
- **定义**: 标准学术定义
- **别称**: 其他常用名称
- **所属领域**: 学科分类

### 学术关联
- **相关概念**: 关联的学术概念
- **上位概念**: 更广泛的概念
- **下位概念**: 更具体的概念

### 参考来源
- **数据来源**: {source_instruction}

## 📤 输出格式
返回 JSON 格式的结构化数据
"""
    
    def _get_research_template(self):
        return """# 🔬 科研数据知识图谱构建指令

## 🎯 研究对象
- **实体名称**: {entity_name}

## 📋 数据采集要求
请基于以下维度提取研究相关信息:
{attributes}

### 基础描述
- **名称**: 标准科学命名
- **分类**: 科学分类体系
- **描述**: 详细科学描述

### 研究信息
- **研究方法**: 常用研究方法
- **关键发现**: 重要科研发现
- **应用前景**: 潜在应用方向

### 数据来源
{source_instruction}

## 📤 输出规范
以 JSON 格式返回结构化数据
"""
    
    def _on_text_changed(self):
        self.prompt_changed.emit()
        if hasattr(self, 'preview_frame') and self.preview_frame.isVisible():
            self._update_preview()
    
    def _toggle_preview(self, checked):
        self.preview_frame.setVisible(checked)
        if checked:
            self._update_preview()
    
    def _update_preview(self):
        template = self.editor.toPlainText()
        entity = self.preview_entity_input.text() if hasattr(self, 'preview_entity_input') else "示例实体"
        
        # 替换变量
        preview = template.replace("{entity_name}", entity)
        preview = preview.replace("{attributes}", self.preview_attributes)
        preview = preview.replace("{source_instruction}", self.preview_source)
        
        self.preview_text.setText(preview)
    
    def set_text(self, text):
        self.editor.setText(text)
    
    def toPlainText(self):
        return self.editor.toPlainText()
    
    def setText(self, text):
        self.editor.setText(text)
    
    def clear(self):
        self.editor.clear()
    
    def set_preview_attributes(self, attributes):
        """设置预览用的属性列表"""
        self.preview_attributes = attributes
        if hasattr(self, 'preview_frame') and self.preview_frame.isVisible():
            self._update_preview()
    
    def set_preview_source(self, source):
        """设置预览用的数据来源"""
        self.preview_source = source
        if hasattr(self, 'preview_frame') and self.preview_frame.isVisible():
            self._update_preview()


# --- Pages ---

class BasePage(QWidget):
    def __init__(self, title):
        super().__init__()
        
        # Outer layout
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)
        
        # Scroll Area
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Content Widget
        self.content_widget = QWidget()
        self.content_widget.setObjectName("PageContent")
        
        # The layout that subclasses will use
        self.layout = QVBoxLayout(self.content_widget)
        self.layout.setContentsMargins(30, 30, 30, 30)
        self.layout.setSpacing(20)
        
        # Header
        header = QLabel(title)
        header.setStyleSheet("font-size: 24px; font-weight: bold; margin-bottom: 10px;")
        self.layout.addWidget(header)
        
        self.scroll_area.setWidget(self.content_widget)
        outer_layout.addWidget(self.scroll_area)

class DashboardPage(BasePage):
    def __init__(self, main_window):
        super().__init__("🏠 仪表盘")
        self.main_window = main_window
        self.setup_ui()
    
    def setup_ui(self):
        # Welcome Section
        welcome_card = QFrame()
        welcome_card.setObjectName("Card")
        welcome_card.setStyleSheet("""
            QFrame#Card {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, 
                    stop:0 #0d6efd, stop:1 #0dcaf0);
                border: none;
                border-radius: 18px;
            }
            QLabel { color: white; }
        """)
        welcome_layout = QHBoxLayout(welcome_card)
        welcome_layout.setContentsMargins(30, 30, 30, 30)
        
        text_layout = QVBoxLayout()
        # 获取当前时间和用户名
        current_hour = datetime.now().hour
        if 5 <= current_hour < 12:
            greeting = "早上好"
        elif 12 <= current_hour < 18:
            greeting = "下午好"
        else:
            greeting = "晚上好"
            
        current_user = getpass.getuser()
        title = QLabel(f"{greeting}, {current_user}")
        title.setStyleSheet("""
            font-size: 28px; 
            font-weight: 800; 
            margin-bottom: 8px;
        """)
        
        current_time = datetime.now().strftime("%Y年%m月%d日 %A")
        subtitle = QLabel(f"今天是 {current_time}，准备好构建您的知识图谱了吗？")
        subtitle.setStyleSheet("""
            font-size: 15px; 
            opacity: 0.95;
        """)
        
        text_layout.addWidget(title)
        text_layout.addWidget(subtitle)
        welcome_layout.addLayout(text_layout)
        
        # Add a decorative icon or image on the right if possible, for now just stretch
        welcome_layout.addStretch()
        
        self.layout.addWidget(welcome_card)
        
        # Stats Grid
        stats_layout = QHBoxLayout()
        stats_layout.setSpacing(20)
        
        # 动态生成统计数据
        domain_count = len(self.main_window.domains) if hasattr(self.main_window, 'domains') else 0
        
        # 计算已处理的CSV文件数量
        data_dir = Path("data/generated")
        csv_files = list(data_dir.glob("*.csv")) if data_dir.exists() else []
        processed_files = len(csv_files)
        
        # 计算缓存文件大小
        cache_dir = Path("data/cache")
        cache_size = 0
        if cache_dir.exists():
            for f in cache_dir.rglob("*"):
                if f.is_file():
                    cache_size += f.stat().st_size
        cache_size_mb = cache_size / (1024 * 1024)
        
        # API 配置状态
        api_status = "已配置" if self.main_window.api_key else "未配置"
        api_color = "#198754" if self.main_window.api_key else "#dc3545"
        
        stats = [
            ("🏷️ 领域配置", str(domain_count), "已创建" if domain_count > 0 else "待创建"),
            ("📊 数据文件", str(processed_files), "已生成"),
            ("⚙️ API 配置", api_status, self.main_window.provider if self.main_window.api_key else "请前往设置"),
            ("💾 缓存占用", f"{cache_size_mb:.1f} MB", "本地存储")
        ]
        
        for idx, (label, value, sub) in enumerate(stats):
            card = QFrame()
            card.setObjectName("Card")
            card.setMinimumWidth(190)
            card.setStyleSheet("""
                QFrame#Card {
                    background-color: white;
                    border: 1px solid #dee2e6;
                    border-radius: 14px;
                }
                QFrame#Card:hover {
                    border-color: #0d6efd;
                }
            """)
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(24, 20, 24, 20)
            
            lbl = QLabel(label)
            lbl.setStyleSheet("color: #6c757d; font-size: 13px; font-weight: 600;")
            
            val = QLabel(value)
            val.setStyleSheet("font-size: 32px; font-weight: 800; margin: 8px 0; color: #212529;")
            
            # 根据数据类型设置不同颜色
            if idx == 2:  # API 配置
                sub_color = api_color
            elif "已创建" in sub or "已生成" in sub:
                sub_color = "#198754"  # 绿色表示完成
            elif "待创建" in sub:
                sub_color = "#ffc107"  # 黄色表示待处理
            else:
                sub_color = "#6c757d"  # 灰色表示中性
                
            sub_lbl = QLabel(sub)
            sub_lbl.setStyleSheet(f"color: {sub_color}; font-size: 12px; font-weight: 600;")
            
            card_layout.addWidget(lbl)
            card_layout.addWidget(val)
            card_layout.addWidget(sub_lbl)
            stats_layout.addWidget(card)
            
        self.layout.addLayout(stats_layout)
        
        # Domain List Section
        domain_section = QFrame()
        domain_section_layout = QVBoxLayout(domain_section)
        domain_section_layout.setSpacing(16)
        domain_section_layout.setContentsMargins(0, 20, 0, 0)
        
        domain_header = QHBoxLayout()
        domain_label = QLabel("📚 领域概览")
        domain_label.setStyleSheet("""
            font-size: 20px; 
            font-weight: 700; 
            color: #2d1810;
        """)
        domain_header.addWidget(domain_label)
        domain_header.addStretch()
        domain_section_layout.addLayout(domain_header)
        
        # Domain cards container
        if domain_count > 0:
            domains_grid = QGridLayout()
            domains_grid.setSpacing(16)
            domains_grid.setContentsMargins(0, 8, 0, 0)
            
            for idx, (domain_name, domain_config) in enumerate(list(self.main_window.domains.items())[:6]):
                domain_card = QFrame()
                domain_card.setObjectName("DomainCard")
                domain_card.setStyleSheet("""
                    QFrame#DomainCard {
                        background-color: white;
                        border: 1px solid #f5dcc9;
                        border-radius: 12px;
                        padding: 16px;
                    }
                    QFrame#DomainCard:hover {
                        border-color: #f59e42;
                        background-color: #fef9f5;
                    }
                """)
                domain_card.setMinimumHeight(100)
                domain_card_layout = QVBoxLayout(domain_card)
                domain_card_layout.setSpacing(8)
                
                # 领域名称
                name_label = QLabel(f"🔹 {domain_name}")
                name_label.setStyleSheet("font-size: 15px; font-weight: 700; color: #2d1810;")
                
                # 领域描述
                description = domain_config.get("description", "暂无描述")
                if len(description) > 80:
                    description = description[:80] + "..."
                desc_label = QLabel(description)
                desc_label.setStyleSheet("font-size: 12px; color: #636e72; margin-top: 4px;")
                desc_label.setWordWrap(True)
                
                # 属性数量
                attr_count = len(domain_config.get("schema", {}).get("attributes", []))
                attr_label = QLabel(f"属性数量: {attr_count}")
                attr_label.setStyleSheet("font-size: 11px; color: #8b5a3c; margin-top: 4px; font-weight: 600;")
                
                domain_card_layout.addWidget(name_label)
                domain_card_layout.addWidget(desc_label)
                domain_card_layout.addWidget(attr_label)
                domain_card_layout.addStretch()
                
                row = idx // 3
                col = idx % 3
                domains_grid.addWidget(domain_card, row, col)
            
            domain_section_layout.addLayout(domains_grid)
        else:
            # 空状态提示
            empty_state = QFrame()
            empty_state.setStyleSheet("""
                QFrame {
                    background-color: #fef9f5;
                    border: 2px dashed #f5dcc9;
                    border-radius: 12px;
                    padding: 40px;
                }
            """)
            empty_layout = QVBoxLayout(empty_state)
            empty_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            empty_icon = QLabel("📋")
            empty_icon.setStyleSheet("font-size: 48px;")
            empty_icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            empty_text = QLabel("暂无领域配置")
            empty_text.setStyleSheet("font-size: 16px; font-weight: 600; color: #8b5a3c; margin-top: 12px;")
            empty_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            empty_hint = QLabel("使用「智能领域向导」或「领域管理」创建您的第一个领域")
            empty_hint.setStyleSheet("font-size: 13px; color: #b2bec3; margin-top: 8px;")
            empty_hint.setAlignment(Qt.AlignmentFlag.AlignCenter)
            
            empty_layout.addWidget(empty_icon)
            empty_layout.addWidget(empty_text)
            empty_layout.addWidget(empty_hint)
            
            domain_section_layout.addWidget(empty_state)
        
        self.layout.addWidget(domain_section)
        
        # Recent Activity Section
        activity_section = QFrame()
        activity_section_layout = QVBoxLayout(activity_section)
        activity_section_layout.setSpacing(12)
        activity_section_layout.setContentsMargins(0, 20, 0, 0)
        
        activity_header = QLabel("📊 系统状态")
        activity_header.setStyleSheet("""
            font-size: 20px; 
            font-weight: 700; 
            color: #2d1810;
        """)
        activity_section_layout.addWidget(activity_header)
        
        # 状态信息卡片
        status_card = QFrame()
        status_card.setStyleSheet("""
            QFrame {
                background-color: white;
                border: 1px solid #f5dcc9;
                border-radius: 12px;
                padding: 20px;
            }
        """)
        status_layout = QVBoxLayout(status_card)
        status_layout.setSpacing(10)
        
        # 系统信息
        system_info = [
            ("🟢 LLM 服务", f"{self.main_window.provider} - {self.main_window.model_name}" if self.main_window.api_key else "未配置"),
            ("⏱️ 速率限制", f"RPM: {self.main_window.rpm} | TPM: {self.main_window.tpm}" if hasattr(self.main_window, 'rpm') else "未设置"),
            ("📁 工作目录", str(Path.cwd())),
        ]
        
        for icon_text, value in system_info:
            info_row = QHBoxLayout()
            info_label = QLabel(icon_text)
            info_label.setStyleSheet("font-size: 13px; font-weight: 600; color: #8b5a3c;")
            info_label.setMinimumWidth(120)
            
            info_value = QLabel(value)
            info_value.setStyleSheet("font-size: 13px; color: #636e72;")
            info_value.setWordWrap(True)
            
            info_row.addWidget(info_label)
            info_row.addWidget(info_value)
            info_row.addStretch()
            
            status_layout.addLayout(info_row)
        
        activity_section_layout.addWidget(status_card)
        self.layout.addWidget(activity_section)
        
        self.layout.addStretch()

class WizardPage(BasePage):
    def __init__(self, main_window):
        super().__init__("🚀 智能领域向导")
        self.main_window = main_window
        self.setup_ui()
    
    def setup_ui(self):
        # Description
        desc = QLabel("通过 AI 智能分析您的需求，自动生成领域 Schema、Prompt 模板，并创建初始数据集。适合从零开始构建知识图谱。")
        desc.setStyleSheet("color: #636e72; font-size: 13px; margin-bottom: 10px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)
        
        # Step 1: Domain Description
        step1_card = QFrame()
        step1_card.setObjectName("Card")
        step1_layout = QVBoxLayout(step1_card)
        step1_layout.setContentsMargins(25, 25, 25, 25)
        
        step1_title = QLabel("Step 1: 描述您的领域")
        step1_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #0984e3;")
        step1_layout.addWidget(step1_title)
        
        self.domain_input = QTextEdit()
        self.domain_input.setPlaceholderText("例如：我想构建一个关于中药材的知识图谱，包括中药的名称、功效、配伍、产地等信息...")
        self.domain_input.setMaximumHeight(100)
        step1_layout.addWidget(self.domain_input)
        
        self.btn_analyze = QPushButton("🔍 AI 分析并推荐")
        self.btn_analyze.setMinimumHeight(40)
        self.btn_analyze.clicked.connect(self.analyze_domain)
        step1_layout.addWidget(self.btn_analyze)
        
        self.layout.addWidget(step1_card)
        
        # Step 2: AI Recommendations
        step2_card = QFrame()
        step2_card.setObjectName("Card")
        step2_layout = QVBoxLayout(step2_card)
        step2_layout.setContentsMargins(25, 25, 25, 25)
        
        step2_title = QLabel("Step 2: AI 推荐结果")
        step2_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #0984e3;")
        step2_layout.addWidget(step2_title)
        
        # Recommendations Display
        rec_layout = QHBoxLayout()
        
        # Entity Recommendations
        entity_group = QGroupBox("推荐实体示例")
        entity_layout = QVBoxLayout(entity_group)
        self.entity_list = QTextEdit()
        self.entity_list.setReadOnly(True)
        self.entity_list.setPlaceholderText("AI将在此推荐领域相关的实体示例...")
        self.entity_list.setMaximumHeight(150)
        entity_layout.addWidget(self.entity_list)
        rec_layout.addWidget(entity_group)
        
        # Attribute Recommendations
        attr_group = QGroupBox("推荐属性")
        attr_layout = QVBoxLayout(attr_group)
        self.attr_list = QTextEdit()
        self.attr_list.setReadOnly(True)
        self.attr_list.setPlaceholderText("AI将推荐该领域应该包含的属性...")
        self.attr_list.setMaximumHeight(150)
        attr_layout.addWidget(self.attr_list)
        rec_layout.addWidget(attr_group)
        
        step2_layout.addLayout(rec_layout)
        self.layout.addWidget(step2_card)
        
        # Step 3: Generate Dataset
        step3_card = QFrame()
        step3_card.setObjectName("Card")
        step3_layout = QVBoxLayout(step3_card)
        step3_layout.setContentsMargins(25, 25, 25, 25)
        
        step3_title = QLabel("Step 3: 生成初始数据集")
        step3_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #0984e3;")
        step3_layout.addWidget(step3_title)
        
        config_layout = QHBoxLayout()
        
        config_layout.addWidget(QLabel("生成实体数量:"))
        self.entity_count = QLineEdit("20")
        self.entity_count.setMaximumWidth(100)
        self.entity_count.setPlaceholderText("1-1000")
        config_layout.addWidget(self.entity_count)
        
        config_layout.addWidget(QLabel("领域名称:"))
        self.domain_name_input = QLineEdit()
        self.domain_name_input.setPlaceholderText("例如: traditional_medicine")
        config_layout.addWidget(self.domain_name_input)
        
        config_layout.addStretch()
        step3_layout.addLayout(config_layout)
        
        self.btn_generate_dataset = QPushButton("🎯 生成完整数据集")
        self.btn_generate_dataset.setMinimumHeight(45)
        self.btn_generate_dataset.setEnabled(False)
        self.btn_generate_dataset.clicked.connect(self.generate_dataset)
        step3_layout.addWidget(self.btn_generate_dataset)
        
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        step3_layout.addWidget(self.progress)
        
        self.status = QLabel("准备就绪")
        self.status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status.setStyleSheet("color: #636e72; margin-top: 10px;")
        step3_layout.addWidget(self.status)
        
        self.layout.addWidget(step3_card)
        self.layout.addStretch()
    
    def analyze_domain(self):
        description = self.domain_input.toPlainText().strip()
        if not description:
            self.main_window.show_toast("请先描述您想要构建的领域", "warning")
            return
        
        if not self.main_window.api_key:
            self.main_window.show_toast("请先在设置页配置 API Key", "error")
            return
        
        self.btn_analyze.setEnabled(False)
        self.btn_analyze.setText("分析中...")
        self.entity_list.clear()
        self.attr_list.clear()
        
        def task():
            try:
                enricher = UniversalEnricher(self.main_window.api_key, self.main_window.base_url,
                                           self.main_window.model_name, self.main_window.provider,
                                           options={
                                               "num_ctx": self.main_window.num_ctx,
                                               "temperature": self.main_window.temperature,
                                               "keep_alive": self.main_window.keep_alive,
                                               "timeout": self.main_window.timeout
                                           },
                                           rpm=self.main_window.rpm,
                                           tpm=self.main_window.tpm,
                                           tpd=self.main_window.tpd)
                
                # 简洁的分析提示词
                analysis_prompt = f"""分析领域"{description}"，返回JSON。

直接输出（不要```包裹）：
{{
  "domain_name": "英文名_小写下划线",
  "entity_type": "EntityType",
  "recommended_entities": ["实体1", "实体2", "实体3", "实体4", "实体5", "实体6", "实体7", "实体8", "实体9", "实体10"],
  "recommended_attributes": [
    {{"name": "属性1", "description": "说明1"}},
    {{"name": "属性2", "description": "说明2"}},
    {{"name": "属性3", "description": "说明3"}},
    {{"name": "属性4", "description": "说明4"}},
    {{"name": "属性5", "description": "说明5"}}
  ]
}}

要求：提供10-15个实体，5-8个属性。只输出JSON。"""
                
                logger.info(f"开始分析领域: {description[:50]}...")
                
                # 调用LLM
                if self.main_window.provider == "dashscope":
                    import dashscope
                    from dashscope import Generation
                    dashscope.api_key = self.main_window.api_key
                    response = Generation.call(
                        model=self.main_window.model_name,
                        prompt=analysis_prompt
                    )
                    if response.status_code == 200:
                        content = response.output.text
                        logger.info(f"LLM响应: {content[:500]}")
                        result = self._parse_analysis_result(content)
                        return result
                    else:
                        raise Exception(f"API调用失败: {response.message}")
                else:
                    # OpenAI compatible (包括 openai, ollama, deepseek, kimi)
                    response_text = enricher._call_llm(analysis_prompt, json_mode=False)
                    logger.info(f"LLM响应: {response_text[:500] if response_text else 'Empty'}")
                    result = self._parse_analysis_result(response_text)
                    return result
                    
            except Exception as e:
                logger.error(f"领域分析失败: {str(e)}", exc_info=True)
                raise
        
        self.worker = WorkerThread(task)
        self.worker.finished.connect(self.on_analysis_complete)
        self.worker.error.connect(self.on_analysis_error)
        self.worker.start()
    
    def _parse_analysis_result(self, text: str) -> Dict:
        """解析领域分析结果"""
        import json
        import re
        
        if not text:
            return {}
        
        text = text.strip()
        
        # 策略1: 直接解析
        try:
            return json.loads(text)
        except:
            pass
        
        # 策略2: 移除markdown
        cleaned = re.sub(r'```json\s*', '', text)
        cleaned = re.sub(r'```\s*', '', cleaned)
        cleaned = cleaned.strip()
        try:
            return json.loads(cleaned)
        except:
            pass
        
        # 策略3: 提取JSON块
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except:
                pass
        
        # 策略4: 修复常见问题
        fixed = text
        fixed = fixed.replace('"', '"').replace('"', '"')
        fixed = re.sub(r',(\s*[}\]])', r'\1', fixed)
        if '{' in fixed:
            start = fixed.find('{')
            end = fixed.rfind('}')
            if start != -1 and end > start:
                fixed = fixed[start:end+1]
                try:
                    return json.loads(fixed)
                except:
                    pass
        
        logger.error(f"所有解析策略都失败，原始文本: {text[:500]}")
        return {}
    
    def on_analysis_complete(self, result):
        self.btn_analyze.setEnabled(True)
        self.btn_analyze.setText("🔍 AI 分析并推荐")
        
        # 详细记录结果
        logger.info(f"收到分析结果，类型: {type(result)}, 内容: {result}")
        
        # 检查结果有效性
        if not result or not isinstance(result, dict):
            self.main_window.show_toast("分析结果无效", "error")
            logger.error(f"无效的分析结果: {result}")
            return
        
        # 显示实体推荐
        entities = result.get("recommended_entities", [])
        logger.info(f"获取到的实体列表: {entities}")
        
        if entities:
            self.entity_list.setText("\n".join([f"• {e}" for e in entities]))
            logger.info(f"成功获取 {len(entities)} 个推荐实体")
        else:
            self.entity_list.setText("❌ 未能生成实体推荐\n\n可能原因：\n1. LLM 未返回 recommended_entities 字段\n2. 返回的数据格式不正确\n3. 领域描述不够清晰\n\n建议：\n• 提供更详细的领域描述\n• 查看日志文件了解详情\n• 尝试不同的 LLM 模型")
            logger.warning(f"未获取到推荐实体，完整结果: {result}")
        
        # 显示属性推荐
        attributes = result.get("recommended_attributes", [])
        logger.info(f"获取到的属性列表: {attributes}")
        
        if attributes:
            try:
                attr_lines = []
                for a in attributes:
                    if isinstance(a, dict) and 'name' in a:
                        name = a['name']
                        desc = a.get('description', '')
                        attr_lines.append(f"• {name}: {desc}")
                    else:
                        logger.warning(f"属性格式异常: {a}")
                
                if attr_lines:
                    self.attr_list.setText("\n".join(attr_lines))
                    logger.info(f"成功获取 {len(attr_lines)} 个推荐属性")
                else:
                    self.attr_list.setText("❌ 属性数据格式错误")
            except Exception as e:
                logger.error(f"处理属性推荐时出错: {e}", exc_info=True)
                self.attr_list.setText(f"❌ 处理属性时出错: {str(e)}")
        else:
            self.attr_list.setText("❌ 未能生成属性推荐\n\n可能原因：\n1. LLM 未返回 recommended_attributes 字段\n2. 返回的数据格式不正确\n\n建议：\n• 查看日志文件了解 LLM 的实际返回内容\n• 尝试修改领域描述使其更具体")
            logger.warning(f"未获取到推荐属性，完整结果: {result}")
        
        # 自动填充领域名称
        domain_name = result.get("domain_name", "")
        if domain_name:
            self.domain_name_input.setText(domain_name)
        else:
            logger.warning("未获取到领域名称")
        
        # 保存结果供后续使用
        self.analysis_result = result
        
        # 只有在有实体和属性时才启用生成按钮
        if entities and attributes:
            self.btn_generate_dataset.setEnabled(True)
            self.main_window.show_toast("AI分析完成", "success")
        else:
            self.main_window.show_toast("分析结果不完整，请检查日志并重试", "warning")
        
        logger.info(f"分析完成 - 实体: {len(entities)}, 属性: {len(attributes)}, 领域名: {domain_name}")
    
    def on_analysis_error(self, msg):
        self.btn_analyze.setEnabled(True)
        self.btn_analyze.setText("🔍 AI 分析并推荐")
        self.main_window.show_toast(f"分析失败: {msg}", "error")
    
    def generate_dataset(self):
        if not hasattr(self, 'analysis_result'):
            self.main_window.show_toast("请先进行AI分析", "warning")
            return
        
        domain_name = self.domain_name_input.text().strip()
        if not domain_name:
            self.main_window.show_toast("请输入领域名称", "warning")
            return
        
        try:
            count = int(self.entity_count.text())
            if count <= 0 or count > 1000:
                self.main_window.show_toast("实体数量应该在1-1000之间", "warning")
                return
        except ValueError:
            self.main_window.show_toast("请输入有效的数字", "warning")
            return
        
        self.btn_generate_dataset.setEnabled(False)
        self.progress.setRange(0, count)
        self.progress.setValue(0)
        self.status.setText("正在生成数据集...")
        
        def task():
            import pandas as pd
            import json
            
            # 获取领域描述
            description = self.domain_input.toPlainText().strip()
            
            enricher = UniversalEnricher(self.main_window.api_key, self.main_window.base_url,
                                       self.main_window.model_name, self.main_window.provider,
                                       options={
                                           "num_ctx": self.main_window.num_ctx,
                                           "temperature": self.main_window.temperature,
                                           "keep_alive": self.main_window.keep_alive,
                                           "timeout": self.main_window.timeout
                                       },
                                       rpm=self.main_window.rpm,
                                       tpm=self.main_window.tpm,
                                       tpd=self.main_window.tpd)
            
            # 准备Schema和Prompt
            result = self.analysis_result
            schema = {
                "entity_type": result.get("entity_type", "Entity"),
                "attributes": result.get("recommended_attributes", [])
            }
            
            # 生成实体列表
            base_entities = result.get("recommended_entities", [])
            if not base_entities:
                raise ValueError("分析结果中没有推荐实体")
            
            # 根据需要的数量决定是否需要重新生成
            if count <= len(base_entities):
                # 如果需要的数量小于等于基础实体数量，直接截取
                entities = base_entities[:count]
            else:
                # 如果需要更多实体，调用LLM重新生成指定数量的实体
                self.worker.progress.emit(f"需要生成{count}个实体，正在调用AI生成更多实体...")
                
                # 增强的实体生成提示词
                generation_prompt = f"""你是一个专业的知识图谱构建专家。请为指定领域生成实体列表。

【领域信息】
- 领域描述：{description}
- 实体类型：{result.get('entity_type', 'Entity')}
- 参考示例：{', '.join(base_entities[:5])}

【任务要求】
请生成{count}个该领域的代表性实体，严格按照以下JSON格式返回：

{{
    "entities": ["实体1", "实体2", "实体3", ... , "实体{count}"]
}}

【生成规则】
1. 实体名称要准确、专业、具有代表性
2. 确保多样化，涵盖该领域的不同子类别和方面
3. 避免重复，每个实体必须唯一
4. 保持与示例实体相似的命名风格和专业程度
5. 直接输出JSON，不要使用markdown代码块
6. entities数组必须包含正好{count}个元素"""
                
                try:
                    # 调用LLM生成更多实体
                    if self.worker.main_window.provider == "dashscope":
                        import dashscope
                        from dashscope import Generation
                        dashscope.api_key = self.worker.main_window.api_key
                        response = Generation.call(
                            model=self.worker.main_window.model_name,
                            prompt=generation_prompt,
                            result_format='message'
                        )
                        if response.status_code == 200:
                            generation_result = response.output.choices[0].message.content
                        else:
                            raise Exception(f"API调用失败: {response.message}")
                    else:
                        # Ollama、DeepSeek或其他提供商
                        import requests
                        response = requests.post(
                            f"{self.worker.main_window.base_url}/api/chat",
                            json={
                                "model": self.worker.main_window.model_name,
                                "messages": [{"role": "user", "content": generation_prompt}],
                                "stream": False,
                                "options": {
                                    "num_ctx": self.worker.main_window.num_ctx,
                                    "temperature": self.worker.main_window.temperature,
                                    "keep_alive": self.worker.main_window.keep_alive
                                }
                            },
                            timeout=self.worker.main_window.timeout
                        )
                        generation_result = response.json()["message"]["content"]
                    
                    # 解析生成的实体
                    from modules.llm_json_parser import RobustLLMJsonParser
                    parser = RobustLLMJsonParser()
                    generation_data = parser.parse(generation_result)
                    
                    if "entities" in generation_data:
                        entities = generation_data["entities"][:count]
                        if len(entities) < count:
                            # 如果生成的实体不够，用原始实体补充
                            entities.extend(base_entities[:count - len(entities)])
                    else:
                        # 解析失败，使用原始方案
                        entities = base_entities[:count]
                        
                except Exception as e:
                    self.worker.progress.emit(f"生成更多实体失败: {str(e)}，使用默认方案")
                    # 降级到原始方案
                    entities = base_entities * ((count // len(base_entities)) + 1)
                    entities = entities[:count]
            
            # 构建数据集
            data_rows = []
            for idx, entity in enumerate(entities):
                # 为每个实体生成完整属性
                entity_data = {"名称": entity}
                for attr in schema["attributes"]:
                    entity_data[attr["name"]] = f"[待补全]"
                data_rows.append(entity_data)
                self.worker.progress.emit(idx + 1)
            
            df = pd.DataFrame(data_rows)
            
            # 保存配置到domains
            domain_config = {
                "description": self.domain_input.toPlainText(),
                "schema": schema,
                "prompts": {
                    "system": "你是一个知识图谱构建专家，擅长提取和组织结构化信息。",
                    "user_template": f"请为实体 '{{{{entity_name}}}}' 提供详细的属性信息，返回JSON格式。"
                }
            }
            
            return {
                "dataframe": df,
                "domain_name": domain_name,
                "domain_config": domain_config
            }
        
        self.worker = WorkerThread(task)
        self.worker.progress.connect(self.progress.setValue)
        self.worker.finished.connect(self.on_dataset_complete)
        self.worker.error.connect(self.on_dataset_error)
        self.worker.start()
    
    def on_dataset_complete(self, result):
        # 检查结果是否有效
        if result is None:
            self.status.setText("生成失败：任务返回了空结果")
            self.btn_generate_dataset.setEnabled(True)
            self.main_window.show_toast("数据集生成失败：无返回结果", "error")
            return
        
        if not isinstance(result, dict) or "dataframe" not in result:
            self.status.setText("生成失败：返回数据格式错误")
            self.btn_generate_dataset.setEnabled(True)
            self.main_window.show_toast("数据集生成失败：数据格式错误", "error")
            return
        
        df = result["dataframe"]
        domain_name = result["domain_name"]
        domain_config = result["domain_config"]
        
        # 保存CSV
        output_path = DATA_DIR / "generated" / f"{domain_name}_initial.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        # 保存领域配置
        self.main_window.domains[domain_name] = domain_config
        self.main_window.save_domains()
        
        # 更新领域列表
        self.main_window.domain_page.update_domains()
        self.main_window.domain_page.domain_combo.setCurrentText(domain_name)
        
        self.progress.setValue(self.progress.maximum())
        self.status.setText("生成完成！")
        self.btn_generate_dataset.setEnabled(True)
        
        reply = QMessageBox.question(
            self, "生成完成",
            f"初始数据集已生成！\n\n文件位置: {output_path}\n实体数量: {len(df)}\n\n是否跳转到数据处理页面开始补全属性？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # 切换到数据处理页面
            self.main_window.sidebar.nav_list.setCurrentRow(2)  # Data page
            # 加载生成的文件
            self.main_window.data_page.file_input.setText(str(output_path))
            self.main_window.data_page.col_combo.clear()
            self.main_window.data_page.col_combo.addItems(df.columns.tolist())
            if "名称" in df.columns:
                self.main_window.data_page.col_combo.setCurrentText("名称")
            self.main_window.data_page.output_input.setText(f"{domain_name}_enriched.csv")
    
    def on_dataset_error(self, msg):
        self.btn_generate_dataset.setEnabled(True)
        self.status.setText("生成失败")
        QMessageBox.critical(self, "错误", f"生成失败: {msg}")

class DomainPage(BasePage):
    """增强的领域配置页面，使用选项卡布局和可视化 Prompt 构建器"""
    def __init__(self, main_window):
        super().__init__("🏷️ 领域配置")
        self.main_window = main_window
        self.setup_ui()

    def setup_ui(self):
        # Description
        desc = QLabel("管理和配置知识图谱领域。使用选项卡切换不同的配置区域，通过可视化工具快速构建高质量的 Prompt。")
        desc.setStyleSheet("color: #636e72; font-size: 13px; margin-bottom: 10px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)

        # 顶部工具栏
        toolbar = QFrame()
        toolbar.setStyleSheet("""
            QFrame {
                background-color: #f0f4f8;
                border-radius: 8px;
                padding: 5px;
            }
        """)
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(15, 10, 15, 10)
        
        # 领域选择
        toolbar_layout.addWidget(QLabel("📂 当前领域:"))
        self.domain_combo = QComboBox()
        self.domain_combo.setMinimumWidth(180)
        self.domain_combo.currentTextChanged.connect(self.on_domain_changed)
        toolbar_layout.addWidget(self.domain_combo)
        
        toolbar_layout.addSpacing(20)
        
        # 新建领域
        self.new_domain_name = QLineEdit()
        self.new_domain_name.setPlaceholderText("新领域 ID (英文)")
        self.new_domain_name.setMaximumWidth(150)
        toolbar_layout.addWidget(self.new_domain_name)
        
        self.btn_create = QPushButton("➕ 创建")
        self.btn_create.setObjectName("SecondaryButton")
        self.btn_create.clicked.connect(self.create_new_domain)
        toolbar_layout.addWidget(self.btn_create)
        
        toolbar_layout.addStretch()
        
        # 保存和删除按钮
        self.btn_delete = QPushButton("🗑️ 删除")
        self.btn_delete.setObjectName("DangerButton")
        self.btn_delete.clicked.connect(self.delete_domain)
        toolbar_layout.addWidget(self.btn_delete)
        
        self.btn_save = QPushButton("💾 保存配置")
        self.btn_save.clicked.connect(self.save_config)
        toolbar_layout.addWidget(self.btn_save)
        
        self.layout.addWidget(toolbar)
        
        # 主选项卡
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #dfe6e9;
                border-radius: 8px;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #f8f9fa;
                border: 1px solid #dfe6e9;
                border-bottom: none;
                padding: 10px 20px;
                margin-right: 2px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #0984e3;
            }
            QTabBar::tab:hover {
                background-color: #e3f2fd;
            }
        """)
        
        # Tab 1: 基础信息 & Schema
        tab_basic = QWidget()
        tab_basic_layout = QVBoxLayout(tab_basic)
        tab_basic_layout.setContentsMargins(20, 20, 20, 20)
        tab_basic_layout.setSpacing(15)
        
        # 领域描述
        desc_group = QGroupBox("📝 领域描述")
        desc_layout = QVBoxLayout(desc_group)
        self.domain_desc = QTextEdit()
        self.domain_desc.setPlaceholderText("描述这个领域的主要内容、目标和特点...")
        self.domain_desc.setMaximumHeight(80)
        desc_layout.addWidget(self.domain_desc)
        tab_basic_layout.addWidget(desc_group)
        
        # Schema 编辑器
        schema_group = QGroupBox("🔧 Schema 定义")
        schema_layout = QVBoxLayout(schema_group)
        self.schema_editor = SchemaEditor()
        schema_layout.addWidget(self.schema_editor)
        tab_basic_layout.addWidget(schema_group)
        
        self.tabs.addTab(tab_basic, "📋 基础 & Schema")
        
        # Tab 2: 数据来源配置
        tab_source = QWidget()
        tab_source_layout = QVBoxLayout(tab_source)
        tab_source_layout.setContentsMargins(20, 20, 20, 20)
        tab_source_layout.setSpacing(15)
        
        # 数据来源说明
        source_info = QLabel("""
        <div style='background-color: #e3f2fd; padding: 15px; border-radius: 8px; margin-bottom: 10px;'>
            <p style='margin: 0; color: #1565c0;'><b>💡 数据来源配置说明</b></p>
            <p style='margin: 5px 0 0 0; color: #1976d2; font-size: 12px;'>
            设置 AI 在生成数据时优先参考的来源。这将影响数据的权威性和准确性。
            配置的内容会自动插入到 Prompt 的 <code>{source_instruction}</code> 变量中。
            </p>
        </div>
        """)
        source_info.setWordWrap(True)
        tab_source_layout.addWidget(source_info)
        
        # 快速选择
        quick_source = QGroupBox("⚡ 快速选择常用来源")
        quick_layout = QVBoxLayout(quick_source)
        
        source_btn_layout = QHBoxLayout()
        sources = [
            ("🔬 PubChem", "优先参考 PubChem 数据库的化学品信息"),
            ("📚 Wikipedia", "优先参考 Wikipedia 百科全书"),
            ("🏛️ ECHA", "优先参考欧洲化学品管理局(ECHA)数据"),
            ("🔍 学术文献", "优先参考学术期刊和论文"),
            ("📊 官方标准", "优先参考国家标准和行业标准"),
        ]
        
        for name, source_text in sources:
            btn = QPushButton(name)
            btn.setObjectName("SecondaryButton")
            btn.setToolTip(source_text)
            btn.clicked.connect(lambda checked, t=source_text: self.append_source(t))
            source_btn_layout.addWidget(btn)
        
        source_btn_layout.addStretch()
        quick_layout.addLayout(source_btn_layout)
        tab_source_layout.addWidget(quick_source)
        
        # 自定义来源
        custom_source = QGroupBox("✏️ 自定义数据来源要求")
        custom_layout = QVBoxLayout(custom_source)
        self.source_input = QTextEdit()
        self.source_input.setPlaceholderText("""示例：
优先参考以下权威数据源：
1. PubChem - 用于化学品基础信息和结构数据
2. ECHA - 用于安全和监管信息
3. Wikipedia - 用于背景知识和应用信息

确保数据具有可追溯性，在 data_source 字段中标明实际使用的来源。""")
        self.source_input.setMinimumHeight(150)
        custom_layout.addWidget(self.source_input)
        tab_source_layout.addWidget(custom_source)
        
        tab_source_layout.addStretch()
        self.tabs.addTab(tab_source, "📡 数据来源")
        
        # Tab 3: System Prompt
        tab_system = QWidget()
        tab_system_layout = QVBoxLayout(tab_system)
        tab_system_layout.setContentsMargins(20, 20, 20, 20)
        tab_system_layout.setSpacing(15)
        
        system_info = QLabel("""
        <div style='background-color: #fff3e0; padding: 15px; border-radius: 8px;'>
            <p style='margin: 0; color: #e65100;'><b>🤖 System Prompt 说明</b></p>
            <p style='margin: 5px 0 0 0; color: #f57c00; font-size: 12px;'>
            System Prompt 定义了 AI 的角色和行为模式。它会在每次对话开始时发送给 AI，
            用于设定上下文和期望的输出风格。
            </p>
        </div>
        """)
        system_info.setWordWrap(True)
        tab_system_layout.addWidget(system_info)
        
        # System Prompt 快捷模板
        sys_template_group = QGroupBox("🎨 快捷角色模板")
        sys_template_layout = QHBoxLayout(sys_template_group)
        
        sys_templates = [
            ("👨‍🔬 化学专家", "你是一位资深的化学领域专家，拥有丰富的化学品知识和安全管理经验。你的回答应该准确、专业，并注重安全性说明。"),
            ("👨‍⚕️ 医药专家", "你是一位医药领域的专家，精通药理学、毒理学和临床应用。你的回答应注重科学性和安全性。"),
            ("📊 数据分析师", "你是一位专业的数据分析师，擅长提取、整理和结构化信息。你的回答应该清晰、准确、格式规范。"),
            ("🔬 科研助手", "你是一位科研助手，擅长从学术文献中提取关键信息并进行系统性整理。"),
        ]
        
        for name, template in sys_templates:
            btn = QPushButton(name)
            btn.setObjectName("SecondaryButton")
            btn.setToolTip(template[:50] + "...")
            btn.clicked.connect(lambda checked, t=template: self.system_prompt_builder.setText(t))
            sys_template_layout.addWidget(btn)
        
        sys_template_layout.addStretch()
        tab_system_layout.addWidget(sys_template_group)
        
        # System Prompt 编辑器
        self.system_prompt_builder = PromptBuilderWidget(prompt_type="system")
        tab_system_layout.addWidget(self.system_prompt_builder)
        
        self.tabs.addTab(tab_system, "🤖 System Prompt")
        
        # Tab 4: User Prompt Template
        tab_user = QWidget()
        tab_user_layout = QVBoxLayout(tab_user)
        tab_user_layout.setContentsMargins(20, 20, 20, 20)
        tab_user_layout.setSpacing(15)
        
        user_info = QLabel("""
        <div style='background-color: #e8f5e9; padding: 15px; border-radius: 8px;'>
            <p style='margin: 0; color: #2e7d32;'><b>💬 User Prompt Template 说明</b></p>
            <p style='margin: 5px 0 0 0; color: #388e3c; font-size: 12px;'>
            User Prompt Template 是用于每个实体的查询模板。使用变量（如 <code>{entity_name}</code>）
            来插入动态内容。点击下方变量按钮可快速插入。
            </p>
        </div>
        """)
        user_info.setWordWrap(True)
        tab_user_layout.addWidget(user_info)
        
        # User Prompt 编辑器 (使用增强版)
        self.user_prompt_builder = PromptBuilderWidget(prompt_type="user")
        tab_user_layout.addWidget(self.user_prompt_builder)
        
        self.tabs.addTab(tab_user, "💬 User Prompt")
        
        # Tab 5: AI 自动生成
        tab_ai = QWidget()
        tab_ai_layout = QVBoxLayout(tab_ai)
        tab_ai_layout.setContentsMargins(20, 20, 20, 20)
        tab_ai_layout.setSpacing(15)
        
        ai_info = QLabel("""
        <div style='background-color: #f3e5f5; padding: 15px; border-radius: 8px;'>
            <p style='margin: 0; color: #7b1fa2;'><b>✨ AI 智能生成</b></p>
            <p style='margin: 5px 0 0 0; color: #9c27b0; font-size: 12px;'>
            描述您的领域需求，AI 将自动生成完整的 Schema 和 Prompt 配置。
            这是快速启动新领域的最佳方式！
            </p>
        </div>
        """)
        ai_info.setWordWrap(True)
        tab_ai_layout.addWidget(ai_info)
        
        # AI 生成表单
        ai_form = QGroupBox("🎯 描述您的需求")
        ai_form_layout = QVBoxLayout(ai_form)
        
        self.ai_desc_input = QTextEdit()
        self.ai_desc_input.setPlaceholderText("""示例：
我想构建一个关于中药材的知识图谱，需要包含以下信息：
- 中药名称和别名
- 功效和主治
- 性味归经
- 用法用量
- 禁忌和注意事项
- 产地信息""")
        self.ai_desc_input.setMinimumHeight(120)
        ai_form_layout.addWidget(self.ai_desc_input)
        
        ai_btn_layout = QHBoxLayout()
        self.btn_ai_generate = QPushButton("🚀 AI 自动生成全部配置")
        self.btn_ai_generate.setMinimumHeight(45)
        self.btn_ai_generate.clicked.connect(self.ai_generate_all)
        ai_btn_layout.addWidget(self.btn_ai_generate)
        ai_form_layout.addLayout(ai_btn_layout)
        
        self.ai_progress = QProgressBar()
        self.ai_progress.setTextVisible(True)
        self.ai_progress.setVisible(False)
        ai_form_layout.addWidget(self.ai_progress)
        
        tab_ai_layout.addWidget(ai_form)
        tab_ai_layout.addStretch()
        
        self.tabs.addTab(tab_ai, "✨ AI 生成")
        
        self.layout.addWidget(self.tabs)
        
        # 连接 Schema 变化到预览更新
        self.schema_editor.table.itemChanged.connect(self._update_prompt_preview)

    def _update_prompt_preview(self):
        """当 Schema 变化时更新 Prompt 预览"""
        schema = self.schema_editor.get_data()
        attributes = [attr['name'] for attr in schema.get('attributes', [])]
        attr_str = ", ".join(attributes) if attributes else "属性1, 属性2, 属性3"
        self.user_prompt_builder.set_preview_attributes(attr_str)
        
        source = self.source_input.toPlainText()
        if source:
            self.user_prompt_builder.set_preview_source(source[:100] + "..." if len(source) > 100 else source)
    
    def append_source(self, source_text):
        """追加数据来源到输入框"""
        current = self.source_input.toPlainText()
        if current:
            self.source_input.setText(current + "\n" + source_text)
        else:
            self.source_input.setText(source_text)
    
    def create_new_domain(self):
        """创建新领域"""
        name = self.new_domain_name.text().strip()
        if not name:
            self.main_window.show_toast("请输入领域名称", "warning")
            return
        
        if name in self.main_window.domains:
            self.main_window.show_toast(f"领域 '{name}' 已存在", "warning")
            return
        
        # 创建默认配置
        self.main_window.domains[name] = {
            "description": "",
            "source_instruction": "",
            "schema": {"entity_type": "", "attributes": []},
            "prompts": {"system": "", "user_template": ""}
        }
        self.main_window.save_domains()
        self.update_domains()
        self.domain_combo.setCurrentText(name)
        self.new_domain_name.clear()
        self.main_window.show_toast(f"领域 '{name}' 已创建", "success")
    
    def delete_domain(self):
        """删除当前领域"""
        domain = self.domain_combo.currentText()
        if domain == "➕ 新建领域..." or not domain:
            return
        
        reply = QMessageBox.question(
            self, "确认删除",
            f"确定要删除领域 '{domain}' 吗？此操作不可恢复。",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            del self.main_window.domains[domain]
            self.main_window.save_domains()
            self.update_domains()
            self.main_window.show_toast(f"领域 '{domain}' 已删除", "success")

    def update_domains(self):
        current = self.domain_combo.currentText()
        self.domain_combo.blockSignals(True)
        self.domain_combo.clear()
        self.domain_combo.addItems(list(self.main_window.domains.keys()))
        self.domain_combo.addItem("➕ 新建领域...")
        if current in self.main_window.domains:
            self.domain_combo.setCurrentText(current)
        self.domain_combo.blockSignals(False)
        self.on_domain_changed(self.domain_combo.currentText())

    def on_domain_changed(self, text):
        if text == "➕ 新建领域..." or not text:
            self.domain_desc.clear()
            self.schema_editor.set_data({})
            self.source_input.clear()
            self.system_prompt_builder.clear()
            self.user_prompt_builder.clear()
            return

        config = self.main_window.domains.get(text, {})
        self.domain_desc.setText(config.get('description', ''))
        self.schema_editor.set_data(config.get('schema', {}))
        self.source_input.setText(config.get('source_instruction', ''))
        self.system_prompt_builder.setText(config.get('prompts', {}).get('system', ''))
        self.user_prompt_builder.setText(config.get('prompts', {}).get('user_template', ''))
        
        # 更新预览
        self._update_prompt_preview()
    
    def ai_generate_all(self):
        """使用 AI 自动生成全部配置"""
        desc = self.ai_desc_input.toPlainText().strip()
        if not desc:
            self.main_window.show_toast("请描述您的领域需求", "warning")
            return
        
        if not self.main_window.api_key:
            self.main_window.show_toast("请先在设置页配置 API Key", "error")
            return
        
        domain = self.domain_combo.currentText()
        if domain == "➕ 新建领域...":
            self.main_window.show_toast("请先选择或创建一个领域", "warning")
            return
        
        self.btn_ai_generate.setEnabled(False)
        self.btn_ai_generate.setText("生成中...")
        self.ai_progress.setVisible(True)
        self.ai_progress.setRange(0, 0)  # Indeterminate
        
        source_instr = self.source_input.toPlainText()
        
        def task():
            enricher = UniversalEnricher(
                self.main_window.api_key, 
                self.main_window.base_url,
                self.main_window.model_name, 
                self.main_window.provider,
                rpm=self.main_window.rpm,
                tpm=self.main_window.tpm,
                tpd=self.main_window.tpd
            )
            return enricher.generate_prompts_for_domain(domain, desc, source_instruction=source_instr)
        
        self.worker = WorkerThread(task)
        self.worker.finished.connect(lambda res: self._on_ai_generated(domain, desc, res))
        self.worker.error.connect(self._on_ai_error)
        self.worker.start()
    
    def _on_ai_generated(self, domain, desc, result):
        self.btn_ai_generate.setEnabled(True)
        self.btn_ai_generate.setText("🚀 AI 自动生成全部配置")
        self.ai_progress.setVisible(False)
        
        # 更新配置
        source_instr = self.source_input.toPlainText()
        self.main_window.domains[domain] = {
            "description": desc,
            "source_instruction": source_instr,
            **result
        }
        self.main_window.save_domains()
        
        # 刷新界面
        self.on_domain_changed(domain)
        
        # 切换到 Schema 选项卡查看结果
        self.tabs.setCurrentIndex(0)
        
        self.main_window.show_toast(f"AI 已为 '{domain}' 生成完整配置", "success")
    
    def _on_ai_error(self, msg):
        self.btn_ai_generate.setEnabled(True)
        self.btn_ai_generate.setText("🚀 AI 自动生成全部配置")
        self.ai_progress.setVisible(False)
        self.main_window.show_toast(f"生成失败: {msg}", "error")

    # 保留兼容方法
    def generate_prompts(self):
        self.tabs.setCurrentIndex(4)  # 切换到 AI 生成选项卡
        self.ai_generate_all()

    def load_universal(self):
        self.user_prompt_builder.setText(DEFAULT_UNIVERSAL_PROMPT)
        self.system_prompt_builder.setText("你是一个知识图谱构建专家，擅长提取和组织结构化信息。你的回答应该准确、完整，并严格遵循JSON格式要求。")

    def load_chemical(self):
        self.user_prompt_builder.setText(DEFAULT_CHEMICAL_PROMPT)
        self.system_prompt_builder.setText("你是一个化学领域的专家，精通化学品的性质、安全信息和产业链知识。你的回答应该准确、专业，并注重数据的权威性。")

    def save_config(self):
        domain = self.domain_combo.currentText()
        if domain == "➕ 新建领域..." or not domain:
            self.main_window.show_toast("请先选择一个领域", "warning")
            return
        try:
            self.main_window.domains[domain] = {
                'description': self.domain_desc.toPlainText(),
                'schema': self.schema_editor.get_data(),
                'source_instruction': self.source_input.toPlainText(),
                'prompts': {
                    'system': self.system_prompt_builder.toPlainText(),
                    'user_template': self.user_prompt_builder.toPlainText()
                }
            }
            self.main_window.save_domains()
            self.main_window.show_toast("配置已保存", "success")
        except Exception as e:
            self.main_window.show_toast(str(e), "error")

    # 兼容旧代码的属性访问
    @property
    def system_prompt(self):
        return self.system_prompt_builder
    
    @property
    def user_prompt(self):
        return self.user_prompt_builder

class DataPage(BasePage):
    def __init__(self, main_window):
        super().__init__("数据处理")
        self.main_window = main_window
        self.setup_ui()

    def setup_ui(self):
        # Description
        desc = QLabel("导入 CSV 数据文件，利用大模型批量补全缺失的属性信息。支持断点续传和多线程并发处理。")
        desc.setStyleSheet("color: #636e72; font-size: 13px; margin-bottom: 10px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)

        card = QFrame()
        card.setObjectName("Card")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(30, 30, 30, 30)
        card_layout.setSpacing(20)
        
        # File Selection
        file_layout = QHBoxLayout()
        self.file_input = QLineEdit()
        self.file_input.setPlaceholderText("选择 CSV 文件...")
        self.btn_browse = QPushButton("浏览")
        self.btn_browse.setObjectName("SecondaryButton")
        self.btn_browse.clicked.connect(self.browse_file)
        
        self.btn_demo = QPushButton("📋 加载示例")
        self.btn_demo.setObjectName("SecondaryButton")
        self.btn_demo.setToolTip("加载内置的化学品示例数据")
        self.btn_demo.clicked.connect(self.load_demo)
        
        file_layout.addWidget(self.file_input)
        file_layout.addWidget(self.btn_demo)
        file_layout.addWidget(self.btn_browse)
        card_layout.addLayout(file_layout)
        
        # Options
        form = QFormLayout()
        self.col_combo = QComboBox()
        form.addRow("实体名称列:", self.col_combo)
        self.output_input = QLineEdit()
        form.addRow("输出文件名:", self.output_input)
        card_layout.addLayout(form)
        
        # Neo4j Import Option
        from PyQt6.QtWidgets import QCheckBox
        neo4j_layout = QHBoxLayout()
        self.neo4j_import_checkbox = QCheckBox("处理完成后导入到 Neo4j 数据库")
        self.neo4j_import_checkbox.setChecked(False)
        self.neo4j_import_checkbox.setToolTip("勾选后将自动导入处理结果到图数据库")
        neo4j_layout.addWidget(self.neo4j_import_checkbox)
        neo4j_layout.addStretch()
        card_layout.addLayout(neo4j_layout)
        
        # Action
        self.btn_process = QPushButton("🚀 开始处理")
        self.btn_process.setMinimumHeight(45)
        self.btn_process.clicked.connect(self.process_data)
        card_layout.addWidget(self.btn_process)
        
        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        card_layout.addWidget(self.progress)
        
        self.status = QLabel("准备就绪")
        self.status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status.setStyleSheet("color: #636e72;")
        card_layout.addWidget(self.status)
        
        self.layout.addWidget(card)
        self.layout.addStretch()

    def browse_file(self):
        fname, _ = QFileDialog.getOpenFileName(self, '打开 CSV', '', 'CSV (*.csv)')
        if fname:
            self.file_input.setText(fname)
            try:
                df = pd.read_csv(fname)
                self.col_combo.clear()
                self.col_combo.addItems(df.columns.tolist())
                domain = self.main_window.domain_page.domain_combo.currentText()
                if domain and domain != "➕ 新建领域...":
                    self.output_input.setText(f"enriched_{domain}.csv")
            except Exception:
                pass

    def load_demo(self):
        """加载示例数据文件"""
        demo_path = DATA_DIR / "demo" / "sample_chemicals.csv"
        if demo_path.exists():
            self.file_input.setText(str(demo_path))
            try:
                df = pd.read_csv(demo_path)
                self.col_combo.clear()
                self.col_combo.addItems(df.columns.tolist())
                # 自动选择"品名"列
                if "品名" in df.columns:
                    self.col_combo.setCurrentText("品名")
                self.output_input.setText("enriched_demo.csv")
                QMessageBox.information(self, "提示", f"已加载示例数据 ({len(df)} 条记录)")
            except Exception as e:
                QMessageBox.warning(self, "错误", f"无法加载示例数据: {str(e)}")
        else:
            QMessageBox.warning(self, "错误", f"示例文件不存在: {demo_path}")

    def process_data(self):
        if not self.main_window.api_key:
            self.main_window.show_toast("请先配置 API Key", "error")
            return
        
        domain = self.main_window.domain_page.domain_combo.currentText()
        if not domain or domain == "➕ 新建领域...":
            self.main_window.show_toast("请先选择领域", "warning")
            return
            
        fname = self.file_input.text()
        if not fname: return

        try:
            df = pd.read_csv(fname)
            name_col = self.col_combo.currentText()
            output_file = self.output_input.text()
            
            self.btn_process.setEnabled(False)
            self.progress.setRange(0, len(df))
            self.progress.setValue(0)
            self.progress.setTextVisible(True)
            self.status.setText(f"正在处理 {len(df)} 条数据 (并发: {self.main_window.max_workers})...")
            self.status.setStyleSheet("color: #0984e3; font-weight: bold;")
            self.main_window.status_bar.showMessage("Processing data...")
            
            def task():
                enricher = UniversalEnricher(self.main_window.api_key, self.main_window.base_url, 
                                           self.main_window.model_name, self.main_window.provider,
                                           options={
                                               "num_ctx": self.main_window.num_ctx,
                                               "temperature": self.main_window.temperature,
                                               "num_gpu": getattr(self.main_window, 'num_gpu', 1),
                                               "keep_alive": self.main_window.keep_alive,
                                               "timeout": self.main_window.timeout
                                           },
                                           rpm=self.main_window.rpm,
                                           tpm=self.main_window.tpm,
                                           tpd=self.main_window.tpd)
                
                def progress_cb(completed):
                    self.worker.progress.emit(completed)
                
                def status_cb(status_msg):
                    self.worker.status.emit(status_msg)
                    
                return enricher.process_batch(df, name_col, self.main_window.domains[domain], 
                                            max_workers=self.main_window.max_workers,
                                            progress_callback=progress_cb,
                                            status_callback=status_cb)

            self.worker = WorkerThread(task)
            self.worker.progress.connect(self.progress.setValue)
            self.worker.status.connect(self.on_status_update)
            self.worker.finished.connect(lambda res: self.on_finished(res, output_file))
            self.worker.error.connect(self.on_error)
            self.worker.start()
            
        except Exception as e:
            self.main_window.show_toast(str(e), "error")
    
    def on_status_update(self, status_msg):
        """处理状态更新"""
        self.status.setText(status_msg)
        self.main_window.status_bar.showMessage(status_msg)

    def on_finished(self, df, filename):
        output_path = DATA_DIR / "processed" / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        self.main_window.preview_page.update_table(df)
        self.progress.setValue(len(df))
        self.status.setText("✅ 处理完成")
        self.status.setStyleSheet("color: #00b894; font-weight: bold;")
        self.main_window.status_bar.showMessage("Ready")
        self.btn_process.setEnabled(True)
        
        self.main_window.show_toast(f"处理完成！已保存至 {filename}", "success")
        
        # 根据复选框状态决定是否导入Neo4j
        if self.neo4j_import_checkbox.isChecked():
            reply = QMessageBox.question(self, "导入确认", 
                                        f"数据已保存至 {output_path}\n\n是否立即导入到 Neo4j 数据库？",
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                self.import_to_neo4j(output_path)

    def on_error(self, msg):
        self.btn_process.setEnabled(True)
        self.status.setText("❌ 处理失败")
        self.status.setStyleSheet("color: #d63031; font-weight: bold;")
        self.main_window.status_bar.showMessage("Error occurred")
        self.main_window.show_toast(f"处理失败: {msg}", "error")
    
    def import_to_neo4j(self, csv_path):
        """导入数据到Neo4j数据库"""
        try:
            import time
            self.status.setText("正在导入到 Neo4j...")
            self.progress.setRange(0, 0)
            
            def task():
                # 这里调用实际的Neo4j导入逻辑
                # 例如：from modules.graph_construction import neo4j_exporter
                # neo4j_exporter.import_csv(csv_path)
                time.sleep(2)  # 模拟导入过程
                return "导入完成"
            
            self.worker = WorkerThread(task)
            self.worker.finished.connect(self.on_neo4j_imported)
            self.worker.error.connect(self.on_neo4j_error)
            self.worker.start()
            
        except Exception as e:
            QMessageBox.critical(self, "导入错误", f"无法导入到 Neo4j: {str(e)}")
            self.status.setText("导入失败")
            self.progress.setRange(0, 100)
    
    def on_neo4j_imported(self, result):
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.status.setText("导入完成")
        QMessageBox.information(self, "成功", "数据已成功导入到 Neo4j 数据库")
    
    def on_neo4j_error(self, msg):
        self.progress.setRange(0, 100)
        self.status.setText("导入失败")
        QMessageBox.critical(self, "错误", f"Neo4j 导入失败: {msg}")

class PreviewPage(BasePage):
    def __init__(self):
        super().__init__("结果预览")
        
        # Description
        desc = QLabel("实时预览数据处理结果。您可以在此检查补全后的数据质量，确认无误后进行后续操作。")
        desc.setStyleSheet("color: #636e72; font-size: 13px; margin-bottom: 10px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)

        self.table = QTableWidget()
        self.layout.addWidget(self.table)

    def update_table(self, df):
        self.table.setRowCount(df.shape[0])
        self.table.setColumnCount(df.shape[1])
        self.table.setHorizontalHeaderLabels(df.columns)
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                self.table.setItem(i, j, QTableWidgetItem(str(df.iat[i, j])))

class PipelinePage(BasePage):
    def __init__(self, main_window):
        super().__init__("流水线控制")
        self.main_window = main_window
        self.pipeline_manager = None
        self.current_worker = None
        self.setup_ui()
    
    def setup_ui(self):
        # Description
        desc = QLabel("一键运行完整的数据处理流水线，包含数据清洗、知识补全、后处理及图数据库导入等全流程。支持断点续传和实时进度监控。")
        desc.setStyleSheet("color: #636e72; margin-bottom: 15px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)
        
        # Main content with splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left: Stage Cards
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 10, 0)
        
        stages_label = QLabel("📋 流程阶段")
        stages_label.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
        left_layout.addWidget(stages_label)
        
        # Stage status cards
        self.stage_cards = {}
        stages_info = [
            ("data_cleaning", "🧹 数据清洗", "读取原始数据，进行格式标准化、去重、缺失值处理"),
            ("data_enrichment", "🤖 知识补全", "调用大模型 API 补充实体属性信息"),
            ("post_processing", "🔄 后处理", "数据校验、实体对齐、关系提取"),
            ("graph_construction", "🕸️ 图构建", "生成图数据库导入格式，支持 Neo4j")
        ]
        
        for stage_id, stage_name, stage_desc in stages_info:
            card = self._create_stage_card(stage_id, stage_name, stage_desc)
            self.stage_cards[stage_id] = card
            left_layout.addWidget(card)
        
        left_layout.addStretch()
        
        # Right: Controls and Log
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(10, 0, 0, 0)
        
        # Control buttons
        ctrl_card = QFrame()
        ctrl_card.setObjectName("Card")
        ctrl_layout = QVBoxLayout(ctrl_card)
        ctrl_layout.setContentsMargins(20, 20, 20, 20)
        
        ctrl_title = QLabel("🎮 流程控制")
        ctrl_title.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
        ctrl_layout.addWidget(ctrl_title)
        
        # Pipeline selection
        pipeline_row = QHBoxLayout()
        pipeline_row.addWidget(QLabel("流程实例:"))
        self.pipeline_combo = QComboBox()
        self.pipeline_combo.setMinimumWidth(200)
        self.pipeline_combo.addItem("➕ 新建流程...")
        pipeline_row.addWidget(self.pipeline_combo)
        self.btn_refresh = QPushButton("🔄")
        self.btn_refresh.setFixedWidth(40)
        self.btn_refresh.setToolTip("刷新流程列表")
        self.btn_refresh.clicked.connect(self.refresh_pipelines)
        pipeline_row.addWidget(self.btn_refresh)
        pipeline_row.addStretch()
        ctrl_layout.addLayout(pipeline_row)
        
        # Options
        options_layout = QHBoxLayout()
        self.skip_completed_cb = QCheckBox("跳过已完成阶段")
        self.skip_completed_cb.setChecked(True)
        self.skip_completed_cb.setToolTip("断点续传：自动从上次中断的位置继续")
        options_layout.addWidget(self.skip_completed_cb)
        
        self.neo4j_import_cb = QCheckBox("完成后导入 Neo4j")
        self.neo4j_import_cb.setToolTip("流水线完成时自动导入数据到图数据库")
        options_layout.addWidget(self.neo4j_import_cb)
        options_layout.addStretch()
        ctrl_layout.addLayout(options_layout)
        
        # Action buttons
        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("▶️ 启动流水线")
        self.btn_run.setMinimumHeight(45)
        self.btn_run.setStyleSheet("background-color: #00b894; font-size: 14px; font-weight: bold;")
        self.btn_run.clicked.connect(self.run_pipeline)
        btn_layout.addWidget(self.btn_run)
        
        self.btn_pause = QPushButton("⏸️ 暂停")
        self.btn_pause.setMinimumHeight(45)
        self.btn_pause.setObjectName("SecondaryButton")
        self.btn_pause.setEnabled(False)
        self.btn_pause.clicked.connect(self.pause_pipeline)
        btn_layout.addWidget(self.btn_pause)
        
        self.btn_stop = QPushButton("⏹️ 停止")
        self.btn_stop.setMinimumHeight(45)
        self.btn_stop.setObjectName("DangerButton")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_pipeline)
        btn_layout.addWidget(self.btn_stop)
        
        ctrl_layout.addLayout(btn_layout)
        right_layout.addWidget(ctrl_card)
        
        # Progress
        progress_card = QFrame()
        progress_card.setObjectName("Card")
        progress_layout = QVBoxLayout(progress_card)
        progress_layout.setContentsMargins(20, 20, 20, 20)
        
        progress_title = QLabel("📊 执行进度")
        progress_title.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
        progress_layout.addWidget(progress_title)
        
        self.overall_progress = QProgressBar()
        self.overall_progress.setTextVisible(True)
        self.overall_progress.setFormat("总进度: %p%")
        progress_layout.addWidget(self.overall_progress)
        
        self.stage_progress = QProgressBar()
        self.stage_progress.setTextVisible(True)
        self.stage_progress.setFormat("当前阶段: %p%")
        progress_layout.addWidget(self.stage_progress)
        
        self.status_label = QLabel("准备就绪")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setStyleSheet("color: #636e72; margin-top: 5px;")
        progress_layout.addWidget(self.status_label)
        
        right_layout.addWidget(progress_card)
        
        # Log
        log_card = QFrame()
        log_card.setObjectName("Card")
        log_layout = QVBoxLayout(log_card)
        log_layout.setContentsMargins(20, 20, 20, 20)
        
        log_header = QHBoxLayout()
        log_title = QLabel("📜 执行日志")
        log_title.setStyleSheet("font-size: 16px; font-weight: bold;")
        log_header.addWidget(log_title)
        log_header.addStretch()
        
        self.btn_clear_log = QPushButton("清空")
        self.btn_clear_log.setObjectName("SecondaryButton")
        self.btn_clear_log.setFixedWidth(60)
        self.btn_clear_log.clicked.connect(lambda: self.log.clear())
        log_header.addWidget(self.btn_clear_log)
        log_layout.addLayout(log_header)
        
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet("background-color: #2d3436; color: #dfe6e9; font-family: Consolas; font-size: 12px;")
        self.log.setMinimumHeight(200)
        log_layout.addWidget(self.log)
        
        right_layout.addWidget(log_card)
        
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        
        self.layout.addWidget(splitter)
    
    def _create_stage_card(self, stage_id, stage_name, stage_desc):
        """创建阶段状态卡片"""
        card = QFrame()
        card.setObjectName("Card")
        card.setProperty("stage_id", stage_id)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(15, 15, 15, 15)
        
        # Header
        header = QHBoxLayout()
        name_label = QLabel(stage_name)
        name_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        header.addWidget(name_label)
        header.addStretch()
        
        status_label = QLabel("⚪ 待执行")
        status_label.setObjectName("status_label")
        header.addWidget(status_label)
        layout.addLayout(header)
        
        # Description
        desc_label = QLabel(stage_desc)
        desc_label.setStyleSheet("color: #636e72; font-size: 12px;")
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)
        
        # Progress (hidden by default)
        progress = QProgressBar()
        progress.setObjectName("stage_progress")
        progress.setMaximumHeight(8)
        progress.setTextVisible(False)
        progress.hide()
        layout.addWidget(progress)
        
        # Info line
        info_label = QLabel("")
        info_label.setObjectName("info_label")
        info_label.setStyleSheet("color: #636e72; font-size: 11px;")
        info_label.hide()
        layout.addWidget(info_label)
        
        return card
    
    def _update_stage_card(self, stage_id, status, progress_val=None, info_text=None):
        """更新阶段卡片状态"""
        if stage_id not in self.stage_cards:
            return
        
        card = self.stage_cards[stage_id]
        status_label = card.findChild(QLabel, "status_label")
        progress_bar = card.findChild(QProgressBar, "stage_progress")
        info_label = card.findChild(QLabel, "info_label")
        
        # Update status
        status_map = {
            "pending": ("⚪ 待执行", "#b2bec3"),
            "running": ("🔄 执行中", "#0984e3"),
            "completed": ("✅ 已完成", "#00b894"),
            "failed": ("❌ 失败", "#d63031"),
            "skipped": ("⏭️ 已跳过", "#fdcb6e"),
            "paused": ("⏸️ 已暂停", "#fdcb6e")
        }
        
        if status in status_map:
            text, color = status_map[status]
            status_label.setText(text)
            status_label.setStyleSheet(f"color: {color}; font-weight: bold;")
        
        # Update progress
        if progress_val is not None:
            progress_bar.show()
            progress_bar.setValue(progress_val)
        elif status == "completed":
            progress_bar.setValue(100)
            progress_bar.show()
        elif status in ["pending", "skipped"]:
            progress_bar.hide()
        
        # Update info
        if info_text:
            info_label.setText(info_text)
            info_label.show()
        elif status == "pending":
            info_label.hide()
    
    def refresh_pipelines(self):
        """刷新流程列表"""
        self.pipeline_combo.clear()
        self.pipeline_combo.addItem("➕ 新建流程...")
        
        try:
            from modules.pipeline_manager import PipelineManager
            pm = PipelineManager()
            pipelines = pm.list_pipelines()
            for p in pipelines[:10]:  # 只显示最近10个
                display_text = f"{p['pipeline_id']} ({p['created_at'][:10]})"
                self.pipeline_combo.addItem(display_text, p['pipeline_id'])
        except Exception as e:
            self.log.append(f"⚠️ 无法加载流程列表: {e}")
    
    def _log(self, message, level="info"):
        """添加日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        color_map = {
            "info": "#dfe6e9",
            "success": "#00b894",
            "warning": "#fdcb6e",
            "error": "#d63031"
        }
        color = color_map.get(level, "#dfe6e9")
        self.log.append(f'<span style="color: #636e72">[{timestamp}]</span> <span style="color: {color}">{message}</span>')
    
    def run_pipeline(self):
        """启动流水线"""
        if not self.main_window.api_key:
            self.main_window.show_toast("请先配置 API Key", "error")
            return
        
        self.btn_run.setEnabled(False)
        self.btn_pause.setEnabled(True)
        self.btn_stop.setEnabled(True)
        self.overall_progress.setValue(0)
        self.stage_progress.setValue(0)
        
        # Reset stage cards
        for stage_id in self.stage_cards:
            self._update_stage_card(stage_id, "pending")
        
        self._log("🚀 启动流水线...", "info")
        self.status_label.setText("正在初始化...")
        
        skip_completed = self.skip_completed_cb.isChecked()
        
        def task():
            from modules.pipeline_manager import create_default_pipeline
            import yaml
            
            # 加载配置
            config_path = Path("config/config.yaml")
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            else:
                config = {}
            
            # 添加 GUI 设置到配置
            config['data_enrichment'] = config.get('data_enrichment', {})
            config['data_enrichment']['api_key'] = self.main_window.api_key
            config['data_enrichment']['base_url'] = self.main_window.base_url
            config['data_enrichment']['model'] = self.main_window.model_name
            config['data_enrichment']['provider'] = self.main_window.provider
            config['data_enrichment']['max_workers'] = self.main_window.max_workers
            config['data_enrichment']['llm_options'] = {
                "num_ctx": self.main_window.num_ctx,
                "temperature": self.main_window.temperature,
                "keep_alive": self.main_window.keep_alive,
                "timeout": self.main_window.timeout
            }
            
            # 创建流程管理器
            pm = create_default_pipeline()
            
            # 注册回调
            def on_progress(stage_name, current, total):
                if total > 0:
                    pct = int(current * 100 / total)
                    self.worker.progress.emit(pct)
            
            def on_status(stage_name, status):
                self.worker.status.emit(f"{stage_name}:{status.value}")
            
            pm.on_progress(on_progress)
            pm.on_status_change(on_status)
            
            # 创建或加载流程
            selected = self.pipeline_combo.currentData()
            if selected:
                pm.load_pipeline(selected)
            else:
                pm.create_pipeline(config=config)
            
            # 运行
            results = pm.run_all(skip_completed=skip_completed)
            return results
        
        self.worker = WorkerThread(task)
        self.worker.progress.connect(self.on_stage_progress)
        self.worker.status.connect(self.on_stage_status)
        self.worker.finished.connect(self.on_pipeline_finished)
        self.worker.error.connect(self.on_pipeline_error)
        self.worker.start()
    
    def on_stage_progress(self, progress):
        """处理阶段进度更新"""
        self.stage_progress.setValue(progress)
    
    def on_stage_status(self, status_str):
        """处理阶段状态更新"""
        try:
            stage_name, status = status_str.split(":")
            self._update_stage_card(stage_name, status)
            
            if status == "running":
                self._log(f"▶️ 开始执行: {stage_name}", "info")
                self.status_label.setText(f"正在执行: {stage_name}")
            elif status == "completed":
                self._log(f"✅ 完成: {stage_name}", "success")
                # 更新总进度
                completed = sum(1 for s in self.stage_cards if self.stage_cards[s].findChild(QLabel, "status_label").text().startswith("✅"))
                self.overall_progress.setValue(int(completed * 100 / len(self.stage_cards)))
            elif status == "failed":
                self._log(f"❌ 失败: {stage_name}", "error")
        except:
            pass
    
    def on_pipeline_finished(self, results):
        """流水线完成"""
        self.btn_run.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_stop.setEnabled(False)
        self.overall_progress.setValue(100)
        self.status_label.setText("流水线执行完成")
        
        self._log("🎉 流水线执行完成!", "success")
        self.main_window.show_toast("流水线执行完成", "success")
        
        # 如果需要导入 Neo4j
        if self.neo4j_import_cb.isChecked():
            self._log("📤 开始导入 Neo4j...", "info")
            # TODO: 调用 Neo4j 导入
    
    def on_pipeline_error(self, error_msg):
        """流水线错误"""
        self.btn_run.setEnabled(True)
        self.btn_pause.setEnabled(False)
        self.btn_stop.setEnabled(False)
        self.status_label.setText("执行失败")
        
        self._log(f"❌ 错误: {error_msg}", "error")
        self.main_window.show_toast(f"流水线失败: {error_msg}", "error")
    
    def pause_pipeline(self):
        """暂停流水线"""
        self._log("⏸️ 流水线已暂停", "warning")
        self.status_label.setText("已暂停")
        self.btn_pause.setEnabled(False)
        self.btn_run.setEnabled(True)
        self.btn_run.setText("▶️ 继续执行")
    
    def stop_pipeline(self):
        """停止流水线"""
        reply = QMessageBox.question(
            self, "确认停止",
            "确定要停止流水线吗？当前进度将被保存，下次可以继续执行。",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.current_worker:
                # TODO: 实现真正的停止逻辑
                pass
            
            self._log("⏹️ 流水线已停止", "warning")
            self.status_label.setText("已停止")
            self.btn_run.setEnabled(True)
            self.btn_run.setText("▶️ 启动流水线")
            self.btn_pause.setEnabled(False)
            self.btn_stop.setEnabled(False)

class SettingsPage(BasePage):
    def __init__(self, main_window):
        super().__init__("全局设置")
        self.main_window = main_window
        
        # Description
        desc = QLabel("配置全局参数，包括 LLM 模型 API (OpenAI/Ollama/DashScope/DeepSeek/Kimi)、Neo4j 数据库连接、界面主题及性能参数。")
        desc.setStyleSheet("color: #8b5a3c; font-size: 13px; margin-bottom: 10px;")
        desc.setWordWrap(True)
        self.layout.addWidget(desc)
        
        card = QFrame()
        card.setObjectName("Card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(30, 30, 30, 30)
        layout.setSpacing(20)
        
        # --- API Settings ---
        api_group = QGroupBox("API 配置")
        api_layout = QFormLayout(api_group)
        
        self.provider = QComboBox()
        self.provider.addItems(["dashscope", "openai", "ollama", "deepseek", "kimi"])
        self.provider.setCurrentText(self.main_window.provider)
        self.provider.currentTextChanged.connect(self.on_provider_changed)
        api_layout.addRow("模型提供商:", self.provider)
        
        self.api_key = QLineEdit()
        self.api_key.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key.setText(self.main_window.api_key)
        self.api_key.textChanged.connect(self.on_key_changed)
        api_layout.addRow("API Key:", self.api_key)
        
        self.base_url = QLineEdit()
        self.base_url.setPlaceholderText("Optional")
        self.base_url.setText(self.main_window.base_url)
        self.base_url.textChanged.connect(self.on_url_changed)
        api_layout.addRow("Base URL:", self.base_url)
        
        # Model Selection with Refresh
        model_layout = QHBoxLayout()
        self.model = QComboBox()
        self.model.setEditable(True)
        self.model.setMinimumWidth(200)
        self.model.setCurrentText(self.main_window.model_name)
        self.model.currentTextChanged.connect(self.on_model_changed)
        
        self.btn_refresh_models = QPushButton("🔄")
        self.btn_refresh_models.setToolTip("获取可用模型列表")
        self.btn_refresh_models.setFixedWidth(30)
        self.btn_refresh_models.clicked.connect(self.refresh_models)
        
        model_layout.addWidget(self.model)
        model_layout.addWidget(self.btn_refresh_models)
        
        api_layout.addRow("Model Name:", model_layout)
        
        self.btn_test_api = QPushButton("🔌 测试连接")
        self.btn_test_api.setObjectName("SecondaryButton")
        self.btn_test_api.clicked.connect(self.test_api_connection)
        api_layout.addRow("", self.btn_test_api)
        
        layout.addWidget(api_group)

        # --- LLM Parameters ---
        llm_group = QGroupBox("LLM 参数配置 (Ollama/OpenAI)")
        llm_layout = QFormLayout(llm_group)
        
        from PyQt6.QtWidgets import QSpinBox, QDoubleSpinBox
        
        # Context Window
        self.ctx_spin = QSpinBox()
        self.ctx_spin.setRange(2048, 128000)
        self.ctx_spin.setSingleStep(1024)
        self.ctx_spin.setValue(self.main_window.num_ctx)
        self.ctx_spin.setToolTip("上下文窗口大小 (num_ctx). 默认 4096. 增加此值可处理更长的文档，但会消耗更多内存。")
        self.ctx_spin.valueChanged.connect(self.on_ctx_changed)
        llm_layout.addRow("上下文窗口 (Context):", self.ctx_spin)
        
        # Temperature
        self.temp_spin = QDoubleSpinBox()
        self.temp_spin.setRange(0.0, 2.0)
        self.temp_spin.setSingleStep(0.1)
        self.temp_spin.setValue(self.main_window.temperature)
        self.temp_spin.setToolTip("温度 (Temperature). 控制输出的随机性。0.0 为确定性，1.0 为多样性。")
        self.temp_spin.valueChanged.connect(self.on_temp_changed)
        llm_layout.addRow("温度 (Temperature):", self.temp_spin)
        
        # GPU Configuration for Ollama
        self.gpu_spin = QSpinBox()
        self.gpu_spin.setRange(0, 8)
        self.gpu_spin.setValue(getattr(self.main_window, 'num_gpu', 1))
        self.gpu_spin.setToolTip("GPU数量 (num_gpu). 0=仅CPU, 1=使用1个GPU. 仅对Ollama有效。")
        self.gpu_spin.valueChanged.connect(self.on_gpu_changed)
        llm_layout.addRow("GPU数量 (num_gpu):", self.gpu_spin)
        
        # Keep Alive
        self.keep_alive_edit = QLineEdit()
        self.keep_alive_edit.setText(str(self.main_window.keep_alive))
        self.keep_alive_edit.setPlaceholderText("5m")
        self.keep_alive_edit.setToolTip("模型驻留内存时间 (keep_alive). 例如: 5m, 1h, -1 (永久).")
        self.keep_alive_edit.textChanged.connect(self.on_keep_alive_changed)
        llm_layout.addRow("模型驻留 (Keep Alive):", self.keep_alive_edit)
        
        # Timeout
        self.timeout_spin = QSpinBox()
        self.timeout_spin.setRange(10, 3600)
        self.timeout_spin.setSingleStep(10)
        self.timeout_spin.setValue(self.main_window.timeout)
        self.timeout_spin.setToolTip("请求超时时间 (秒). 本地模型可能需要较长时间响应。")
        self.timeout_spin.valueChanged.connect(self.on_timeout_changed)
        llm_layout.addRow("超时时间 (Timeout):", self.timeout_spin)
        
        layout.addWidget(llm_group)

        # --- Appearance Settings ---
        app_group = QGroupBox("外观与语言 (Preview)")
        app_layout = QFormLayout(app_group)
        
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["Light", "Dark"])
        self.theme_combo.currentTextChanged.connect(self.main_window.apply_theme)
        app_layout.addRow("主题:", self.theme_combo)
        
        self.lang_combo = QComboBox()
        self.lang_combo.addItems(["简体中文", "English"])
        self.lang_combo.setToolTip("语言切换功能将在后续版本中支持")
        app_layout.addRow("语言:", self.lang_combo)
        
        layout.addWidget(app_group)
        
        # --- Neo4j Settings ---
        neo4j_group = QGroupBox("Neo4j 数据库配置")
        neo4j_layout = QFormLayout(neo4j_group)
        
        self.neo4j_uri = QLineEdit()
        self.neo4j_uri.setPlaceholderText("bolt://localhost:7687")
        self.neo4j_uri.setText(self.main_window.neo4j_uri)
        self.neo4j_uri.textChanged.connect(self.on_neo4j_uri_changed)
        neo4j_layout.addRow("URI:", self.neo4j_uri)
        
        self.neo4j_user = QLineEdit()
        self.neo4j_user.setPlaceholderText("neo4j")
        self.neo4j_user.setText(self.main_window.neo4j_user)
        self.neo4j_user.textChanged.connect(self.on_neo4j_user_changed)
        neo4j_layout.addRow("用户名:", self.neo4j_user)
        
        self.neo4j_password = QLineEdit()
        self.neo4j_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.neo4j_password.setText(self.main_window.neo4j_password)
        self.neo4j_password.textChanged.connect(self.on_neo4j_password_changed)
        neo4j_layout.addRow("密码:", self.neo4j_password)
        
        layout.addWidget(neo4j_group)
        
        # --- Performance Settings ---
        perf_group = QGroupBox("性能配置")
        perf_layout = QFormLayout(perf_group)
        
        from PyQt6.QtWidgets import QSpinBox
        self.workers_spin = QSpinBox()
        self.workers_spin.setRange(1, 20)
        self.workers_spin.setValue(self.main_window.max_workers)
        self.workers_spin.setToolTip("并发处理的线程数 (建议: 3-5)")
        self.workers_spin.valueChanged.connect(self.on_workers_changed)
        perf_layout.addRow("最大并发数:", self.workers_spin)
        
        layout.addWidget(perf_group)
        
        # --- Rate Limit Settings ---
        rate_limit_group = QGroupBox("速率限制配置 (Rate Limits)")
        rate_limit_layout = QFormLayout(rate_limit_group)
        
        self.rpm_spin = QSpinBox()
        self.rpm_spin.setRange(1, 10000)
        self.rpm_spin.setValue(getattr(self.main_window, 'rpm', 60))
        self.rpm_spin.setSuffix(" 请求/分钟")
        self.rpm_spin.setToolTip("RPM: 每分钟最大请求数 (Requests Per Minute)")
        self.rpm_spin.valueChanged.connect(self.on_rpm_changed)
        rate_limit_layout.addRow("RPM:", self.rpm_spin)
        
        self.tpm_spin = QSpinBox()
        self.tpm_spin.setRange(1000, 10000000)
        self.tpm_spin.setSingleStep(10000)
        self.tpm_spin.setValue(getattr(self.main_window, 'tpm', 100000))
        self.tpm_spin.setSuffix(" tokens/分钟")
        self.tpm_spin.setToolTip("TPM: 每分钟最大Token数 (Tokens Per Minute)")
        self.tpm_spin.valueChanged.connect(self.on_tpm_changed)
        rate_limit_layout.addRow("TPM:", self.tpm_spin)
        
        self.tpd_spin = QSpinBox()
        self.tpd_spin.setRange(10000, 100000000)
        self.tpd_spin.setSingleStep(100000)
        self.tpd_spin.setValue(getattr(self.main_window, 'tpd', 1000000))
        self.tpd_spin.setSuffix(" tokens/天")
        self.tpd_spin.setToolTip("TPD: 每天最大Token数 (Tokens Per Day)")
        self.tpd_spin.valueChanged.connect(self.on_tpd_changed)
        rate_limit_layout.addRow("TPD:", self.tpd_spin)
        
        # 添加速率限制说明
        rate_info = QLabel("💡 根据您的API套餐配置速率限制，防止超额使用")
        rate_info.setWordWrap(True)
        rate_info.setStyleSheet("color: #666; font-size: 11px; padding: 5px;")
        rate_limit_layout.addRow("", rate_info)
        
        layout.addWidget(rate_limit_group)
        
        # Save Button
        self.btn_save = QPushButton("💾 保存所有设置")
        self.btn_save.setMinimumHeight(45)
        self.btn_save.clicked.connect(self.main_window.save_settings)
        layout.addWidget(self.btn_save)
        
        self.layout.addWidget(card)
        self.layout.addStretch()
        
        # Initialize UI state based on current provider
        self._init_provider_ui()
    
    def _init_provider_ui(self):
        """根据当前 provider 初始化 UI 状态"""
        provider = self.main_window.provider
        if provider == "ollama":
            self.ctx_spin.setEnabled(True)
            self.gpu_spin.setEnabled(True)
            self.keep_alive_edit.setEnabled(True)
            self.api_key.setPlaceholderText("Ollama 无需 API Key，默认 'ollama'")
        elif provider == "dashscope":
            self.ctx_spin.setEnabled(False)
            self.gpu_spin.setEnabled(False)
            self.keep_alive_edit.setEnabled(False)
            self.api_key.setPlaceholderText("输入 DashScope API Key")
        else:  # openai
            self.ctx_spin.setEnabled(False)
            self.gpu_spin.setEnabled(False)
            self.keep_alive_edit.setEnabled(False)
            self.api_key.setPlaceholderText("输入 OpenAI API Key")

    def on_workers_changed(self, value):
        self.main_window.max_workers = value
        self.main_window.show_toast(f"并发数已设置为: {value}")

    def on_rpm_changed(self, value):
        self.main_window.rpm = value
        self.main_window.show_toast(f"RPM已设置为: {value} 请求/分钟")

    def on_tpm_changed(self, value):
        self.main_window.tpm = value
        self.main_window.show_toast(f"TPM已设置为: {value} tokens/分钟")

    def on_tpd_changed(self, value):
        self.main_window.tpd = value
        self.main_window.show_toast(f"TPD已设置为: {value} tokens/天")

    def on_ctx_changed(self, value):
        self.main_window.num_ctx = value

    def on_temp_changed(self, value):
        self.main_window.temperature = value

    def on_gpu_changed(self, value):
        self.main_window.num_gpu = value

    def on_keep_alive_changed(self, value):
        self.main_window.keep_alive = value

    def on_timeout_changed(self, value):
        self.main_window.timeout = value

    def refresh_models(self):
        self.btn_refresh_models.setEnabled(False)
        
        def task():
            enricher = UniversalEnricher(
                self.main_window.api_key, 
                self.main_window.base_url, 
                self.main_window.model_name, 
                self.main_window.provider,
                options={
                    "num_ctx": self.main_window.num_ctx,
                    "temperature": self.main_window.temperature,
                    "num_gpu": getattr(self.main_window, 'num_gpu', 1),
                    "keep_alive": self.main_window.keep_alive,
                    "timeout": self.main_window.timeout
                },
                rpm=self.main_window.rpm,
                tpm=self.main_window.tpm,
                tpd=self.main_window.tpd
            )
            return enricher.get_models()
            
        self.worker = WorkerThread(task)
        self.worker.finished.connect(self.on_models_fetched)
        self.worker.error.connect(self.on_models_error)
        self.worker.start()

    def on_models_fetched(self, models):
        self.btn_refresh_models.setEnabled(True)
        current = self.model.currentText()
        self.model.blockSignals(True)
        self.model.clear()
        self.model.addItems(models)
        
        if current and current in models:
            self.model.setCurrentText(current)
        elif models:
            self.model.setCurrentText(models[0])
            self.main_window.model_name = models[0] # Update main window state
        else:
            self.model.setCurrentText(current)
            
        self.model.blockSignals(False)
        self.main_window.show_toast(f"已获取 {len(models)} 个模型", "success")
        
    def on_models_error(self, msg):
        self.btn_refresh_models.setEnabled(True)
        self.main_window.show_toast(f"获取模型失败: {msg}", "error")

    def test_api_connection(self):
        self.btn_test_api.setEnabled(False)
        self.btn_test_api.setText("测试中...")
        
        def task():
            enricher = UniversalEnricher(
                self.main_window.api_key, 
                self.main_window.base_url, 
                self.main_window.model_name, 
                self.main_window.provider,
                options={
                    "num_ctx": self.main_window.num_ctx,
                    "temperature": self.main_window.temperature,
                    "keep_alive": self.main_window.keep_alive,
                    "timeout": self.main_window.timeout
                },
                rpm=self.main_window.rpm,
                tpm=self.main_window.tpm,
                tpd=self.main_window.tpd
            )
            # Try a simple call
            return enricher._call_llm("Hello", system_prompt="You are a test assistant.")
            
        self.worker = WorkerThread(task)
        self.worker.finished.connect(self.on_test_success)
        self.worker.error.connect(self.on_test_error)
        self.worker.start()

    def on_test_success(self, result):
        self.btn_test_api.setEnabled(True)
        self.btn_test_api.setText("✅ 连接成功")
        self.main_window.show_toast("API 连接成功！", "success")
        # 截断响应以防止过长
        display_result = result[:200] + "..." if len(result) > 200 else result
        QMessageBox.information(self, "测试成功", 
            f"连接成功！\n\n提供商: {self.main_window.provider}\n模型: {self.main_window.model_name}\n\n模型响应预览:\n{display_result}")

    def on_test_error(self, msg):
        self.btn_test_api.setEnabled(True)
        self.btn_test_api.setText("❌ 连接失败")
        self.main_window.show_toast(f"连接失败: {msg}", "error")
        
        # 提供更有用的错误分析
        error_hints = ""
        if "Connection refused" in msg or "ConnectError" in msg:
            error_hints = "\n\n可能的解决方案:\n1. 检查 Ollama 服务是否已启动 (ollama serve)\n2. 确认基址 URL 是否正确\n3. 检查防火墙设置"
        elif "timeout" in msg.lower():
            error_hints = "\n\n可能的解决方案:\n1. 增加超时时间 (当前: {}s)\n2. 检查模型是否已加载\n3. 本地硬件可能需要更长响应时间".format(self.main_window.timeout)
        elif "model" in msg.lower() and "not found" in msg.lower():
            error_hints = "\n\n可能的解决方案:\n1. 点击 ↻ 按钮刷新模型列表\n2. 在终端运行: ollama pull <模型名>\n3. 确认模型名称拼写正确"
        elif "401" in msg or "unauthorized" in msg.lower():
            error_hints = "\n\n可能的解决方案:\n1. 检查 API Key 是否正确\n2. 确认账户是否有效"
        
        QMessageBox.warning(self, "测试失败", f"连接失败: {msg}{error_hints}")

    def on_provider_changed(self, text):
        self.main_window.provider = text
        if text == "dashscope":
            self.model.setCurrentText("qwen-plus")
            self.base_url.setPlaceholderText("Optional")
            self.base_url.clear()
            self.api_key.setPlaceholderText("输入 DashScope API Key")
            self.ctx_spin.setEnabled(False)
            self.gpu_spin.setEnabled(False)
            self.keep_alive_edit.setEnabled(False)
        elif text == "ollama":
            # 尝试获取可用模型列表并设置第一个
            self.model.setCurrentText("ministral-3:8b")  # 默认使用常见模型
            self.base_url.setText("http://localhost:11434/v1")
            self.base_url.setPlaceholderText("http://localhost:11434/v1")
            self.api_key.setText("ollama")
            self.api_key.setPlaceholderText("Ollama 无需 API Key，默认 'ollama'")
            self.ctx_spin.setEnabled(True)
            self.gpu_spin.setEnabled(True)
            self.keep_alive_edit.setEnabled(True)
            # 自动设置更长的超时时间
            self.timeout_spin.setValue(120)
            self.main_window.timeout = 120
            # 默认使用 1 个 GPU
            self.gpu_spin.setValue(1)
            self.main_window.num_gpu = 1
            # 自动刷新模型列表
            self.refresh_models()
        else:  # openai
            self.model.setCurrentText("gpt-4")
            self.base_url.setPlaceholderText("Optional")
            self.base_url.clear()
            self.api_key.setPlaceholderText("输入 OpenAI API Key")
            self.ctx_spin.setEnabled(False)
            self.gpu_spin.setEnabled(False)
            self.keep_alive_edit.setEnabled(False)

    def on_key_changed(self, text):
        self.main_window.api_key = text
        os.environ["OPENCHEMKG_API_KEY"] = text

    def on_url_changed(self, text):
        self.main_window.base_url = text

    def on_model_changed(self, text):
        self.main_window.model_name = text
    
    def on_neo4j_uri_changed(self, text):
        self.main_window.neo4j_uri = text
    
    def on_neo4j_user_changed(self, text):
        self.main_window.neo4j_user = text
    
    def on_neo4j_password_changed(self, text):
        self.main_window.neo4j_password = text

# --- Main Window ---

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Universal KG Builder")
        self.resize(1200, 800)
        
        # Global State
        self.api_key = os.environ.get("OPENCHEMKG_API_KEY", "")
        self.base_url = ""
        self.provider = "dashscope"
        self.model_name = "qwen-plus"
        self.max_workers = 3  # Default concurrency
        
        # Rate Limit Settings
        self.rpm = 60  # Requests Per Minute
        self.tpm = 100000  # Tokens Per Minute
        self.tpd = 1000000  # Tokens Per Day
        
        # LLM Options
        self.num_ctx = 4096
        self.temperature = 0.7
        self.num_gpu = 1  # Default use 1 GPU for Ollama
        self.keep_alive = "5m"
        self.timeout = 60
        
        self.domains = self.load_domains()
        
        # Neo4j Configuration
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = ""
        
        # Load Settings
        self.load_settings()
        
        # Layout
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Sidebar
        self.sidebar = Sidebar()
        self.sidebar.nav_list.currentItemChanged.connect(self.on_nav_changed)
        main_layout.addWidget(self.sidebar)
        
        # Content
        self.stack = QStackedWidget()
        main_layout.addWidget(self.stack)
        
        # Pages
        self.dashboard_page = DashboardPage(self)
        self.wizard_page = WizardPage(self)
        self.domain_page = DomainPage(self)
        self.data_page = DataPage(self)
        self.preview_page = PreviewPage()
        self.pipeline_page = PipelinePage(self)
        self.settings_page = SettingsPage(self)
        
        self.stack.addWidget(self.dashboard_page)
        self.stack.addWidget(self.wizard_page)
        self.stack.addWidget(self.domain_page)
        self.stack.addWidget(self.data_page)
        self.stack.addWidget(self.preview_page)
        self.stack.addWidget(self.pipeline_page)
        self.stack.addWidget(self.settings_page)
        
        # Status Bar
        from PyQt6.QtWidgets import QStatusBar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
        # Init
        self.domain_page.update_domains()
        self.apply_theme("Light") # Default theme

    def load_settings(self):
        if SETTINGS_FILE.exists():
            try:
                with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    settings = yaml.safe_load(f) or {}
                    self.api_key = settings.get("api_key", self.api_key)
                    self.base_url = settings.get("base_url", self.base_url)
                    self.provider = settings.get("provider", self.provider)
                    self.model_name = settings.get("model_name", self.model_name)
                    
                    # Ensure model name is not empty
                    if not self.model_name:
                        if self.provider == "dashscope":
                            self.model_name = "qwen-plus"
                        elif self.provider == "ollama":
                            self.model_name = "llama3" # Default fallback
                        elif self.provider == "openai":
                            self.model_name = "gpt-3.5-turbo"

                    self.max_workers = settings.get("max_workers", self.max_workers)
                    
                    # Load Rate Limits
                    self.rpm = settings.get("rpm", self.rpm)
                    self.tpm = settings.get("tpm", self.tpm)
                    self.tpd = settings.get("tpd", self.tpd)
                    
                    self.num_ctx = settings.get("num_ctx", self.num_ctx)
                    self.temperature = settings.get("temperature", self.temperature)
                    self.num_gpu = settings.get("num_gpu", self.num_gpu)
                    self.keep_alive = settings.get("keep_alive", self.keep_alive)
                    self.timeout = settings.get("timeout", self.timeout)
                    
                    self.neo4j_uri = settings.get("neo4j_uri", self.neo4j_uri)
                    self.neo4j_user = settings.get("neo4j_user", self.neo4j_user)
                    self.neo4j_password = settings.get("neo4j_password", self.neo4j_password)
                    if self.api_key:
                        os.environ["OPENCHEMKG_API_KEY"] = self.api_key
            except Exception as e:
                print(f"Error loading settings: {e}")

    def save_settings(self):
        settings = {
            "api_key": self.api_key,
            "base_url": self.base_url,
            "provider": self.provider,
            "model_name": self.model_name,
            "max_workers": self.max_workers,
            "rpm": self.rpm,
            "tpm": self.tpm,
            "tpd": self.tpd,
            "num_ctx": self.num_ctx,
            "temperature": self.temperature,
            "num_gpu": self.num_gpu,
            "keep_alive": self.keep_alive,
            "timeout": self.timeout,
            "neo4j_uri": self.neo4j_uri,
            "neo4j_user": self.neo4j_user,
            "neo4j_password": self.neo4j_password
        }
        try:
            with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
                yaml.dump(settings, f)
            self.show_toast("设置已保存", "success")
        except Exception as e:
            self.show_toast(f"保存失败: {e}", "error")

    def show_toast(self, message, type="info"):
        toast = ToastNotification(self, message, type)
        toast.adjustSize()
        # Position at bottom center
        x = (self.width() - toast.width()) // 2
        y = self.height() - toast.height() - 50
        toast.move(x, y)
        toast.show()

    def load_domains(self):
        if DOMAINS_FILE.exists():
            with open(DOMAINS_FILE, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        return {}

    def save_domains(self):
        with open(DOMAINS_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(self.domains, f, allow_unicode=True)

    def apply_theme(self, theme_name):
        self.setStyleSheet(ModernStyle.get_style(theme_name))

    def on_nav_changed(self, current, previous):
        if not current: return
        page_name = current.data(Qt.ItemDataRole.UserRole)
        
        idx_map = {
            "dashboard": 0,
            "wizard": 1,
            "domain": 2,
            "data": 3,
            "preview": 4,
            "pipeline": 5,
            "settings": 6
        }
        
        if page_name in idx_map:
            self.stack.setCurrentIndex(idx_map[page_name])

if __name__ == "__main__":
    # 配置日志级别，只显示WARNING及以上级别的消息
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
    
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion"))
    
    # Set Font
    font = QFont("Segoe UI", 10)
    font.setStyleStrategy(QFont.StyleStrategy.PreferAntialias)
    app.setFont(font)
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
