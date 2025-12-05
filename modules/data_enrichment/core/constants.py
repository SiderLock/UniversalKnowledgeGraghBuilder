# modules_new/core/constants.py
"""
常量定义模块

定义项目中使用的所有常量
"""

from enum import Enum
from typing import List, Dict

# 版本信息
VERSION = "2.0.0"
PROJECT_NAME = "Chemical Data Processing System"

# 文件相关常量
DEFAULT_ENCODING = "utf-8-sig"
BACKUP_ENCODING = "utf-8"
CSV_CHUNK_SIZE = 10000
MAX_FILE_SIZE_MB = 500

# 数据处理常量
DEFAULT_BATCH_SIZE = 100
DEFAULT_BACKUP_INTERVAL = 50
MAX_RETRY_ATTEMPTS = 3
DEFAULT_API_DELAY = 1.0
DEFAULT_TIMEOUT = 60

# 数据质量阈值
MIN_QUALITY_SCORE = 60.0
HIGH_QUALITY_THRESHOLD = 80.0
LOW_COMPLETENESS_THRESHOLD = 0.2
HIGH_COMPLETENESS_THRESHOLD = 0.8

# 化学品属性字段
PROPERTIES_COLUMNS = [
    'CAS号', '外观与性状', '室温状态', '建议存储方式', '建议存储条件',
    '熔点/凝固点', '沸点/沸程', '密度/相对密度', '蒸气密度（相对于空气）',
    '饱和蒸气压', '闪点', '爆炸极限（LEL/UEL）', '自燃温度', '溶解性',
    'pH值', '分解温度', '物理危害', '健康危害', '环境危害', '数据来源'
]

# 智能列名映射配置
COLUMN_MAPPINGS = {
    # 化学品名称相关的列名变体
    'chemical_name': ['中文名称', '品名', '化学品名称', '物质名称', '名称', 'name', 'chemical_name', 'substance_name'],
    'aliases': ['别名', '别称', '其他名称', 'aliases', 'other_names'],
    'cas_number': ['CAS号或流水号', 'CAS号', 'CAS', 'cas_number', 'cas_no', 'CAS_NO', '流水号'],
    'remarks': ['备注', '说明', '注释', 'remarks', 'notes', 'comments'],
    'serial_number': ['序号', '编号', '序列号', 'serial', 'number', 'id', '序号'],
    
    # 物理化学性质相关
    'appearance': ['外观与性状', '外观', '性状', '外观性状', 'appearance', 'physical_state'],
    'room_temp_state': ['室温状态', '常温状态', '状态', 'state_at_room_temp', 'physical_form'],
    'storage_method': ['建议存储方式', '存储方式', '储存方式', 'storage_method', 'storage_condition'],
    'storage_condition': ['建议存储条件', '存储条件', '储存条件', 'storage_conditions'],
    'melting_point': ['熔点/凝固点', '熔点', '凝固点', 'melting_point', 'freezing_point'],
    'boiling_point': ['沸点/沸程', '沸点', '沸程', 'boiling_point', 'boiling_range'],
    'density': ['密度/相对密度', '密度', '相对密度', 'density', 'relative_density'],
    'vapor_density': ['蒸气密度（相对于空气）', '蒸气密度', 'vapor_density'],
    'vapor_pressure': ['饱和蒸气压', '蒸气压', 'vapor_pressure', 'saturated_vapor_pressure'],
    'flash_point': ['闪点', 'flash_point'],
    'explosion_limit': ['爆炸极限（LEL/UEL）', '爆炸极限', 'explosion_limit', 'explosive_limit'],
    'autoignition_temp': ['自燃温度', '自燃点', 'autoignition_temperature', 'ignition_temperature'],
    'solubility': ['溶解性', 'solubility'],
    'ph_value': ['pH值', 'pH', 'ph_value'],
    'decomposition_temp': ['分解温度', '分解点', 'decomposition_temperature'],
    
    # 危害信息相关
    'physical_hazard': ['物理危害', '物理危险', 'physical_hazard', 'physical_danger'],
    'health_hazard': ['健康危害', '健康危险', 'health_hazard', 'health_danger'],
    'environmental_hazard': ['环境危害', '环境危险', 'environmental_hazard', 'environmental_danger'],
    'data_source': ['数据来源', '来源', 'data_source', 'source']
}

# 必需的核心列（至少需要化学品名称）
REQUIRED_COLUMNS = ['chemical_name']

# 默认目录名
DEFAULT_OUTPUT_FOLDER = "output_batches"
DEFAULT_BACKUP_FOLDER = "backups"
DEFAULT_LOG_FOLDER = "logs"

# 文件名模式
BACKUP_FILE_PATTERN = "backup_*.csv"
OUTPUT_FILE_PATTERN = "output_*.csv"
LOG_FILE_PATTERN = "*.log"

# API相关常量
API_MAX_TOKENS = 8000
API_TEMPERATURE = 0.1
API_MAX_CONCURRENT_REQUESTS = 5

# 正则表达式模式
CAS_NUMBER_PATTERN = r"^\d{1,7}-\d{2}-\d$"
EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

# 化学品属性字段
CHEMICAL_PROPERTIES = [
    'CAS号', '外观与性状', '室温状态', '建议存储方式', '建议存储条件',
    '熔点/凝固点', '沸点/沸程', '密度/相对密度', '蒸气密度（相对于空气）',
    '饱和蒸气压', '闪点', '爆炸极限（LEL/UEL）', '自燃温度', '溶解性',
    'pH值', '分解温度', '物理危害', '健康危害', '环境危害', '数据来源'
]

# 列名映射字典
COLUMN_MAPPINGS = {
    # 化学品名称相关的列名变体
    'chemical_name': ['品名', '化学品名称', '物质名称', '名称', 'name', 'chemical_name', 'substance_name'],
    'aliases': ['别名', '别称', '其他名称', 'aliases', 'other_names'],
    'cas_number': ['CAS号', 'CAS', 'cas_number', 'cas_no', 'CAS_NO'],
    'remarks': ['备注', '说明', '注释', 'remarks', 'notes', 'comments'],
    'serial_number': ['序号', '编号', '序列号', 'serial', 'number', 'id'],
    
    # 物理化学性质相关
    'appearance': ['外观与性状', '外观', '性状', '外观性状', 'appearance', 'physical_state'],
    'room_temp_state': ['室温状态', '常温状态', '状态', 'state_at_room_temp', 'physical_form'],
    'storage_method': ['建议存储方式', '存储方式', '储存方式', 'storage_method'],
    'storage_condition': ['建议存储条件', '存储条件', '储存条件', 'storage_conditions'],
    'melting_point': ['熔点/凝固点', '熔点', '凝固点', 'melting_point', 'freezing_point'],
    'boiling_point': ['沸点/沸程', '沸点', '沸程', 'boiling_point', 'boiling_range'],
    'density': ['密度/相对密度', '密度', '相对密度', 'density', 'relative_density'],
    'vapor_density': ['蒸气密度（相对于空气）', '蒸气密度', 'vapor_density'],
    'vapor_pressure': ['饱和蒸气压', '蒸气压', 'vapor_pressure', 'saturated_vapor_pressure'],
    'flash_point': ['闪点', 'flash_point'],
    'explosion_limit': ['爆炸极限（LEL/UEL）', '爆炸极限', 'explosion_limit', 'explosive_limit'],
    'autoignition_temp': ['自燃温度', '自燃点', 'autoignition_temperature', 'ignition_temperature'],
    'solubility': ['溶解性', 'solubility'],
    'ph_value': ['pH值', 'pH', 'ph_value'],
    'decomposition_temp': ['分解温度', '分解点', 'decomposition_temperature'],
    
    # 危害信息相关
    'physical_hazard': ['物理危害', '物理危险', 'physical_hazard', 'physical_danger'],
    'health_hazard': ['健康危害', '健康危险', 'health_hazard', 'health_danger'],
    'environmental_hazard': ['环境危害', '环境危险', 'environmental_hazard', 'environmental_danger'],
    'data_source': ['数据来源', '来源', 'data_source', 'source']
}


class ProcessingStatus(Enum):
    """处理状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataType(Enum):
    """数据类型枚举"""
    TEXT = "text"
    NUMERIC = "numeric"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    UNKNOWN = "unknown"


class ValidationLevel(Enum):
    """验证级别枚举"""
    STRICT = "strict"
    NORMAL = "normal"
    LOOSE = "loose"


class APIService(Enum):
    """API服务枚举"""
    GEMINI = "gemini"
    GEMINI_GROUNDING = "gemini_grounding"
    DEEPSEEK = "deepseek"
    CUSTOM = "custom"


# 错误代码常量
class ErrorCodes:
    """错误代码常量"""
    FILE_NOT_FOUND = "E001"
    INVALID_FORMAT = "E002"
    VALIDATION_FAILED = "E003"
    API_ERROR = "E004"
    PROCESSING_ERROR = "E005"
    CONFIG_ERROR = "E006"
    UNKNOWN_ERROR = "E999"


# 消息常量
class Messages:
    """消息常量"""
    PROCESSING_STARTED = "数据处理已开始"
    PROCESSING_COMPLETED = "数据处理已完成"
    PROCESSING_FAILED = "数据处理失败"
    FILE_LOADED = "文件加载成功"
    FILE_SAVED = "文件保存成功"
    VALIDATION_PASSED = "数据验证通过"
    VALIDATION_FAILED = "数据验证失败"


# 配置默认值
DEFAULT_CONFIG = {
    "batch_size": DEFAULT_BATCH_SIZE,
    "backup_interval": DEFAULT_BACKUP_INTERVAL,
    "max_retries": MAX_RETRY_ATTEMPTS,
    "api_delay": DEFAULT_API_DELAY,
    "timeout": DEFAULT_TIMEOUT,
    "quality_threshold": MIN_QUALITY_SCORE,
    "encoding": DEFAULT_ENCODING,
    "output_folder": DEFAULT_OUTPUT_FOLDER,
    "backup_folder": DEFAULT_BACKUP_FOLDER,
    "log_folder": DEFAULT_LOG_FOLDER
}

# 支持的文件扩展名
SUPPORTED_FILE_EXTENSIONS = [".csv", ".xlsx", ".xls"]

# 编码检测顺序
ENCODING_DETECTION_ORDER = [
    "utf-8-sig", "utf-8", "gbk", "gb2312", "gb18030", "latin1"
]

# 日期时间格式
DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y"
]

# 空值表示
NULL_VALUES = ["", "NULL", "null", "N/A", "n/a", "NA", "na", "NaN", "nan", "None", "none"]

# 布尔值表示
BOOLEAN_TRUE_VALUES = ["true", "True", "TRUE", "yes", "Yes", "YES", "1", "on", "On", "ON"]
BOOLEAN_FALSE_VALUES = ["false", "False", "FALSE", "no", "No", "NO", "0", "off", "Off", "OFF"]
