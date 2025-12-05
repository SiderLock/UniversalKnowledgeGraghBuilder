"""
文本标准化工具
用于统一相似但表述不同的内容
"""

import re
from typing import List, Dict, Tuple, Optional, Any
try:
    import jieba
except ImportError:
    jieba = None
try:
    from fuzzywuzzy import fuzz, process
except ImportError:
    fuzz = None
    process = None
try:
    import yaml
except ImportError:
    yaml = None


class TextNormalizer:
    def __init__(self, config_path: Optional[str] = None):
        """初始化文本标准化器"""
        self.synonyms_map = {}
        self.unit_map = {}
        self.unit_conversions = {
            'pressure': {
                'kpa': 1.0,
                'pa': 0.001,
                'hpa': 0.1,
                'mpa': 1000.0,
                'bar': 100.0,
                'atm': 101.325,
                'mmhg': 0.133322,
            }
        }
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str):
        """加载配置文件"""
        if yaml is None:
            print("Warning: PyYAML not installed. Using default configuration.")
            return
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 构建同义词映射
        synonyms = config.get('processing', {}).get('text_normalization', {}).get('synonyms', {})
        for category, synonym_groups in synonyms.items():
            for group in synonym_groups:
                canonical = group[0]  # 第一个作为标准形式
                for synonym in group:
                    self.synonyms_map[synonym] = canonical
        
        # 构建单位映射
        units = config.get('processing', {}).get('data_cleaning', {}).get('unit_normalization', {})
        for unit_type, unit_list in units.items():
            canonical = unit_list[0]
            for unit in unit_list:
                self.unit_map[unit] = canonical
    
    def normalize_text(self, text: str) -> Optional[str]:
        """标准化文本"""
        if not text or text.strip() in ['N/A', '处理异常', '', 'null']:
            return None
        
        text = text.strip()
        
        # 应用同义词映射
        for synonym, canonical in self.synonyms_map.items():
            text = text.replace(synonym, canonical)
        
        return text
    
    def normalize_units(self, text: str) -> str:
        """标准化单位"""
        if not text:
            return text
        
        for unit, canonical in self.unit_map.items():
            text = text.replace(unit, canonical)
        
        return text
    
    def _convert_temp_to_celsius(self, value: float, unit: str) -> float:
        """将温度转换为摄氏度"""
        unit = unit.upper()
        if unit == 'F':
            return (value - 32) * 5 / 9
        if unit == 'K':
            return value - 273.15
        return value

    def extract_temperature(self, text: str) -> List[Dict[str, Any]]:
        """
        提取温度值，处理范围，并统一单位为摄氏度。
        返回一个字典列表，例如:
        [{'type': 'point', 'value': 25, 'unit': '°C'}]
        [{'type': 'range', 'min_value': 10, 'max_value': 15, 'unit': '°C'}]
        """
        if not text:
            return []
        
        # 模式：(可选负号)数字(可选小数) (可选范围指示符) (可选负号)数字(可选小数) (单位)
        pattern = r'(-?\d+(?:\.\d+)?)\s*(?:-|~|to|至)\s*(-?\d+(?:\.\d+)?)\s*([°℃CKF])'
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        results = []
        for start_str, end_str, unit_str in matches:
            try:
                min_val = float(start_str)
                max_val = float(end_str)
                unit = unit_str.upper().replace('℃', 'C')

                min_c = self._convert_temp_to_celsius(min_val, unit)
                max_c = self._convert_temp_to_celsius(max_val, unit)

                results.append({
                    'type': 'range',
                    'min_value': round(min_c, 2),
                    'max_value': round(max_c, 2),
                    'unit': '°C',
                    'original': f"{start_str}-{end_str} {unit_str}"
                })
            except (ValueError, IndexError):
                continue
        
        # 如果没有找到范围，则查找单个值
        if not results:
            pattern = r'(-?\d+(?:\.\d+)?)\s*([°℃CKF])'
            matches = re.findall(pattern, text, re.IGNORECASE)
            for val_str, unit_str in matches:
                try:
                    value = float(val_str)
                    unit = unit_str.upper().replace('℃', 'C')
                    val_c = self._convert_temp_to_celsius(value, unit)
                    results.append({
                        'type': 'point',
                        'value': round(val_c, 2),
                        'unit': '°C',
                        'original': f"{val_str} {unit_str}"
                    })
                except ValueError:
                    continue

        return results

    def extract_pressure(self, text: str) -> List[Dict[str, Any]]:
        """
        提取压力值，统一单位为kPa，并提取相关条件（如温度）。
        返回: [{'value': 101.3, 'unit': 'kPa', 'condition': 'at 25°C'}]
        """
        if not text:
            return []
        
        # 模式: 数字 (单位) (可选的 "at" 或 "@" 或 "在") (可选的温度条件)
        pattern = r'(\d+(?:\.\d+)?)\s*(kPa|Pa|hPa|MPa|bar|atm|mmHg)(?:\s*(?:at|@|在)\s*(-?\d+(?:\.\d+)?\s*[°℃CKF]))?'
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        results = []
        for value_str, unit_str, condition_str in matches:
            try:
                value = float(value_str)
                unit = unit_str.lower()
                
                # 单位换算
                conversion_factor = self.unit_conversions['pressure'].get(unit, 1.0)
                converted_value = value * conversion_factor
                
                result = {
                    'value': round(converted_value, 4),
                    'unit': 'kPa',
                    'original': f"{value_str} {unit_str}"
                }
                if condition_str:
                    result['condition'] = condition_str.strip()
                
                results.append(result)
            except ValueError:
                continue
        
        return results

    def extract_density(self, text: str) -> List[Dict[str, Any]]:
        """
        提取密度值，并提取相关条件（如温度）。
        返回: [{'value': 1.0, 'unit': 'g/cm³', 'condition': 'at 4°C'}]
        """
        if not text:
            return []
        
        # 模式: 数字 (单位) (可选的 "at" 或 "@" 或 "在") (可选的温度条件)
        pattern = r'(\d+(?:\.\d+)?)\s*(g/cm³|g/ml|kg/m³)(?:\s*(?:at|@|在)\s*(-?\d+(?:\.\d+)?\s*[°℃CKF]))?'
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        results = []
        for value_str, unit_str, condition_str in matches:
            try:
                value = float(value_str)
                unit = unit_str.lower()
                
                # 单位换算 (g/ml -> g/cm³, kg/m³ -> g/cm³)
                if unit == 'g/ml':
                    final_unit = 'g/cm³'
                elif unit == 'kg/m³':
                    value = value / 1000
                    final_unit = 'g/cm³'
                else:
                    final_unit = 'g/cm³'

                result = {
                    'value': round(value, 4),
                    'unit': final_unit,
                    'original': f"{value_str} {unit_str}"
                }
                if condition_str:
                    result['condition'] = condition_str.strip()
                
                results.append(result)
            except ValueError:
                continue

        return results

    def extract_quantitative_solubility(self, text: str) -> List[Dict[str, Any]]:
        """
        提取定量的溶解度信息。
        例如: "10g/100mL (20°C)"
        返回: [{'solute_mass': 10, 'solute_unit': 'g', 'solvent_volume': 100, 'solvent_unit': 'mL', 'condition': '20°C'}]
        """
        if not text:
            return []
        
        # 模式: (数字)(单位)/(数字)(单位) (可选条件)
        pattern = r'(\d+(?:\.\d+)?)\s*(g|mg)\s*/\s*(\d+(?:\.\d+)?)\s*(mL|L|g|mg)\s*(?:\((.+?)\))?'
        matches = re.findall(pattern, text, re.IGNORECASE)
        
        results = []
        for solute_mass, solute_unit, solvent_val, solvent_unit, condition in matches:
            try:
                results.append({
                    'solute_mass': float(solute_mass),
                    'solute_unit': solute_unit,
                    'solvent_volume': float(solvent_val),
                    'solvent_unit': solvent_unit,
                    'condition': condition.strip() if condition else None
                })
            except ValueError:
                continue
        return results

    def split_list_string(self, text: str, separators: Optional[List[str]] = None) -> List[str]:
        """分割列表型字符串"""
        if not text:
            return []
        
        if separators is None:
            separators = [';', '；', ',', '，', '、']
        
        # 使用多个分隔符分割
        parts = [text]
        for sep in separators:
            new_parts = []
            for part in parts:
                new_parts.extend(part.split(sep))
            parts = new_parts
        
        # 清理和过滤空值
        return [part.strip() for part in parts if part.strip()]

    def find_similar_terms(self, term: str, term_list: List[str], threshold: int = 80) -> List[str]:
        """查找相似的术语"""
        if not term or not term_list or process is None:
            return []
        
        similar = process.extractBests(term, term_list, score_cutoff=threshold)
        return [match[0] for match in similar]
    
    def standardize_chemical_names(self, names: List[str]) -> Dict[str, str]:
        """标准化化学品名称"""
        if not names:
            return {}
        
        # 构建名称映射
        name_groups = {}
        processed = set()
        
        for name in names:
            if name in processed:
                continue
            
            # 查找相似名称
            similar = self.find_similar_terms(name, [n for n in names if n not in processed])
            
            if similar:
                # 选择最短的名称作为标准名称
                canonical = min(similar, key=len)
                for sim_name in similar:
                    name_groups[sim_name] = canonical
                    processed.add(sim_name)
        
        return name_groups
    
    def extract_explosion_limits(self, text: str) -> Dict[str, float]:
        """提取爆炸极限"""
        if not text:
            return {}
        
        results = {}
        
        # 提取LEL
        lel_pattern = r'LEL[:\s]*(\d+(?:\.\d+)?)%?'
        lel_match = re.search(lel_pattern, text, re.IGNORECASE)
        if lel_match:
            results['LEL'] = float(lel_match.group(1))
        
        # 提取UEL
        uel_pattern = r'UEL[:\s]*(\d+(?:\.\d+)?)%?'
        uel_match = re.search(uel_pattern, text, re.IGNORECASE)
        if uel_match:
            results['UEL'] = float(uel_match.group(1))
        
        return results
    
    def categorize_hazards(self, hazard_text: str) -> Dict[str, List[str]]:
        """分类危害信息"""
        if not hazard_text:
            return {}
        
        categories = {
            '急性毒性': [],
            '皮肤刺激': [],
            '眼部刺激': [],
            '呼吸道刺激': [],
            '致癌性': [],
            '致突变性': [],
            '生殖毒性': [],
            '其他': []
        }
        
        # 分割危害描述
        hazards = self.split_list_string(hazard_text, [';', '；', '。'])
        
        for hazard in hazards:
            hazard = hazard.strip()
            if not hazard:
                continue
            
            # 根据关键词分类
            if any(keyword in hazard for keyword in ['中毒', '有毒', '致命', '急性']):
                categories['急性毒性'].append(hazard)
            elif any(keyword in hazard for keyword in ['皮肤', '灼伤', '刺激']):
                categories['皮肤刺激'].append(hazard)
            elif any(keyword in hazard for keyword in ['眼', '眼睛', '眼部']):
                categories['眼部刺激'].append(hazard)
            elif any(keyword in hazard for keyword in ['呼吸', '吸入']):
                categories['呼吸道刺激'].append(hazard)
            elif any(keyword in hazard for keyword in ['致癌', '癌']):
                categories['致癌性'].append(hazard)
            elif any(keyword in hazard for keyword in ['致突变', '突变']):
                categories['致突变性'].append(hazard)
            elif any(keyword in hazard for keyword in ['生殖', '胎儿']):
                categories['生殖毒性'].append(hazard)
            else:
                categories['其他'].append(hazard)
        
        # 移除空分类
        return {k: v for k, v in categories.items() if v}
