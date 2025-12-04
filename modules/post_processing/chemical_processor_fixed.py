#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
鍖栧鍝佹暟鎹鐞嗕富妯″潡
鐢ㄤ簬鍚堝苟銆佹竻娲楀寲瀛﹀搧鏁版嵁骞朵负Neo4j鍥炬暟鎹簱鍋氬噯澶?
瀹炵幇闃舵涓€锛氭暟鎹悎骞朵笌棰勫鐞嗭紝闃舵浜岋細鍥炬暟鎹牸寮忓寲涓庡叧绯绘彁鍙?
"""

import os
import pandas as pd
import numpy as np
import re
from pathlib import Path
import chardet
from typing import List, Dict, Tuple, Optional, Set
import logging
from datetime import datetime
import hashlib
import json
import psutil  # 绯荤粺璧勬簮鐩戞帶
import multiprocessing as mp  # 澶氳繘绋嬫敮鎸?

# 閰嶇疆鏃ュ織
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chemical_processor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ChemicalProcessor:
    """鍖栧鍝佹暟鎹鐞嗗櫒 - 瀹炵幇瀹屾暣鐨勬暟鎹鐞嗘祦绋?""
    
    def __init__(self, base_path: str):
        """
        鍒濆鍖栨暟鎹鐞嗗櫒 - 绗簩浠ｆ櫤鑳戒紭鍖栫増鏈?
        
        Args:
            base_path: 宸ヤ綔鐩綍鍩虹璺緞
        """
        self.base_path = Path(base_path)
        self.input_dir = self.base_path / "宸茶ˉ鍏ㄦ枃浠?
        self.dangerous_dir = self.base_path / "鍗卞寲鍝佺洰褰?
        self.success_dir = self.base_path / "澶勭悊鎴愬姛"
        self.failed_dir = self.base_path / "澶勭悊澶辫触"
        
        # 鍒涘缓杈撳嚭鐩綍
        self.success_dir.mkdir(exist_ok=True)
        self.failed_dir.mkdir(exist_ok=True)
        
        # 鍒涘缓澧為噺鏇存柊鐩稿叧鐩綍
        self.incremental_dir = self.base_path / "澧為噺鏇存柊"
        self.metadata_dir = self.base_path / "鍏冩暟鎹?
        self.incremental_dir.mkdir(exist_ok=True)
        self.metadata_dir.mkdir(exist_ok=True)
        
        # 瀛樺偍鍗卞寲鍝佺洰褰曟暟鎹?
        self.dangerous_chemicals = None
        
        # 澧為噺鏇存柊鐘舵€佺鐞?
        self.processed_files_record = self.metadata_dir / "processed_files.json"
        self.data_fingerprint_record = self.metadata_dir / "data_fingerprints.json"
        
        # 缂撳瓨鍖栧鍝佸悕绉帮紝閬垮厤鍦ㄥ叧绯绘彁鍙栨椂閲嶅鍔犺浇鏂囦欢
        self._cached_chemical_names = None
        
        # 绗簩浠ｆ櫤鑳介厤缃郴缁?
        self._init_adaptive_config()
        
        logger.info(f"鍒濆鍖栧寲瀛﹀搧鏁版嵁澶勭悊鍣?- 绗簩浠ｆ櫤鑳戒紭鍖栫増鏈?)
        logger.info(f"杈撳叆鐩綍: {self.input_dir}")
        logger.info(f"鍗卞寲鍝佺洰褰? {self.dangerous_dir}")
        logger.info(f"鎴愬姛杈撳嚭鐩綍: {self.success_dir}")
        logger.info(f"澶辫触杈撳嚭鐩綍: {self.failed_dir}")
        logger.info(f"澧為噺鏇存柊鐩綍: {self.incremental_dir}")
        logger.info(f"鍏冩暟鎹洰褰? {self.metadata_dir}")
        logger.info(f"鏅鸿兘閰嶇疆: 宸ヤ綔杩涚▼={self.max_workers}, 鍒嗗潡澶у皬={self.chunk_size}")
    
    def _init_adaptive_config(self):
        """鍒濆鍖栬嚜閫傚簲閰嶇疆绯荤粺"""
        try:
            # 绯荤粺璧勬簮妫€娴?
            cpu_cores = mp.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            available_memory = psutil.virtual_memory().available / (1024**3)
            
            # 鑷€傚簲宸ヤ綔杩涚▼閰嶇疆
            if memory_gb > 16 and available_memory > 8:
                self.max_workers = min(cpu_cores * 2, 24)  # 楂樺唴瀛樼郴缁?
                self.chunk_size = 3000
            elif memory_gb > 8 and available_memory > 4:
                self.max_workers = min(cpu_cores + 4, 16)  # 涓瓑鍐呭瓨绯荤粺
                self.chunk_size = 2000
            else:
                self.max_workers = cpu_cores  # 淇濆畧閰嶇疆
                self.chunk_size = 1000
            
            # 绯荤粺璐熻浇鍔ㄦ€佽皟鏁?
            cpu_percent = psutil.cpu_percent(interval=0.5)
            if cpu_percent < 30 and available_memory > memory_gb * 0.6:
                # 绯荤粺绌洪棽鏃跺惎鐢ㄦ縺杩涙ā寮?
                self.max_workers = min(self.max_workers + 2, 32)
                self.chunk_size = int(self.chunk_size * 1.2)
                logger.info(f"绯荤粺璐熻浇杈冧綆({cpu_percent:.1f}%)锛屽惎鐢ㄦ縺杩涘鐞嗘ā寮?)
            
            # 鍏朵粬閰嶇疆
            self.similarity_threshold = 0.8
            self.enable_parallel = True
            self.adaptive_mode = True
            
            logger.info(f"鑷€傚簲閰嶇疆瀹屾垚: CPU={cpu_cores}鏍? 鍐呭瓨={memory_gb:.1f}GB, 鍙敤={available_memory:.1f}GB")
            
        except Exception as e:
            logger.warning(f"鑷€傚簲閰嶇疆澶辫触: {e}")
            # 闄嶇骇鍒板畨鍏ㄩ厤缃?
            self.max_workers = mp.cpu_count()
            self.chunk_size = 1000
            self.similarity_threshold = 0.8
            self.enable_parallel = False
            self.adaptive_mode = False

    def detect_encoding(self, file_path: Path) -> str:
        """妫€娴嬫枃浠剁紪鐮?""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except:
            return 'utf-8'

    def load_csv_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """瀹夊叏鍔犺浇CSV鏂囦欢"""
        try:
            encoding = self.detect_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"鎴愬姛鍔犺浇鏂囦欢: {file_path.name}, 鍏眥len(df)}鏉¤褰?)
            return df
        except Exception as e:
            logger.error(f"鍔犺浇鏂囦欢澶辫触 {file_path.name}: {e}")
            return None

    def load_dangerous_chemicals(self) -> pd.DataFrame:
        """鍔犺浇鍗卞寲鍝佺洰褰曟暟鎹?""
        try:
            dangerous_file = self.dangerous_dir / "鍗卞寲鍝侀渶琛ュ厖_enriched.csv"
            if dangerous_file.exists():
                self.dangerous_chemicals = self.load_csv_file(dangerous_file)
                if self.dangerous_chemicals is not None:
                    logger.info(f"鎴愬姛鍔犺浇鍗卞寲鍝佺洰褰曪紝鍏眥len(self.dangerous_chemicals)}鏉¤褰?)
                    return self.dangerous_chemicals
                else:
                    logger.warning("鍗卞寲鍝佺洰褰曟枃浠跺姞杞藉け璐?)
                    return pd.DataFrame()
            else:
                logger.warning("鍗卞寲鍝佺洰褰曟枃浠朵笉瀛樺湪")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"鍔犺浇鍗卞寲鍝佺洰褰曞け璐? {e}")
            return pd.DataFrame()

    def calculate_file_fingerprint(self, file_path: Path) -> str:
        """璁＄畻鏂囦欢鎸囩汗鐢ㄤ簬澧為噺鏇存柊妫€娴?""
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                return hashlib.md5(content).hexdigest()
        except Exception as e:
            logger.error(f"璁＄畻鏂囦欢鎸囩汗澶辫触 {file_path}: {e}")
            return ""

    def load_processed_files_record(self) -> Dict[str, str]:
        """鍔犺浇宸插鐞嗘枃浠惰褰?""
        try:
            if self.processed_files_record.exists():
                with open(self.processed_files_record, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"鍔犺浇宸插鐞嗘枃浠惰褰曞け璐? {e}")
        return {}

    def save_processed_files_record(self, record: Dict[str, str]):
        """淇濆瓨宸插鐞嗘枃浠惰褰?""
        try:
            with open(self.processed_files_record, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"淇濆瓨宸插鐞嗘枃浠惰褰曞け璐? {e}")

    def get_new_files(self) -> List[Path]:
        """鑾峰彇闇€瑕佸閲忓鐞嗙殑鏂版枃浠?""
        processed_record = self.load_processed_files_record()
        new_files = []
        
        csv_files = list(self.input_dir.glob("*.csv"))
        
        for file_path in csv_files:
            current_fingerprint = self.calculate_file_fingerprint(file_path)
            file_key = str(file_path.relative_to(self.base_path))
            
            # 妫€鏌ユ枃浠舵槸鍚︽槸鏂扮殑鎴栧凡淇敼
            if (file_key not in processed_record or 
                processed_record[file_key] != current_fingerprint):
                new_files.append(file_path)
                
        logger.info(f"鍙戠幇 {len(new_files)} 涓柊鏂囦欢鎴栧凡淇敼鏂囦欢闇€瑕佸鐞?)
        return new_files

    def validate_chemical_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """鏁版嵁楠岃瘉瑙勫垯 - 澧炲姞鏇翠弗鏍肩殑鏁版嵁璐ㄩ噺妫€鏌?""
        if df.empty:
            return df, pd.DataFrame(), []

        logger.info("寮€濮嬫墽琛屾暟鎹獙璇佽鍒?..")
        validation_errors = []
        failed_records = []
        
        # 楠岃瘉瑙勫垯1: 涓枃鍚嶇О蹇呴』瀛樺湪涓斾负鏈夋晥涓枃
        chinese_name_pattern = r'^[\u4e00-\u9fa5\u3400-\u4dbf\u20000-\u2a6df\u2a700-\u2b73f\u2b740-\u2b81f\u2ceb0-\u2ebef\uff00-\uffef]+'
        name_invalid = df['涓枃鍚嶇О'].isna() | (df['涓枃鍚嶇О'].astype(str).str.strip() == '') | (~df['涓枃鍚嶇О'].astype(str).str.match(chinese_name_pattern))
        
        if name_invalid.any():
            invalid_names = df[name_invalid].copy()
            invalid_names['validation_error'] = '涓枃鍚嶇О鏃犳晥鎴栫己澶?
            failed_records.append(invalid_names)
            validation_errors.append(f"涓枃鍚嶇О楠岃瘉澶辫触: {name_invalid.sum()} 鏉¤褰?)
        
        # 楠岃瘉瑙勫垯2: CAS鍙锋牸寮忛獙璇?
        cas_pattern = r'^\d{1,7}-\d{2}-\d$'
        cas_invalid = df['CAS鍙锋垨娴佹按鍙?].notna() & (~df['CAS鍙锋垨娴佹按鍙?].astype(str).str.match(cas_pattern)) & (df['CAS鍙锋垨娴佹按鍙?].astype(str).str.strip() != '')
        
        if cas_invalid.any():
            invalid_cas = df[cas_invalid].copy()
            invalid_cas['validation_error'] = 'CAS鍙锋牸寮忔棤鏁?
            failed_records.append(invalid_cas)
            validation_errors.append(f"CAS鍙锋牸寮忛獙璇佸け璐? {cas_invalid.sum()} 鏉¤褰?)
        
        # 楠岃瘉瑙勫垯3: 鍒嗗瓙寮忔牸寮忛獙璇?
        molecular_formula_pattern = r'^[A-Za-z0-9\(\)\[\]\.路\-\+]+$'
        formula_invalid = df['鍒嗗瓙寮?].notna() & (~df['鍒嗗瓙寮?].astype(str).str.match(molecular_formula_pattern)) & (df['鍒嗗瓙寮?].astype(str).str.strip() != '')
        
        if formula_invalid.any():
            invalid_formula = df[formula_invalid].copy()
            invalid_formula['validation_error'] = '鍒嗗瓙寮忔牸寮忔棤鏁?
            failed_records.append(invalid_formula)
            validation_errors.append(f"鍒嗗瓙寮忔牸寮忛獙璇佸け璐? {formula_invalid.sum()} 鏉¤褰?)
        
        # 楠岃瘉瑙勫垯4: 鍗卞寲鍝佸繀椤绘湁鍗卞淇℃伅
        dangerous_missing_hazard = (df['鏄惁涓哄嵄鍖栧搧'].astype(str).str.strip() == '鏄?) & (df['鍗卞'].isna() | (df['鍗卞'].astype(str).str.strip() == ''))
        
        if dangerous_missing_hazard.any():
            missing_hazard = df[dangerous_missing_hazard].copy()
            missing_hazard['validation_error'] = '鍗卞寲鍝佺己灏戝嵄瀹充俊鎭?
            failed_records.append(missing_hazard)
            validation_errors.append(f"鍗卞寲鍝佸嵄瀹充俊鎭獙璇佸け璐? {dangerous_missing_hazard.sum()} 鏉¤褰?)
        
        # 楠岃瘉瑙勫垯5: 鏁版嵁瀹屾暣鎬ф鏌?
        required_fields = ['涓枃鍚嶇О', 'CAS鍙锋垨娴佹按鍙?]
        for field in required_fields:
            if field in df.columns:
                field_missing = df[field].isna() | (df[field].astype(str).str.strip() == '')
                if field_missing.any():
                    validation_errors.append(f"蹇呭～瀛楁 {field} 缂哄け: {field_missing.sum()} 鏉¤褰?)
        
        # 楠岃瘉瑙勫垯6: 閲嶅璁板綍妫€娴?
        duplicate_mask = df.duplicated(subset=['涓枃鍚嶇О', 'CAS鍙锋垨娴佹按鍙?], keep='first')
        if duplicate_mask.any():
            duplicate_records = df[duplicate_mask].copy()
            duplicate_records['validation_error'] = '閲嶅璁板綍'
            failed_records.append(duplicate_records)
            validation_errors.append(f"鍙戠幇閲嶅璁板綍: {duplicate_mask.sum()} 鏉?)
        
        # 鍚堝苟鎵€鏈夐獙璇佸け璐ョ殑璁板綍
        all_failed_mask = name_invalid | cas_invalid | formula_invalid | dangerous_missing_hazard | duplicate_mask
        valid_data = df[~all_failed_mask].copy()
        
        if failed_records:
            failed_df = pd.concat(failed_records, ignore_index=True)
        else:
            failed_df = pd.DataFrame()
        
        logger.info(f"鏁版嵁楠岃瘉瀹屾垚: 鏈夋晥璁板綍 {len(valid_data)} 鏉? 澶辫触璁板綍 {len(failed_df)} 鏉?)
        
        return valid_data, failed_df, validation_errors

    def get_known_chemical_names(self) -> Set[str]:
        """鑾峰彇宸茬煡鐨勫寲瀛﹀搧鍚嶇О搴?""
        known_names = set()
        
        # 浠庡嵄鍖栧搧鐩綍鑾峰彇
        if self.dangerous_chemicals is not None:
            if '鍝佸悕' in self.dangerous_chemicals.columns:
                known_names.update(self.dangerous_chemicals['鍝佸悕'].dropna().astype(str))
        
        # 浠庡凡澶勭悊鐨勬暟鎹幏鍙?
        success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
        for file_path in success_files:
            df = self.load_csv_file(file_path)
            if df is not None and '涓枃鍚嶇О' in df.columns:
                known_names.update(df['涓枃鍚嶇О'].dropna().astype(str))
        
        # 甯歌鍖栧鍝佸悕绉板簱
        common_chemicals = {
            '纭吀', '鐩愰吀', '纭濋吀', '姘㈡哀鍖栭挔', '姘㈡哀鍖栭捑', '姘ㄦ按', '涔欓唶', '鐢查唶',
            '涓欓叜', '鐢茶嫰', '鑻?, '涔欓啔', '姘豢', '鍥涙隘鍖栫⒊', '涔欓吀', '鐢查吀',
            '姘㈡皵', '姘ф皵', '姘皵', '浜屾哀鍖栫⒊', '涓€姘у寲纰?, '姘皵', '姘ㄦ皵',
            '鑻厷', '鐢查啗', '涔欓啗', '涔欑儻', '涓欑儻', '涓佺兎', '鐢茬兎', '涔欑兎',
            # 娣诲姞鏇村鍩虹鍖栧鍝?
            '鐭虫补', '澶╃劧姘?, '鐓?, '鐭宠湣', '姹芥补', '鏌存补', '鐓ゆ补',
            '涓欑兎', '涓佺儻', '寮備竵鐑?, '鐜繁鐑?, '姝ｅ繁鐑?, '搴氱兎', '杈涚兎',
            '鑻箼鐑?, '鐢茶嫰', '浜岀敳鑻?, '钀?, '钂?, '鑿?,
            '涓欑儻閰?, '閱嬮吀', '涓欓吀', '涓侀吀', '鎴婇吀',
            '鑱氫箼鐑?, '鑱氫笝鐑?, '鑱氳嫰涔欑儻', '鑱氭隘涔欑儻',
            '姘?, '閲嶆按', '杩囨哀鍖栨阿', '纭寲姘?, '浜屾哀鍖栫～', '涓夋哀鍖栫～'
        }
        known_names.update(common_chemicals)
        
        return known_names

    def get_all_chemical_names_from_data(self) -> Set[str]:
        """浠庢墍鏈夊彲鐢ㄦ暟鎹簮鑾峰彇鍖栧鍝佸悕绉板垪琛?- 浣跨敤缂撳瓨閬垮厤閲嶅鍔犺浇"""
        # 濡傛灉宸茬粡缂撳瓨浜嗭紝鐩存帴杩斿洖
        if self._cached_chemical_names is not None:
            return self._cached_chemical_names
        
        logger.info("棣栨鍔犺浇鍖栧鍝佸悕绉板簱...")
        all_chemicals = set()
        
        # 浠庡嵄鍖栧搧鐩綍鑾峰彇
        if self.dangerous_chemicals is not None:
            if '鍝佸悕' in self.dangerous_chemicals.columns:
                all_chemicals.update(self.dangerous_chemicals['鍝佸悕'].dropna().astype(str))
        
        # 娣诲姞甯歌鐨勫熀纭€鍖栧鍝侊紙閬垮厤閲嶅鏂囦欢璇诲彇锛?
        basic_chemicals = {
            '纭吀', '鐩愰吀', '纭濋吀', '姘㈡哀鍖栭挔', '姘㈡哀鍖栭捑', '姘ㄦ按', '涔欓唶', '鐢查唶',
            '涓欓叜', '鐢茶嫰', '鑻?, '涔欓啔', '姘豢', '鍥涙隘鍖栫⒊', '涔欓吀', '鐢查吀',
            '姘㈡皵', '姘ф皵', '姘皵', '浜屾哀鍖栫⒊', '涓€姘у寲纰?, '姘皵', '姘ㄦ皵',
            '鑻厷', '鐢查啗', '涔欓啗', '涔欑儻', '涓欑儻', '涓佺兎', '鐢茬兎', '涔欑兎',
            '鐭虫补', '澶╃劧姘?, '鐓?, '鐭宠湣', '姹芥补', '鏌存补', '鐓ゆ补',
            '涓欑兎', '涓佺儻', '寮備竵鐑?, '鐜繁鐑?, '姝ｅ繁鐑?, '搴氱兎', '杈涚兎',
            '鑻箼鐑?, '鐢茶嫰', '浜岀敳鑻?, '钀?, '钂?, '鑿?,
            '涓欑儻閰?, '閱嬮吀', '涓欓吀', '涓侀吀', '鎴婇吀',
            '鑱氫箼鐑?, '鑱氫笝鐑?, '鑱氳嫰涔欑儻', '鑱氭隘涔欑儻',
            '姘?, '閲嶆按', '杩囨哀鍖栨阿', '纭寲姘?, '浜屾哀鍖栫～', '涓夋哀鍖栫～',
            # 鏂板閲嶈鐨勫熀纭€鐭冲寲鍖栧鍝?
            '涔欒嫰', '寮備笝鑻?, '閭讳簩鐢茶嫰', '闂翠簩鐢茶嫰', '瀵逛簩鐢茶嫰',
            '鐜繁鐑?, '鐢插熀鐜繁鐑?, '1,3-涓佷簩鐑?, '寮傛垔浜岀儻',
            '姝ｄ竵鐑?, '寮備竵鐑?, '姝ｆ垔鐑?, '寮傛垔鐑?, '鏂版垔鐑?,
            '鐜笝鐑?, '鐜竵鐑?, '鐜垔鐑?, '鐜簹鐑?, '鐜緵鐑?,
            '涓欑倲', '1-涓佺倲', '2-涓佺倲', '1-鎴婄倲', '鑻倲',
            '鐢茶嫰鑳?, '鑻兒', '浜岀敳鑻兒', '纭濆熀鑻?, '姘嫰',
            '瀵规隘鐢茶嫰', '閭绘隘鐢茶嫰', '闂存隘鐢茶嫰', '涓夋隘鑻?,
            '鍛嬪杻', '鍣诲惄', '鍚″暥', '鍚插摎', '鍠瑰晧', '鍜斿攽',
            '鍘熸补', '鐭宠剳娌?, '閲嶆补', '娑︽粦娌?, '娌ラ潚'
        }
        all_chemicals.update(basic_chemicals)
        
        # 杩囨护鎺夋棤鏁堢殑鍖栧鍝佸悕绉?
        valid_chemicals = set()
        for chem in all_chemicals:
            if isinstance(chem, str) and len(chem) >= 2 and len(chem) <= 20:
                # 绉婚櫎鏄庢樉涓嶆槸鍖栧鍝佺殑璇嶆眹
                if not any(word in chem for word in ['宸ヤ笟', '浜т笟', '鍏徃', '浼佷笟', '鏈夐檺', '鑲′唤']):
                    valid_chemicals.add(chem)
        
        # 缂撳瓨缁撴灉
        self._cached_chemical_names = valid_chemicals
        logger.info(f"鍖栧鍝佸悕绉板簱鍔犺浇瀹屾垚锛屽叡 {len(valid_chemicals)} 涓寲瀛﹀搧")
        
        return valid_chemicals

    def is_valid_chemical_name(self, name: str) -> bool:
        """楠岃瘉鏄惁涓烘湁鏁堢殑鍖栧鍝佸悕绉?- 绠€鍖栫増鏈?""
        if not name or len(name) < 1 or len(name) > 20:
            return False
        
        # 鎺掗櫎鏄庢樉涓嶆槸鍖栧鍝佺殑璇嶆眹
        invalid_words = {
            '绛?, '绫?, '涓?, '涓?, '涓?, '鍓?, '鍚?, '宸?, '鍙?, '鍐?, '澶?, 
            '楂?, '浣?, '澶?, '灏?, '澶?, '灏?, '濂?, '鍧?, '鏂?, '鏃?, '蹇?, '鎱?, 
            '鐑?, '鍐?, '宸ヤ笟', '鍐滀笟', '鍖昏嵂', '椋熷搧', '鍖栧伐', '鐢熶骇', '鍒堕€?, 
            '鍔犲伐', '澶勭悊', '浣跨敤', '搴旂敤', '鏂规硶', '鎶€鏈?, '宸ヨ壓', '鍏徃', 
            '浼佷笟', '鏈夐檺', '鑲′唤', '闆嗗洟', '鍙嶅簲', '鍒跺', '鍚堟垚', '鍖呮嫭',
            '閫氳繃鐭虫补', '缁忚繃鐭虫补', '鍒╃敤鐭虫补', '浣跨敤鐭虫补', '涔欑儻姘㈡皵', '閫氳繃'
        }
        
        if name in invalid_words:
            return False
        
        # 妫€鏌ユ槸鍚﹀寘鍚棤鏁堝瓧绗?
        if re.search(r'[0-9%锛匼-\+\=\*\#\@\$\&\^\~\`\|\\\"\'\[\]<>銆娿€媇', name):
            return False
        
        # 鐗规畩閲嶈鍖栧鍝侊紙鍗曠嫭鍒楀嚭锛?
        special_chemicals = {'鑻?, '鐢茶嫰', '浜岀敳鑻?, '鐭虫补', '鐓?, '澶╃劧姘?, '姘?, '姘?, '姘㈡皵', '姘ф皵', '姘皵'}
        if name in special_chemicals:
            return True
        
        # 鍩轰簬鍖栧鍝佸懡鍚嶈寰嬬殑绠€鍗曢獙璇?
        chemical_indicators = [
            # 甯歌鍖栧鍚庣紑
            '閰?, '纰?, '鐩?, '閱?, '閱?, '閰?, '閰?, '閱?, '鐑?, '鐑?, '鐐?, '鑻?, '閰?, '鑵?,
            # 甯歌鍖栧鍓嶇紑
            '鐢?, '涔?, '涓?, '涓?, '鎴?, '宸?, '搴?, '杈?, '澹?, '鐧?, '姝?, '寮?, '鍙?, '浠?,
            # 浣嶇疆鎸囩ず璇?
            '閭?, '闂?, '瀵?, '椤?, '鍙?,
            # 甯歌鍏冪礌
            '姘?, '姘?, '姘?, '姘?, '纭?, '閽?, '閽?, '閽?, '闀?, '閾?, '閾?, '閿?, '閾?, '姹?, '閾?,
            # 鍏朵粬鍖栧鍝佺壒寰?
            '鑱?, '鐜?, '浜?, '涓?, '鍥?, '浜?, '鍏?, '涓?, '鍏?, '涔?, '鍗?, '姘?, '娌?, '鑴?, '鑳?
        ]
        
        # 濡傛灉鍖呭惈浠讳綍鍖栧鍝佹寚绀鸿瘝锛岃涓烘槸鏈夋晥鐨?
        return any(indicator in name for indicator in chemical_indicators)

    def add_new_chemical_to_database(self, chemical_name: str):
        """灏嗘柊鍖栧鍝佹坊鍔犲埌鏁版嵁搴撲腑"""
        try:
            # 鍒涘缓鏂板寲瀛﹀搧璁板綍
            new_chemical_record = {
                '涓枃鍚嶇О': chemical_name,
                'CAS鍙锋垨娴佹按鍙?: '',  # 鏂板彂鐜扮殑鍖栧鍝佹殏鏃舵病鏈塁AS鍙?
                '鍒嗗瓙寮?: '',
                '鑻辨枃鍚嶇О': '',
                '鏄惁涓哄嵄鍖栧搧': '',  # 寰呯‘瀹?
                '鐢ㄩ€?: '',
                '鍗卞': '',
                '闃茶寖': '',
                '鐢熶骇鏉ユ簮': '',
                '涓婃父鍘熸枡': '',
                'data_source': '绯荤粺鑷姩璇嗗埆',
                '澶囨敞': f'浠庣敓浜ф潵婧愭枃鏈腑鑷姩璇嗗埆鐨勬柊鍖栧鍝侊紝娣诲姞鏃堕棿锛歿datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            }
            
            # 淇濆瓨鍒版柊鍙戠幇鍖栧鍝佹枃浠?
            new_chemicals_file = self.metadata_dir / "new_discovered_chemicals.csv"
            
            if new_chemicals_file.exists():
                # 鏂囦欢瀛樺湪锛岃拷鍔犺褰?
                existing_df = pd.read_csv(new_chemicals_file, encoding='utf-8-sig')
                # 妫€鏌ユ槸鍚﹀凡缁忓瓨鍦?
                if chemical_name not in existing_df['涓枃鍚嶇О'].values:
                    new_df = pd.concat([existing_df, pd.DataFrame([new_chemical_record])], ignore_index=True)
                    new_df.to_csv(new_chemicals_file, index=False, encoding='utf-8-sig')
                    logger.info(f"鏂板鍖栧鍝佸埌鏁版嵁搴? {chemical_name}")
            else:
                # 鏂囦欢涓嶅瓨鍦紝鍒涘缓鏂版枃浠?
                new_df = pd.DataFrame([new_chemical_record])
                new_df.to_csv(new_chemicals_file, index=False, encoding='utf-8-sig')
                logger.info(f"鍒涘缓鏂板彂鐜板寲瀛﹀搧鏂囦欢骞舵坊鍔? {chemical_name}")
            
            # 鏇存柊缂撳瓨涓殑鍖栧鍝佸悕绉板垪琛紝閬垮厤閲嶅鍔犺浇
            if self._cached_chemical_names is not None:
                self._cached_chemical_names.add(chemical_name)
                
        except Exception as e:
            logger.warning(f"娣诲姞鏂板寲瀛﹀搧鍒版暟鎹簱澶辫触 {chemical_name}: {e}")

    def extract_batch_number(self, filename: str) -> Optional[int]:
        """浠庢枃浠跺悕鎻愬彇鎵规鍙?""
        # 鍖归厤 batch_鏁板瓧 鏍煎紡
        batch_match = re.search(r'batch_(\d+)', filename)
        if batch_match:
            return int(batch_match.group(1))
        
        # 鍖归厤 part_鏁板瓧 鏍煎紡  
        part_match = re.search(r'part_(\d+)', filename)
        if part_match:
            return int(part_match.group(1))
        
        return None

    def calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """璁＄畻鏁版嵁瀹屾暣搴﹀緱鍒?""
        if df is None or len(df) == 0:
            return 0.0
        
        # 閲嶈瀛楁鏉冮噸
        important_fields = {
            '涓枃鍚嶇О': 3,
            'CAS鍙锋垨娴佹按鍙?: 3,
            '鍒嗗瓙寮?: 2,
            '鑻辨枃鍚嶇О': 2,
            '鏄惁涓哄嵄鍖栧搧': 2,
            '鐢ㄩ€?: 1,
            '鍗卞': 1,
            '闃茶寖': 1
        }
        
        total_score = 0
        max_score = 0
        
        for field, weight in important_fields.items():
            if field in df.columns:
                # 璁＄畻闈炵┖鍊兼瘮渚?
                non_null_ratio = df[field].notna().sum() / len(df)
                # 璁＄畻骞冲潎鏂囨湰闀垮害锛堝浜庢枃鏈瓧娈碉級
                if df[field].dtype == 'object':
                    avg_length = df[field].dropna().astype(str).str.len().mean()
                    length_score = min(avg_length / 50, 1.0)  # 鍋囪50瀛楃涓烘弧鍒?
                else:
                    length_score = 1.0
                
                field_score = non_null_ratio * length_score * weight
                total_score += field_score
            
            max_score += weight
        
        return total_score / max_score if max_score > 0 else 0.0
    
    def organize_files_by_batch(self) -> Dict[int, List[Path]]:
        """闃舵涓€锛氭寜搴忓彿缁勭粐鏂囦欢"""
        logger.info("寮€濮嬫寜搴忓彿缁勭粐鏂囦欢...")
        
        csv_files = list(self.input_dir.glob("*.csv"))
        files_by_batch = {}
        
        for file_path in csv_files:
            batch_num = self.extract_batch_number(file_path.name)
            if batch_num is not None:
                if batch_num not in files_by_batch:
                    files_by_batch[batch_num] = []
                files_by_batch[batch_num].append(file_path)
        
        logger.info(f"鎵惧埌 {len(csv_files)} 涓枃浠讹紝鎸夊簭鍙峰垎缁勪负 {len(files_by_batch)} 涓壒娆?)
        return files_by_batch

    def merge_duplicate_batches(self, files_by_batch: Dict[int, List[Path]]) -> Dict[int, pd.DataFrame]:
        """鍚堝苟閲嶅鎵规鏂囦欢锛岄€夋嫨鏈€浼樼増鏈?""
        logger.info("寮€濮嬪悎骞堕噸澶嶆壒娆℃枃浠?..")
        merged_batches = {}
        
        for batch_num, file_list in files_by_batch.items():
            if len(file_list) == 1:
                # 鍙湁涓€涓枃浠讹紝鐩存帴鍔犺浇
                df = self.load_csv_file(file_list[0])
                if df is not None:
                    merged_batches[batch_num] = df
            else:
                # 澶氫釜鏂囦欢锛岄€夋嫨鏈€浼樼増鏈?
                logger.info(f"鎵规 {batch_num} 鏈?{len(file_list)} 涓枃浠讹紝閫夋嫨鏈€浼樼増鏈?..")
                best_df = None
                best_score = -1
                best_file = None
                
                for file_path in file_list:
                    df = self.load_csv_file(file_path)
                    if df is not None:
                        score = self.calculate_completeness_score(df)
                        logger.info(f"  鏂囦欢 {file_path.name} 瀹屾暣搴﹀緱鍒? {score:.3f}")
                        
                        if score > best_score:
                            best_score = score
                            best_df = df
                            best_file = file_path
                
                if best_df is not None and best_file is not None:
                    merged_batches[batch_num] = best_df
                    logger.info(f"  鎵规 {batch_num} 閫夋嫨鏂囦欢: {best_file.name}")
        
        return merged_batches

    def check_missing_batches(self, merged_batches: Dict[int, pd.DataFrame]) -> List[int]:
        """妫€鏌ョ己澶辩殑鎵规搴忓彿"""
        if not merged_batches:
            return []
        
        batch_numbers = sorted(merged_batches.keys())
        missing_batches = []
        
        for i in range(min(batch_numbers), max(batch_numbers) + 1):
            if i not in batch_numbers:
                missing_batches.append(i)
        
        if missing_batches:
            logger.warning(f"鍙戠幇缂哄け鐨勬壒娆″簭鍙? {missing_batches}")
        
        return missing_batches

    def identify_failed_records(self, merged_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """璇嗗埆澶辫触璁板綍"""
        failed_records = pd.DataFrame()
        success_records = merged_data.copy()
        
        # 妫€鏌rocessing_status瀛楁
        if 'processing_status' in merged_data.columns:
            failed_mask = merged_data['processing_status'].str.contains('澶辫触', na=False)
            if failed_mask.any():
                failed_records = merged_data[failed_mask].copy()
                success_records = merged_data[~failed_mask].copy()
                logger.info(f"鍙戠幇 {len(failed_records)} 鏉″け璐ヨ褰?)
        
        return success_records, failed_records

    def find_missing_dangerous_chemicals(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """妫€鏌ュ嵄鍖栧搧鐩綍涓湭琚鍏ョ殑鍖栧鍝?""
        if self.dangerous_chemicals is None or len(self.dangerous_chemicals) == 0:
            return pd.DataFrame()
        
        logger.info("妫€鏌ュ嵄鍖栧搧鐩綍涓己澶辩殑鍖栧鍝?..")
        missing_records = []
        
        # 鍒涘缓宸插鐞嗘暟鎹殑鏌ユ壘闆嗗悎
        processed_names = set()
        processed_cas = set()
        
        if len(processed_data) > 0:
            if '涓枃鍚嶇О' in processed_data.columns:
                processed_names = set(processed_data['涓枃鍚嶇О'].dropna().astype(str))
            if 'CAS鍙锋垨娴佹按鍙? in processed_data.columns:
                processed_cas = set(processed_data['CAS鍙锋垨娴佹按鍙?].dropna().astype(str))
        
        for _, dangerous_row in self.dangerous_chemicals.iterrows():
            dangerous_name = str(dangerous_row.get('鍝佸悕', '')).strip()
            dangerous_cas = str(dangerous_row.get('CAS鍙?, '')).strip()
            
            found = False
            
            # 鎸塁AS鍙锋煡鎵?
            if dangerous_cas and dangerous_cas in processed_cas:
                found = True
            
            # 鎸夊悕绉版煡鎵?
            if not found and dangerous_name and dangerous_name in processed_names:
                found = True
            
            if not found:
                missing_record = {
                    '搴忓彿': dangerous_row.get('搴忓彿', ''),
                    '涓枃鍚嶇О': dangerous_name,
                    'CAS鍙锋垨娴佹按鍙?: dangerous_cas,
                    '鏄惁涓哄嵄鍖栧搧': '鏄?,
                    '澶囨敞': '鏈湪宸茶ˉ鍏ㄦ枃浠朵腑鎵惧埌',
                    'data_source': '鍗卞寲鍝佺洰褰?
                }
                missing_records.append(missing_record)
        
        if missing_records:
            logger.info(f"鍙戠幇 {len(missing_records)} 鏉＄己澶辩殑鍗卞寲鍝?)
            return pd.DataFrame(missing_records)
        else:
            logger.info("鏈彂鐜扮己澶辩殑鍗卞寲鍝?)
            return pd.DataFrame()

    def save_failed_data(self, failed_records: pd.DataFrame, missing_batches: List[int], missing_dangerous: pd.DataFrame):
        """淇濆瓨澶辫触鏁版嵁"""
        all_failed_data = []
        
        # 娣诲姞澶辫触璁板綍
        if not failed_records.empty:
            all_failed_data.append(failed_records)
        
        # 娣诲姞缂哄け鎵规璁板綍
        if missing_batches:
            missing_batch_records = pd.DataFrame([
                {'搴忓彿': batch_num, '澶囨敞': f'鎵规{batch_num}鏂囦欢缂哄け'} 
                for batch_num in missing_batches
            ])
            all_failed_data.append(missing_batch_records)
        
        # 娣诲姞缂哄け鍗卞寲鍝佽褰?
        if not missing_dangerous.empty:
            all_failed_data.append(missing_dangerous)
        
        if all_failed_data:
            combined_failed = pd.concat(all_failed_data, ignore_index=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failed_filename = f"failed_chemicals_{timestamp}.csv"
            failed_filepath = self.failed_dir / failed_filename
            combined_failed.to_csv(failed_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"淇濆瓨澶辫触鏁版嵁: {failed_filename}, 鍏眥len(combined_failed)}鏉¤褰?)
        else:
            logger.info("娌℃湁澶辫触鏁版嵁闇€瑕佷繚瀛?)

    def save_batch_data(self, data: pd.DataFrame, batch_size: int = 3000):
        """闃舵涓€锛氭寜鎵规淇濆瓨鎴愬姛鏁版嵁"""
        if len(data) == 0:
            logger.warning("娌℃湁鏁版嵁闇€瑕佷繚瀛?)
            return
        
        num_batches = (len(data) + batch_size - 1) // batch_size
        logger.info(f"灏?{len(data)} 鏉¤褰曞垎涓?{num_batches} 涓壒娆′繚瀛?)
        
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_data = data.iloc[start_idx:end_idx]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processed_chemicals_batch_{i+1}_{timestamp}.csv"
            filepath = self.success_dir / filename
            
            batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"淇濆瓨鎵规 {i+1}: {filename}, 鍏眥len(batch_data)}鏉¤褰?)

    def format_for_neo4j(self, df: pd.DataFrame) -> pd.DataFrame:
        """闃舵浜岋細灏嗘暟鎹牸寮忓寲浠ラ€傚簲Neo4j瀵煎叆锛屽寘鍚悕绉版竻鐞?""
        if df.empty:
            return pd.DataFrame()

        logger.info("寮€濮嬫牸寮忓寲鏁版嵁浠ラ€傚簲Neo4j...")

        # 鍒楁槧灏?
        column_mapping = {
            '涓枃鍚嶇О': 'name:ID',
            'CAS鍙锋垨娴佹按鍙?: 'cas:string',
            '涓枃鍒悕': 'aliases:string[]',
            '鑻辨枃鍚嶇О': 'english_name:string',
            '鑻辨枃鍒悕': 'english_aliases:string[]',
            '鍒嗗瓙寮?: 'molecular_formula:string',
            '鍒嗗瓙閲?: 'molecular_weight:float',
            '鍗卞': 'hazard:string',
            '闃茶寖': 'prevention:string',
            '鍗卞澶勭疆': 'hazard_disposal:string',
            '鐢ㄩ€?: 'uses:string',
            '鑷劧鏉ユ簮': 'natural_source:string',
            '鐢熶骇鏉ユ簮': 'production_source:string',
            '涓婃父鍘熸枡': 'upstream_materials:string',
            '鎬ц川': 'properties:string',
            'data_source': 'source:string',
            '娴撳害闃堝€?: 'concentration_threshold:string'
        }

        # 绛涢€夊瓨鍦ㄧ殑鍒?
        existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        formatted_df = df[existing_columns.keys()].copy()
        formatted_df.rename(columns=existing_columns, inplace=True)

        # 娓呯悊鍖栧鍝佸悕绉帮紙淇濈暀-鍜?锛屽幓闄ゅ叾浠栫鍙凤級
        if 'name:ID' in formatted_df.columns:
            formatted_df['name:ID'] = formatted_df['name:ID'].astype(str).apply(self.clean_chemical_name_for_neo4j)
            # 杩囨护鎺夋竻鐞嗗悗涓虹┖鐨勮褰?
            formatted_df = formatted_df[formatted_df['name:ID'].str.strip() != '']
        
        # 娓呯悊鑻辨枃鍚嶇О
        if 'english_name:string' in formatted_df.columns:
            formatted_df['english_name:string'] = formatted_df['english_name:string'].astype(str).apply(self.clean_chemical_name_for_neo4j)

        # 娓呯悊CAS鍙蜂腑鐨勪腑鏂囧瓧绗?
        if 'cas:string' in formatted_df.columns:
            chinese_char_pattern = re.compile(r'[\u4e00-\u9fa5]')
            formatted_df['cas:string'] = formatted_df['cas:string'].astype(str).apply(
                lambda x: '' if chinese_char_pattern.search(x) else x
            )

        # 鍒涘缓鑺傜偣鏍囩
        if '鏄惁涓哄嵄鍖栧搧' in df.columns:
            formatted_df[':LABEL'] = np.where(
                df['鏄惁涓哄嵄鍖栧搧'].astype(str).str.strip() == '鏄?,
                'DangerousChemical;Chemical',
                'Chemical'
            )
        else:
            formatted_df[':LABEL'] = 'Chemical'

        # 杩囨护闈炲嵄鍖栧搧鐨勬祿搴﹂槇鍊?
        if 'concentration_threshold:string' in formatted_df.columns and '鏄惁涓哄嵄鍖栧搧' in df.columns:
            formatted_df.loc[df['鏄惁涓哄嵄鍖栧搧'].astype(str).str.strip() != '鏄?, 'concentration_threshold:string'] = ''

        # 澶勭悊鏁扮粍鍒楁牸寮忥紝骞舵竻鐞嗗埆鍚?
        array_columns = ['aliases:string[]', 'english_aliases:string[]']
        for col in array_columns:
            if col in formatted_df.columns:
                # 娓呯悊鍒悕涓殑绗﹀彿锛岀劧鍚庢牸寮忓寲
                formatted_df[col] = formatted_df[col].astype(str).apply(
                    lambda x: '|'.join([self.clean_chemical_name_for_neo4j(alias.strip()) 
                                      for alias in re.split(r'[;锛泑]', x) 
                                      if self.clean_chemical_name_for_neo4j(alias.strip())])
                )

        # 澶勭悊鏁版嵁鏉ユ簮瀛楁锛屽簲鐢ㄨ姳鎷彿鍐呭鎻愬彇鍜屾湰鍦版悳绱㈡爣璇?
        if 'source:string' in formatted_df.columns:
            formatted_df['source:string'] = formatted_df['source:string'].astype(str).apply(self.process_data_source_field)
        elif 'data_source' in df.columns:
            # 濡傛灉鍘熷鏁版嵁涓湁data_source瀛楁锛屼篃瑕佸鐞?
            formatted_df['source:string'] = df['data_source'].astype(str).apply(self.process_data_source_field)

        # 澶勭悊鐢熶骇鏉ユ簮瀛楁锛岀Щ闄ょ敓浜у晢淇℃伅
        if 'production_source:string' in formatted_df.columns:
            formatted_df['production_source:string'] = formatted_df['production_source:string'].astype(str).apply(self.remove_producer_info)

        # 濉厖绌哄€?
        formatted_df.fillna('', inplace=True)

        logger.info(f"鏁版嵁鏍煎紡鍖栧畬鎴愶紝鐢熸垚 {len(formatted_df)} 鏉¤褰?)
        return formatted_df

    def deduplicate_relationships(self, relationships: List[dict]) -> List[dict]:
        """鍘婚櫎閲嶅鍜岀浉杩戠殑鍏崇郴"""
        if not relationships:
            return relationships
        
        # 浣跨敤闆嗗悎鍘婚櫎瀹屽叏鐩稿悓鐨勫叧绯?
        unique_relationships = []
        seen_relationships = set()
        
        for rel in relationships:
            # 娓呯悊鍏崇郴涓殑鍚嶇О
            start_id = self.clean_chemical_name_for_neo4j(str(rel[':START_ID']))
            end_id = self.clean_chemical_name_for_neo4j(str(rel[':END_ID']))
            rel_type = rel[':TYPE']
            
            # 璺宠繃绌哄悕绉?
            if not start_id or not end_id:
                continue
            
            # 鍒涘缓鍏崇郴鐨勫敮涓€鏍囪瘑绗?
            rel_key = (start_id, end_id, rel_type)
            
            if rel_key not in seen_relationships:
                seen_relationships.add(rel_key)
                unique_relationships.append({
                    ':START_ID': start_id,
                    ':END_ID': end_id,
                    ':TYPE': rel_type
                })
        
        # 杩涗竴姝ュ鐞嗙浉杩戝叧绯伙細濡傛灉涓や釜鍏崇郴鐨勮捣鐐瑰拰缁堢偣鐩镐技搴﹀緢楂橈紝淇濈暀杈冪煭鐨勫悕绉?
        filtered_relationships = []
        grouped_by_type = {}
        
        # 鎸夊叧绯荤被鍨嬪垎缁?
        for rel in unique_relationships:
            rel_type = rel[':TYPE']
            if rel_type not in grouped_by_type:
                grouped_by_type[rel_type] = []
            grouped_by_type[rel_type].append(rel)
        
        # 瀵规瘡绉嶅叧绯荤被鍨嬭繘琛屽幓閲?
        for rel_type, rels in grouped_by_type.items():
            processed_rels = []
            
            for rel in rels:
                is_similar = False
                start_id = rel[':START_ID']
                end_id = rel[':END_ID']
                
                # 妫€鏌ユ槸鍚︿笌宸插鐞嗙殑鍏崇郴鐩镐技
                for processed_rel in processed_rels:
                    proc_start = processed_rel[':START_ID']
                    proc_end = processed_rel[':END_ID']
                    
                    # 璁＄畻鐩镐技搴︼細濡傛灉涓€涓悕绉板寘鍚彟涓€涓悕绉帮紝璁や负鐩镐技
                    start_similar = (start_id in proc_start or proc_start in start_id)
                    end_similar = (end_id in proc_end or proc_end in end_id)
                    
                    if start_similar and end_similar:
                        is_similar = True
                        # 濡傛灉褰撳墠鍏崇郴鐨勫悕绉版洿鐭紙鏇寸畝娲侊級锛屾浛鎹㈠凡澶勭悊鐨勫叧绯?
                        if len(start_id) + len(end_id) < len(proc_start) + len(proc_end):
                            processed_rels.remove(processed_rel)
                            processed_rels.append(rel)
                        break
                
                if not is_similar:
                    processed_rels.append(rel)
            
            filtered_relationships.extend(processed_rels)
        
        logger.info(f"鍏崇郴鍘婚噸锛氫粠 {len(relationships)} 鏉″噺灏戝埌 {len(filtered_relationships)} 鏉?)
        return filtered_relationships

    def extract_relationships(self, df: pd.DataFrame) -> pd.DataFrame:
        """闃舵浜岋細姝ｇ‘鐨勫叧绯绘彁鍙栭€昏緫 - 涓婃父鍘熸枡鍜屼笅娓镐骇涓氬叧绯伙紝鍖呭惈鍘婚噸"""
        if df.empty:
            logger.info("娌℃湁鏁版嵁锛岃烦杩囧叧绯绘彁鍙?)
            return pd.DataFrame()

        logger.info("寮€濮嬫彁鍙栦笂娓稿師鏂欏叧绯?..")
        relationships = []
        
        # 鑾峰彇宸茬煡鍖栧鍝佸悕绉板簱
        known_chemicals = self.get_known_chemical_names()
        logger.info(f"宸茬煡鍖栧鍝佸簱鍖呭惈 {len(known_chemicals)} 涓寲瀛﹀搧鍚嶇О")
        
        for _, row in df.iterrows():
            chemical_name = self.clean_chemical_name_for_neo4j(row['name:ID'])
            
            # 鎻愬彇涓婃父鍘熸枡鍏崇郴锛氱敓浜ф潵婧?鈫?宸ヨ壓 鈫?褰撳墠鍖栧鍝?
            if 'production_source:string' in row.index and pd.notna(row['production_source:string']):
                source_text = str(row['production_source:string'])
                upstream_materials = self.extract_upstream_materials(source_text, known_chemicals, chemical_name)
                
                if upstream_materials:
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical_name)
                    if clean_chemical:
                        # 涓烘瘡涓洰鏍囧寲瀛﹀搧鍒涘缓涓€涓粺涓€鐨勫伐鑹鸿妭鐐?
                        process_name = f"{clean_chemical}_鐢熶骇宸ヨ壓"
                        
                        # 宸ヨ壓 -> 褰撳墠鍖栧鍝?(姝ゅ叧绯诲姣忎釜鐩爣鍖栧鍝佸彧鍒涘缓涓€娆?
                        relationships.append({
                            ':START_ID': process_name,
                            ':END_ID': clean_chemical,
                            ':TYPE': '鐢熶骇浜у搧'
                        })

                        # 鎵€鏈変笂娓稿師鏂欓兘鎸囧悜杩欎竴涓伐鑹鸿妭鐐?
                        for material in upstream_materials:
                            clean_material = self.clean_chemical_name_for_neo4j(material)
                            if clean_material and clean_material != clean_chemical:
                                relationships.append({
                                    ':START_ID': clean_material,
                                    ':END_ID': process_name,
                                    ':TYPE': '鍙備笌宸ヨ壓'
                                })

        # 鍘婚噸澶嶅叧绯?
        relationships = self.deduplicate_relationships(relationships)
        
        logger.info(f"鍏崇郴鎻愬彇瀹屾垚锛屽叡鎻愬彇 {len(relationships)} 鏉″叧绯?)
        return pd.DataFrame(relationships)

    def extract_process_nodes_from_relationships(self, relationships_df: pd.DataFrame) -> pd.DataFrame:
        """浠庡叧绯讳腑鎻愬彇宸ヨ壓鑺傜偣骞跺垱寤鸿妭鐐规暟鎹?""
        if relationships_df.empty:
            return pd.DataFrame()
        
        process_nodes = []
        process_names = set()
        
        # 浠庡叧绯讳腑鏀堕泦鎵€鏈夊伐鑹鸿妭鐐瑰悕绉?
        for _, row in relationships_df.iterrows():
            start_id = str(row.get(':START_ID', ''))
            end_id = str(row.get(':END_ID', ''))
            
            # 璇嗗埆宸ヨ壓鑺傜偣锛堝寘鍚?宸ヨ壓"鍏抽敭瀛楃殑鑺傜偣锛?
            if '宸ヨ壓' in start_id and start_id not in process_names:
                process_names.add(start_id)
            if '宸ヨ壓' in end_id and end_id not in process_names:
                process_names.add(end_id)
        
        # 涓烘瘡涓伐鑹鸿妭鐐瑰垱寤鸿妭鐐规暟鎹?
        for process_name in process_names:
            # 瑙ｆ瀽宸ヨ壓鍚嶇О
            if '_鍒跺_' in process_name and process_name.endswith('_宸ヨ壓'):
                parts = process_name.replace('_宸ヨ壓', '').split('_鍒跺_')
                if len(parts) == 2:
                    upstream_material = parts[0]
                    target_chemical = parts[1]
                    
                    process_nodes.append({
                        'name:ID': process_name,
                        ':LABEL': 'Process',
                        'process_type:string': '鍖栧鍒跺宸ヨ壓',
                        'upstream_material:string': upstream_material,
                        'target_product:string': target_chemical,
                        'description:string': f'浣跨敤{upstream_material}鍒跺{target_chemical}鐨勫伐鑹烘祦绋?,
                        'source:string': 'system_generated',
                        'data_status:string': 'placeholder'
                    })
        
        logger.info(f"浠庡叧绯讳腑鎻愬彇浜?{len(process_nodes)} 涓伐鑹鸿妭鐐?)
        return pd.DataFrame(process_nodes)

    def clean_chemical_name_for_neo4j(self, name: str) -> str:
        """娓呯悊鍖栧鍝佸悕绉扮敤浜嶯eo4j褰曞叆锛屼繚鐣?鍜?锛屽幓闄ゅ叾浠栫鍙?""
        if not name:
            return name
        
        # 淇濈暀涓枃瀛楃銆佽嫳鏂囧瓧姣嶃€佹暟瀛椼€佽繛瀛楃(-)鍜岄€楀彿(,)
        # 鍘婚櫎鍏朵粬鐗规畩绗﹀彿濡傦細锛堬級[]{}銆愩€?"''锛?锛?绛夌瓑路锛?锛?
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\-,锛宂', '', name)
        
        # 娓呯悊澶氫綑鐨勯€楀彿鍜岃繛瀛楃
        cleaned = re.sub(r'[-,锛宂+$', '', cleaned)  # 鍘婚櫎鏈熬鐨勮繛瀛楃鍜岄€楀彿
        cleaned = re.sub(r'^[-,锛宂+', '', cleaned)  # 鍘婚櫎寮€澶寸殑杩炲瓧绗﹀拰閫楀彿
        cleaned = re.sub(r'[-]{2,}', '-', cleaned)  # 澶氫釜杩炲瓧绗﹀悎骞朵负涓€涓?
        cleaned = re.sub(r'[,锛宂{2,}', ',', cleaned)  # 澶氫釜閫楀彿鍚堝苟涓轰竴涓?
        
        return cleaned.strip()

    def extract_upstream_materials(self, source_text: str, known_chemicals: Set[str], target_chemical: str) -> Set[str]:
        """浠庣敓浜ф潵婧愭枃鏈腑鎻愬彇涓婃父鍘熸枡鍖栧鍝?- 澧炲己鐨勬彁鍙栭€昏緫锛屽寘鍚悕绉版竻鐞?""
        upstream_materials = set()
        
        # 鍏堟帓闄ょ敓浜у晢淇℃伅
        cleaned_text = self.remove_producer_info(source_text)
        
        # 鑾峰彇鎵€鏈夊凡鐭ュ寲瀛﹀搧鍚嶇О - 浣跨敤缂撳瓨鐗堟湰锛岄伩鍏嶉噸澶嶆枃浠跺姞杞?
        all_chemicals = self.get_all_chemical_names_from_data()
        all_chemicals.update(known_chemicals)
        
        # 鏂规硶1: 閽堝甯歌鏍煎紡妯″紡鎻愬彇锛屽鍔犳洿澶氭ā寮?
        upstream_patterns = [
            r'涓婃父鍘熸枡[锛?]?\s*鍖呮嫭\s*([^銆傦紱;]+)',
            r'鍘熸枡(?:鍖呮嫭)?[锛?]\s*([^銆傦紱;]+)',
            r'浠s*([^涓篯+?)\s*涓哄師鏂?,
            r'鐢盶s*([^鍒禲+?)\s*(?:鍒跺緱|鍒舵垚|鍒跺)',
            r'浠嶾s*([^涓璢+?)\s*涓??:鎻愬彇|鍒嗙|寰楀埌)',
            r'([^銆傦紱;锛?]+?)\s*(?:瑁傝В|姘у寲|鍒嗚В)(?:寰楀埌|鍒跺緱|浜х敓)',
            # 鏂板妯″紡锛氭洿濂藉湴鍖归厤鍖栧鍝佸垪琛?
            r'鍘熸枡.*?鍖呮嫭\s*([^銆傦紱;]+)',
            r'涓昏鍘熸枡[锛?]?\s*([^銆傦紱;]+)',
            r'鎵€闇€鍘熸枡[锛?]?\s*([^銆傦紱;]+)',
            r'浠s*([^銆傦紱;锛?]*(?:銆乕^銆傦紱;锛?]*)*)\s*(?:绛塡s*)?(?:鍖栧鍝亅鍘熸枡|鐗╄川)',
            r'浣跨敤\s*([^銆傦紱;锛?]*(?:銆乕^銆傦紱;锛?]*)*)\s*(?:绛塡s*)?(?:浣滀负|涓?\s*鍘熸枡',
            r'浠嶾s*([^銆傦紱;锛?]*(?:銆乕^銆傦紱;锛?]*)*)\s*(?:绛塡s*)?涓?,
        ]
        
        for pattern in upstream_patterns:
            matches = re.findall(pattern, cleaned_text, re.IGNORECASE)
            for match in matches:
                # 鏇存櫤鑳界殑鍒嗗壊锛氬鐞?銆?銆?鍜?銆?涓?銆?,"绛夊垎闅旂
                chemicals_in_match = re.split(r'[,锛屻€?锛涘拰涓庡強鎴栫瓑]+', match.strip())
                
                for chem_candidate in chemicals_in_match:
                    clean_name = chem_candidate.strip()
                    # 绉婚櫎甯歌鍓嶇紑鍜屽悗缂€
                    clean_name = re.sub(r'^(?:閫氳繃|缁忚繃|鍒╃敤|浣跨敤|涓昏|浠巪鐢?\s*', '', clean_name)
                    clean_name = re.sub(r'\s*(?:绛墊绫粅鍙嶅簲|鍒跺緱|鍒跺|鍒舵垚|瑁傝В|姘у寲|鍒嗚В|鍖栧鍝亅鍘熸枡|鐗╄川).*$', '', clean_name)
                    clean_name = clean_name.strip()
                    
                    # 瀵瑰寲瀛﹀搧鍚嶇О杩涜娓呯悊锛堝幓闄ら櫎浜?鍜?浠ュ鐨勭鍙凤級
                    clean_name = self.clean_chemical_name_for_neo4j(clean_name)
                    
                    # 妫€鏌ユ槸鍚︿负鏈夋晥鍖栧鍝佸悕绉?
                    if (clean_name and 
                        len(clean_name) >= 2 and 
                        clean_name != target_chemical and
                        not re.search(r'[鍒跺緱鍒跺鍒舵垚鍙嶅簲瑁傝В姘у寲鍒嗚В]', clean_name)):  # 鎺掗櫎鍖呭惈鍔ㄤ綔璇嶇殑鐗囨
                        
                        if clean_name in all_chemicals:
                            # 宸茬煡鍖栧鍝侊紝鐩存帴娣诲姞
                            upstream_materials.add(clean_name)
                            logger.debug(f"鍙戠幇宸茬煡涓婃父鍖栧鍝? {clean_name}")
                        else:
                            # 楠岃瘉鏄惁涓烘湁鏁堢殑鍖栧鍝佸悕绉?
                            if self.is_valid_chemical_name(clean_name):
                                # 鏂板彂鐜扮殑鍖栧鍝侊紝娣诲姞鍒版暟鎹簱
                                self.add_new_chemical_to_database(clean_name)
                                upstream_materials.add(clean_name)
                                logger.info(f"鍙戠幇骞舵坊鍔犳柊涓婃父鍖栧鍝? {clean_name}")
        
        # 鏂规硶3: 鐩存帴鎵弿宸茬煡鍖栧鍝佸悕绉帮紙閽堝鎬у尮閰嶏級
        # 鎸夐暱搴︽帓搴忥紝浼樺厛鍖归厤闀垮悕绉?
        sorted_chemicals = sorted(all_chemicals, key=len, reverse=True)
        for chemical in sorted_chemicals:
            if chemical != target_chemical and len(chemical) >= 2 and chemical in cleaned_text:
                # 纭繚鍖归厤鍒扮殑鏄畬鏁磋瘝姹囷紝涓嶆槸鍏朵粬璇嶇殑涓€閮ㄥ垎
                pattern = r'(?:^|[^a-zA-Z\u4e00-\u9fa5])' + re.escape(chemical) + r'(?:[^a-zA-Z\u4e00-\u9fa5]|$)'
                if re.search(pattern, cleaned_text):
                    # 瀵瑰尮閰嶅埌鐨勫寲瀛﹀搧鍚嶇О杩涜娓呯悊
                    clean_chemical = self.clean_chemical_name_for_neo4j(chemical)
                    if clean_chemical:
                        upstream_materials.add(clean_chemical)
                        logger.debug(f"閫氳繃鎵弿鍙戠幇涓婃父鍖栧鍝? {clean_chemical}")
                        
                        # 闄愬埗缁撴灉鏁伴噺锛岄伩鍏嶈繃搴﹀尮閰?
                        if len(upstream_materials) >= 5:
                            break
                
        logger.info(f"涓?{target_chemical} 鎻愬彇鍒颁笂娓稿師鏂? {upstream_materials}")
        return upstream_materials

    def remove_producer_info(self, text: str) -> str:
        """绉婚櫎鐢熶骇鍟嗕俊鎭?- 澧炲己鐗堬紝鍖呮嫭鐢熷巶鍟嗗拰鐢熶骇鍟嗗悗闈㈢殑鎵€鏈夋枃瀛?""
        if not text or text == 'nan' or pd.isna(text):
            return ""
        
        text = str(text)  # 纭繚鏄瓧绗︿覆
        
        # 鐢熶骇鍟嗕俊鎭瘑鍒ā寮?- 鏇村叏闈㈢殑鍖归厤
        producer_patterns = [
            # 鍖归厤"鐢熶骇鍟?銆?鐢熷巶鍟?鍙婂叾鍚庣殑鎵€鏈夊唴瀹圭洿鍒板彞瀛愮粨鏉?
            r'鐢熶骇鍟?*?(?=[銆傦紱;锛?]|$)',
            r'鐢熷巶鍟?*?(?=[銆傦紱;锛?]|$)',
            r'涓昏鐢熶骇鍟?*?(?=[銆傦紱;锛?]|$)',
            r'鍒堕€犲晢.*?(?=[銆傦紱;锛?]|$)', 
            r'鐢熶骇浼佷笟.*?(?=[銆傦紱;锛?]|$)',
            r'鍒堕€犱紒涓?*?(?=[銆傦紱;锛?]|$)',
            # 鍖归厤鍖呭惈鍏徃銆佷紒涓氥€佸巶銆侀泦鍥㈢瓑鐨勬暣涓煭璇?
            r'[^銆傦紱;锛?]*(?:鍏徃|浼佷笟|鍘倈闆嗗洟|鏈夐檺璐ｄ换鍏徃|鑲′唤鏈夐檺鍏徃|宸ュ巶|鍒堕€犲巶)[^銆傦紱;锛?]*(?:[銆傦紱;锛?]|$)',
            # 鍖归厤"涓昏鐢熶骇鍟嗗寘鎷?鏈?杩欑妯″紡鍚庣殑鎵€鏈夊唴瀹?
            r'涓昏鐢熶骇鍟哰鏈夊寘鎷細:].+?(?=[銆傦紱;]|$)',
            r'鐢熶骇鍟哰鏈夊寘鎷細:].+?(?=[銆傦紱;]|$)',
            r'鍒堕€犲晢[鏈夊寘鎷細:].+?(?=[銆傦紱;]|$)',
        ]
        
        cleaned_text = text
        for pattern in producer_patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # 鍙竻鐞嗗浣欑殑鏍囩偣绗﹀彿锛屼繚鐣欏垪琛ㄥ垎闅旂锛堥€楀彿锛夊拰瀛楁鍒嗛殧绗︼紙鍐掑彿锛?
        # 绉婚櫎杩炵画鐨勬爣鐐圭鍙凤紝浣嗕繚鐣欏崟涓€楀彿鍜屽啋鍙?
        cleaned_text = re.sub(r'[锛?]+', '', cleaned_text)  # 绉婚櫎鍒嗗彿
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)   # 鏍囧噯鍖栫┖鏍?
        
    
    def extract_braced_content(self, text: str) -> List[str]:
        """鎻愬彇鑺辨嫭鍙穥}鍐呯殑鍐呭骞舵寜绗﹀彿鍒嗛殧"""
        if not text:
            return []
        
        # 鎻愬彇鎵€鏈夎姳鎷彿鍐呯殑鍐呭
        braced_pattern = r'\{([^}]+)\}'
        matches = re.findall(braced_pattern, text)
        
        result = []
        for match in matches:
            # 鎸夊父瑙佸垎闅旂鍒嗗壊鍐呭锛屼絾瑕佽€冭檻鐗规畩鎯呭喌
            # 棣栧厛鎸変富瑕佸垎闅旂鍒嗗壊
            main_separators = [';', '锛?, '銆?]
            items = [match]
            
            # 浣跨敤涓昏鍒嗛殧绗﹁繘琛屽垎鍓?
            for sep in main_separators:
                new_items = []
                for item in items:
                    new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                items = new_items
            
            # 濡傛灉娌℃湁涓昏鍒嗛殧绗︼紝鍐嶅皾璇曟瑕佸垎闅旂
            if len(items) == 1:
                secondary_separators = [',', '锛?]
                for sep in secondary_separators:
                    new_items = []
                    for item in items:
                        # 瀵逛簬鍖呭惈"GB/T"銆?ISO"绛夋爣鍑嗗彿鐨勬儏鍐碉紝瑕佸皬蹇冨鐞?
                        if any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                            # 濡傛灉鍖呭惈鏍囧噯鍙凤紝鍙湪娌℃湁鏍囧噯鍙锋牸寮忕殑閫楀彿澶勫垎鍓?
                            parts = item.split(sep)
                            temp_result = []
                            i = 0
                            while i < len(parts):
                                current = parts[i].strip()
                                # 妫€鏌ヤ笅涓€閮ㄥ垎鏄惁鏄爣鍑嗗彿鐨勪竴閮ㄥ垎
                                if (i + 1 < len(parts) and 
                                    any(std in current for std in ['GB', 'ISO', 'ASTM', 'JIS', 'DIN']) and
                                    re.match(r'^\s*[T\s]*\d+', parts[i + 1])):
                                    # 鍚堝苟鏍囧噯鍙烽儴鍒?
                                    current = current + sep + parts[i + 1].strip()
                                    i += 1
                                if current:
                                    temp_result.append(current)
                                i += 1
                            new_items.extend(temp_result)
                        else:
                            new_items.extend([i.strip() for i in item.split(sep) if i.strip()])
                    items = new_items
            
            # 杩涗竴姝ュ鐞嗗叾浠栧垎闅旂锛堜絾瑕佹洿璋ㄦ厧锛?
            final_items = []
            for item in items:
                # 鍙涓嶅寘鍚爣鍑嗗彿鐨勯」鐩娇鐢ㄥ叾浠栧垎闅旂
                if not any(std in item for std in ['GB/T', 'ISO', 'ASTM', 'JIS', 'DIN']):
                    other_separators = ['|', '/', '\\', '&', '+']
                    temp_items = [item]
                    for sep in other_separators:
                        new_temp_items = []
                        for temp_item in temp_items:
                            new_temp_items.extend([i.strip() for i in temp_item.split(sep) if i.strip()])
                        temp_items = new_temp_items
                    final_items.extend(temp_items)
                else:
                    final_items.append(item)
            
            # 杩囨护鎺夌┖鍊煎拰杩囩煭鐨勫唴瀹?
            valid_items = [item for item in final_items if len(item.strip()) > 1]
            result.extend(valid_items)
        
        # 鍘婚噸骞朵繚鎸侀『搴?
        seen = set()
        unique_result = []
        for item in result:
            if item not in seen:
                seen.add(item)
                unique_result.append(item)
        
        return unique_result

    def process_data_source_field(self, text: str) -> str:
        """澶勭悊鏁版嵁鏉ユ簮瀛楁锛屾彁鍙栬姳鎷彿鍐呭鎴栨爣鏄庢湰鍦版悳绱㈡潵婧?""
        if not text or text == 'nan' or pd.isna(text):
            return "鏍规嵁閫氫箟澶фā鍨嬭捀棣忚€屾潵"
        
        text = str(text)  # 纭繚鏄瓧绗︿覆
        
        # 鎻愬彇鑺辨嫭鍙峰唴瀹?
        braced_content = self.extract_braced_content(text)
        
        if braced_content:
            # 灏嗘彁鍙栫殑鍐呭鐢ㄥ垎鍙疯繛鎺?
            return '; '.join(braced_content)
        else:
            # 濡傛灉娌℃湁鑺辨嫭鍙峰唴瀹癸紝璇存槑鏄湰鍦版悳绱㈡潵婧?
            return "鏍规嵁閫氫箟澶фā鍨嬭捀棣忚€屾潵"

    def clean_chemical_name_advanced(self, name: str) -> str:
        """楂樼骇鍖栧鍝佸悕绉版竻鐞?""
        if not name:
            return ""
        
        # 绉婚櫎甯歌鐨勪腑鏂囧～鍏呰瘝姹?
        exclude_words = {
            '鍖呮嫭', '绛?, '绫?, '涓?, '鐨?, '鍜?, '涓?, '鍙?, '鎴?, '鍚勭', '澶氱', 
            '涓昏', '骞挎硾', '甯歌', '涓€鑸?, '閫氬父', '鐗瑰埆', '灏ゅ叾', '鐗规畩',
            '浠ュ強', '杩樻湁', '鍙﹀', '姝ゅ', '鍚屾椂', '鍙?, '鍏朵粬', '閮ㄥ垎',
            '濡?, '渚嬪', '姣斿', '璇稿', '鍍?, '璀', '濂芥瘮', '濡傚悓',
            '绛夌瓑', '涔嬬被', '宸﹀彸', '涓婁笅', '绾?, '澶х害', '鎺ヨ繎', '宸笉澶?,
            '鎷?, '锛?, '锛?, '(', ')', '[', ']', '銆?, '銆?
        }
        
        clean_name = name.strip()
        
        # 绉婚櫎鎷彿鍙婂叾鍐呭
        clean_name = re.sub(r'[锛?][^锛?]*[锛?]', '', clean_name)
        clean_name = re.sub(r'[銆怽[][^\銆慭]]*[\銆慭]]', '', clean_name)
        
        # 绉婚櫎濉厖璇嶆眹
        for word in exclude_words:
            clean_name = clean_name.replace(word, '')
        
        # 绉婚櫎鏁板瓧缂栧彿
        clean_name = re.sub(r'^\d+[\.銆乚\s*', '', clean_name)
        
        # 绉婚櫎澶氫綑鐨勭┖鏍煎拰鏍囩偣
        clean_name = re.sub(r'[锛?锛?锛?銆俓.]+', '', clean_name)
        clean_name = re.sub(r'\s+', '', clean_name)
        
        # 纭繚鏄湁鏁堢殑鍖栧鍝佸悕绉?
        if len(clean_name) < 2 or len(clean_name) > 50:
            return ""
        
        # 鎺掗櫎鏄庢樉涓嶆槸鍖栧鍝佺殑璇嶆眹
        non_chemical_patterns = [
            r'^[绛夌被涓殑鍜屼笌鍙婃垨]+$',
            r'^[\d\s\.,锛屻€傦紱;锛?]+$',
            r'^[鐢ㄤ簬浣滀负閫氳繃缁忚繃]+',
        ]
        
        for pattern in non_chemical_patterns:
            if re.match(pattern, clean_name):
                return ""
        
        return clean_name

    def clean_chemical_name(self, name: str) -> str:
        """娓呯悊鍜屾爣鍑嗗寲鍖栧鍝佸悕绉?""
        if not name:
            return ""
        
        # 绉婚櫎鎷彿鍐呭鍜岀壒娈婄鍙?
        clean_name = re.sub(r'[()锛堬級銆愩€慭[\]<>銆娿€媇.*?[()锛堬級銆愩€慭[\]<>銆娿€媇', '', name)
        clean_name = re.sub(r'[()锛堬級銆愩€慭[\]<>銆娿€媇', '', clean_name)
        
        # 绉婚櫎鑻辨枃鎻忚堪
        clean_name = re.sub(r'[A-Za-z\s]+', '', clean_name)
        
        # 绉婚櫎鏁板瓧鍜岀壒娈婄鍙凤紙淇濈暀鍖栧鍝佸悕绉颁腑鐨勫父瑙佸瓧绗︼級
        clean_name = re.sub(r'[0-9%锛匼-\+\=\*\#\@\$\&\^\~\`\|\\\"\']', '', clean_name)
        
        # 鍙繚鐣欎腑鏂囧瓧绗﹀拰灏戞暟鐗规畩鍖栧绗﹀彿
        clean_name = re.sub(r'[^\u4e00-\u9fa5路]', '', clean_name)
        
        # 绉婚櫎杩囩煭鎴栬繃闀跨殑鍚嶇О
        clean_name = clean_name.strip()
        if len(clean_name) < 2 or len(clean_name) > 20:
            return ""
        
        # 绉婚櫎鏄庢樉涓嶆槸鍖栧鍝佺殑璇嶆眹
        exclude_words = {
            '绛?, '绫?, '涓?, '涓?, '涓?, '鍓?, '鍚?, '宸?, '鍙?, '鍐?, '澶?, '楂?, '浣?, 
            '澶?, '灏?, '澶?, '灏?, '濂?, '鍧?, '鏂?, '鏃?, '蹇?, '鎱?, '鐑?, '鍐?,
            '宸ヤ笟', '鍐滀笟', '鍖昏嵂', '椋熷搧', '鍖栧伐', '鐭虫补', '鐓ょ偔', '澶╃劧姘?,
            '鐢熶骇', '鍒堕€?, '鍔犲伐', '澶勭悊', '浣跨敤', '搴旂敤', '鏂规硶', '鎶€鏈?, '宸ヨ壓'
        }
        
        if clean_name in exclude_words:
            return ""
        
        return clean_name
        
    def validate_chemical_name(self, name: str, known_chemicals: Set[str]) -> bool:
        """楠岃瘉鏄惁涓烘湁鏁堢殑鍖栧鍝佸悕绉?""
        if not name or len(name) < 2:
            return False
            
        # 绮剧‘鍖归厤宸茬煡鍖栧鍝?
        if name in known_chemicals:
            return True
            
        # 妯＄硦鍖归厤锛氭鏌ユ槸鍚﹀寘鍚湪宸茬煡鍖栧鍝佷腑
        for known in known_chemicals:
            if name in known or known in name:
                if abs(len(name) - len(known)) <= 2:  # 闀垮害宸紓涓嶅ぇ
                    return True
        
        # 鍩轰簬鍖栧鍝佸懡鍚嶈寰嬬殑楠岃瘉
        chemical_suffixes = ['閰?, '纰?, '鐩?, '閱?, '閱?, '閰?, '閰?, '閱?, '鐑?, '鐑?, '鐐?, '鑻?, '閰?]
        chemical_prefixes = ['鐢?, '涔?, '涓?, '涓?, '鎴?, '宸?, '搴?, '杈?, '澹?, '鐧?]
        
        # 妫€鏌ユ槸鍚︾鍚堝寲瀛﹀搧鍛藉悕妯″紡
        has_chemical_pattern = (
            any(name.endswith(suffix) for suffix in chemical_suffixes) or
            any(name.startswith(prefix) for prefix in chemical_prefixes) or
            '姘? in name or '姘? in name or '姘? in name or '姘? in name or '纭? in name or
            '閽? in name or '閽? in name or '閽? in name or '闀? in name or '閾? in name or
            '閾? in name or '閿? in name or '閾? in name or '姹? in name or '閾? in name
        )
        
        return has_chemical_pattern
        
    def determine_relationship_type(self, text: str, source: str, target: str) -> str:
        """鏍规嵁涓婁笅鏂囩‘瀹氬叧绯荤被鍨?""
        text_lower = text.lower()
        
        if any(keyword in text_lower for keyword in ['涓婃父', '鍘熸枡', '鍒跺', '鍒堕€?, '鐢熶骇']):
            return '宸ヨ壓'
        elif any(keyword in text_lower for keyword in ['鍙嶅簲', '鍖栧悎', '鍒嗚В', '姘у寲', '杩樺師']):
            return '鍖栧鍙嶅簲'
        elif any(keyword in text_lower for keyword in ['鐢ㄩ€?, '搴旂敤', '鍒跺彇']):
            return '鐢ㄩ€?
        elif any(keyword in text_lower for keyword in ['杞寲', '鍙樻垚', '寰楀埌']):
            return '杞寲'
            return '宸ヨ壓'  # 榛樿鍏崇郴绫诲瀷

    def create_standard_legal_relationships(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """涓烘瘡涓寲瀛﹀搧鍒涘缓鏍囧噯/娉曞緥鍏崇郴"""
        logger.info("寮€濮嬪垱寤烘爣鍑?娉曞緥鍏崇郴...")
        
        if nodes_df.empty:
            logger.info("娌℃湁鑺傜偣鏁版嵁锛岃烦杩囨爣鍑?娉曞緥鍏崇郴鍒涘缓")
            return pd.DataFrame()
        
        # 灏濊瘯瀵煎叆鏍囧噯/娉曞緥绠＄悊鍣?
        try:
            import sys
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            
            # 浣跨敤鏍囧噯/娉曞緥绠＄悊鍣?
            manager = StandardLegalManager(str(self.base_path))
            _, relationships = manager.update_standard_legal_relationships(nodes_df)
            
            # 妫€鏌ユ槸鍚︽湁鏈夋晥鐨勬爣鍑嗘硶寰嬫暟鎹?
            if not relationships.empty and len(relationships) > 0:
                # 妫€鏌ョ涓€琛屾暟鎹槸鍚︿负鍗犱綅绗?
                first_row = relationships.iloc[0]
                if ':END_ID' in first_row and str(first_row[':END_ID']).startswith('鏍囧噯娉曞緥_'):
                    logger.info("妫€娴嬪埌鍗犱綅绗︽爣鍑嗘硶寰嬫暟鎹紝璺宠繃鍒涘缓鏃犳剰涔夌殑鍏崇郴")
                    return pd.DataFrame()
                else:
                    logger.info(f"浣跨敤鏍囧噯/娉曞緥绠＄悊鍣ㄥ垱寤轰簡 {len(relationships)} 鏉℃爣鍑?娉曞緥鍏崇郴")
                    return relationships
            else:
                logger.info("娌℃湁鏈夋晥鐨勬爣鍑?娉曞緥鏁版嵁锛岃烦杩囧叧绯诲垱寤?)
                return pd.DataFrame()
                
        except ImportError:
            logger.warning("鏃犳硶瀵煎叆鏍囧噯/娉曞緥绠＄悊鍣紝璺宠繃鏍囧噯/娉曞緥鍏崇郴鍒涘缓")
            # 涓嶅啀鍒涘缓鍗犱綅绗﹀叧绯伙紝鐩存帴杩斿洖绌篋ataFrame
            logger.info("璺宠繃鍒涘缓鍗犱綅绗︽爣鍑?娉曞緥鍏崇郴")
            return pd.DataFrame()

    def create_standard_legal_nodes(self, nodes_df: pd.DataFrame) -> pd.DataFrame:
        """鍒涘缓鏍囧噯/娉曞緥鑺傜偣"""
        logger.info("寮€濮嬪垱寤烘爣鍑?娉曞緥鑺傜偣...")
        
        if nodes_df.empty:
            logger.info("娌℃湁鍖栧鍝佽妭鐐规暟鎹紝璺宠繃鏍囧噯/娉曞緥鑺傜偣鍒涘缓")
            return pd.DataFrame()
        
        # 灏濊瘯浣跨敤鏍囧噯/娉曞緥绠＄悊鍣?
        try:
            import sys
            sys.path.append(str(self.base_path / "chemical-data-processor"))
            from standard_legal_manager import StandardLegalManager
            
            # 浣跨敤鏍囧噯/娉曞緥绠＄悊鍣?
            manager = StandardLegalManager(str(self.base_path))
            nodes, _ = manager.update_standard_legal_relationships(nodes_df)
            
            # 妫€鏌ユ槸鍚︽湁鏈夋晥鐨勬爣鍑嗘硶寰嬫暟鎹?
            if not nodes.empty and len(nodes) > 0:
                # 妫€鏌ョ涓€琛屾暟鎹槸鍚︿负鍗犱綅绗?
                first_row = nodes.iloc[0]
                if 'name:ID' in first_row and str(first_row['name:ID']).startswith('鏍囧噯娉曞緥_'):
                    logger.info("妫€娴嬪埌鍗犱綅绗︽爣鍑嗘硶寰嬭妭鐐癸紝璺宠繃鍒涘缓鏃犳剰涔夌殑鑺傜偣")
                    return pd.DataFrame()
                else:
                    logger.info(f"浣跨敤鏍囧噯/娉曞緥绠＄悊鍣ㄥ垱寤轰簡 {len(nodes)} 涓爣鍑?娉曞緥鑺傜偣")
                    return nodes
            else:
                logger.info("娌℃湁鏈夋晥鐨勬爣鍑?娉曞緥鏁版嵁锛岃烦杩囪妭鐐瑰垱寤?)
                return pd.DataFrame()
                
        except ImportError:
            logger.warning("鏃犳硶瀵煎叆鏍囧噯/娉曞緥绠＄悊鍣紝璺宠繃鏍囧噯/娉曞緥鑺傜偣鍒涘缓")
            # 涓嶅啀鍒涘缓鍗犱綅绗﹁妭鐐癸紝鐩存帴杩斿洖绌篋ataFrame
            logger.info("璺宠繃鍒涘缓鍗犱綅绗︽爣鍑?娉曞緥鑺傜偣")
            return pd.DataFrame()

    def save_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame, include_standard_legal: bool = True):
        """淇濆瓨Neo4j鏍煎紡鐨勬暟鎹?""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 鍒涘缓鏍囧噯/娉曞緥鑺傜偣鍜屽叧绯?
        if include_standard_legal and not nodes_df.empty:
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            
            # 鍚堝苟鑺傜偣鏁版嵁
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:
                combined_nodes = nodes_df
                
            # 鍚堝苟鍏崇郴鏁版嵁
            if not standard_legal_relationships.empty:
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:
                combined_relationships = relationships_df
        else:
            combined_nodes = nodes_df
            combined_relationships = relationships_df

        # 淇濆瓨鑺傜偣鏁版嵁锛堝垎鎵癸級
        if not combined_nodes.empty:
            batch_size = 3000
            num_batches = (len(combined_nodes) + batch_size - 1) // batch_size
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(combined_nodes))
                batch_data = combined_nodes.iloc[start_idx:end_idx]
                
                filename = f"neo4j_ready_chemicals_batch_{i+1}_{timestamp}.csv"
                filepath = self.success_dir / filename
                batch_data.to_csv(filepath, index=False, encoding='utf-8-sig')
                logger.info(f"淇濆瓨Neo4j鑺傜偣鎵规 {i+1}: {filename}")

        # 淇濆瓨鍏崇郴鏁版嵁
        if not combined_relationships.empty:
            rel_filename = f"neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.success_dir / rel_filename
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"淇濆瓨Neo4j鍏崇郴鏁版嵁: {rel_filename}")
            
        # 缁熻淇℃伅
        chemical_count = len(nodes_df) if not nodes_df.empty else 0
        process_count = 0  # 鐢变簬鍘婚櫎浜嗕骇涓氬姛鑳斤紝宸ヨ壓鑺傜偣缁熻涓?
        relationship_count = len(relationships_df) if not relationships_df.empty else 0
        
        logger.info(f"鏁版嵁淇濆瓨瀹屾垚:")
        logger.info(f"  鍖栧鍝佽妭鐐? {chemical_count} 涓?)
        logger.info(f"  宸ヨ壓鑺傜偣: {process_count} 涓?)
        logger.info(f"  鍏崇郴: {relationship_count} 鏉?)
        
        if include_standard_legal:
            standard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
            standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
            if standard_count > 0 or standard_rel_count > 0:
                logger.info(f"  鏍囧噯/娉曞緥鑺傜偣: {standard_count} 涓?)
                logger.info(f"  鏍囧噯/娉曞緥鍏崇郴: {standard_rel_count} 鏉?)

    def process_files(self, incremental_mode: bool = False):
        """闃舵涓€锛氭暟鎹悎骞朵笌棰勫鐞嗭紙鏀寔澧為噺鏇存柊锛?""
        logger.info("=" * 50)
        if incremental_mode:
            logger.info("寮€濮嬮樁娈典竴锛氬閲忔暟鎹鐞?)
        else:
            logger.info("寮€濮嬮樁娈典竴锛氬畬鏁存暟鎹悎骞朵笌棰勫鐞?)
        
        # 1. 鍔犺浇鍗卞寲鍝佺洰褰?
        self.load_dangerous_chemicals()
        
        # 2. 鏍规嵁妯″紡閫夋嫨鏂囦欢
        if incremental_mode:
            new_files = self.get_new_files()
            if not new_files:
                logger.info("娌℃湁鏂版枃浠堕渶瑕佸鐞?)
                return
            logger.info(f"澧為噺妯″紡锛氬鐞?{len(new_files)} 涓柊鏂囦欢")
            
            # 鎸夋枃浠跺鐞嗗閲忔暟鎹?
            files_by_batch = {}
            for file_path in new_files:
                batch_num = self.extract_batch_number(file_path.name)
                if batch_num is not None:
                    if batch_num not in files_by_batch:
                        files_by_batch[batch_num] = []
                    files_by_batch[batch_num].append(file_path)
        else:
            # 鎸夊簭鍙风粍缁囨墍鏈夋枃浠?
            files_by_batch = self.organize_files_by_batch()
            if not files_by_batch:
                logger.error("鏈壘鍒颁换浣昩atch鏂囦欢")
                return
        
        # 3. 鍚堝苟閲嶅鎵规鏂囦欢
        merged_batches = self.merge_duplicate_batches(files_by_batch)
        
        # 4. 妫€鏌ョ己澶辨壒娆★紙浠呭湪瀹屾暣妯″紡涓嬶級
        if not incremental_mode:
            missing_batches = self.check_missing_batches(merged_batches)
        
        # 5. 鍚堝苟鎵€鏈夋暟鎹?
        if merged_batches:
            all_data = pd.concat(merged_batches.values(), ignore_index=True)
            logger.info(f"鍚堝苟瀹屾垚锛屾€昏 {len(all_data)} 鏉¤褰?)
        else:
            logger.error("娌℃湁鎴愬姛鍚堝苟浠讳綍鏁版嵁")
            return

        # 6. 鏁版嵁楠岃瘉 - 鏂板涓ユ牸鐨勬暟鎹川閲忔鏌?
        logger.info("寮€濮嬫暟鎹獙璇?..")
        valid_data, validation_failed_data, validation_errors = self.validate_chemical_data(all_data)
        
        if validation_errors:
            logger.warning("鏁版嵁楠岃瘉鍙戠幇闂:")
            for error in validation_errors:
                logger.warning(f"  - {error}")

        # 7. 璇嗗埆澶勭悊澶辫触璁板綍
        success_data, processing_failed_data = self.identify_failed_records(valid_data)

        # 8. 妫€鏌ョ己澶辩殑鍗卞寲鍝侊紙浠呭湪瀹屾暣妯″紡涓嬶級
        if not incremental_mode:
            missing_dangerous = self.find_missing_dangerous_chemicals(success_data)
        else:
            missing_dangerous = pd.DataFrame()

        # 9. 鍚堝苟鎵€鏈夊け璐ユ暟鎹?
        all_failed_data = []
        if len(validation_failed_data) > 0:
            all_failed_data.append(validation_failed_data)
        if len(processing_failed_data) > 0:
            all_failed_data.append(processing_failed_data)

        # 10. 淇濆瓨澶辫触鏁版嵁
        combined_failed = pd.concat(all_failed_data, ignore_index=True) if all_failed_data else pd.DataFrame()
        self.save_failed_data(combined_failed, missing_batches, missing_dangerous)

        
        if not success_data.empty:
            if incremental_mode:
                # 澧為噺妯″紡锛氫繚瀛樺埌涓撻棬鐨勫閲忕洰褰?
                self.save_incremental_data(success_data)
            else:
                # 瀹屾暣妯″紡锛氭甯镐繚瀛?
                self.save_batch_data(success_data)
                
            # 鏇存柊宸插鐞嗘枃浠惰褰?
            if incremental_mode:
                self.update_processed_files_record(new_files)
                
            logger.info(f"闃舵涓€瀹屾垚锛屾垚鍔熷鐞?{len(success_data)} 鏉¤褰?)
        else:
            logger.error("闃舵涓€锛氭病鏈夋垚鍔熷鐞嗙殑鏁版嵁")

    def save_incremental_data(self, data: pd.DataFrame):
        """淇濆瓨澧為噺鏇存柊鏁版嵁"""
        if len(data) == 0:
            logger.warning("娌℃湁澧為噺鏁版嵁闇€瑕佷繚瀛?)
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"incremental_update_{timestamp}.csv"
        filepath = self.incremental_dir / filename
        data.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"淇濆瓨澧為噺鏁版嵁: {filename}, 鍏眥len(data)}鏉¤褰?)

    def update_processed_files_record(self, files: List[Path]):
        """鏇存柊宸插鐞嗘枃浠惰褰?""
        record = self.load_processed_files_record()
        
        for file_path in files:
            fingerprint = self.calculate_file_fingerprint(file_path)
            file_key = str(file_path.relative_to(self.base_path))
            record[file_key] = fingerprint
            
        self.save_processed_files_record(record)
        logger.info(f"鏇存柊浜?{len(files)} 涓枃浠剁殑澶勭悊璁板綍")

    def prepare_for_neo4j(self, incremental_mode: bool = False):
        """闃舵浜岋細鍥炬暟鎹牸寮忓寲涓庡叧绯绘彁鍙栵紙鏀寔澧為噺妯″紡锛?""
        logger.info("=" * 50)
        if incremental_mode:
            logger.info("寮€濮嬮樁娈典簩锛氬閲忔暟鎹甆eo4j鏍煎紡鍖栦笌鍏崇郴鎻愬彇")
        else:
            logger.info("寮€濮嬮樁娈典簩锛氬浘鏁版嵁鏍煎紡鍖栦笌鍏崇郴鎻愬彇")
        logger.info("=" * 50)

        # 鏍规嵁妯″紡閫夋嫨鏁版嵁婧?
        if incremental_mode:
