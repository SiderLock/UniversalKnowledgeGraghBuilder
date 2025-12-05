#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用数据清洗处理工具

功能：
- 读取待处理实体数据
- 与参考数据清单进行比对
- 添加额外属性信息
- 输出处理后的数据文件
"""

import pandas as pd
import os
import re
import time
import csv
from pathlib import Path
import chardet

# 配置常量
DEFAULT_ENCODINGS = ['utf-8-sig', 'utf-8', 'gbk', 'gb2312', 'cp1252', 'ansi', 'cp936', 'latin1']
OUTPUT_ENCODING = 'utf-8-sig'
SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls']

def detect_file_encoding(file_path):
    """
    自动检测文件编码
    
    Args:
        file_path (str): 文件路径
    
    Returns:
        str: 检测到的编码
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # 读取前10KB用于检测
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            
            # 处理ANSI编码的特殊情况
            if encoding and encoding.lower() in ['ascii', 'windows-1252']:
                encoding = 'cp1252'  # Windows ANSI
            elif encoding and 'gb' in encoding.lower():
                encoding = 'gbk'  # 中文编码
            
            print(f"🔍 自动检测文件编码: {encoding} (置信度: {confidence:.2f})")
            return encoding
    except Exception as e:
        print(f"⚠️  编码检测失败: {e}")
        return None

def read_file_with_encoding(file_path, encodings=None):
    """
    尝试用多种编码读取CSV/Excel文件，支持处理包含换行符的字段
    
    Args:
        file_path (str): 文件路径
        encodings (list): 编码列表，默认使用DEFAULT_ENCODINGS
    
    Returns:
        tuple: (DataFrame, 使用的编码/文件类型)
    
    Raises:
        ValueError: 所有方法都失败时抛出异常
    """
    file_path = Path(file_path)
    file_extension = file_path.suffix.lower()
    
    # 处理Excel文件
    if file_extension in ['.xlsx', '.xls']:
        try:
            print(f"📖 正在读取Excel文件: {file_path.name}")
            if file_extension == '.xlsx':
                df = pd.read_excel(file_path, engine='openpyxl')
            else:
                df = pd.read_excel(file_path, engine='xlrd')
            
            print(f"✅ Excel文件读取成功: {file_path.name}")
            return df, f"Excel-{file_extension}"
        except Exception as e:
            print(f"❌ Excel文件读取失败: {e}")
            raise ValueError(f"无法读取Excel文件 {file_path}: {e}")
    
    # 处理CSV文件
    if encodings is None:
        encodings = DEFAULT_ENCODINGS.copy()
    
    # 首先尝试自动检测编码
    detected_encoding = detect_file_encoding(file_path)
    if detected_encoding and detected_encoding not in encodings:
        encodings.insert(0, detected_encoding)
    
    for encoding in encodings:
        try:
            # 特殊处理ANSI编码
            if encoding.lower() == 'ansi':
                encoding = 'cp1252'  # Windows ANSI通常是cp1252
            
            # 使用quoting=csv.QUOTE_ALL来正确处理包含换行符的字段
            df = pd.read_csv(
                file_path, 
                encoding=encoding,
                quoting=1,  # csv.QUOTE_ALL - 处理所有引号
                skipinitialspace=True,  # 跳过分隔符后的空格
                keep_default_na=False,  # 保持空值为空字符串而不是NaN
                na_filter=False,  # 不自动转换NA值
                on_bad_lines='skip'  # 跳过有问题的行
            )
            print(f"📖 文件 {file_path.name} 使用编码: {encoding}")
            return df, encoding
        except (UnicodeDecodeError, UnicodeError, FileNotFoundError, pd.errors.ParserError) as e:
            print(f"⚠️  编码 {encoding} 读取失败: {e}")
            continue
    
    raise ValueError(f"无法读取文件 {file_path}，已尝试编码: {encodings}")

# 保持向后兼容性的别名
read_csv_with_encoding = read_file_with_encoding

def generate_output_filename(input_filename):
    """
    根据输入文件名生成输出文件名
    
    Args:
        input_filename (str): 输入文件名
    
    Returns:
        str: 生成的输出文件名
    """
    # 获取不带扩展名的文件名
    name_without_ext = Path(input_filename).stem
    
    # 获取当前时间戳
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    
    # 生成新的文件名：原文件名_已处理_时间戳.csv
    output_filename = f"{name_without_ext}_已处理_{timestamp}.csv"
    
    return output_filename

def find_data_files(directory):
    """
    查找目录中的所有数据文件（CSV和Excel）
    
    Args:
        directory (Path): 目录路径
    
    Returns:
        list: 数据文件路径列表
    """
    data_files = []
    if directory.exists():
        for ext in SUPPORTED_EXTENSIONS:
            data_files.extend(list(directory.glob(f"*{ext}")))
            data_files.extend(list(directory.glob(f"*{ext.upper()}")))  # 支持大写扩展名
    return sorted(data_files)  # 排序确保处理顺序一致

# 保持向后兼容性的别名
find_csv_files = find_data_files

def merge_empty_rows(df, chinese_name_col, cas_col=None):
    """
    合并空行：如果某行的中文名和CAS号都为空，将其与上一行合并
    
    Args:
        df (pd.DataFrame): 输入数据框
        chinese_name_col (str): 中文名列名
        cas_col (str): CAS号列名（可选）
    
    Returns:
        pd.DataFrame: 处理后的数据框
    """
    if len(df) <= 1:
        return df
    
    print("🔧 正在检查并合并空行...")
    
    # 创建副本避免修改原始数据
    df_merged = df.copy()
    
    # 确保相关列为字符串类型
    df_merged[chinese_name_col] = df_merged[chinese_name_col].astype(str).fillna('').str.strip()
    if cas_col:
        df_merged[cas_col] = df_merged[cas_col].astype(str).fillna('').str.strip()
    
    # 标记需要删除的行
    rows_to_delete = []
    merge_count = 0
    
    for i in range(1, len(df_merged)):
        current_row = df_merged.iloc[i]
        prev_row = df_merged.iloc[i-1]
        
        # 检查当前行的中文名是否为空
        chinese_name_empty = (
            current_row[chinese_name_col] == '' or 
            current_row[chinese_name_col] == 'nan' or 
            pd.isna(current_row[chinese_name_col])
        )
        
        # 检查当前行的CAS号是否为空（如果有CAS列）
        cas_empty = True
        if cas_col:
            cas_empty = (
                current_row[cas_col] == '' or 
                current_row[cas_col] == 'nan' or 
                pd.isna(current_row[cas_col])
            )
        
        # 如果中文名和CAS号都为空，则合并到上一行
        if chinese_name_empty and cas_empty:
            merge_count += 1
            
            # 合并所有非空字段到上一行
            for col in df_merged.columns:
                current_value = str(current_row[col]).strip()
                prev_value = str(prev_row[col]).strip()
                
                # 如果当前行的值不为空且与上一行不同，则追加
                if (current_value != '' and 
                    current_value not in ['nan', 'None'] and 
                    not pd.isna(current_value)):
                    
                    if (prev_value == '' or 
                        prev_value in ['nan', 'None'] or 
                        pd.isna(prev_value)):
                        # 上一行为空，直接使用当前行的值
                        df_merged.iloc[i-1, df_merged.columns.get_loc(col)] = current_value
                    elif current_value != prev_value:
                        # 两行都有值且不同，用空格连接
                        merged_value = f"{prev_value} {current_value}"
                        df_merged.iloc[i-1, df_merged.columns.get_loc(col)] = merged_value
            
            # 标记当前行为删除
            rows_to_delete.append(i)
    
    # 删除已合并的空行
    if rows_to_delete:
        df_merged = df_merged.drop(df_merged.index[rows_to_delete]).reset_index(drop=True)
        print(f"✅ 已合并 {merge_count} 个空行，删除了 {len(rows_to_delete)} 行")
        print(f"📊 数据行数从 {len(df)} 减少到 {len(df_merged)}")
    else:
        print("📊 未发现需要合并的空行")
    
    return df_merged

def merge_empty_columns(df, data_name):
    """
    检查并合并空列名的列，根据语言类型（中文/英文）智能合并到最近的相同语言类型列，然后删除所有剩余的空列
    
    合并规则：
    1. 只将空列合并到已有定义的列（非空列名）
    2. 根据列内容的语言类型（中文/英文/混合）进行匹配
    3. 优先选择距离最近的相同语言类型列
    4. 只合并非重复的内容，避免覆盖已有数据
    
    Args:
        df (pd.DataFrame): 数据框
        data_name (str): 数据名称，用于日志输出
    
    Returns:
        pd.DataFrame: 处理后的数据框
    """
    original_cols = df.columns.tolist()
    empty_indices = []
    
    # 找到所有空列名的索引
    for i, col in enumerate(original_cols):
        col_str = str(col).strip().lower()
        if (pd.isna(col) or 
            col_str == '' or 
            col_str == 'nan' or 
            col_str.startswith('unnamed:') or
            col_str.startswith('unnamed ') or
            col_str == 'unnamed'):
            empty_indices.append(i)
    
    if not empty_indices:
        print(f"   ✅ {data_name} 未发现空列名")
        return df
    
    print(f"   🔍 {data_name} 发现 {len(empty_indices)} 个空列名，位置: {empty_indices}")
    
    # 强制删除所有空列（简化版本）
    cols_to_drop = []
    
    # 按索引降序删除，避免索引变化影响
    empty_indices.sort(reverse=True)
    for idx in empty_indices:
        col_name = original_cols[idx]
        df = df.drop(df.columns[idx], axis=1)
        cols_to_drop.append(f"列{idx}({col_name})")
    
    print(f"   🗑️  {data_name} 强制删除所有空列: {len(empty_indices)} 个")
    for col_info in cols_to_drop:
        print(f"      - 删除: {col_info}")
    print(f"   📊 最终列数: {len(df.columns)} (原: {len(original_cols)})")
    
    return df

def merge_alias_columns(df, data_name):
    """
    合并数据框中的别名列，但保留英文别名作为独立列
    
    规则：
    - 将系统生成的"别名"列合并到"中文别名"列中
    - 保留"英文别名"列作为独立列
    - 如果没有"中文别名"列，将"别名"列重命名为"中文别名"
    
    Args:
        df (pd.DataFrame): 数据框
        data_name (str): 数据名称，用于日志输出
    
    Returns:
        pd.DataFrame: 处理后的数据框
    """
    # 查找相关的别名列
    chinese_alias_col = None
    english_alias_col = None
    alias_col = None
    
    for col in df.columns:
        col_str = str(col).strip()
        if col_str == '中文别名':
            chinese_alias_col = col
        elif col_str == '英文别名':
            english_alias_col = col
        elif col_str == '别名':
            alias_col = col
    
    alias_cols_found = []
    if chinese_alias_col:
        alias_cols_found.append(f"中文别名: {chinese_alias_col}")
    if english_alias_col:
        alias_cols_found.append(f"英文别名: {english_alias_col}")
    if alias_col:
        alias_cols_found.append(f"别名: {alias_col}")
    
    if not alias_cols_found:
        print(f"   ℹ️  {data_name} 未发现任何别名列")
        return df
    
    print(f"   � {data_name} 发现别名列: {', '.join(alias_cols_found)}")
    
    # 处理别名列合并
    merge_operations = []
    
    # 情况1：如果同时存在"中文别名"和"别名"列，将"别名"合并到"中文别名"
    if chinese_alias_col and alias_col:
        print(f"   📋 {data_name} 将'别名'列合并到'中文别名'列")
        
        # 获取两列的内容
        chinese_content = df[chinese_alias_col].astype(str).fillna('').str.strip()
        alias_content = df[alias_col].astype(str).fillna('').str.strip()
        
        # 合并内容
        merged_content = []
        merge_count = 0
        
        for i in range(len(df)):
            chinese_val = chinese_content.iloc[i]
            alias_val = alias_content.iloc[i]
            
            # 清理空值
            if chinese_val.lower() in ['', 'nan', 'null']:
                chinese_val = ''
            if alias_val.lower() in ['', 'nan', 'null']:
                alias_val = ''
            
            # 合并逻辑
            if chinese_val == '' and alias_val == '':
                merged_content.append('')
            elif chinese_val == '':
                merged_content.append(alias_val)
                if alias_val != '':
                    merge_count += 1
            elif alias_val == '':
                merged_content.append(chinese_val)
            else:
                # 检查是否重复
                if alias_val not in chinese_val.split('；') and alias_val not in chinese_val.split(';'):
                    merged_content.append(chinese_val + '；' + alias_val)
                    merge_count += 1
                else:
                    merged_content.append(chinese_val)
        
        # 更新中文别名列
        df[chinese_alias_col] = merged_content
        
        # 删除别名列
        df = df.drop(columns=[alias_col])
        merge_operations.append(f"列'别名' -> 列'中文别名' [合并{merge_count}个值]")
        
    # 情况2：如果只有"别名"列而没有"中文别名"列，将"别名"列重命名为"中文别名"
    elif alias_col and not chinese_alias_col:
        print(f"   📋 {data_name} 将'别名'列重命名为'中文别名'")
        df = df.rename(columns={alias_col: '中文别名'})
        merge_operations.append("列'别名' -> 重命名为'中文别名'")
    
    # 输出操作结果
    if merge_operations:
        print(f"   ✅ {data_name} 别名列处理完成:")
        for op in merge_operations:
            print(f"      - {op}")
        
        # 显示最终的别名列结构
        final_alias_cols = []
        if '中文别名' in df.columns:
            final_alias_cols.append('中文别名')
        if english_alias_col and english_alias_col in df.columns:
            final_alias_cols.append('英文别名')
        
        if final_alias_cols:
            print(f"   📊 保留别名列: {', '.join(final_alias_cols)}")
    else:
        print(f"   ✅ {data_name} 别名列无需处理")
    
    return df

def process_data(input_file, hazardous_chemicals_file, output_file):
    """
    处理化学品数据，与国家危化品清单进行比对，并添加新列。

    Args:
        input_file (str): 待处理数据文件路径
        hazardous_chemicals_file (str): 国家危化品清单文件路径
        output_file (str): 处理后文件的保存路径
    
    Returns:
        bool: 处理是否成功
    """
    start_time = time.time()
    print("🚀 开始处理化学品数据...")
    
    try:
        # 读取文件
        print("📂 正在读取数据文件...")
        df_input, input_encoding = read_file_with_encoding(input_file)
        df_hazardous, hazardous_encoding = read_file_with_encoding(hazardous_chemicals_file)

        # 处理空列名和合并相同内容的列
        print("🔧 正在检查并处理空列名...")
        df_input = merge_empty_columns(df_input, "待处理数据")
        df_hazardous = merge_empty_columns(df_hazardous, "危化品清单")

        # 合并别名和中文别名列
        print("🔧 正在合并别名和中文别名列...")
        df_input = merge_alias_columns(df_input, "待处理数据")
        df_hazardous = merge_alias_columns(df_hazardous, "危化品清单")

        # 智能检测列名
        print("🔍 正在智能检测列名...")
        
        # 检测危化品清单的品名列
        product_name_col = detect_column_names(df_hazardous, 'product_name')
        if not product_name_col:
            raise ValueError(f"危化品清单文件中未找到品名相关列。可用列: {list(df_hazardous.columns)}")
        print(f"📋 危化品清单品名列: {product_name_col}")
        
        # 检测危化品清单的别名列
        hazard_alias_col = detect_column_names(df_hazardous, 'alias')
        if not hazard_alias_col:
            print("⚠️  危化品清单中未找到别名列，将使用品名作为别名")
            hazard_alias_col = product_name_col
        else:
            print(f"📋 危化品清单别名列: {hazard_alias_col}")
        
        # 检测待处理数据的中文名列
        chinese_name_col = detect_column_names(df_input, 'chinese_name')
        if not chinese_name_col:
            raise ValueError(f"待处理数据中未找到中文名相关列。可用列: {list(df_input.columns)}")
        print(f"📋 待处理数据中文名列: {chinese_name_col}")
        
        # 检测待处理数据的英文名列（可选）
        english_name_col = detect_column_names(df_input, 'english_name')
        if english_name_col:
            print(f"📋 待处理数据英文名列: {english_name_col}")
        else:
            print("📋 未检测到英文名列，将在中文名后直接插入别名列")

        # 检测CAS号列（用于辅助匹配）
        input_cas_col = detect_column_names(df_input, 'cas')
        hazard_cas_col = detect_column_names(df_hazardous, 'cas')
        
        if input_cas_col and hazard_cas_col:
            print(f"📋 待处理数据CAS号列: {input_cas_col}")
            print(f"📋 危化品清单CAS号列: {hazard_cas_col}")
            print("🔍 将使用中文名 + CAS号进行双重匹配")
        else:
            print("⚠️  CAS号列检测不完整，仅使用中文名进行匹配")

        # 显示数据框详细信息
        display_dataframe_info(df_input, "待处理数据", {
            "中文名列": chinese_name_col,
            "英文名列": english_name_col,
            "CAS号列": input_cas_col
        })
        
        display_dataframe_info(df_hazardous, "危化品清单", {
            "品名列": product_name_col,
            "别名列": hazard_alias_col,
            "CAS号列": hazard_cas_col
        })

        # 为了方便比对，我们先从危化品清单的"品名"中提取出化学品名称和浓度
        # 例如：从 "对䓝基化过氧氢[72%＜含量≤100%]" 提取 "对䓝基化过氧氢" 和 "[72%＜含量≤100%]"
        # 处理可能的编码问题和空值，以及包含换行符的字段
        print("正在处理危化品清单数据...")
        df_hazardous[product_name_col] = df_hazardous[product_name_col].astype(str).fillna('')
        
        # 统计包含换行符的记录数
        newline_count_before = df_hazardous[product_name_col].str.contains(r'[\n\r]', regex=True).sum()
        if newline_count_before > 0:
            print(f"🔧 发现 {newline_count_before} 条品名记录包含换行符，正在清理...")
        
        # 处理包含换行符的品名字段：将换行符替换为空格，然后清理多余空格
        df_hazardous[product_name_col] = df_hazardous[product_name_col].str.replace('\n', ' ', regex=False)
        df_hazardous[product_name_col] = df_hazardous[product_name_col].str.replace('\r', ' ', regex=False)
        df_hazardous[product_name_col] = df_hazardous[product_name_col].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # 同样处理别名字段
        if hazard_alias_col != product_name_col:
            df_hazardous[hazard_alias_col] = df_hazardous[hazard_alias_col].astype(str).fillna('')
            alias_newline_count = df_hazardous[hazard_alias_col].str.contains(r'[\n\r]', regex=True).sum()
            if alias_newline_count > 0:
                print(f"🔧 发现 {alias_newline_count} 条别名记录包含换行符，正在清理...")
            df_hazardous[hazard_alias_col] = df_hazardous[hazard_alias_col].str.replace('\n', ' ', regex=False)
            df_hazardous[hazard_alias_col] = df_hazardous[hazard_alias_col].str.replace('\r', ' ', regex=False)
            df_hazardous[hazard_alias_col] = df_hazardous[hazard_alias_col].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        df_hazardous['品名_纯净'] = df_hazardous[product_name_col].str.replace(r'\[.*?\]', '', regex=True).str.strip()
        df_hazardous['浓度阈值'] = df_hazardous[product_name_col].str.extract(r'(\[.*?\])', expand=False).fillna('')
        
        # 去除重复的品名_纯净，保留第一个
        df_hazardous = df_hazardous.drop_duplicates(subset=['品名_纯净'], keep='first')
        print(f"危化品清单共有 {len(df_hazardous)} 条有效记录")

        # 合并空行处理
        print("🔧 正在进行数据预处理...")
        df_input = merge_empty_rows(df_input, chinese_name_col, input_cas_col)

        # 创建新的列，并用默认值填充
        print("正在处理待匹配数据...")
        df_input['别名'] = ''
        df_input['是否为危化品'] = '否'
        df_input['浓度阈值'] = ''

        # 再次合并别名列（处理新创建的"别名"列）
        print("🔧 正在处理新创建的别名列...")
        df_input = merge_alias_columns(df_input, "待处理数据")

        # 处理输入数据中可能的编码问题
        # 确保检测到的中文名列为字符串类型
        df_input[chinese_name_col] = df_input[chinese_name_col].astype(str).fillna('')

        # 使用向量化操作提升性能，创建危化品字典用于快速查找
        # 需要同时保存别名和浓度阈值信息
        hazardous_dict = df_hazardous.set_index('品名_纯净')[['浓度阈值', hazard_alias_col]].to_dict('index')
        
        # 如果有CAS号列，创建基于CAS号的字典用于辅助匹配
        cas_dict = {}
        if input_cas_col and hazard_cas_col:
            # 清理CAS号数据，处理可能的换行符
            df_hazardous[hazard_cas_col] = df_hazardous[hazard_cas_col].astype(str).fillna('').str.strip()
            df_hazardous[hazard_cas_col] = df_hazardous[hazard_cas_col].str.replace('\n', '', regex=False)
            df_hazardous[hazard_cas_col] = df_hazardous[hazard_cas_col].str.replace('\r', '', regex=False)
            df_hazardous[hazard_cas_col] = df_hazardous[hazard_cas_col].str.replace(r'\s+', '', regex=True)
            
            # 创建CAS号到化学品信息的映射
            for _, row in df_hazardous.iterrows():
                cas_no = row[hazard_cas_col]
                if cas_no and cas_no.lower() not in ['nan', '', 'null']:
                    cas_dict[cas_no] = {
                        '品名_纯净': row['品名_纯净'],
                        '浓度阈值': row['浓度阈值'],
                        hazard_alias_col: row[hazard_alias_col]
                    }
            print(f"📊 创建了 {len(cas_dict)} 个CAS号映射")
        
        # 清理中文名数据
        df_input['中文名_清理'] = df_input[chinese_name_col].astype(str).fillna('').str.strip()
        
        # 如果有CAS号列，也清理CAS号数据
        if input_cas_col:
            df_input['CAS号_清理'] = df_input[input_cas_col].astype(str).fillna('').str.strip()
            df_input['CAS号_清理'] = df_input['CAS号_清理'].str.replace('\n', '', regex=False)
            df_input['CAS号_清理'] = df_input['CAS号_清理'].str.replace('\r', '', regex=False) 
            df_input['CAS号_清理'] = df_input['CAS号_清理'].str.replace(r'\s+', '', regex=True)
        
        # 使用向量化操作进行匹配
        def match_chemical(row):
            name = row['中文名_清理'] if '中文名_清理' in row.index else ''
            cas_no = row['CAS号_清理'] if 'CAS号_清理' in row.index else ''
            
            # 优先使用CAS号匹配（更准确）
            if cas_no and cas_no in cas_dict:
                match_info = cas_dict[cas_no]
                alias_name = match_info[hazard_alias_col]
                concentration = match_info['浓度阈值']
                hazard_name = match_info['品名_纯净']  # 获取危化品清单中的品名
                
                # 如果别名为空或NaN，保持为空字符串
                if pd.isna(alias_name) or str(alias_name).strip() == '' or str(alias_name).lower() == 'nan':
                    alias_name = ''
                else:
                    alias_name = str(alias_name).strip()
                
                # 检查中文名是否匹配，如果不匹配则返回修正后的中文名
                corrected_name = hazard_name if name != hazard_name else ''
                
                return pd.Series(['是', alias_name, concentration, corrected_name])
            
            # 如果CAS号没有匹配到，使用中文名匹配
            elif name and name in hazardous_dict:
                # 获取别名和浓度阈值
                alias_name = hazardous_dict[name][hazard_alias_col]
                concentration = hazardous_dict[name]['浓度阈值']
                
                # 如果别名为空或NaN，保持为空字符串
                if pd.isna(alias_name) or str(alias_name).strip() == '' or str(alias_name).lower() == 'nan':
                    alias_name = ''
                else:
                    alias_name = str(alias_name).strip()
                
                # 中文名匹配时不需要修正
                return pd.Series(['是', alias_name, concentration, ''])
            
            else:
                return pd.Series(['否', '', '', ''])
        
        # 应用匹配函数，直接更新中文别名列
        result_cols = ['是否为危化品', '中文别名', '浓度阈值', '中文名修正']
        if input_cas_col:
            # 如果有CAS号列，传递包含中文名和CAS号的数据
            df_input[result_cols] = df_input[['中文名_清理', 'CAS号_清理']].apply(match_chemical, axis=1)
        else:
            # 如果没有CAS号列，只传递中文名
            df_input[result_cols] = df_input['中文名_清理'].apply(lambda name: match_chemical(pd.Series({'中文名_清理': name})))
        
        # 处理中文名修正：如果中文名修正不为空，则更新原始中文名列
        has_corrections = df_input['中文名修正'] != ''
        correction_count = has_corrections.sum()
        
        if correction_count > 0:
            print(f"🔧 发现 {correction_count} 条记录需要中文名修正（CAS号匹配但中文名不匹配）")
            
            # 创建合并后的中文名列：以危化品目录中的名称为准
            # 对于有修正的记录，使用危化品目录中的标准名称
            # 对于没有修正的记录，保持原始中文名
            df_input['合并中文名'] = df_input[chinese_name_col].copy()
            df_input.loc[has_corrections, '合并中文名'] = df_input.loc[has_corrections, '中文名修正']
            
            # 将合并后的中文名替换原始中文名列
            df_input[chinese_name_col] = df_input['合并中文名']
            
            # 删除临时列
            df_input.drop('合并中文名', axis=1, inplace=True)
            
            print(f"✅ 已将 {correction_count} 条记录的中文名更新为危化品目录中的标准名称")
        
        # 删除临时的中文名修正列
        df_input.drop('中文名修正', axis=1, inplace=True)
        
        # 删除临时列
        df_input.drop('中文名_清理', axis=1, inplace=True)
        if input_cas_col:
            df_input.drop('CAS号_清理', axis=1, inplace=True)
        
        # 统计结果
        total_rows = len(df_input)
        matched_count = len(df_input[df_input['是否为危化品'] == '是'])
        corrected_count = correction_count if 'correction_count' in locals() else 0
        print(f"处理完成：共处理 {total_rows} 条记录，匹配到危化品 {matched_count} 条")
        if corrected_count > 0:
            print(f"其中 {corrected_count} 条记录的中文名已根据CAS号进行了修正")

        # 调整列的顺序，确保中文别名在合适的位置
        print("正在调整列顺序...")
        cols = df_input.columns.tolist()
        
        # 检查是否需要调整中文别名列的位置
        if '中文别名' in cols:
            # 找到中文名列的位置
            cn_name_index = cols.index(chinese_name_col)
            chinese_alias_index = cols.index('中文别名')
            
            # 确定理想的插入位置（中文名之后）
            ideal_position = cn_name_index + 1
            
            # 如果存在英文名列，在英文名前插入中文别名
            if english_name_col and english_name_col in cols:
                en_name_index = cols.index(english_name_col)
                # 如果英文名在中文名之后，则在英文名前插入
                if en_name_index > cn_name_index:
                    ideal_position = en_name_index
                print(f"📋 中文别名列位置调整：在 {chinese_name_col} 和 {english_name_col} 之间")
            else:
                print(f"📋 中文别名列位置调整：在 {chinese_name_col} 之后")
            
            # 如果中文别名列不在理想位置，则调整
            if chinese_alias_index != ideal_position and chinese_alias_index != ideal_position - 1:
                # 移动中文别名列到理想位置
                cols.insert(ideal_position, cols.pop(chinese_alias_index))
                df_input = df_input[cols]
                print(f"✅ 已调整中文别名列位置")
            else:
                print(f"✅ 中文别名列位置无需调整")
        else:
            print(f"⚠️  未找到中文别名列，跳过列顺序调整")

        # 保存处理后的文件，如果行数超过5000，则分块保存
        print("正在保存处理结果...")
        output_dir = Path(os.path.dirname(output_file))
        output_dir.mkdir(parents=True, exist_ok=True)

        chunk_size = 200

        if total_rows > chunk_size:
            num_chunks = (total_rows + chunk_size - 1) // chunk_size
            print(f"📊 数据量较大 ({total_rows} 行)，将分 {num_chunks} 个文件保存 (每份 {chunk_size} 行)")

            base_filename = Path(output_file).stem
            output_extension = Path(output_file).suffix

            for i in range(num_chunks):
                start_row = i * chunk_size
                end_row = start_row + chunk_size
                chunk_df = df_input.iloc[start_row:end_row]

                # 生成分块文件名
                chunk_filename = f"{base_filename}_part_{i+1}{output_extension}"
                chunk_output_path = output_dir / chunk_filename

                # 保存分块文件
                chunk_df.to_csv(chunk_output_path, index=False, encoding=OUTPUT_ENCODING)
                print(f"   - ✅ 已保存分块文件: {chunk_output_path.name}")

            print(f"✅ 处理完成，所有分块文件已保存至: {output_dir}")
        else:
            # 如果数据量不大，直接保存
            df_input.to_csv(output_file, index=False, encoding=OUTPUT_ENCODING)
            print(f"✅ 处理完成，文件已保存至: {output_file}")

        print(f"📄 输出文件编码: {OUTPUT_ENCODING}")
        match_rate = (matched_count / total_rows * 100) if total_rows > 0 else 0
        print(f"📊 数据统计: 总记录数 {total_rows}，危化品 {matched_count} 条，匹配率 {match_rate:.1f}%")
        print(f"⏱️  总用时: {time.time() - start_time:.2f} 秒")
        
        return True, matched_count, total_rows

    except FileNotFoundError as e:
        print(f"❌ 错误：找不到文件 {e.filename}。请检查文件路径是否正确。")
        return False, 0, 0
    except ValueError as e:
        print(f"❌ 错误: {e}")
        return False, 0, 0
    except Exception as e:
        print(f"❌ 处理过程中发生错误: {e}")
        return False, 0, 0

def process_all_files():
    """批量处理所有待处理文件"""
    print("=" * 80)
    print("🧪 化学品数据批量处理工具 v2.1")
    print("=" * 80)
    
    # 定义文件路径 - 数据文件在父目录中
    current_dir = Path(__file__).parent.parent  # 回到项目根目录
    input_dir = current_dir / '待处理数据'
    hazardous_list_path = current_dir / '国家危化品清单' / '国家危化品清单.csv'
    output_dir = current_dir / '已完成基础处理的文件'

    # 检查危化品清单文件是否存在
    if not hazardous_list_path.exists():
        print(f"❌ 危化品清单文件不存在: {hazardous_list_path}")
        return False

    # 查找所有待处理的数据文件（CSV和Excel）
    data_files = find_data_files(input_dir)
    
    if not data_files:
        print(f"❌ 在目录 {input_dir} 中未找到任何支持的数据文件")
        print(f"📋 支持的文件格式: {', '.join(SUPPORTED_EXTENSIONS)}")
        return False
    
    print(f"📁 找到 {len(data_files)} 个数据文件待处理:")
    for i, file_path in enumerate(data_files, 1):
        print(f"   {i}. {file_path.name}")
    
    # 统计信息
    total_files = len(data_files)
    successful_files = 0
    total_records = 0
    total_hazardous = 0
    failed_files = []
    
    # 批量处理所有文件
    for i, input_file_path in enumerate(data_files, 1):
        print(f"\n{'='*60}")
        print(f"📝 正在处理第 {i}/{total_files} 个文件: {input_file_path.name}")
        print(f"{'='*60}")
        
        # 生成输出文件名
        output_filename = generate_output_filename(input_file_path.name)
        output_file_path = output_dir / output_filename
        
        # 处理单个文件
        success, hazardous_count, record_count = process_data(
            str(input_file_path), 
            str(hazardous_list_path), 
            str(output_file_path)
        )
        
        if success:
            successful_files += 1
            total_records += record_count
            total_hazardous += hazardous_count
            print(f"✅ 文件 {input_file_path.name} 处理成功")
        else:
            failed_files.append(input_file_path.name)
            print(f"❌ 文件 {input_file_path.name} 处理失败")
    
    # 输出最终统计结果
    print(f"\n{'='*80}")
    print("📈 批量处理完成 - 总结报告")
    print(f"{'='*80}")
    print(f"📁 总文件数: {total_files}")
    print(f"✅ 成功处理: {successful_files}")
    print(f"❌ 处理失败: {len(failed_files)}")
    print(f"📊 总记录数: {total_records:,}")
    print(f"🧪 总危化品: {total_hazardous}")
    if total_records > 0:
        print(f"📈 总体匹配率: {total_hazardous/total_records*100:.2f}%")
    
    if failed_files:
        print(f"\n❌ 处理失败的文件:")
        for file_name in failed_files:
            print(f"   - {file_name}")
    
    if successful_files == total_files:
        print(f"\n🎉 所有文件处理成功完成！")
        return True
    elif successful_files > 0:
        print(f"\n⚠️  部分文件处理完成，请检查失败的文件。")
        return True
    else:
        print(f"\n💥 所有文件处理失败，请检查错误信息。")
        return False

def detect_column_names(df, column_type):
    """
    智能检测DataFrame中的列名
    
    Args:
        df (pd.DataFrame): 数据框
        column_type (str): 列类型 ('chinese_name', 'english_name', 'product_name')
    
    Returns:
        str or None: 检测到的列名，如果未找到返回None
    """
    # 定义可能的列名模式
    patterns = {
        'chinese_name': [
            '中文名', '中文名称', '化学品中文名', '物质中文名', '产品中文名',
            '中文', '名称', '化学名称', '物质名称', '产品名称', 'chinese_name',
            'cn_name', 'chinese', 'name_cn'
        ],
        'english_name': [
            '英文名', '英文名称', '化学品英文名', '物质英文名', '产品英文名',
            '英文', 'english_name', 'en_name', 'english', 'name_en',
            'chemical_name', 'substance_name'
        ],
        'product_name': [
            '品名', '产品名', '物质名', '化学品名', '名称', 'product_name',
            'chemical_name', 'substance_name', 'name', '品名称'
        ],
        'alias': [
            '别名', '别称', '俗名', '通用名', '商品名', 'alias', 'aliases',
            'alternative_name', 'common_name', 'trade_name', 'synonym'
        ],
        'cas': [
            'CAS号', 'CAS', 'cas号', 'cas', 'CAS_NO', 'cas_no', 'CAS-NO',
            'cas_number', 'CAS_Number', 'registry_number', 'CAS_RN'
        ]
    }
    
    # 获取对应类型的可能列名
    possible_names = patterns.get(column_type, [])
    
    # 检查列名（不区分大小写）
    df_columns_lower = [col.lower().strip() for col in df.columns]
    
    for pattern in possible_names:
        pattern_lower = pattern.lower().strip()
        # 精确匹配
        if pattern_lower in df_columns_lower:
            return df.columns[df_columns_lower.index(pattern_lower)]
        
        # 模糊匹配（包含关系）
        for i, col in enumerate(df_columns_lower):
            if pattern_lower in col or col in pattern_lower:
                return df.columns[i]
    
    return None

def display_dataframe_info(df, name, detected_cols=None):
    """
    显示数据框的基本信息
    
    Args:
        df (pd.DataFrame): 数据框
        name (str): 数据框名称
        detected_cols (dict): 检测到的列信息
    """
    print(f"\n📊 {name} 数据信息:")
    print(f"   📏 数据形状: {df.shape[0]} 行 × {df.shape[1]} 列")
    print(f"   📋 所有列名: {list(df.columns)}")
    
    if detected_cols:
        print(f"   🎯 检测到的关键列:")
        for col_type, col_name in detected_cols.items():
            if col_name:
                print(f"      - {col_type}: {col_name}")
            else:
                print(f"      - {col_type}: 未检测到")
    
    # 显示前几行数据的预览
    print(f"   👀 数据预览:")
    print("   " + str(df.head(2).to_string()).replace('\n', '\n   '))
    print()

def main():
    """主函数"""
    return process_all_files()

if __name__ == '__main__':
    main()
