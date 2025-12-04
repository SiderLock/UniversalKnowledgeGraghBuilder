import os
import pandas as pd
import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def enrich_data(input_path: str, output_path: str, api_key: str, model: str = "qwen-plus"):
    """
    Simple data enrichment using DashScope (Qwen) as an example.
    """
    try:
        import dashscope
        from dashscope import Generation
    except ImportError:
        logger.error("dashscope package is not installed. Please install it via pip install dashscope")
        return

    dashscope.api_key = api_key

    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} records from {input_path}")

    # Check if '品名' column exists
    name_col = None
    for col in ['品名', 'Product Name', 'Chemical Name', '中文名']:
        if col in df.columns:
            name_col = col
            break
    
    if not name_col:
        logger.error("Could not find a column for chemical name.")
        return

    # Create result columns if not exist
    target_cols = ['物理性质', '危险性', '用途']
    for col in target_cols:
        if col not in df.columns:
            df[col] = ""

    # Process each row (Demo: process first 5 rows to save tokens)
    # In production, remove the [:5] limit
    logger.info("Processing records... (Demo mode)")
    
    for index, row in df.iterrows():
        chem_name = row[name_col]
        if pd.isna(chem_name):
            continue
            
        prompt = f"请提供化学品 '{chem_name}' 的以下信息：物理性质、危险性、主要用途。请以JSON格式返回。"
        
        try:
            # This is a simplified call. In reality, you'd want robust error handling and parsing.
            # response = Generation.call(model=model, prompt=prompt)
            # if response.status_code == 200:
            #     content = response.output.text
            #     # Parse content and update row
            #     logger.info(f"Processed {chem_name}")
            # else:
            #     logger.warning(f"Failed to process {chem_name}: {response.message}")
            
            # For now, we just log that we would process it
            logger.info(f"Would query API for: {chem_name}")
            time.sleep(0.1) 
            
        except Exception as e:
            logger.error(f"Error processing {chem_name}: {e}")

    # Save result
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"Saved enriched data to {output_path}")
