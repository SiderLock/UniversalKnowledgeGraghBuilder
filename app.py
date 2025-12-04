import streamlit as st
import yaml
import os
import pandas as pd
from pathlib import Path
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.universal_enricher import UniversalEnricher

# Page Config
st.set_page_config(
    page_title="OpenChemKG - 通用知识图谱构建",
    page_icon="🧪",
    layout="wide"
)

# Constants
CONFIG_DIR = Path("config")
DOMAINS_FILE = CONFIG_DIR / "domains.yaml"
DATA_DIR = Path("data")

# Helper Functions
def load_domains():
    if DOMAINS_FILE.exists():
        with open(DOMAINS_FILE, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}

def save_domains(domains):
    with open(DOMAINS_FILE, 'w', encoding='utf-8') as f:
        yaml.dump(domains, f, allow_unicode=True)

def get_api_key():
    # Try to get from session state, then env var
    return st.session_state.get("api_key") or os.environ.get("OPENCHEMKG_API_KEY")

# --- Sidebar: Configuration ---
with st.sidebar:
    st.header("⚙️ 设置")
    
    st.subheader("LLM API 配置")
    provider = st.selectbox("Provider", ["dashscope", "openai"], index=0)
    api_key = st.text_input("API Key", value=os.environ.get("OPENCHEMKG_API_KEY", ""), type="password")
    base_url = st.text_input("Base URL (Optional)", value="")
    model = st.text_input("Model Name", value="qwen-plus" if provider == "dashscope" else "gpt-4")
    
    if api_key:
        st.session_state["api_key"] = api_key
        os.environ["OPENCHEMKG_API_KEY"] = api_key # Set for current session

    st.divider()
    st.info("OpenChemKG v1.0.0")

# --- Main Content ---
st.title("🧪 OpenChemKG 通用构建平台")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🏷️ 领域与提示词", "📂 数据处理", "📊 结果预览", "⚙️ 完整流水线"])

# Load Domains
domains = load_domains()

with tab1:

    st.header("领域配置 (Domain Configuration)")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("选择领域")
        domain_names = list(domains.keys())
        selected_domain_name = st.selectbox("当前领域", domain_names + ["➕ 新建领域..."])
        
        if selected_domain_name == "➕ 新建领域...":
            new_domain_name = st.text_input("输入新领域名称 (英文ID)", placeholder="e.g., biology")
            new_domain_desc = st.text_area("描述该领域", placeholder="例如：生物学，关注蛋白质结构和功能...")
            
            if st.button("✨ 自动生成提示词"):
                if not api_key:
                    st.error("请先配置 API Key")
                elif not new_domain_name:
                    st.error("请输入领域名称")
                else:
                    with st.spinner("正在调用 LLM 生成提示词..."):
                        try:
                            enricher = UniversalEnricher(api_key, base_url, model, provider)
                            generated_config = enricher.generate_prompts_for_domain(new_domain_name, new_domain_desc)
                            
                            # Save to domains
                            domains[new_domain_name] = {
                                "description": new_domain_desc,
                                **generated_config
                            }
                            save_domains(domains)
                            st.success(f"领域 '{new_domain_name}' 创建成功！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"生成失败: {e}")

    with col2:
        if selected_domain_name and selected_domain_name != "➕ 新建领域...":
            current_config = domains[selected_domain_name]
            
            st.subheader(f"配置: {selected_domain_name}")
            st.caption(current_config.get('description', ''))
            
            with st.expander("查看/编辑 Schema (JSON)", expanded=True):
                schema_editor = st.text_area("Schema Definition", 
                                           value=yaml.dump(current_config.get('schema', {}), allow_unicode=True),
                                           height=200)
            
            with st.expander("查看/编辑 Prompts", expanded=True):
                system_prompt = st.text_area("System Prompt", 
                                           value=current_config.get('prompts', {}).get('system', ''))
                user_template = st.text_area("User Prompt Template", 
                                           value=current_config.get('prompts', {}).get('user_template', ''))
            
            if st.button("💾 保存修改"):
                try:
                    new_schema = yaml.safe_load(schema_editor)
                    domains[selected_domain_name]['schema'] = new_schema
                    domains[selected_domain_name]['prompts']['system'] = system_prompt
                    domains[selected_domain_name]['prompts']['user_template'] = user_template
                    save_domains(domains)
                    st.success("配置已保存")
                except Exception as e:
                    st.error(f"保存失败: {e}")

with tab2:
    st.header("数据处理流水线")
    
    if not selected_domain_name or selected_domain_name == "➕ 新建领域...":
        st.warning("请先在“领域与提示词”标签页选择一个有效领域。")
    else:
        st.info(f"当前工作领域: **{selected_domain_name}**")
        
        uploaded_file = st.file_uploader("上传 CSV 文件 (需包含实体名称列)", type=['csv'])
        
        if uploaded_file:
            df = pd.read_csv(uploaded_file)
            st.write("数据预览:", df.head())
            
            cols = df.columns.tolist()
            name_col = st.selectbox("选择实体名称列 (Entity Name Column)", cols)
            
            output_filename = st.text_input("输出文件名", value=f"enriched_{selected_domain_name}.csv")
            
            if st.button("🚀 开始补全数据"):
                if not api_key:
                    st.error("请先配置 API Key")
                else:
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    try:
                        enricher = UniversalEnricher(api_key, base_url, model, provider)
                        domain_config = domains[selected_domain_name]
                        
                        status_text.text("正在处理数据... (这可能需要一些时间)")
                        
                        # Process
                        result_df = enricher.process_batch(df, name_col, domain_config)
                        
                        # Save
                        output_path = DATA_DIR / "processed" / output_filename
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                        
                        progress_bar.progress(100)
                        status_text.success(f"处理完成！文件已保存至 {output_path}")
                        
                        # Store in session state for preview
                        st.session_state['last_result'] = result_df
                        
                    except Exception as e:
                        st.error(f"处理过程中发生错误: {e}")

with tab3:
    st.header("结果预览")
    if 'last_result' in st.session_state:
        st.dataframe(st.session_state['last_result'])
    else:
        st.info("暂无处理结果，请先在“数据处理”标签页运行任务。")

with tab4:
    st.header("完整流水线控制")
    st.markdown("在此处可以运行完整的 OpenChemKG 流水线，包括数据清洗、补全、后处理和图构建。")
    
    if st.button("▶️ 运行完整流水线"):
        if not api_key:
            st.error("请先在侧边栏配置 API Key")
        else:
            # Set env var for the pipeline process
            os.environ["OPENCHEMKG_API_KEY"] = api_key
            
            with st.spinner("正在运行流水线... 请查看终端日志"):
                try:
                    # Import Pipeline here to avoid circular imports or early init
                    from main import Pipeline
                    
                    # Capture logs? For now just run it.
                    pipeline = Pipeline()
                    pipeline.run()
                    
                    st.success("流水线运行完成！")
                except Exception as e:
                    st.error(f"流水线运行失败: {e}")


