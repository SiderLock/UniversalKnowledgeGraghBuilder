        # 根据模式选择数据源
        if incremental_mode:
            # 增量模式：读取增量数据
            success_files = list(self.incremental_dir.glob("incremental_update_*.csv"))
            if not success_files:
                logger.info("没有增量数据需要格式化")
                return
        else:
            # 完整模式：读取阶段一的成功数据（只加载最新批次）
            success_files = list(self.success_dir.glob("processed_chemicals_batch_*.csv"))
            if not success_files:
                logger.error("未找到阶段一的处理结果，请先运行阶段一")
                return
                
            # 按时间戳分组，只取最新的一组
            latest_timestamp = None
            latest_files = []
            
            for file_path in success_files:
                # 提取时间戳 (例如: processed_chemicals_batch_1_20250719_153149.csv)
                filename = file_path.name
                timestamp_match = re.search(r'_(\d{8}_\d{6})\.csv$', filename)
                if timestamp_match:
                    timestamp = timestamp_match.group(1)
                    if latest_timestamp is None or timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        latest_files = [file_path]
                    elif timestamp == latest_timestamp:
                        latest_files.append(file_path)
            
            success_files = latest_files
            logger.info(f"选择最新时间戳 {latest_timestamp} 的 {len(success_files)} 个文件")

        # 合并所有数据
        all_success_data = []
        for file_path in success_files:
            df = self.load_csv_file(file_path)
            if df is not None:
                all_success_data.append(df)
        
        if not all_success_data:
            logger.error("无法加载数据")
            return
            
        combined_data = pd.concat(all_success_data, ignore_index=True)
        logger.info(f"加载数据完成，共 {len(combined_data)} 条记录")

        # 格式化为Neo4j格式
        neo4j_nodes = self.format_for_neo4j(combined_data)
        
        # 提取关系（使用改进的算法）
        neo4j_relationships = self.extract_relationships(neo4j_nodes)
        
        # 保存Neo4j数据
        if incremental_mode:
            self.save_incremental_neo4j_data(neo4j_nodes, neo4j_relationships)
        else:
            self.save_neo4j_data(neo4j_nodes, neo4j_relationships)
        
        logger.info("阶段二完成！")

    def save_incremental_neo4j_data(self, nodes_df: pd.DataFrame, relationships_df: pd.DataFrame):
        """保存增量Neo4j格式的数据"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 创建标准/法律节点和关系
        if not nodes_df.empty:
            standard_legal_nodes = self.create_standard_legal_nodes(nodes_df)
            standard_legal_relationships = self.create_standard_legal_relationships(nodes_df)
            
            # 合并节点数据
            if not standard_legal_nodes.empty:
                combined_nodes = pd.concat([nodes_df, standard_legal_nodes], ignore_index=True)
            else:
                combined_nodes = nodes_df
                
            # 合并关系数据
            if not standard_legal_relationships.empty:
                combined_relationships = pd.concat([relationships_df, standard_legal_relationships], ignore_index=True)
            else:
                combined_relationships = relationships_df
        else:
            combined_nodes = nodes_df
            combined_relationships = relationships_df

        # 保存增量节点数据
        if not combined_nodes.empty:
            filename = f"incremental_neo4j_nodes_{timestamp}.csv"
            filepath = self.incremental_dir / filename
            combined_nodes.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j节点数据: {filename}")

        # 保存增量关系数据
        if not combined_relationships.empty:
            rel_filename = f"incremental_neo4j_relationships_{timestamp}.csv"
            rel_filepath = self.incremental_dir / rel_filename
            combined_relationships.to_csv(rel_filepath, index=False, encoding='utf-8-sig')
            logger.info(f"保存增量Neo4j关系数据: {rel_filename}")

        # 统计信息
        chemical_count = len(nodes_df) if not nodes_df.empty else 0
        standard_count = len(standard_legal_nodes) if 'standard_legal_nodes' in locals() and not standard_legal_nodes.empty else 0
        original_rel_count = len(relationships_df) if not relationships_df.empty else 0
        standard_rel_count = len(standard_legal_relationships) if 'standard_legal_relationships' in locals() and not standard_legal_relationships.empty else 0
        
        logger.info(f"增量数据保存完成:")
        logger.info(f"  化学品节点: {chemical_count} 个")
        logger.info(f"  标准/法律节点: {standard_count} 个")
        logger.info(f"  原有关系: {original_rel_count} 条")
        logger.info(f"  标准/法律关系: {standard_rel_count} 条")

    def run_complete_pipeline(self, incremental_mode: bool = False):
        """运行完整的处理流程（支持增量更新）"""
        if incremental_mode:
            logger.info("开始运行增量更新的化学品数据处理流程...")
        else:
            logger.info("开始运行完整的化学品数据处理流程...")
        
        try:
            # 阶段一：数据合并与预处理
            self.process_files(incremental_mode=incremental_mode)
            
            # 阶段二：图数据格式化与关系提取
            self.prepare_for_neo4j(incremental_mode=incremental_mode)
            
            logger.info("流程执行成功！")
            
        except Exception as e:
            logger.error(f"流程执行过程中发生错误: {e}")
            raise

    def run_incremental_update(self):
        """运行增量更新"""
        logger.info("执行增量更新...")
        self.run_complete_pipeline(incremental_mode=True)


def main():
    """主函数"""
    import sys
    
    # 获取工作目录
    base_path = Path(__file__).parent.parent.parent
    
    # 创建处理器实例
    processor = ChemicalProcessor(str(base_path))
    
    # 检查命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
        # 增量更新模式
        processor.run_incremental_update()
    else:
        # 完整处理模式
        processor.run_complete_pipeline()


if __name__ == "__main__":
    main()
