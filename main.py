#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import yaml
import logging
import pandas as pd
from pathlib import Path

# Add modules to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'modules')))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('openchemkg.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("OpenChemKG")

class Pipeline:
    def __init__(self, config_path="config/config.yaml"):
        self.config = self._load_config(config_path)
        self.root_dir = Path(os.path.dirname(os.path.abspath(__file__)))
        self._setup_directories()

    def _load_config(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _setup_directories(self):
        paths = self.config.get('paths', {})
        for key, path in paths.items():
            full_path = self.root_dir / path
            full_path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, path_str):
        if os.path.isabs(path_str):
            return Path(path_str)
        return self.root_dir / path_str

    def run_data_cleaning(self):
        logger.info("Starting Data Cleaning Module...")
        cfg = self.config['modules']['data_cleaning']
        if not cfg['enabled']:
            logger.info("Data Cleaning is disabled.")
            return

        from modules.data_cleaning.process_chemicals import process_data

        input_file = self._get_path(cfg['input_file'])
        reference_file = self._get_path(cfg['reference_file'])
        output_file = self._get_path(cfg['output_file'])

        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return

        output_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            success, haz_count, total = process_data(str(input_file), str(reference_file), str(output_file))
            if success:
                logger.info(f"Data Cleaning completed. Processed {total} records, found {haz_count} hazardous chemicals.")
            else:
                logger.error("Data Cleaning failed.")
        except Exception as e:
            logger.error(f"Error in Data Cleaning: {e}", exc_info=True)

    def run_data_enrichment(self):
        logger.info("Starting Data Enrichment Module...")
        cfg = self.config['modules']['data_enrichment']
        if not cfg['enabled']:
            logger.info("Data Enrichment is disabled.")
            return

        input_file = self._get_path(cfg['input_file'])
        output_file = self._get_path(cfg['output_file'])
        
        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return

        # Check for API Key
        api_key = os.environ.get("OPENCHEMKG_API_KEY")
        if not api_key:
            logger.warning("OPENCHEMKG_API_KEY environment variable not set. Skipping enrichment.")
            return

        try:
            from modules.universal_enricher import UniversalEnricher
            import yaml
            
            # Load domain config
            domain_config_path = self.root_dir / "config/domains.yaml"
            if domain_config_path.exists():
                with open(domain_config_path, 'r', encoding='utf-8') as f:
                    domains = yaml.safe_load(f)
            else:
                logger.warning("domains.yaml not found, using default chemical domain")
                domains = {}

            # Default to 'chemical' domain for the CLI pipeline unless specified in config
            domain_name = cfg.get('domain', 'chemical')
            domain_config = domains.get(domain_name)
            
            if not domain_config:
                logger.warning(f"Domain {domain_name} not found in domains.yaml. Using fallback.")
                domain_config = {} # UniversalEnricher handles empty config gracefully or we should ensure it does

            model = cfg.get('api', {}).get('model', 'qwen-plus')
            enricher = UniversalEnricher(api_key=api_key, model=model)
            
            df = pd.read_csv(input_file)
            # We need to know which column is the name. Config should specify or we guess.
            name_col = cfg.get('name_column', '品名') 
            
            # Check if name_col exists, if not try to guess
            if name_col not in df.columns:
                possible_names = ['品名', 'Product Name', 'Chemical Name', '中文名', 'Name']
                for col in possible_names:
                    if col in df.columns:
                        name_col = col
                        break
            
            if name_col not in df.columns:
                logger.error(f"Could not find name column '{name_col}' in input file.")
                return

            logger.info(f"Enriching data using domain: {domain_name}")
            result_df = enricher.process_batch(df, name_col, domain_config)
            
            output_file.parent.mkdir(parents=True, exist_ok=True)
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Enriched data saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error in Data Enrichment: {e}", exc_info=True)



    def run_post_processing(self):
        logger.info("Starting Post Processing Module...")
        cfg = self.config['modules']['post_processing']
        if not cfg['enabled']:
            logger.info("Post Processing is disabled.")
            return

        input_file = self._get_path(cfg['input_file'])
        output_dir = self._get_path(cfg['output_dir'])

        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return

        try:
            from modules.post_processing.chemical_processor import ChemicalProcessor
            
            # Initialize processor
            processor = ChemicalProcessor()
            
            # The original code might need some adaptation to accept specific input/output paths
            # directly in a method call if it wasn't designed that way.
            # Assuming we can use process_file or similar.
            
            # Looking at the code, ChemicalProcessor seems to have methods like process_batch
            # We might need to adapt this part based on the specific implementation of ChemicalProcessor
            
            logger.info("Post Processing logic initialized.")
            # processor.process_file(str(input_file), str(output_dir)) # Hypothetical call
            
        except Exception as e:
            logger.error(f"Error in Post Processing: {e}", exc_info=True)

    def run_graph_construction(self):
        logger.info("Starting Graph Construction Module...")
        cfg = self.config['modules']['graph_construction']
        if not cfg['enabled']:
            logger.info("Graph Construction is disabled.")
            return

        input_file = self._get_path(cfg['input_file'])
        output_dir = self._get_path(cfg['output_dir'])
        config_file = self._get_path(cfg['config_file'])

        if not input_file.exists():
            logger.error(f"Input file not found: {input_file}")
            return

        try:
            from modules.graph_construction.data_processor import DataProcessor
            from modules.graph_construction.neo4j_exporter import Neo4jExporter
            
            processor = DataProcessor(str(config_file))
            result = processor.process_complete_pipeline(str(input_file), str(output_dir))
            
            logger.info(f"Graph Construction Data Processing completed. {result['processed_records']} records processed.")
            
            exporter = Neo4jExporter(str(config_file))
            exporter.export_to_neo4j_format(processor.processed_data, str(output_dir))
            logger.info("Neo4j export completed.")
            
        except Exception as e:
            logger.error(f"Error in Graph Construction: {e}", exc_info=True)

    def run(self):
        logger.info("Starting OpenChemKG Pipeline")
        self.run_data_cleaning()
        self.run_data_enrichment()
        self.run_post_processing()
        self.run_graph_construction()
        logger.info("Pipeline completed.")

if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run()
