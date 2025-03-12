import sys
import pandas as pd
import json
import ijson
from src.utils.logger import get_logger  # Import logger

class JSONToCSVConverter:
    def __init__(self, json_file, csv_file, column_mapping_str):
        self.json_file = json_file
        self.csv_file = f"{csv_file}.csv"
        self.column_mapping = self.parse_column_mapping(column_mapping_str)
        self.logger = get_logger(__name__)

    def parse_column_mapping(self, column_mapping_str):
        try:
            self.logger.info(f"Received column mapping string: {column_mapping_str}")
            column_mapping = json.loads(column_mapping_str)
            self.logger.info(f"Parsed column mapping: {column_mapping}")
            return column_mapping
        except json.JSONDecodeError as e:
            self.logger.error(f"🚨 ERROR: Column mapping is not a valid JSON string! {e}")
            sys.exit(1)

    def detect_json_structure(self, file):
        first_char = file.read(1)
        file.seek(0)  # Reset file pointer
        
        if first_char == '{':
            self.logger.info("Detected JSON as Object with Keys")
            return ijson.kvitems(file, ''), True
        elif first_char == '[':
            self.logger.info("Detected JSON as Array of Objects")
            return ijson.items(file, 'item'), False
        else:
            self.logger.error("🚨 ERROR: Unrecognized JSON structure!")
            sys.exit(1)

    def convert(self, batch_size=10_000):
        try:
            self.logger.info(f"Starting JSON streaming from {self.json_file}...")
            with open(self.json_file, 'r', encoding='utf-8') as f:
                objects, process_as_dict = self.detect_json_structure(f)
                batch = []
                first_write = True
                row_count = 0

                for key, obj in objects if process_as_dict else enumerate(objects):
                    batch.append(obj)
                    row_count += 1

                    if len(batch) >= batch_size:
                        self.write_to_csv(batch, first_write)
                        self.logger.info(f"✅ Processed {row_count} rows so far...")
                        first_write = False
                        batch = []

                if batch:
                    self.write_to_csv(batch, first_write)

            self.logger.info(f"✅ Final CSV saved successfully: {self.csv_file}")
            self.logger.info(f"✅ Total rows processed: {row_count}")
        except Exception as e:
            self.logger.error(f"🚨 ERROR: {e}")
            sys.exit(1)

    def write_to_csv(self, batch, first_write):
        df = pd.DataFrame(batch)
        df.rename(columns=self.column_mapping, inplace=True)
        df.to_csv(self.csv_file, mode='a', index=False, header=first_write)