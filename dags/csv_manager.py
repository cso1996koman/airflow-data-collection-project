import csv
import os
from typing import Dict, List, Union
class CsvManager:    
    def save_csv(self, json_data: Union[Dict, List[Dict]], csv_path: str):
        directory = os.path.dirname(csv_path)
        if not os.path.exists(directory):
            os.makedirs(directory, mode=0o755, exist_ok=True)
        try:
            with open(csv_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                if isinstance(json_data, list) and json_data:
                    header = json_data[0].keys()
                    writer.writerow(header)
                    for item in json_data:
                        writer.writerow(item.values())
                elif isinstance(json_data, dict):
                    header = json_data.keys()
                    writer.writerow(header)
                    writer.writerow(json_data.values())
                else:
                    raise ValueError("json_data must be a non-empty list of dictionaries or a dictionary")                    
            print(f"CSV file saved successfully at {csv_path}")
        except Exception as e:
            print(f"Failed to save CSV file: {e}")
            raise