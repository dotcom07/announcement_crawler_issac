# json_manager.py

import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import os
from threading import Lock

class JsonManager:
    _lock = Lock()

    @staticmethod
    def save_to_jsonl(json_data, file_path):
        with JsonManager._lock:
            try:
                with open(file_path, 'a', encoding='utf-8') as f:
                    json_line = json.dumps(json_data, ensure_ascii=False)
                    f.write(json_line + '\n')
                print(f"JSONL saved to file: {file_path}")
            except Exception as e:
                print(f"Failed to save JSONL: {e}")
