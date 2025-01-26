# /home/ubuntu/multiturn_ver1/new crawler/modules/saver.py

import os
import threading
import time
import json
import logging
import shutil

class Saver:
    def __init__(self, original_file, logger, batch_size=1, max_file_size=50 * 1024 * 1024, log_dir=None):
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)  # 디렉토리 생성 (존재하지 않으면)
            self.original_file = os.path.join(log_dir, original_file)
        else:
            self.original_file = original_file

        self.logger = logger
        self.batch_size = batch_size
        self.max_file_size = max_file_size
        self.lock = threading.Lock()

    def check_file_size_and_rotate(self, file_path):
        if os.path.exists(file_path) and os.path.getsize(file_path) > self.max_file_size:
            # 기존 파일 이름에 타임스탬프를 붙여 백업
            base_name, ext = os.path.splitext(file_path)
            rotated_file = f"{base_name}_{int(time.time())}{ext}"
            try:
                shutil.move(file_path, rotated_file)
                self.logger.info(f"파일 크기 초과로 새로운 파일 생성: {rotated_file}")
            except Exception as e:
                self.logger.error(f"파일 회전 중 오류 발생: {e}")

    def save_original_data(self, original_data):
        with self.lock:
            self.check_file_size_and_rotate(self.original_file)
            try:
                with open(self.original_file, 'a', encoding='utf-8') as f_original:
                    json.dump(original_data, f_original, ensure_ascii=False)
                    f_original.write('\n')  # JSONL 형식으로 저장
                self.logger.info(f"원본 데이터 저장 완료: {original_data.get('url', 'No URL')}")
            except Exception as e:
                self.logger.error(f"원본 데이터 저장 실패: {e}")

    def final_save(self):
        pass  # 요약 기능 제거로 인해 특별한 동작이 필요 없음