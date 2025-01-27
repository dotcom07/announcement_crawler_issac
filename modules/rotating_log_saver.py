import os
import json
from datetime import datetime

class RotatingLogSaver:
    def __init__(self, base_dir, prefix, max_file_size_mb=10):
        """
        base_dir: 로그 저장 기본 디렉토리
        prefix: 로그 파일 접두사 (예: 'issac_logs', 'opensearch_logs')
        max_file_size_mb: 각 로그 파일 최대 크기 (MB)
        """
        self.base_dir = base_dir
        self.prefix = prefix
        self.max_file_size = max_file_size_mb * 1024 * 1024  # MB to bytes
        self.current_file = None
        self.current_file_path = None
        
        # 로그 디렉토리 생성
        os.makedirs(base_dir, exist_ok=True)
        
        # 현재 사용할 로그 파일 설정
        self._set_current_file()
    
    def _set_current_file(self):
        """현재 쓸 로그 파일 설정 (없거나 용량 초과시 새로 생성)"""
        # 현재 파일이 있고 용량 제한 이내면 계속 사용
        if self.current_file_path and os.path.exists(self.current_file_path):
            if os.path.getsize(self.current_file_path) < self.max_file_size:
                return
            
            # 기존 파일 닫기
            if self.current_file:
                self.current_file.close()
        
        # 새 로그 파일 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_file_path = os.path.join(
            self.base_dir, 
            f"{self.prefix}_{timestamp}.jsonl"
        )
        self.current_file = open(self.current_file_path, "a", encoding="utf-8")
    
    def save_log(self, log_data):
        """로그 데이터 저장"""
        try:
            # 현재 파일 크기 체크 & 필요시 새 파일 생성
            if (os.path.getsize(self.current_file_path) >= self.max_file_size):
                self._set_current_file()
            
            # 로그 저장
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "data": log_data
            }
            self.current_file.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
            self.current_file.flush()
            
        except Exception as e:
            print(f"Error saving log: {e}")
    
    def __del__(self):
        """소멸자: 열린 파일 정리"""
        if self.current_file:
            self.current_file.close()