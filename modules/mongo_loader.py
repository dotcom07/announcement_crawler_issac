import os
import json
from pymongo import MongoClient
from datetime import datetime
from pytz import timezone

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from config.site_config import SITES  # SITES에서 source 목록 가져오기

# MongoDB 설정
MONGO_URI = "mongodb+srv://admin:!dkdlwkr03@cluster0.y0emv.mongodb.net/"
DB_NAME = "isaac"
CRAWLER_COLLECTION = "crawler_states"
OUTPUT_DIR = "output/crawler_state/"

KST = timezone('Asia/Seoul')



def save_crawler_states_to_files():
    """
    MongoDB에서 크롤러 상태 정보를 가져와 각 source별 JSON 파일로 저장
    기존 파일이 있으면 삭제 후 저장
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[CRAWLER_COLLECTION]

    os.makedirs(OUTPUT_DIR, exist_ok=True)  # 출력 폴더 생성 (없으면)

    for source in SITES.keys():  # SITES에서 source 목록 가져오기
        state_data = collection.find_one({"source": source}, {"_id": 0})

        if not state_data:
            print(f"[INFO] No data found for {source}. Skipping...")
            continue  # 데이터가 없는 경우 저장하지 않음

        # 🔹 datetime을 문자열로 변환
        if "updated_at" in state_data and isinstance(state_data["updated_at"], datetime):
            state_data["updated_at"] = state_data["updated_at"].isoformat()

        file_path = os.path.join(OUTPUT_DIR, f"announcement_state_{source}.json")

        # ✅ 기존 파일이 있으면 삭제
        if os.path.exists(file_path):
            os.remove(file_path)

        # ✅ 데이터가 `PSYCHOLOGY`일 경우 article_ids만 저장
        if source == "PSYCHOLOGY":
            file_path = os.path.join(OUTPUT_DIR, "article_ids_PSYCHOLOGY.txt")
            article_ids = state_data.get("article_ids", [])

            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", encoding="utf-8") as f:
                for article_id in article_ids:
                    f.write(f"{article_id}\n")

            print(f"[INFO] {source} article IDs saved to {file_path}")

        # ✅ 데이터가 `ARCHITECTURE_ENGINEERING`일 경우 JSON 저장
        elif source == "ARCHITECTURE_ENGINEERING":
            file_path = os.path.join(OUTPUT_DIR, "announcement_state_ARCHITECTURE_ENGINEERING.json")

            if os.path.exists(file_path):
                os.remove(file_path)

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(state_data, f, indent=4, ensure_ascii=False)

            print(f"[INFO] {source} state saved to {file_path}")

        # ✅ 일반 크롤러 데이터 저장
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(state_data, f, indent=4, ensure_ascii=False)

            print(f"[INFO] {source} state saved to {file_path}")

    client.close()




if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # 출력 폴더 생성 (없으면)
    save_crawler_states_to_files()
