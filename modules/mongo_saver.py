import os
import json
from pymongo import MongoClient
from datetime import datetime
from pytz import timezone 

# MongoDB 설정
MONGO_URI = "mongodb+srv://admin:!dkdlwkr03@cluster0.y0emv.mongodb.net/"
DB_NAME = "isaac"
COLLECTION_NAME = "crawler_states"  # 모든 데이터를 저장할 통합 컬렉션
KST = timezone('Asia/Seoul')  

# 파일 경로 설정
OUTPUT_DIR = "output/crawler_state/"
PSYCHOLOGY_FILE = os.path.join(OUTPUT_DIR, "article_ids_PSYCHOLOGY.txt")
ARCHITECTURE_FILE = os.path.join(OUTPUT_DIR, "announcement_state_ARCHITECTURE_ENGINEERING.json")

def save_to_mongo(source, data):
    """
    크롤러 데이터를 MongoDB에 저장하는 함수
    """
    if not data:
        return

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    document = {"source": source, "updated_at": datetime.now(KST)}

    # ✅ PSYCHOLOGY 크롤러 (article_ids 리스트로 저장)
    if source == "PSYCHOLOGY":
        document["article_ids"] = data.get("article_ids", [])

    # ✅ ARCHITECTURE_ENGINEERING 크롤러 (last_date_id, seen_title_hashes 저장)
    elif source == "ARCHITECTURE_ENGINEERING":
        document["last_date_id"] = data.get("last_date_id")
        document["seen_title_hashes"] = data.get("seen_title_hashes", [])
        document["is_first_crawl_done"] = data.get("is_first_crawl_done", False)

    # ✅ 일반 크롤러 (last_article_no, last_page_url 저장)
    else:
        document["last_article_no"] = data.get("last_article_no")
        document["last_page_url"] = data.get("last_page_url")

    # 기존 데이터가 있으면 업데이트, 없으면 삽입 (upsert)
    collection.update_one(
        {"source": source},
        {"$set": document},
        upsert=True
    )

    print(f"[{source}] 데이터 저장 완료: {document}")
    client.close()

def save_crawler_states_to_mongo(crawlers):
    """
    모든 Crawler의 state_file 정보를 읽어와 MongoDB에 저장하는 함수
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    for source, crawler in crawlers.items():
        state_file = crawler.state_file

        if os.path.exists(state_file):
            try:
                with open(state_file, "r") as f:
                    state_data = json.load(f)

                last_article_no = state_data.get("last_article_no")
                last_page_url = state_data.get("last_page_url")

                if last_article_no:  # article_no가 존재하는 경우만 저장
                    save_to_mongo(source, {
                        "last_article_no": last_article_no,
                        "last_page_url": last_page_url
                    })

                    crawler.logger.info(f"[{source}] State saved to MongoDB. last_article_no={last_article_no}")

            except Exception as e:
                crawler.logger.error(f"[{source}] Error reading state_file: {e}")
        else:
            crawler.logger.info(f"[{source}] No state file found. Skipping.")

    client.close()


def save_psychology_article_ids():
    """
    PSYCHOLOGY 크롤러의 article_id를 파일에서 읽어 MongoDB에 저장하는 함수
    (한 번에 리스트로 저장)
    """
    if not os.path.exists(PSYCHOLOGY_FILE):
        print("[PSYCHOLOGY] No article_ids file found. Skipping.")
        return

    with open(PSYCHOLOGY_FILE, "r", encoding="utf-8") as f:
        article_ids = [line.strip() for line in f if line.strip()]  # 리스트로 저장

    if not article_ids:
        print("[PSYCHOLOGY] No valid article IDs found. Skipping.")
        return

    save_to_mongo("PSYCHOLOGY", {"article_ids": article_ids})  # 리스트로 저장


def save_architecture_engineering_state():
    """
    ARCHITECTURE_ENGINEERING 크롤러의 상태 파일을 읽어 MongoDB에 저장하는 함수
    """
    if not os.path.exists(ARCHITECTURE_FILE):
        print("[ARCHITECTURE_ENGINEERING] No state file found. Skipping.")
        return

    try:
        with open(ARCHITECTURE_FILE, "r", encoding="utf-8") as f:
            architecture_data = json.load(f)

        save_to_mongo("ARCHITECTURE_ENGINEERING", architecture_data)

    except json.JSONDecodeError:
        print("[ARCHITECTURE_ENGINEERING] JSON 파일이 올바르지 않습니다. Skipping.")