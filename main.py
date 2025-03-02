# /home/ubuntu/multiturn_ver1/new crawler/main.py

import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pytz import timezone 
from config.site_config import SITES    
from modules.announcement_crawler import AnnouncementCrawler
from modules.announcement_crawler_for_notice_list import ListAnnouncementCrawler
from modules.announcement_crawler_for_ARCHITECTURE_ENGINEERING import ARCHITECTURE_ENGINEERING_AnnouncementCrawler
import os
from modules.mongo_saver import save_crawler_states_to_mongo, save_psychology_article_ids, save_architecture_engineering_state
from modules.mongo_loader import save_crawler_states_to_files
KST = timezone('Asia/Seoul')  

def setup_logger():
    logger = logging.getLogger("AnnouncementCrawler")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def process_site(source, crawler):
    """
    스레드 풀에서 실행할 함수.
    공지사항 목록 페이지와 일반 공지사항 페이지를 구분하여 크롤링을 진행한다.
    """
    try:
        if isinstance(crawler, ListAnnouncementCrawler):
            print("List형 크롤러 새로운 공지사항인지 체크 시작")
            if source=="INDUSTRY_ENGINEERING":
                crawler.check_for_new_notices(max_pages=14)
            else :
                crawler.check_for_new_notices(max_pages=15)
        elif isinstance(crawler, AnnouncementCrawler):
            crawler.check_for_new_notices(max_checks=1)
        elif isinstance(crawler, ARCHITECTURE_ENGINEERING_AnnouncementCrawler):
            crawler.check_for_new_notices()
    except Exception as e:
        crawler.logger.error(f"[{source}] Error in process_site: {e}")

def get_sleep_duration():
    """현재 한국 시간(KST)에 따라 sleep 시간을 결정하는 함수"""
    now = datetime.now(KST)
    current_time = now.time()
    current_weekday = now.weekday()  # 0: 월요일 ~ 6: 일요일

    # 주말 (토요일=5, 일요일=6) → 6시간(21600초) 간격 실행
    if current_weekday in [5, 6]:  
        return 21600  # 6시간

    # 평일 (월~금)
    if current_time >= datetime.strptime("00:00", "%H:%M").time() and current_time < datetime.strptime("09:00", "%H:%M").time():
        return None  # 00:00 ~ 09:00 크롤링 실행 안함
    elif current_time >= datetime.strptime("09:00", "%H:%M").time() and current_time < datetime.strptime("19:00", "%H:%M").time():
        return 600  # 09:00 ~ 19:00 (10분)
    elif current_time >= datetime.strptime("19:00", "%H:%M").time() and current_time < datetime.strptime("23:59", "%H:%M").time():
        return 7200  # 19:00 ~ 23:59 (2시간)

    return None  # 혹시 모를 예외 처리


def main():
    logger = setup_logger()
    

    save_crawler_states_to_files()

    # 1) 사이트별 Crawler 인스턴스 생성
    crawlers = {}

    # 최초 db에서 file 정보 업데이트 하기
    for source, config in SITES.items():
        # if source != "POLITICAL_SCIENCE":
            # continue
        if source == "ARCHITECTURE_ENGINEERING":
            crawler = ARCHITECTURE_ENGINEERING_AnnouncementCrawler(
                source=source,
                base_url=config["base_url"],
                start_url=config["start_url"],
                url_number=config["url_number"],
                sub_category_selector=config["sub_category_selector"],
                next_page_selector=config["next_page_selector"],
                title_selector=config["title_selector"],
                date_selector=config["date_selector"],
                author_selector=config["author_selector"],
                content_selector=config["content_selector"],
                logger=logger,
            )
        elif config["next_page_selector"] == "null":
            crawler = ListAnnouncementCrawler(
                source=source,
                base_url=config["base_url"],
                start_url=config["start_url"],
                url_number=config["url_number"],
                sub_category_selector=config["sub_category_selector"],
                next_page_selector=config["next_page_selector"],
                title_selector=config["title_selector"],
                date_selector=config["date_selector"],
                author_selector=config["author_selector"],
                content_selector=config["content_selector"],
                logger=logger,
            )
        else:
             crawler = AnnouncementCrawler(
                 source=source,
                 base_url=config["base_url"],
                 start_url=config["start_url"],
                 url_number=config["url_number"],
                 sub_category_selector=config["sub_category_selector"],
                 next_page_selector=config["next_page_selector"],
                 title_selector=config["title_selector"],
                 date_selector=config["date_selector"],
                 author_selector=config["author_selector"],
                 content_selector=config["content_selector"],
                 logger=logger,
             )
        
        crawlers[source] = crawler

    # 2) 무한 반복(각 사이트 최대 2번 검사 → 병렬 2개까지)
    while True:
        logger.info("=== Start checking all sites ===")
        
        sleep_duration = get_sleep_duration()

        if sleep_duration is None:
            logger.info("현재 시간에는 크롤링을 실행하지 않습니다. (자정~오전 9시)")
            now = datetime.now(KST)
            next_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
            
            if now > next_start:
                next_start += timedelta(days=1)

            sleep_time = (next_start - now).total_seconds()
            sleep_hours = int(sleep_time // 3600)  # 시간 단위 변환
            sleep_minutes = int((sleep_time % 3600) // 60)  # 분 단위 변환

            logger.info(f"{sleep_hours}시간 {sleep_minutes}분 후 다시 시작합니다.")
            time.sleep(sleep_time)
            continue

        
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            
            for source, crawler in crawlers.items():
                # 스레드 풀에 작업 제출
                future = executor.submit(process_site, source, crawler)
                futures.append((source, future))
            
            # 모든 작업 완료 대기
            for source, future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[{source}] Future Error: {e}")
        
        wait_hours = sleep_duration // 3600
        wait_minutes = (sleep_duration % 3600) // 60
        save_crawler_states_to_mongo(crawlers)
        save_psychology_article_ids()
        save_architecture_engineering_state()
        logger.info(f"=== Finished checking all sites. Waiting for {wait_hours} hour(s) {wait_minutes} minute(s)... ===")
        time.sleep(sleep_duration)


if __name__ == "__main__":
    main()