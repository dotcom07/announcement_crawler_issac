# /home/ubuntu/multiturn_ver1/new crawler/main.py


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

import time  # time 모듈
from datetime import time as dt_time  # datetime.time을 다른 이름으로 불러오기

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

def get_next_run_time():
    """다음 실행 시간을 정확히 계산하는 함수"""
    now = datetime.now(KST)
    current_weekday = now.weekday()  # 0: 월요일 ~ 6: 일요일

    if current_weekday in [5, 6]:  # 주말 (토, 일)
        next_times = ["09:00", "12:00", "18:00", "00:00"]
    else:  # 평일 (월~금)
        # 09:00 ~ 18:00까지 10분 간격 생성
        next_times = []
        start_time = dt_time(9, 0)  # 09:00
        end_time = dt_time(18, 0)  # 18:00
        current_time = datetime.combine(now.date(), start_time)

        while current_time.time() <= end_time:
            next_times.append(current_time.strftime("%H:%M"))
            current_time += timedelta(minutes=10)

        # 18:00 이후 추가 시간
        next_times += ["18:00", "20:00", "22:00", "00:00"]

    # 현재 시간 이후의 실행 시간을 찾음
    for next_time in next_times:
        next_run = datetime.strptime(next_time, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        next_run = KST.localize(next_run)  # 여기가 핵심!
        if next_time == "00:00":
            next_run += timedelta(days=1)  # 자정은 다음 날로 설정
        
        if now < next_run:
            return next_run

    return None  # 혹시 예외가 발생하면 None 반환


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
        
        next_run = get_next_run_time()
        if not next_run:
            logger.error("다음 실행 시간을 계산할 수 없습니다.")
            break
        
        sleep_time = (next_run - datetime.now(KST)).total_seconds()
        sleep_hours = int(sleep_time // 3600)  
        sleep_minutes = int((sleep_time % 3600) // 60)  

        save_crawler_states_to_mongo(crawlers)
        save_psychology_article_ids()
        save_architecture_engineering_state()
        logger.info(f"=== Finished checking all sites. Next run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
              f"({sleep_hours}시간 {sleep_minutes}분 후) ===")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()