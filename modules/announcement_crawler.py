# /home/ubuntu/multiturn_ver1/new crawler/modules/announcement_crawler.py
import sys
import os
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .json_manager import JsonManager
from .announcement_parser import AnnouncementParser
from .fetcher import Fetcher
import requests
import os
from .saver import Saver
from urllib.parse import parse_qs, urlparse
from .rotating_log_saver import RotatingLogSaver


ISSAC_ENDPOINT = "https://issac.issac-dev.click/api/v1/notices"
ISSAC_USER = "issac"
ISSAC_PASSWORD = "StrongPass!2025"

OPENSEARCH_ENDPOINT = "https://search-isaac-7vp3trcpuwavry2wcbl4fkmamq.ap-northeast-2.es.amazonaws.com"
OPENSEARCH_USER = "issac"
OPENSEARCH_PASSWORD = "StrongPass!2025"
OPENSEARCH_INDEX = "notice"


class AnnouncementCrawler:
    def __init__(self, source, base_url, start_url, url_number, sub_category_selector, next_page_selector, title_selector, date_selector, author_selector, content_selector, logger):
        
        
        self.source= source
        self.base_url = base_url
        self.start_url = start_url
        self.url_number = url_number
        self.sub_category_selector = sub_category_selector
        self.next_page_selector = next_page_selector
        self.title_selector = title_selector
        self.date_selector = date_selector
        self.author_selector = author_selector
        self.content_selector = content_selector
        self.logger = logger

        self.processed_urls = set()

        # 모든 파일을 저장할 최상위 output 디렉토리
        self.output_dir = "./output"

        # 1) 크롤링 state 파일 (이전 상태) 저장 경로
        self.state_dir = os.path.join(self.output_dir, "crawler_state")
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f"announcement_state_{self.source}.json")

        # 2) Notices 저장할 디렉토리
        self.notices_dir = os.path.join(self.output_dir, "notices")
        os.makedirs(self.notices_dir, exist_ok=True)

        # 3) state 로드
        self.last_article_no = None
        self.last_page_url = None
        self.load_last_state()

        # Parser, Fetcher, Saver 초기화
        self.parser = AnnouncementParser(self.base_url, self.logger)
        self.fetcher = Fetcher(user_agents=None, logger=self.logger)

        # Saver를 이용한 로그(또는 배치처리) 저장 경로
        # original_file: 실제로 적재될 파일 이름
        # log_dir: 그 파일을 저장할 디렉토리
        self.saver = Saver(
            original_file="data_insert_logs.jsonl",
            logger=self.logger,
            batch_size=1,
            log_dir=os.path.join(self.output_dir, "insert_logs"),
        )
        self.log_dir = "./log"
        # Rotating Log Savers 초기화
        log_base_dir = os.path.join(self.log_dir, "api_logs")
        self.issac_logger = RotatingLogSaver(log_base_dir, "issac_logs")
        self.opensearch_logger = RotatingLogSaver(log_base_dir, "opensearch_logs")

    def load_last_state(self):
        """사이트별 state 파일에서 이전 상태를 로드"""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.last_article_no = state.get("last_article_no")
                self.last_page_url = state.get("last_page_url")
                self.logger.info(f"[{self.source}] Loaded state. last_article_no={self.last_article_no}")
        else:
            self.logger.info(f"[{self.source}] No previous state found.")

    def save_last_state(self, url, article_no):
        """사이트별 state 파일에 상태 저장"""
        
        # article_no가 None이거나 빈 문자열이면 저장하지 않음
        if not article_no:
            return

        state = {
            "last_article_no": article_no,
            "last_page_url": url,
        }
        
        with open(self.state_file, "w") as f:
            json.dump(state, f)

        # self.logger.info(f"[{self.source}] Saved state. last_article_no={article_no}")
        self.last_article_no = article_no
    
    
    def check_for_new_notices(self, max_checks=2):
        """
        신규 공지가 있는지 최대 max_checks번까지 확인하고,
        발견되는 즉시 크롤링 진행.
        """
        session = requests.Session()

        is_first_check = None

        # 현재 페이지 URL(초기에는 state 없으면 start_url에서 시작)
        if not self.last_page_url:
            is_first_check = True
            current_url = self.start_url
        else:
            is_first_check = False
            current_url = self.last_page_url

        # 최대 max_checks번 공지를 확인
        checks_done = 0
        while checks_done < max_checks:
            self.logger.info(f"[{self.source}] Check {checks_done+1}/{max_checks} for new notice...")
            new_notice_found = False

            # 1) 현재 URL 내용 파싱
            content = self.fetcher.fetch_page_content(session, current_url, source=self.source)
            if not content:
                self.logger.warning(f"[{self.source}] Failed to fetch content: {current_url}")
                break

            soup = BeautifulSoup(content, "html.parser")

            if is_first_check and self.is_new_post(current_url):
                self.logger.info(f"[{self.source}] New notice found: {current_url}")
                self.crawl_notices(current_url, session)
                new_notice_found = True
            else:
                self.logger.info(f"[{self.source}] Not first check")
                next_notice_url = self.get_next_notice_url(soup)
                if next_notice_url:
                    # 새 공지인지 확인
                    if self.is_new_post(next_notice_url):
                        self.logger.info(f"[{self.source}] New notice found: {next_notice_url}")
                        self.crawl_notices(next_notice_url, session)
                        new_notice_found = True
                    else:
                        self.logger.info(f"[{self.source}] Next notice is not new.")
                        # 다음 공지가 이미 본 공지라면 => 더 이상 새 공지 없음
                else:
                    self.logger.info(f"[{self.source}] No next notice link found.")
            
            checks_done += 1

            # 새 공지를 찾지 못했다면 반복 진행
            # (새 공지를 찾았더라도 '연속으로' 여러 개 있을 수 있으니 한 번 더 확인할 수도 있음)
            # ※ 여기서는 예시상 다음 공지를 "하나씩"만 추적하지만,
            #    상황에 따라 "while 다음 공지가 새 것이면 계속 가져오기" 로직으로 확장 가능
            if not new_notice_found:
                self.logger.info(f"[{self.source}] No new notice found in this check.")
                # 다음 check_for_new_notices() 호출 때 다시 시도
            else:
                # 한 번 새 공지를 찾았으면 다음 loop에서 또 다른 새 공지가 있는지 확인 가능
                # (새 공지가 여러 개 연속으로 있을 수 있으므로)
                pass

        session.close()


    def crawl_notices(self, notice_url, session=None):
        """
        notice_url부터 시작해서 '다음 공지'가 없거나
        새 글이 아닐 때까지 연쇄적으로 크롤링.
        """
        if session is None:
            session = requests.Session()

        url = notice_url
        while True:
            # self.logger.info(f"[{self.source}] Crawling notice: {url}")
            content = self.fetcher.fetch_page_content(session, url, source=self.source)
            if not content:
                self.logger.warning(f"[{self.source}] Failed to fetch content: {url}")
                break

            soup = BeautifulSoup(content, "html.parser")
            base_domain = self.parser.extract_domain(url)
            
            # (1) HTML -> JSON 변환
            json_data = self.parser.parse_notice(
                soup,
                base_domain,
                url,
                self.source,
                self.title_selector,
                self.date_selector,
                self.author_selector,
                self.content_selector,
                self.sub_category_selector,
            )

            # (2) 로컬 jsonl 저장
            file_path = os.path.join(self.notices_dir, f"notices_{self.source}.jsonl")
            JsonManager.save_to_jsonl(json_data, file_path)

            # (3) 원격 Insert (Opensearch 혹은 ISSAC API)
            self.index_to_issac(json_data)

            # self.index_to_opensearch(json_data)

            # (4) state 업데이트
            article_no = self.get_article_no_from_url(url)
            self.save_last_state(url, article_no)

            # 다음 공지 확인
            next_notice_url = self.get_next_notice_url(soup)
            if not next_notice_url or not self.is_new_post(next_notice_url):
                # 더 이상 새 글이 아니면 stop
                break
            url = next_notice_url

    def get_next_notice_url(self, soup):
        """
        다음 페이지/공지 링크를 selector로 찾고, javascript:...이 아닌 실제 URL이면 반환.
        """
        link = soup.select_one(self.next_page_selector)
        if not link:
            return None
        
        text = link.get_text(strip=True)
        # "등록된 글이 없습니다" 같은 문구 필터
        if "등록된 글이 없습니다" in text or "다음글이 없습니다." in text:
            return None
        href = link.get("href")
        if not href or "javascript" in href.lower():
            return None
        
        if self.source == "RC_EDUCATION":
            base_url = self.base_url + "/main/"
            return urljoin(base_url, href)

        return urljoin(self.base_url, href)


    def get_article_no_from_url(self, url):
        parsed = urlparse(url)
        # 1. 쿼리 파라미터에서 값 추출
        if self.url_number:
            query = parse_qs(parsed.query)
            if self.url_number in query:
                return query.get(self.url_number, [None])[0]

        # 2. 경로에서 값 추출 (url_number가 None이면 경로 처리)
        path_segments = parsed.path.strip('/').split('/')
        for segment in path_segments:
            if segment.isdigit():  # 숫자로 된 값 추출
                return segment

        # 값이 없으면 None 반환
        return None

    def is_new_post(self, url):
        """
        URL이 이전에 처리된 적이 없는 새로운 글인지 확인
        """
        if url in self.processed_urls:
            return False
        
        self.processed_urls.add(url)
        return True

    def index_to_issac(self, doc):
        """ISSAC API로 공지 데이터를 전송"""
        headers = {"Content-Type": "application/json"}
        log_data = {
            "status": "unknown",
            "url": doc.get("url"),
            "response": None
        }
        
        try:
            response = requests.post(
                ISSAC_ENDPOINT,
                auth=(ISSAC_USER, ISSAC_PASSWORD),
                headers=headers,
                json=doc,
            )
            if response.status_code in [200, 201]:
                log_data["status"] = "success"
                log_data["response"] = response.json()
                self.logger.info(f"[{self.source}] Inserted document to ISSAC.")
            else:
                log_data["status"] = "failure"
                log_data["response"] = {
                    "status_code": response.status_code,
                    "response_text": response.text
                }
                self.logger.error(f"[{self.source}] Failed to insert. {response.status_code}, {response.text}")
        except Exception as e:
            log_data["status"] = "error"
            log_data["response"] = {"exception": str(e)}
            self.logger.error(f"[{self.source}] Exception in index_to_issac: {e}")
        
        # 로그 저장
        self.issac_logger.save_log(log_data)

    def index_to_opensearch(self, doc):
        """OpenSearch에 문서 업로드"""
        api_url = f"{OPENSEARCH_ENDPOINT}/{OPENSEARCH_INDEX}/_doc"
        headers = {"Content-Type": "application/json"}
        
        log_data = {
            "status": "unknown",
            "url": doc.get("url"),
            "response": None
        }
        
        try:
            response = requests.post(
                api_url, 
                auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
                headers=headers,
                json=doc
            )
            if response.status_code in (200, 201):
                _id = response.json()["_id"]
                log_data["status"] = "success"
                log_data["response"] = {"_id": _id}
                self.logger.info(f"Successfully inserted document!")
            else:
                log_data["status"] = "failure"
                log_data["response"] = {
                    "status_code": response.status_code,
                    "response_text": response.text
                }
                self.logger.error(f"Failed to insert document. status: {response.status_code}, response: {response.text}")
        except Exception as e:
            log_data["status"] = "error"
            log_data["response"] = {"exception": str(e)}
            self.logger.error(f"Exception occurred while inserting document: {e}")
        
        # 로그 저장
        self.opensearch_logger.save_log(log_data)