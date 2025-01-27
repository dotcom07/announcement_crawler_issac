# /home/ubuntu/multiturn_ver1/new crawler/modules/announcement_crawler_for_ARCHITECTURE_ENGINEERING.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import os
import re
import json
from datetime import datetime

# 예시이므로, 실제로는 사용자 코드 구조에 맞춰 import 경로 수정 필요
from .announcement_parser import AnnouncementParser
from .fetcher import Fetcher
from .json_manager import JsonManager


class ARCHITECTURE_ENGINEERING_AnnouncementCrawler:
    def __init__(
        self, 
        source,
        base_url,
        start_url,
        url_number,
        title_selector,
        date_selector,
        author_selector,
        content_selector,
        sub_category_selector,
        next_page_selector,
        logger,
        max_pages=10
    ):
        self.source = source
        self.base_url = base_url
        self.start_url = start_url
        self.url_number = url_number
        self.title_selector = title_selector
        self.date_selector = date_selector
        self.author_selector = author_selector
        self.content_selector = content_selector
        self.sub_category_selector = sub_category_selector
        self.next_page_selector = next_page_selector

        # 한 번에 몇 페이지까지 역순으로 확인할지
        self.max_pages = max_pages

        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  # 로그 레벨 INFO 정도로

        # 출력/저장 디렉토리
        self.output_dir = r"./output"
        os.makedirs(self.output_dir, exist_ok=True)

        self.notices_dir = os.path.join(self.output_dir, "notices")
        os.makedirs(self.notices_dir, exist_ok=True)

        # 크롤러 state 저장 디렉토리
        self.state_dir = os.path.join(self.output_dir, "crawler_state")
        os.makedirs(self.state_dir, exist_ok=True)
        self.state_file = os.path.join(self.state_dir, f"announcement_state_{self.source}.json")

        # 과거 상태 (중복 체크, last_date_id 등)
        self.last_date_id = 0
        self.seen_title_hashes = set()

        # 현재 페이지 (for sticky 처리용)
        self.current_page = 0

        # state 로드
        self.is_first_crawl_done = False
        
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
                self.last_date_id = state.get('last_date_id', 0)
                self.seen_title_hashes = set(state.get('seen_title_hashes', []))
                self.is_first_crawl_done = state.get('is_first_crawl_done', False)

        # 파서, 페처 초기화
        self.parser = AnnouncementParser(self.base_url, self.logger)
        self.fetcher = Fetcher(user_agents=None, logger=self.logger)

    def load_state(self):
        """이전 상태 불러오기"""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.last_date_id = data.get("last_date_id", 0)
                seen_list = data.get("seen_titles", [])
                self.seen_title_hashes = set(seen_list)
            self.logger.info(
                f"[{self.source}] 이전 상태 로드: last_date_id={self.last_date_id}, "
                f"seen_titles={len(self.seen_title_hashes)}"
            )
        else:
            self.logger.info(f"[{self.source}] 저장된 state 파일이 없어 새로 시작합니다.")

    def save_state(self):
        """현재 상태 저장"""
        state = {
            'last_date_id': self.last_date_id,
            'seen_title_hashes': list(self.seen_title_hashes),
            'is_first_crawl_done': self.is_first_crawl_done
        }
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        self.logger.info(
            f"[{self.source}] 상태 저장 완료. last_date_id={self.last_date_id}, "
            f"seen_titles={len(self.seen_title_hashes)}"
        )

    def check_for_new_notices(self):
        session = requests.Session()
        temp_seen_hashes = set()  # 임시 해시 저장용
        
        try:
            if not self.is_first_crawl_done:
                self.logger.info(f"[{self.source}] 최초 크롤링: {self.max_pages}페이지부터 1페이지까지")
                
                # 10페이지부터 2페이지까지는 임시 해시셋 사용
                for page_num in range(self.max_pages, 1, -1):
                    self.current_page = page_num
                    list_url = self.build_list_url(page_num)
                    
                    try:
                        html = self.fetcher.fetch_page_content(session, list_url, source=self.source)
                        if not html:
                            continue
                            
                        soup = BeautifulSoup(html, "html.parser")
                        posts = self.parse_list_page(soup)
                        
                        for detail_url, date_id, title, sub_category in posts:
                            title_hash = hash(title)
                            if title_hash not in temp_seen_hashes:
                                self.crawl_detail(session, detail_url, date_id, title, sub_category)
                                temp_seen_hashes.add(title_hash)
                                
                    except Exception as e:
                        self.logger.error(f"[{self.source}] Error on page {page_num}: {str(e)}")
                        continue
                
                # 1페이지는 영구 저장할 해시셋 사용
                self.current_page = 1
                list_url = self.build_list_url(1)
                try:
                    html = self.fetcher.fetch_page_content(session, list_url, source=self.source)
                    if html:
                        soup = BeautifulSoup(html, "html.parser")
                        posts = self.parse_list_page(soup)
                        
                        for detail_url, date_id, title, sub_category in posts:
                            title_hash = hash(title)
                            if title_hash not in self.seen_title_hashes and title_hash not in temp_seen_hashes:
                                self.crawl_detail(session, detail_url, date_id, title, sub_category)
                                self.seen_title_hashes.add(title_hash)  # 첫 페이지 해시만 영구 저장
                except Exception as e:
                    self.logger.error(f"[{self.source}] Error on page 1: {str(e)}")
                
                # 최초 크롤링 완료
                self.is_first_crawl_done = True
                self.save_state()
                
            else:
                # 이후 크롤링: 1페이지만 확인
                self.current_page = 1
                list_url = self.build_list_url(1)
                
                try:
                    html = self.fetcher.fetch_page_content(session, list_url, source=self.source)
                    if html:
                        soup = BeautifulSoup(html, "html.parser")
                        posts = self.parse_list_page(soup)
                        
                        for detail_url, date_id, title, sub_category in posts:
                            title_hash = hash(title)
                            if date_id > self.last_date_id and title_hash not in self.seen_title_hashes:
                                self.crawl_detail(session, detail_url, date_id, title, sub_category)
                                self.seen_title_hashes.add(title_hash)  # 첫 페이지 해시만 저장
                                
                except Exception as e:
                    self.logger.error(f"[{self.source}] Error on page 1: {str(e)}")
        
        finally:
            self.save_state()
            session.close()

    def build_list_url(self, page_num):
        """건축공학과 공지사항: 1페이지는 '/notice', 2페이지부터는 '/notice/page/N'"""
        if page_num == 1:
            return f"{self.base_url}/notice"
        else:
            return f"{self.base_url}/notice/page/{page_num}"

    def parse_list_page(self, soup):
        """
        1. sticky 행은 따로 분류하여 정렬
        2. 일반 행도 따로 정렬
        3. 현재 페이지가 1이면 정렬된 sticky도 포함하여 반환
        """
        post_list = []
        sticky_posts = []

        rows = soup.select("table.board-list tbody tr")
        for row in rows:
            # sticky 여부 확인
            is_sticky = "sticky" in row.get("class", [])

            # 카테고리
            cat_a = row.select_one("td.packed > a")
            if not cat_a:
                continue
            sub_category = cat_a.get_text(strip=True)

            # 제목/URL 추출
            title_a = row.select_one("td.title > a")
            if not title_a:
                continue

            # (중요) 모바일 뷰 날짜용 div를 먼저 제거하여, 제목에 섞이지 않게 처리
            mobile_date_div = title_a.select_one("div.hide-on-med-and-up")
            if mobile_date_div:
                mobile_date_div.extract()  # DOM에서 제거

            # 추출 후의 제목
            title_text = title_a.get_text(strip=True)

            # 실제 상세 URL
            detail_url = urljoin(self.base_url, title_a.get("href", ""))
            # URL 끝에 /page/xx 붙어 있으면 정리
            detail_url = re.sub(r'/page/\d+$', '', detail_url)

            # 날짜: PC뷰 기준 td.packed.hide-on-small-only 안쪽의 <a>에서 먼저 시도
            date_a = row.select_one("td.packed.hide-on-small-only > a")
            if date_a:
                date_str = date_a.get_text(strip=True)
            else:
                # 없다면 모바일뷰 div에서 가져와야 하는데, 위에서 extract 해버렸으므로
                # extract하기 전에 미리 가져오거나, 여기서는 row기준 다른 셀에서 찾을 수도 있음
                # 여기서는 그냥 title_a 내의 mobile_date_div에서 가져왔다고 가정
                date_str = mobile_date_div.get_text(strip=True) if mobile_date_div else ""

            date_id = self.convert_date_to_id(date_str)

            # 분류에 따라 배열 분리
            post_data = (detail_url, date_id, title_text, sub_category)
            if is_sticky:
                sticky_posts.append(post_data)
            else:
                post_list.append(post_data)

        # sticky_posts와 post_list를 각각 날짜순으로 정렬
        sticky_posts.sort(key=lambda x: x[1])  # date_id로 정렬
        post_list.sort(key=lambda x: x[1])     # date_id로 정렬

        # 현재 페이지가 1이라면 정렬된 sticky 게시물도 합쳐서 반환
        if self.current_page == 1:
            post_list.extend(sticky_posts)

        return post_list

    def convert_date_to_id(self, date_str):
        """
        '2024-12-30' 같은 형태를 20241230 정수로 변환.
        매칭 실패하거나 오류 시 0 리턴.
        """
        try:
            if not date_str:
                return 0
            temp = date_str.strip().replace('.', '-').replace('/', '-')
            match = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', temp)
            if match:
                y, m, d = match.groups()
                dt = datetime(int(y), int(m), int(d))
                return int(dt.strftime("%Y%m%d"))
            else:
                return 0
        except Exception:
            return 0

    def crawl_detail(self, session, detail_url, date_id, title, sub_category):
        """상세 페이지 크롤 -> JSONL 저장 -> 상태 갱신"""
        try:
            html = self.fetcher.fetch_page_content(session, detail_url, source=self.source)
            if not html:
                self.logger.error(f"[{self.source}] 상세페이지 로드 실패: {detail_url}")
                return

            soup = BeautifulSoup(html, "html.parser")

            # Parser 사용
            # (사용자 환경에 따라 필요 필드들 조정)
            parsed_json = self.parser.parse_notice(
                soup=soup,
                base_domain=self.parser.extract_domain(detail_url),
                url=detail_url,
                source=self.source,
                title_selector=self.title_selector,
                date_selector=self.date_selector,
                author_selector=self.author_selector,
                content_selector=self.content_selector,
                sub_category_selector=self.sub_category_selector
            )

            # 목록에서 이미 얻은 정보(날짜, 서브카테고리, 제목) 보정
            parsed_json["createdDate"] = self.format_date_id(date_id)
            parsed_json["subCategory"] = sub_category
            parsed_json["title"] = title

            # JSONL로 저장
            out_path = os.path.join(self.notices_dir, f"notices_{self.source}.jsonl")
            JsonManager.save_to_jsonl(parsed_json, out_path)

            # 크롤 상태 갱신
            if date_id > self.last_date_id:
                self.last_date_id = date_id
            self.seen_title_hashes.add(hash(title))

            self.logger.info(f"[{self.source}] 새 공지 저장 완료: {title}")

        except Exception as e:
            self.logger.error(f"[{self.source}] 상세 페이지 크롤 중 오류: {detail_url}, {str(e)}")

    def format_date_id(self, date_id):
        """20241230 -> '2024-12-30' 변환"""
        if date_id < 10000101:
            return ""
        ds = str(date_id)
        return f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
