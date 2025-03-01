# /home/ubuntu/multiturn_ver1/new crawler/modules/announcement_crawler_for_notice_list.py

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import logging
import os
import json
import re
from .announcement_crawler import AnnouncementCrawler
from config.site_config import SITES
import time

class ListAnnouncementCrawler(AnnouncementCrawler):
    """
    분할 정복(모듈화) 예시:
      1) check_for_new_notices: 상태(state)에 따라 (a) 전체 역순 크롤링 or (b) 첫 페이지만 확인
      2) _crawl_full_in_reverse: 전체 페이지(또는 offset) 역순 크롤링
      3) _check_only_first_page_for_new: 첫 페이지에서 새 글이 있는지 확인
      4) _process_list_page: 목록 페이지 HTML 파싱 → 상세 페이지 크롤링
      5) crawl_notices: 단일 게시글 상세 페이지 파싱 + 저장 + state 갱신
    """

    def __init__(self, source, base_url, start_url, url_number, **kwargs):
        super().__init__(source, base_url, start_url, url_number, **kwargs)
        self.logger = logging.getLogger("AnnouncementCrawler")
        self.logger.info(f"[{self.source}] Initializing ListAnnouncementCrawler")
        # ------------------------------
        # 사이트별 URL 빌드/목록 파싱 핸들러
        # ------------------------------
        self.build_list_url_handlers = {
            "MAIN_DORM": self._build_list_url_main_dorm,
            "MATERIALS_SCIENCE_ENGINEERING": self._build_list_url_mse,
            "INTERNATIONAL_COLLEGE_STUDENT_SERVICES": self._build_list_url_uic_student_services,
            "INTERNATIONAL_COLLEGE_ACADEMIC_AFFAIRS": self._build_list_url_uic_academic_affairs,
            "ATMOSPHERIC_SCIENCE": self._build_list_url_atmospheric_science,
            "PHYSICS": self._build_list_url_physics,
            "POLITICAL_SCIENCE": self._build_list_url_political_science,
            "PSYCHOLOGY": self._build_list_url_psychology,
            "BUSINESS_COLLEGE" : self._build_list_url_business_college
        }

        self.parse_list_page_handlers = {
            "MAIN_DORM": self._parse_list_page_main_dorm,
            "MATERIALS_SCIENCE_ENGINEERING": self._parse_list_page_mse,
            "INTERNATIONAL_COLLEGE_STUDENT_SERVICES": self._parse_list_page_uic_student_services,
            "INTERNATIONAL_COLLEGE_ACADEMIC_AFFAIRS": self._parse_list_page_uic_academic_affairs,
            "ATMOSPHERIC_SCIENCE": self._parse_list_page_atmospheric_science,
            "PHYSICS": self._parse_list_page_physics,
            "POLITICAL_SCIENCE": self._parse_list_page_political_science,
            "PSYCHOLOGY": self._parse_list_page_psychology,
            "BUSINESS_COLLEGE" : self._parse_list_page_business_college
        }

        # SIT 계열 사이트 자동 매핑
        sit_date_selectors = ("#jwxe_main_content > div > div.board-wrap > div > dl:nth-child(2) > dd" ,"#jwxe_main_content > div > div.board-wrap > div > dl:nth-child(4) > dd" ,"#jwxe_main_content > div > div > div > dl:nth-child(4) > dd")
        print(self.date_selector)
        if self.date_selector in sit_date_selectors:
            print("SIT 스타일 매핑")
            self.build_list_url_handlers[self.source] = self._build_list_url_sit_like
            self.parse_list_page_handlers[self.source] = self._parse_list_page_sit_like

        # offset 기반 사이트인지 핸들러로 판별
        self.is_offset_based = (self.build_list_url_handlers.get(self.source) == self._build_list_url_sit_like)
        
        self.existing_psychology_ids = set()  # 이미 저장된 article_id
        self._load_existing_psychology_ids()  # 초기화 시 한번 로드

    def _load_existing_psychology_ids(self):
        """
        별도의 article_ids 파일에서 기존 ID들을 로드
        """
        if self.source == "PSYCHOLOGY":
            article_ids_file = os.path.join(self.state_dir, f"article_ids_{self.source}.txt")
            if os.path.exists(article_ids_file):
                with open(article_ids_file, "r", encoding="utf-8") as file:
                    for line in file:
                        article_id = line.strip()
                        if article_id:
                            self.existing_psychology_ids.add(article_id)
                self.logger.info(f"Loaded {len(self.existing_psychology_ids)} existing article_ids for {self.source}")
            else:
                self.logger.info(f"No existing article_ids file found for {self.source}. Starting fresh.")

    # --------------------------------------------------
    # A. 메인 진입: 체크 & 크롤링
    # --------------------------------------------------
    def check_for_new_notices(self, max_pages=10, max_checks=2):
        """
        state(=last_article_no)가 없으면 => _crawl_full_in_reverse
        state가 있으면 => _check_only_first_page_for_new
        
        Args:
            max_pages (int): 전체 크롤링 시 최대 페이지 수 (기본값: 10)
            max_checks (int): 첫 페이지 확인 횟수 (기본값: 2)
        """
        if self.source == "PSYCHOLOGY" and self.existing_psychology_ids == set():
            self.logger.info(f"[{self.source}] No existing article_ids file found for {self.source}. Starting fresh.")
            self._crawl_full_in_reverse(max_pages)
        else :
            if self.source != "PSYCHOLOGY" and self.last_article_no is None:
                self.logger.info(f"[{self.source}] No state => FULL CRAWL in reverse order.")
                self._crawl_full_in_reverse(max_pages)
            else:
                self.logger.info(f"[{self.source}] Found last_article_no={self.last_article_no} => check first page {max_checks} times.")
                self._check_only_first_page_for_new(max_checks)

    # --------------------------------------------------
    # B. 전체 역순 크롤링
    # --------------------------------------------------
    def _crawl_full_in_reverse(self, max_pages=10):
        """
        ex) offset 기반이면 40,30,20,10,0
            p 기반이면 10,9,8,7,6...1

        높은 페이지(=오래된 페이지)부터 시작하여,
        각 페이지 내에서는 오래된 글부터 정렬 → JSONL 최종 결과:
        위는 예전글, 아래는 최신글
        """
        session = requests.Session()
        page_list = self._generate_reverse_page_list(max_pages)

        if self.source =="PSYCHOLOGY" :
            self._process_list_page(session, 0, first_crawl=True)
        else :
            for page_param in page_list:
                self._process_list_page(session, page_param, first_crawl=True)

        session.close()

    # --------------------------------------------------
    # C. 첫 페이지에서만 새 글 확인
    # --------------------------------------------------
    def _check_only_first_page_for_new(self, max_checks=2):
        """
        offset=0 또는 p=1 페이지를 가져와
        새 글(article_no > last_article_no)만 상세 크롤링
        
        Args:
            max_checks (int): 첫 페이지 확인 횟수
        """
        session = requests.Session()
        first_page_param = 0 if self.is_offset_based else 1

        for check in range(max_checks):
            self.logger.info(f"[{self.source}] Checking first page (attempt {check + 1}/{max_checks})")
            self._process_list_page(session, first_page_param, first_crawl=False)
        
        session.close()

    # --------------------------------------------------
    # D. 목록 페이지 처리
    # --------------------------------------------------
    def _process_list_page(self, session, page_param, first_crawl=False):
        """
        1) list_url 빌드
        2) 페이지 가져오기
        3) parse_list_page (링크,url_number)
        4) (첫 크롤링이면) 모두 크롤 / (아니면) 새 글만 크롤
        """
        list_url = self._build_list_url(page_param)
        # self.logger.info(f"[{self.source}] Fetching list page: {list_url}")

        if (self.source=="POLITICAL_SCIENCE") :
            content = self.fetcher.fetch_with_form_data(session, list_url, source=self.source, page_param=page_param)
        else : 
            content = self.fetcher.fetch_page_content(session, list_url, source=self.source)

        if not content:
            # self.logger.warning(f"[{self.source}] Failed to fetch list page: {list_url}")
            return

        soup = BeautifulSoup(content, "html.parser")
        post_links = self.parse_list_page(soup) # 에러 
        print(post_links)
        if not post_links:
            self.logger.info(f"[{self.source}] No posts found at {list_url}")
            return

        # url_number를 기준으로 오름차순 정렬 (오래된 글부터)
        # 숫자가 아닐 수도 있으므로 예외 처리
        def _to_int_or_zero(x):
            try:
                return int(x[1])
            except:
                return 0
        post_links.sort(key=_to_int_or_zero)

        if self.source == "PSYCHOLOGY":
            for post_url, article_id in reversed(post_links):  # 역순으로 순회
                if first_crawl:
                    self.logger.info(f"[{self.source}] (Full) Found post: {post_url} (article_id: {article_id})")
                    self.process_notice_detail(soup, post_url, article_id)
                else:
                    if self.is_new_post_by_id(article_id):
                        self.logger.info(f"[{self.source}] Found NEW post: {post_url} (article_id: {article_id})")
                        self.process_notice_detail(soup, post_url, article_id)
                    else:
                        self.logger.debug(f"[{self.source}] Old post => skip: article_id={article_id}")

        else :
            for post_url, article_id, *optional in post_links:
                sub_category = optional[0] if optional else None  # 존재하면 가져오고, 없으면 None
                if first_crawl:
                    self.logger.info(f"[{self.source}] (Full) Found post: {post_url} (article_id: {article_id})")
                    # self.crawl_notices(post_url, session=session)
                    self.crawl_notices(post_url, session=session, article_id=article_id, sub_category= sub_category)
                else:
                    if self.is_new_post_by_id(article_id):
                        self.logger.info(f"[{self.source}] Found NEW post: {post_url} (article_id: {article_id})")
                        self.crawl_notices(post_url, session=session, sub_category= sub_category)
                    else:
                        self.logger.debug(f"[{self.source}] Old post => skip: article_id={article_id}")

    
    # --------------------------------------------------
    # E. 상세 페이지 크롤링
    # --------------------------------------------------
    def crawl_notices(self, notice_url, session=None, article_id=None, sub_category=None):
        """
        단일 게시글 상세 페이지 파싱 + 저장 + state 갱신
        """
        if session is None:
            session = requests.Session()

        
        # self.logger.info(f"[{self.source}] Crawling detail page: {notice_url}")

        if(self.source=="POLITICAL_SCIENCE") :
            content = self.fetcher.fetch_with_form_data(session, notice_url, source=self.source, no=article_id)
        else :
            content = self.fetcher.fetch_page_content(session, notice_url, source=self.source)

        if not content:
            self.logger.warning(f"[{self.source}] Failed to fetch detail: {notice_url}")
            return

        soup = BeautifulSoup(content, "html.parser") 

        self.process_notice_detail(soup, notice_url, article_id, sub_category= sub_category)

    # --------------------------------------------------
    # E-1. 상세 페이지 데이터를 처리하는 메소드
    # --------------------------------------------------
    def process_notice_detail(self, soup, notice_url, article_id, sub_category= None):
        """
        상세 페이지 데이터를 처리하는 메소드:
        1. HTML -> JSON 변환
        2. 로컬 JSONL 저장
        3. Opensearch/ISSAC 전송 (옵션)
        4. 상태 갱신
        """
        # (1) HTML -> JSON 변환
        if self.source=="PSYCHOLOGY" :
            json_data = self.parser.parse_psychology_notice(
                soup=soup,
                base_domain=self.parser.extract_domain(notice_url),
                url=notice_url,
                source=self.source,
                title_selector=self.title_selector,
                date_selector=self.date_selector,
                author_selector=self.author_selector,
                content_selector=self.content_selector,
                sub_category_selector=self.sub_category_selector,
                article_id=article_id
            )
        else :

            json_data = self.parser.parse_notice(
                soup=soup,
                base_domain=self.parser.extract_domain(notice_url),
                url=notice_url,
                source=self.source,
                title_selector=self.title_selector,
                date_selector=self.date_selector,
                author_selector=self.author_selector,
                content_selector=self.content_selector,
                sub_category_selector=self.sub_category_selector,
                pre_fetched_sub_category=sub_category
            )

        # (2) 로컬 jsonl 저장
        from .json_manager import JsonManager
        file_path = os.path.join(self.notices_dir, f"notices_{self.source}.jsonl")


        if self.source=="PSYCHOLOGY" :
            article_id_psy = json_data["article_id"]
            json_data = {k: v for k, v in json_data.items() if k != "article_id"}
            JsonManager.save_to_jsonl(json_data, file_path)

            # article_id를 별도 파일에 저장
            article_ids_file = os.path.join(self.state_dir, f"article_ids_{self.source}.txt")
            with open(article_ids_file, "a", encoding="utf-8") as f:
                f.write(f"{article_id_psy}\n")
                
            # 메모리의 캐시도 업데이트
            self.existing_psychology_ids.add(article_id_psy)
        else :
            JsonManager.save_to_jsonl(json_data, file_path)

        # (3) 필요하다면 Opensearch/ISSAC 등 원격 전송
        # self.index_to_issac(json_data)
        # self.index_to_opensearch(json_data)


        if(self.source=="POLITICAL_SCIENCE") :
            article_id = article_id
        else :
            article_id = self.get_article_no_from_url(notice_url)


        if article_id and self.is_new_post_by_id(article_id):
            self.save_last_state(notice_url, article_id)

    # --------------------------------------------------
    # F. 도우미 (URL 빌드/목록 파싱/역순 페이지 목록)
    # --------------------------------------------------
    def _generate_reverse_page_list(self, max_pages):
        """
        offset 기반 -> [ (max_pages-1)*10, ..., 0 ]
        p 기반 -> [ max_pages, ..., 1 ]

        즉, "페이지 번호 큰 것부터 → 작은 것 순"으로 반환.
        (보통 page=1이 최신글, page가 클수록 오래된 글)
        """
        if self.is_offset_based:
            start_offset = (max_pages - 1) * 10
            return [start_offset - 10*i for i in range(max_pages)]
        else:
            return list(range(max_pages, 0, -1))

    def _build_list_url(self, page_param):
        handler = self.build_list_url_handlers.get(self.source)
        if handler:
            return handler(page_param)
        else:
            # fallback (offset 기반)
            return f"{self.base_url}?mode=list&articleLimit=10&article.offset={page_param}"

    def parse_list_page(self, soup):
        handler = self.parse_list_page_handlers.get(self.source)
        if handler:
            print("헨들러에 soup pass")
            return handler(soup)
        return []
    
     # --------------------------------------------------
    # G. 새 글 판별
    # --------------------------------------------------
    def is_new_post_by_id(self, article_id):
        """
        article_id로 새 글인지 판별
        """
        if self.source != "PSYCHOLOGY" and self.last_article_no is None:
            return True
        
        # 심리학의 경우: 로컬 JSONL 파일에서 article_id를 확인
        if self.source == "PSYCHOLOGY":
            if article_id in self.existing_psychology_ids:
                self.logger.info(f"[{self.source}] Skipping existing post: {article_id}")
                return False
            return True

        try:
            current = int(article_id)
            saved = int(self.last_article_no)
            return (current > saved)
        except:
            # 변환 실패 시 True
            return True

    # --------------------------------------------------
    # 1) 예시: MAIN_DORM (p 기반)
    # --------------------------------------------------
    def _build_list_url_main_dorm(self, page_index):
        return f"{self.base_url}/board/?id=notice&p={page_index}"

    from urllib.parse import urljoin

    def _parse_list_page_main_dorm(self, soup):
        post_links = []

        # 데스크톱용 게시글 리스트만 처리 (모바일용 제외)
        rows = soup.select("table.table-board tbody tr.hide_when_mobile")

        for row in rows:
            # 게시글 링크 추출 (공지사항과 일반 게시글 모두)
            a_tag = row.select_one("td a")
            if not a_tag:
                continue

            # 게시글 URL 생성 (절대 경로)
            detail_url = urljoin(self.base_url, a_tag.get("href", ""))
            
            # 게시글 ID 추출
            article_id = self.get_article_no_from_url(detail_url)
            
            # 게시글 정보 저장 (ID와 URL)
            if article_id:
                post_links.append((detail_url, article_id))
        
        return post_links


    # --------------------------------------------------
    # 2) 예시: SIT 계열 (offset 기반)
    # --------------------------------------------------
    def _build_list_url_sit_like(self, offset):
        self.logger.info(f"\n[{self.source}] Building list URL with offset: {offset}\n")
        return f"{self.base_url}?mode=list&articleLimit=10&article.offset={offset}"

    def _parse_list_page_sit_like(self, soup):
        post_links = []
        
        # 현재 offset 확인
        print("offset 확인")
        # 1. 페이지 오프셋(현재 페이지 번호) 추출
        if self.source in ("INDUSTRY_ENGINEERING", "SYSTEMS_BIOLOGY"):
            page_offset_el = soup.select_one("div.board-wrap ul.board-list-wrap li:first-child dt a")
        elif self.source in  ("ECONOMICS_COLLEGE", "SPORTS_APPLIED_INDUSTRY", "PHILOSOPHY", "CHINESE_LANGUAGE_LITERATURE","INTERDISCIPLINARY_MAJOR"):
            page_offset_el = soup.select_one("tbody tr:first-child td.text-left div.c-board-title-wrap a.c-board-title")
        else:
            page_offset_el = soup.select_one("a.c-board-title")

        href = page_offset_el.get("href", "") if page_offset_el else ""
        if not href:
            print("해당 요소를 찾을 수 없습니다.")

        current_offset = self.get_current_offset_from_url(href)
        is_first_page = (current_offset is None or current_offset == 0)

        # 2. 게시글 추출
        if self.source in ("INDUSTRY_ENGINEERING", "SYSTEMS_BIOLOGY"):
            # 새 구조 처리 (UL/li 기반)
            board_list_items = soup.select("div.board-wrap ul.board-list-wrap li")
            for li in board_list_items:
                # <span class="board-list-num">의 텍스트에 "공지"가 포함되어 있으면 고정 게시글로 판단
                num_tag = li.select_one("span.board-list-num")
                if num_tag and "공지" in num_tag.get_text(strip=True) and not is_first_page:
                    continue  # 고정 게시글은 첫 페이지에서만 처리

                a_tag = li.select_one("dt.board-list-content-title a")
                if a_tag:
                    detail_url = urljoin(self.base_url, a_tag.get("href", ""))
                    article_id = self.get_article_no_from_url(detail_url)
                    if article_id:
                        post_links.append((detail_url, article_id))
            return post_links

        elif self.source in  ("ECONOMICS_COLLEGE", "SPORTS_APPLIED_INDUSTRY" ,"PHILOSOPHY","CHINESE_LANGUAGE_LITERATURE","INTERDISCIPLINARY_MAJOR"):
            # ECONOMICS_COLLEGE - table 기반 (tbody/tr)
            table_rows = soup.select("tbody tr")
            for row in table_rows:
                # ECONOMICS_COLLEGE의 게시글은 <td class="text-left"> 내부의 <div class="c-board-title-wrap"> 안에 존재함
                a_tag = row.select_one("td.text-left div.c-board-title-wrap a.c-board-title")
                if a_tag:
                    detail_url = urljoin(self.base_url, a_tag.get("href", ""))
                    article_id = self.get_article_no_from_url(detail_url)
                    if article_id:
                        post_links.append((detail_url, article_id))
            return post_links

        else:
            # 기존 구조 처리 (table 기반)
            # 2-1. 상단 고정 공지사항 (첫 페이지에서만)
            if is_first_page:
                fixed_notices = soup.select("tr.c-board-top-wrap")
                for row in fixed_notices:
                    a_tag = row.select_one("div.c-board-title-wrap a.c-board-title")
                    if a_tag:
                        detail_url = urljoin(self.base_url, a_tag.get("href", ""))
                        article_id = self.get_article_no_from_url(detail_url)
                        if article_id:
                            post_links.append((detail_url, article_id))
            # 2-2. 일반 게시글 (모든 페이지)
            normal_posts = soup.select("tr:not(.c-board-top-wrap)")
            for row in normal_posts:
                a_tag = row.select_one("td.text-left a.c-board-title")
                if a_tag:
                    detail_url = urljoin(self.base_url, a_tag.get("href", ""))
                    article_id = self.get_article_no_from_url(detail_url)
                    if article_id:
                        post_links.append((detail_url, article_id))
            return post_links

    def get_current_offset_from_url(self, url):
        """URL에서 현재 offset 값을 추출. 없으면 None 반환"""
        print(url)
        try:
            parsed = parse_qs(urlparse(url).query)
            if 'article.offset' in parsed:
                return int(parsed['article.offset'][0])
            return None
        except:
            return None

    # --------------------------------------------------
    # 3) 예시: MSE (pg 기반)
    # --------------------------------------------------
    def _build_list_url_mse(self, page_index):
        """
        MSE 게시판 목록 URL 생성
        예: https://mse.yonsei.ac.kr/board/board.php?bo_table=notice&cate=undergraduate&pg=1
        """
        # base_url에 /board 추가
        board_base = urljoin(self.base_url, "board/")
        return f"{board_base}board.php?bo_table=notice&cate=undergraduate&pg={page_index}"

    def _parse_list_page_mse(self, soup):
        post_links = []
    
        first_href = soup.select_one("#main > div.section.section1 > div > ul > li a").get("href", "")
        parsed_url = urlparse(first_href)
        query_params = parse_qs(parsed_url.query)
        
        # pg 값 가져오기 (없으면 기본값 1로 설정)
        pg = int(query_params.get("pg", ["1"])[0])  # pg가 없으면 기본값 1 사용
        if(pg==1):
            rows = soup.select("#main > div.section.section1 > div > ul > li")  # top 클래스 제외 저장
        else : 
            rows = soup.select("#main > div.section.section1 > div > ul > li:not(.top)")  # top 클래스 제외 저장
        
        for row in rows:
            # print(row)
            a_tag = row.select_one("a")
            if not a_tag:
                continue

            # href에서 상대 경로를 가져와서 base_url + /board/와 결합
            href = a_tag.get("href", "")

            if href:
                board_base = urljoin(self.base_url, "board/")
                detail_url = urljoin(board_base, href)
                article_id = self.get_article_no_from_url(detail_url)

                if article_id:
                    # print(article_id)
                    post_links.append((detail_url, article_id))
        
        return post_links

    # --------------------------------------------------
    # H. UIC 학생 서비스 게시판
    # --------------------------------------------------
    def _build_list_url_uic_student_services(self, page_index):
        """
        UIC 학생 서비스 게시판 목록 URL 생성
        예: https://uic.yonsei.ac.kr/main/news.asp?mid=m06_01_03&page=1
        """
        return f"{self.base_url}/main/news.asp?mid=m06_01_03&page={page_index}"

    def _parse_list_page_uic_student_services(self, soup):
        post_links = []
        rows = soup.select("table#Board tbody tr")
        
        for row in rows:
            # 제목 링크 추출
            a_tag = row.select_one("td.Subject a")
            if not a_tag:
                continue

            href = a_tag.get("href", "")
            if href:
                detail_url = urljoin(self.base_url, "main/" + href)
                # URL에서 idx 파라미터 추출
                article_id = self.get_article_no_from_url(detail_url)
                
                if article_id:
                    post_links.append((detail_url, article_id))
        
        return post_links
    
    # --------------------------------------------------
    # H. UIC  academic_affairs 게시판
    # --------------------------------------------------
    def _build_list_url_uic_academic_affairs(self, page_index):
        """
        UIC 학생 서비스 게시판 목록 URL 생성
        예: https://uic.yonsei.ac.kr/main/news.asp?mid=m06_01_02&page=1
        """
        return f"{self.base_url}/main/news.asp?mid=m06_01_02&page={page_index}"

    def _parse_list_page_uic_academic_affairs(self, soup):
        post_links = []
        rows = soup.select("table#Board tbody tr")
        
        for row in rows:
            # 제목 링크 추출
            a_tag = row.select_one("td.Subject a")
            if not a_tag:
                continue

            href = a_tag.get("href", "")
            if href:
                detail_url = urljoin(self.base_url, "main/" + href)
                # URL에서 idx 파라미터 추출
                article_id = self.get_article_no_from_url(detail_url)
                
                if article_id:
                    post_links.append((detail_url, article_id))
        
        return post_links
    
    # --------------------------------------------------
    # I. 대기과학과 게시판
    # --------------------------------------------------
    def _build_list_url_atmospheric_science(self, page_index):
        """
        대기과학과 게시판 목록 URL 생성
        - page 1은 기본 URL 사용
        - page 2 이상만 /page/N/ 형식 사용
        """
        if page_index == 1:
            return f"{self.base_url}/categories/%EA%B3%B5%EC%A7%80%EC%82%AC%ED%95%AD/"
        else:
            return f"{self.base_url}/categories/%EA%B3%B5%EC%A7%80%EC%82%AC%ED%95%AD/page/{page_index}/"

    def _parse_list_page_atmospheric_science(self, soup):
        post_links = []
        sections = soup.select("div#blog-listing-medium section.post")
        
        for section in sections:
            # 제목과 링크 추출
            title_link = section.select_one("div.col-xs-11 h3 a")
            if not title_link:
                continue
            
            href = title_link.get("href", "")
            if not href:
                continue
            
            # URL에서 article_id 추출
            # 예: https://atmos.yonsei.ac.kr/blog/2024/12/17/notice_20241217_doublemajor/
            # article_id는 YYYYMMDD 형식으로 추출
            match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/notice_(\d{8})(?:_[^/]*)?/?$', href)
            if match:
                year, month, day, article_id = match.groups()
                # 모든 게시글이 공지사항으로 표시되어 있음 (bullhorn 아이콘)
                post_links.append((href, article_id))
            
        return post_links

    def get_article_no_from_url(self, url):
        """URL에서 article_no 추출"""
        try:
            if self.source == "ATMOSPHERIC_SCIENCE":
                # 대기과학과: YYYYMMDD 형식 추출
                match = re.search(r'/notice_(\d{8})(?:_[^/]*)?/?$', url)
                if match:
                    return match.group(1)  # 예: "20250120"
            else:
                # site_config의 url_number 설정 활용
                url_number = SITES[self.source].get('url_number')
                if url_number:
                    pattern = rf'{url_number}=(\d+)'  # raw string 사용
                    match = re.search(pattern, url)
                    if match:
                        return match.group(1)
            
            # fallback: 기존 로직
            match = re.search(r'articleNo=(\d+)|idx=(\d+)|num=(\d+)|id=(\d+)', url)
            if match:
                return next(g for g in match.groups() if g is not None)
        except:
            pass
        return None
    
     # --------------------------------------------------
    # J. 물리학과 게시판
    # --------------------------------------------------

    def _build_list_url_physics(self, page_index):
        """
        물리학과 게시판 목록 URL 생성
        예: https://physicsyonsei.kr/notice/board?page=1
        https://physicsyonsei.kr
        """
        return f"{self.base_url}/notice/board?page={page_index}"

    def _parse_list_page_physics(self, soup):
        """
        물리학과 게시판 목록 페이지 파싱
        """
        post_links = []
        rows = soup.select("table.bl_list tbody tr")
        
        for row in rows:
            # 제목 링크 추출
            a_tag = row.select_one("td.td-subject a")
            if not a_tag:
                continue
            
            href = a_tag.get("href", "")
            if href:
                detail_url = urljoin(self.base_url, href)
                # URL에서 idx 파라미터 추출
                article_id = self.get_article_no_from_url(detail_url)
                
                if article_id:
                    post_links.append((detail_url, article_id))
        return post_links
    
    # --------------------------------------------------
    # K. 정치외교학과 게시판
    # --------------------------------------------------
    def _build_list_url_political_science(self, page_index):
        """
        정치외교학과 게시판 목록 URL 생성(POLITICAL_SCIENCE)
        POST http://politics.yonsei.ac.kr/board.asp
        """
        return f"{self.base_url}/board.asp"

    def _parse_list_page_political_science(self, soup):
        """
        정치외교학과 게시판 목록 페이지 파싱
        """
        post_links = []

        # 목록 테이블에서 행 선택
        rows = soup.select("table.table_com01.board_table_basic > tr")

        for row in rows:
            # 제목 링크 추출
            a_tag = row.select_one("td.board_table_subject > a[href^='javascript:view']")
            if not a_tag:
                continue
            
            href = a_tag.get("href", "")
            if href:
                detail_url = SITES["POLITICAL_SCIENCE"]["start_url"]
                article_id = href.split("(")[1].split(")")[0]  # javascript:view(숫자)에서 숫자 추출
                
                if article_id:
                    # 필요한 데이터만 저장
                    post_links.append((detail_url, article_id))

        return post_links

    # --------------------------------------------------
    # L. 심리학과 게시판
    # --------------------------------------------------
    def _build_list_url_psychology(self, page_index):
        """
        심리학과 게시판 목록 URL 생성(POLITICAL_SCIENCE)
        GET https://psychsci.yonsei.ac.kr/
        """
        return f"{self.base_url}"

    def _parse_list_page_psychology(self, soup):
        """
        심리학과 게시판 목록 페이지 파싱
        """
        post_links = []
        excluded_ids = {"h.7620e385d18695f5_0", "h.2bc2e14f6cc928f1_3", "h.29ecd76589e67e7d_64"}  # 아예 제외할 ID 집합
        deferred_ids = {"h.3e89b5824c6d8e90_5", "h.1313e84639b07f9c_4"}  # 나중에 추가할 ID 집합
        deferred_links = []  # 나중에 추가할 ID 저장

        # div 태그 중 role="main"인 요소 선택
        main_div = soup.find('div', role='main')
        if main_div:
            # main_div 내부에서 class="yaqOZd qeLZfd"를 가진 section 태그 찾기
            for section in main_div.find_all('section', class_='yaqOZd qeLZfd'):
                section_id = section.get('id')
                if section_id:
                    if section_id in excluded_ids:
                        # 제외할 ID는 처리하지 않음
                        continue
                    elif section_id in deferred_ids:
                        # 나중에 추가할 ID는 deferred_links에 저장
                        deferred_links.append((SITES["PSYCHOLOGY"]["start_url"], section_id))
                    else:
                        # 일반적인 ID는 바로 post_links에 추가
                        post_links.append((SITES["PSYCHOLOGY"]["start_url"], section_id))

        # 나중에 추가할 ID를 post_links의 마지막에 추가
        post_links.extend(deferred_links)

        return post_links

    # --------------------------------------------------
    # K. 경영대학(학부) 게시판
    # --------------------------------------------------
    def _build_list_url_business_college(self, page_index):
        """
        경영대학(학부) 게시판 목록 URL 생성(POLITICAL_SCIENCE)
        GET board.asp?mid=m06%5F01&cmid=m06%5F01&cid=0&eid=&bgn=&bid=7&sOpt=&pact=&mType=&mCourse=&mKisu=&tabidx=&hSel=&act=list&keyword=&uid=&page=37
        """
        return f"{self.base_url}/board.asp?mid=m06%5F01&cmid=m06%5F01&cid=0&eid=&bgn=&bid=7&sOpt=&pact=&mType=&mCourse=&mKisu=&tabidx=&hSel=&act=list&keyword=&uid=&page={page_index}"

    def _parse_list_page_business_college(self, soup):
        # BUSINESS_COLLEGE
        """
        경영대학(학부) 게시판 목록 페이지 파싱
        """
        post_links = []

        # 목록 테이블에서 행 선택
        rows = soup.select("#Board > tbody > tr")
        for row in rows:
            # 제목 링크 추출
            a_tag = row.select_one("td.Subject > a")
            if not a_tag:
                continue
            
            title_text = a_tag.text.strip()  # 제목 텍스트 추출
            
            # [ ] 안의 값(subcategories) 추출
            match = re.search(r"\[(.*?)\]", title_text)
            sub_category = match.group(1) if match else None  # 없으면 None

            # print(f"a_tag: {title_text}, subcategories: {sub_category}")  # 디버깅 출력

            href = a_tag.get("href", "")
            if href:
                detail_url = SITES["BUSINESS_COLLEGE"]["base_url"] + href
                article_id = self.get_article_no_from_url(detail_url)
                post_links.append((detail_url, article_id, sub_category))

        return post_links

    
