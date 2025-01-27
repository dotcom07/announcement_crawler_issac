# /home/ubuntu/multiturn_ver1/new crawler/modules/announcement_parser.py

import sys
import os
from .parser import Parser
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging
import re

class AnnouncementParser(Parser):
    def __init__(self, base_domain, logger):
        super().__init__(base_domain, logger)
        self.source_handlers = {
            "ACADEMIC_NOTICE": self.handle_academic_notice,
            "BUSINESS_SCHOOL": self.handle_business_school,
            "CHEMICAL_ENGINEERING": self.handle_chemical_engineering,
            "SONGDO_DORM": self.handle_songdo_dorm,
            "INTERNATIONAL_COLLEGE_STUDENT_SERVICES": self.handle_international_college,
            "INTERNATIONAL_COLLEGE_ACADEMIC_AFFAIRS" : self.handle_international_college, # UIC 추가
            "ATMOSPHERIC_SCIENCE": self.handle_atmospheric_science,  # 대기과학과 추가
            "PHYSICAL_EDUCATION": self.handle_physical_education,  # 체육교육학과 추가
            "PHYSICS": self.handle_physics,  # 물리학과 추가
            "POLITICAL_SCIENCE" : self.handle_political_science,
        }
        self.file_handlers = {
            "SOCIOLOGY": self.handle_sociology_files,
            "CHEMICAL_ENGINEERING": self.handle_chemical_engineering_files,
            "CHEMISTRY": self.handle_chemistry_files,
            "EARTH_SYSTEM_SCIENCE": self.handle_earth_system_science_files,
            "GLOBAL_TALENT_COLLEGE": self.handle_global_talent_college_files,
             "POLITICAL_SCIENCE" : self.handle_political_science_files,
        }
        self.logger = logger

    # 상위 클래스 Parser의 extract_file_links를 오버라이드
    def extract_file_links(self, soup, base_url, source=None):
        files = []
        
        # 특수 처리가 필요한 사이트인 경우 해당 핸들러 호출
        if source in self.file_handlers:
            return self.file_handlers[source](soup, base_url)

        excluded_urls = [
            "https://che.yonsei.ac.kr/che/reunion/download.do"
        ]

        for link in soup.find_all('a', href=True):
            href = link['href']
            # 확장자 또는 'download'가 포함된 링크만 처리
            if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.hwp']) or 'download' in href.lower():
                file_url = urljoin(base_url, href)  # base_url과 결합
                parsed = urlparse(file_url)

                if file_url in excluded_urls:
                    continue

                # domain 체크를 수행하는 경우
                if parsed.scheme in ['http', 'https']:
                    # 파일명 추출
                    title_attr = link.get('title', '')
                    if title_attr:
                        file_name = title_attr.replace('다운로드', '').strip()
                    else:
                        file_name = link.get_text(strip=True)

                    if not file_name:
                        file_name = os.path.basename(parsed.path)

                    files.append({"name": file_name, "url": file_url})
                    
        return files
    


    def handle_sociology_files(self, soup, base_url):
        files = []
        for button in soup.find_all('button', class_='kboard-button-download'):
            onclick = button.get('onclick', '')
            href_match = re.search(r"window\.location\.href='([^']+)'", onclick)
            if href_match:
                file_url = urljoin(base_url, href_match.group(1))
                file_name = button.get_text(strip=True)
                files.append({
                    "name": file_name,
                    "url": file_url
                })
        return files

    def handle_chemical_engineering_files(self, soup, base_url):
        files = []
        for link in soup.find_all('a', onclick=True):
            onclick = link.get('onclick', '')
            file_id_match = re.search(r"fwBbs\.Down\('(\d+)'\)", onclick)
            if file_id_match:
                file_id = file_id_match.group(1)
                file_name = link.get('title', '') or link.get_text(strip=True)
                files.append({
                    "name": file_name,
                    "url": file_id  # 파일 ID만 문자열로 저장
                })
        return files
    
    def handle_chemistry_files(self, soup, base_url):
        files = []
        for file_item in soup.select("li.nxb-view__files-item"):
            link = file_item.select_one("a.nxb-view__files-link")
            if link:
                href = link.get('href', '')
                # 파일명은 nxb-view__files-text에서 추출하고 용량 정보는 제외
                file_name_elem = file_item.select_one(".nxb-view__files-text")
                if file_name_elem:
                    file_name = file_name_elem.get_text(strip=True)
                    # 용량 정보 제거 (예: "(3.01 MBKB)" 제거)
                    file_name = re.sub(r'\s*\([^)]+\)\s*$', '', file_name)
                    
                    files.append({
                        "name": file_name,
                        "url": urljoin(base_url, href)
                    })
        return files
    
    def handle_earth_system_science_files(self, soup, base_url):
        files = []
        for attachment_div in soup.select("div.attachment"):
            for li in attachment_div.select("ul li"):
                link = li.select_one("span.attach_down a")
                if link:
                    href = link.get('href', '')
                    # 파일명은 이미지 태그 다음의 텍스트 노드에서 추출
                    file_name = ''
                    for content in li.contents:
                        if isinstance(content, str):
                            file_name = content.strip()
                            break
                    
                    if file_name:
                        files.append({
                            "name": file_name,
                            "url": urljoin(base_url, href)
                        })
        return files

    def handle_global_talent_college_files(self, soup, base_url):
        files = []
        for button in soup.find_all('button', class_='kboard-button-download'):
            onclick = button.get('onclick', '')
            href_match = re.search(r"window\.location\.href='([^']+)'", onclick)
            if href_match:
                file_url = urljoin(base_url, href_match.group(1))
                file_name = button.get('title', '') or button.get_text(strip=True)
                files.append({
                    "name": file_name,
                    "url": file_url
                })
        return files
    
    def handle_political_science_files(self, soup, base_url):
        files = []
        # 파일 정보를 포함한 td 태그 선택
        file_elements = soup.select("td.board_file_basic a")

        for file_element in file_elements:
            file_url = file_element.get("href", "").strip()
            file_name = file_element.select_one("u").text.strip() if file_element.select_one("u") else file_element.text.strip()
            
            if file_url:
                # 절대경로로 변환
                full_url = urljoin(base_url, file_url)
                files.append({
                    "name": file_name,
                    "url": full_url
                })
        
        return files

    def extract_domain(self,url):
        try:
            parsed_url = urlparse(url)
            return f"{parsed_url.scheme}://{parsed_url.netloc}/"  # 프로토콜 + 도메인
        except Exception as e:
            print(f"Invalid URL: {e}")
            return None

    def standardize_date(self, date_text):
        """
        다양한 날짜 형식을 YYYY-MM-DD 형식으로 표준화
        날짜 범위의 경우 첫 번째 날짜만 표준화
        """
        if not date_text:
            return ""
        
        # 불필요한 텍스트 제거
        remove_words = [
            "작성일", "등록일", "날짜", "게시일", ":", "작성일자",
            "작성 일자", "게시 일자", "등록 일자"
        ]
        for word in remove_words:
            date_text = date_text.replace(word, "")
        
        # 앞뒤 공백 제거
        date_text = date_text.strip()
        
        # 괄호 제거 (예: "(2024-01-30)" -> "2024-01-30")
        date_text = date_text.strip('()')
        
        # 날짜 범위에서 첫 번째 날짜만 추출
        if '~' in date_text:
            date_text = date_text.split('~')[0].strip()
        
        # 시간 정보가 있는 경우 날짜만 추출
        date_parts = date_text.split()
        if date_parts:
            date_text = date_parts[0]
        
        try:
            # YY-MM-DD 형식을 YYYY-MM-DD로 변환
            if re.match(r'^\d{2}-\d{2}-\d{2}$', date_text):
                year = int(date_text[:2])
                year = f"20{year}" if year < 50 else f"19{year}"
                return f"{year}-{date_text[3:5]}-{date_text[6:8]}"
            
            # 이미 YYYY-MM-DD 형식인 경우
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_text):
                return date_text
            
            # YYYY.MM.DD 형식 처리
            if re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_text):
                return date_text.replace('.', '-')
            
            # YY.MM.DD 형식 처리
            if re.match(r'^\d{2}\.\d{2}\.\d{2}$', date_text):
                year = int(date_text[:2])
                year = f"20{year}" if year < 50 else f"19{year}"
                return f"{year}-{date_text[3:5]}-{date_text[6:8]}"
            
            # YYYYMMDD 형식 처리
            if re.match(r'^\d{8}$', date_text):
                return f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:]}"
            
            # YYYY/MM/DD 형식 처리
            if re.match(r'^\d{4}/\d{2}/\d{2}$', date_text):
                return date_text.replace('/', '-')
            
            # YY/MM/DD 형식 처리
            if re.match(r'^\d{2}/\d{2}/\d{2}$', date_text):
                year = int(date_text[:2])
                year = f"20{year}" if year < 50 else f"19{year}"
                return f"{year}-{date_text[3:5]}-{date_text[6:8]}"
            
            # YYYY.MM.DD / HH:MM 형식 처리
            if re.match(r'^\d{4}\.\d{2}\.\d{2} / \d{2}:\d{2}$', date_text):
                date_only = date_text.split('/')[0].strip()
                return date_only.replace('.', '-')
            
            self.logger.warning(f"Unknown date format: {date_text}")
            return date_text
        
        except Exception as e:
            self.logger.error(f"Error standardizing date '{date_text}': {str(e)}")
            return date_text

    def parse_notice(self, soup, base_domain, url, source, title_selector, date_selector, author_selector, content_selector, sub_category_selector):
        """
        프론트에 넘겨줄 JSON 구조에 맞게 파싱하는 메서드.
        """
        # tables = []

        # subCategory, author 추출
        sub_category = ""
        author_text = ""
        plainText = ""
        content_html = ""

        title = soup.select_one(title_selector)
        title_text = title.get_text(strip=True) if title else ""

        author_tag = soup.select_one(author_selector)
        author_text = author_tag.get_text(strip=True) if author_tag else ""

        sub_category_tag = soup.select_one(sub_category_selector)
        sub_category = sub_category_tag.get_text(strip=True) if sub_category_tag else ""

        date = soup.select_one(date_selector)
        date_text = date.get_text(strip=True) if date else ""
        date_text = self.standardize_date(date_text)

        # content: .fr-view 내부 HTML 전부
        content_element = soup.select_one(content_selector)

        if content_element:
            content_html = str(content_element)
            # 1) \" -> '
            content_html = content_html.replace('\\"', "'")
            
            if base_domain.endswith('/'):
                base_domain = base_domain[:-1]
                
            # 2) src="/..." → src="base_domain/..."
            pattern_src = r'src="(\/[^"]+)"'
            replacement_src = fr'src="{base_domain}\1"'
            content_html = re.sub(pattern_src, replacement_src, content_html)
            
            # 3) href가 http로 시작하지 않는 경우 base_domain 추가
            pattern_href = r'href="(?!http[s]?://)((?!javascript:)[^"]+)"'
            replacement_href = fr'href="{base_domain}\1"'
            content_html = re.sub(pattern_href, replacement_href, content_html)

            for element in content_element.select("*"):  # 모든 자식 태그
                text_content = element.get_text(strip=True)
                if text_content:
                    plainText += text_content + " "

        extracted_files = self.extract_file_links(soup, self.base_domain, source)



        if source in self.source_handlers:
            sub_category, author_text, date_text = self.source_handlers[source](
                soup, sub_category, author_text, date_text
            )
        else:   
            print(f"No handler found for source: {source}")

        json_object = {
            "university": "YONSEI",
            "source": source,
            "url": url,
            "subCategory": sub_category,
            "author": author_text,
            "title": title_text,
            "createdDate": date_text,
            "rawContent": content_html,
            "content": plainText,
            "files": extracted_files,
            # "tables": tables
        }

        return json_object
    
    def handle_academic_notice(self, soup, sub_category, author_text, date_text):
        title_element = soup.select_one("span.title")
        tline_element = soup.select_one("span.title span.tline")

        if title_element and tline_element:
            total_text = title_element.get_text(strip=True)
            tline_text = tline_element.get_text(strip=True)

            if tline_text in total_text:
                sub_category = total_text.split(tline_text)[0].strip()
                author_text = tline_text

        return sub_category, author_text, date_text


    def handle_business_school(self, soup, sub_category, author_text, date_text):
        date_div = soup.select_one("#BoardViewAdd")
        if date_div:
            # "등록일: 2025-01-10" 형식에서 날짜만 추출
            date_match = re.search(r'등록일:\s*(\d{4}-\d{2}-\d{2})', date_div.get_text())
            if date_match:
                date_text = date_match.group(1)
        return sub_category, author_text, date_text

    def handle_chemical_engineering(self, soup, sub_category, author_text, date_text):
        date_li = soup.select_one("li:has(strong:contains('날짜'))")
        if date_li:
            # "2024.12.11" 형식을 "2024-12-11" 형식으로 변환
            date_text = date_li.get_text(strip=True).replace('날짜', '').strip()
            if date_text:
                date_text = date_text.replace('.', '-')
        return sub_category, author_text, date_text

    def handle_songdo_dorm(self, soup, sub_category, author_text, date_text):
        date_div = soup.select_one("#BBSBoardViewDate2")
        if date_div:
            # "날짜2024-10-17" 형식에서 날짜만 추출
            date_text = date_div.get_text(strip=True).replace("날짜", "").strip()
        return sub_category, author_text, date_text
    
    def handle_international_college(self, soup, sub_category, author_text, date_text):
        """
        UIC 게시판 날짜 파싱
        예: "Jan 20, 2025" -> "2025-01-20"
        """
        date_div = soup.select_one("#BoardViewAdd")
        if date_div:
            # "Jan 20, 2025  |  Read: 398" 형식에서 날짜만 추출
            date_str = date_div.get_text(strip=True).split('|')[0].strip()
            
            # 월 이름을 숫자로 변환하기 위한 매핑
            month_map = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            }
            
            try:
                # "Jan 20, 2025" 파싱
                month_str, day_str, year_str = date_str.replace(',', '').split()
                month = month_map.get(month_str, '01')  # 기본값 01
                day = day_str.zfill(2)  # 한 자리 날짜를 두 자리로
                
                # YYYY-MM-DD 형식으로 변환
                date_text = f"{year_str}-{month}-{day}"
            except Exception as e:
                self.logger.warning(f"Failed to parse date: {date_str} - {e}")
        
        return sub_category, author_text, date_text

    def handle_atmospheric_science(self, soup, sub_category, author_text, date_text):
        """
        대기과학과 게시판 날짜 파싱
        예: "December 22, 2021" -> "2021-12-22"
        """
        date_p = soup.select_one("p.text-muted.text-uppercase.mb-small.text-right")
        if date_p:
            date_str = date_p.get_text(strip=True)
            
            # 월 이름을 숫자로 변환하기 위한 매핑 (대소문자 구분 없이)
            month_map = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            try:
                # "December 22, 2021" 파싱
                month_str, day_str, year_str = date_str.replace(',', '').split()
                month = month_map.get(month_str.lower(), '01')  # 대소문자 구분 없이 처리
                day = day_str.zfill(2)  # 한 자리 날짜를 두 자리로
                
                # YYYY-MM-DD 형식으로 변환
                date_text = f"{year_str}-{month}-{day}"
            except Exception as e:
                self.logger.warning(f"Failed to parse date: {date_str} - {e}")
        
        return sub_category, author_text, date_text

    def handle_physical_education(self, soup, sub_category, author_text, date_text):
        """
        체육교육학과 게시판 날짜 파싱
        예: "게시일 : 2024-07-11" -> "2024-07-11"
        """
        date_div = soup.select_one("div.article-date")
        if date_div:
            # "게시일 : " 제거하고 날짜만 추출
            date_text = date_div.get_text(strip=True).replace("게시일 :", "").strip()
            
            # YYYY-MM-DD 형식 검증
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_text):
                return sub_category, author_text, date_text
            
        return sub_category, author_text, date_text

    def handle_physics(self, soup, sub_category, author_text, date_text):
        """
        물리학과 게시판 날짜 파싱
        예: "2024.12.24" -> "2024-12-24"
        """
        # 이미 standardize_date에서 처리되므로 그대로 반환
        return sub_category, author_text, date_text
    
    def handle_political_science(self, soup, sub_category, author_text, date_text):
        return sub_category, author_text, date_text