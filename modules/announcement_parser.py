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
        }
        self.file_handlers = {
            "SOCIOLOGY": self.handle_sociology_files,
            "CHEMICAL_ENGINEERING": self.handle_chemical_engineering_files,
            "CHEMISTRY": self.handle_chemistry_files,
            "EARTH_SYSTEM_SCIENCE": self.handle_earth_system_science_files,
        }

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
            print(file_id_match)
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

    def extract_domain(self,url):
        try:
            parsed_url = urlparse(url)
            return f"{parsed_url.scheme}://{parsed_url.netloc}/"  # 프로토콜 + 도메인
        except Exception as e:
            print(f"Invalid URL: {e}")
            return None

    def parse_notice(self, soup, base_domain, url, source, title_selector, date_selector, author_selector, content_selector, sub_category_selector):
        """
        프론트에 넘겨줄 JSON 구조에 맞게 파싱하는 메서드.
        """

        print(soup)

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
        date_text = date.get_text(strip=True).replace("작성일", "").strip() if date else ""

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