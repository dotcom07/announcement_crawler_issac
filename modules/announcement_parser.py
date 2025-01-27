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

        # 파일 링크를 찾을 수 있는 다양한 패턴 검색
        link_patterns = [
            ('a', {'href': True}),  # 기본 링크
            ('a', {'onclick': lambda x: 'download' in x.lower() if x else False}),  # onclick 다운로드
            ('p.file a', {}),  # 파일 클래스를 가진 p 태그 내의 링크
            ('span[data-ellipsis="true"]', {})  # data-ellipsis 속성을 가진 span
        ]

        for selector, attrs in link_patterns:
            for element in soup.select(selector) if selector.find('.') >= 0 else soup.find_all(selector, attrs):
                href = element.get('href', '')
                
                # "내려받기" 텍스트를 가진 링크 처리
                if element.get_text(strip=True) == "내려받기":
                    parent_p = element.find_parent('p', class_='file')
                    if parent_p:
                        file_name_span = parent_p.find('span', attrs={'data-ellipsis': 'true'})
                        if file_name_span:
                            file_name = file_name_span.get_text(strip=True)
                            file_url = urljoin(base_url, href)
                            if file_url not in excluded_urls:
                                files.append({"name": file_name, "url": file_url})
                    continue

                # 일반적인 파일 링크 처리
                if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.hwp']) or 'download' in href.lower():
                    file_url = urljoin(base_url, href)
                    parsed = urlparse(file_url)

                    if file_url in excluded_urls:
                        continue

                    if parsed.scheme in ['http', 'https']:
                        # 파일명 추출 로직
                        title_attr = element.get('title', '')
                        if title_attr:
                            file_name = title_attr.replace('다운로드', '').strip()
                        else:
                            file_name = element.get_text(strip=True)

                        if not file_name:
                            file_name = os.path.basename(parsed.path)

                        files.append({"name": file_name, "url": file_url})

        # 중복 제거를 위해 URL을 키로 사용하는 딕셔너리 사용
        unique_files = {}
        for file_info in files:
            url = file_info['url']
            if url not in unique_files:
                unique_files[url] = file_info
        
        return list(unique_files.values())
    


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
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_text):
                return date_text
            
            # YYYY.MM.DD 형식 처리
            elif re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_text):
                return date_text.replace('.', '-')
            
            # YY.MM.DD 형식 처리
            elif re.match(r'^\d{2}\.\d{2}\.\d{2}$', date_text):
                year = int(date_text[:2])
                year = f"20{year}" if year < 50 else f"19{year}"
                return f"{year}-{date_text[3:5]}-{date_text[6:8]}"
            
            # YYYYMMDD 형식 처리
            elif re.match(r'^\d{8}$', date_text):
                return f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:]}"
            
            # YYYY/MM/DD 형식 처리
            elif re.match(r'^\d{4}/\d{2}/\d{2}$', date_text):
                return date_text.replace('/', '-')
            
            # YY/MM/DD 형식 처리
            elif re.match(r'^\d{2}/\d{2}/\d{2}$', date_text):
                year = int(date_text[:2])
                year = f"20{year}" if year < 50 else f"19{year}"
                return f"{year}-{date_text[3:5]}-{date_text[6:8]}"
            
            # YYYY.MM.DD / HH:MM 형식 처리
            elif re.match(r'^\d{4}\.\d{2}\.\d{2} / \d{2}:\d{2}$', date_text):
                date_only = date_text.split('/')[0].strip()
                return date_only.replace('.', '-')
            
            else:
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

    def parse_psychology_notice(self, soup, base_domain, url, source, title_selector, date_selector, author_selector, content_selector, sub_category_selector, article_id):
        """
        프론트에 넘겨줄 JSON 구조에 맞게 파싱하는 메서드.
        """
        # tables = []

        # subCategory, author 추출
        sub_category = ""
        author_text = ""
        plainText = ""
        content_html = ""
        title=""
        # 특정 section_id에 해당하는 section 찾기
        section = soup.find("section", id=article_id)

        if not section:
            return f"Section with id '{article_id}' not found."
        
        # section 내부의 class='C9DxTc'인 태그 찾기
        text_elements = section.find_all(class_="C9DxTc")
        if text_elements:
            # 텍스트 합치기
            title = " ".join([element.get_text(strip=True) for element in text_elements]) 
        else:
            # iframe 태그 중 jsname="L5Fo6c"인 친구 찾기
            iframe_element = section.find("iframe", attrs={"jsname": "L5Fo6c"})
            if iframe_element:
                title = iframe_element.get("aria-label")
            else:
                print("No iframe with jsname='L5Fo6c' found.")

        # section 내부의 class='oKdM2c ZZyype'인 태그 찾기
        content_elements = section.find_all(class_="oKdM2c ZZyype")
        if content_elements:
            # 순수 텍스트 합치기 (HTML 태그 제거)
            plainText = " ".join([element.get_text(strip=True) for element in content_elements])

            # src 및 href 처리 (이미지나 링크가 상대경로로 되어 있을 경우 base_domain 추가)
            for element in content_elements:
                # src 처리
                for img in element.find_all('img', src=True):
                    src = img['src']
                    if src.startswith('/'):
                        img['src'] = urljoin(base_domain, src)

                # href 처리
                for a_tag in element.find_all('a', href=True):
                    href = a_tag['href']

                    if not href.startswith('http') and not href.startswith('javascript'):
                        a_tag['href'] = urljoin(base_domain, href)
                    
                    # 구글 폼 버튼 삭제
                    if 'oWHwWc' in a_tag.get('class', []):
                        a_tag.decompose()  # 'oWHwWc' 클래스를 가진 a 태그는 삭제

            # HTML 합치기
            content_html = "".join([str(element) for element in content_elements])


            # content가 있으면서 링크 바로가기가 있는 경우
            first_link_added = False  # 첫 번째 링크인지 확인하는 플래그
            for link_element in section.find_all('span', class_="C9DxTc aw5Odc"):
                if not first_link_added:  # 첫 번째 링크에만 <br/><br/> 추가
                    content_html += "<br/><br/>" + str(link_element)
                    first_link_added = True
                else:
                    content_html += str(link_element)  # 나머지는 그대로 추가
            

        else:
            # content_elements가 없을 경우 제목의 내용을 그대로 넣어준다
            alternative_element = section.find("div", jscontroller="Ae65rd")
            if alternative_element:
                # 순수 텍스트 합치기
                plainText = alternative_element.get_text(strip=True)

                # HTML 내용 가져오기
                content_html = str(alternative_element)

                # src 및 href 처리
                for img in alternative_element.find_all('img', src=True):
                    src = img['src']
                    if src.startswith('/'):
                        img['src'] = urljoin(base_domain, src)

                for a_tag in alternative_element.find_all('a', href=True):
                    href = a_tag['href']
                    if not href.startswith('http') and not href.startswith('javascript'):
                        a_tag['href'] = urljoin(base_domain, href)

        content_html = content_html.replace('\\"', "'")
        extracted_files = self.extract_file_links(soup, self.base_domain, source)

        json_object = {
            "university": "YONSEI",
            "source": source,
            "url": url,
            "subCategory": "",
            "author": "",
            "title": title,
            "createdDate": "2025-01-01",
            "rawContent": content_html,
            "content": plainText,
            "files": extracted_files,
            "article_id" : article_id
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