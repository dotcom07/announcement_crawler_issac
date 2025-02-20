# parser.py

import re
import chardet
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import trafilatura
from boilerpy3 import extractors as boilerpy_extractors
import logging

class Parser:
    def __init__(self, base_domain, logger):
        self.base_domain = base_domain
        self.logger = logger

    def clean_text(self, text):
        if text is None:
            return ""
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'\n', ' ', text)
        return text

    def parse_table(self, table_element, base_url):
        table_object = {"table": []}
        cells_array = []
        rows = table_element.select("tr")

        # 1. 테이블의 최대 열 개수 계산
        max_col_count = 0
        for row in rows:
            col_count = 0
            # direct children을 찾기 위해 recursive=False 사용
            cols = row.find_all(['td', 'th'], recursive=False)
            for col in cols:
                colspan = int(col.get('colspan', 1))
                col_count += colspan
            max_col_count = max(max_col_count, col_count)

        # 2. cellMatrix 초기화 (최대 열 개수 사용)
        cell_matrix = [[None for _ in range(max_col_count)] for _ in range(len(rows))]
        current_row = 0

        for row in rows:
            cols = row.find_all(['td', 'th'], recursive=False)
            current_col = 0
            for col in cols:
                # 이미 채워진 셀인지 확인하고 비어있는 위치를 찾음
                while current_col < max_col_count and cell_matrix[current_row][current_col] is not None:
                    current_col += 1
                if current_col >= max_col_count:
                    break  # 더 이상 열이 없으면 다음 행으로

                cell_object = {}
                cell_object["text"] = self.clean_text(col.get_text())

                colspan = int(col.get('colspan', 1))
                rowspan = int(col.get('rowspan', 1))

                if colspan > 1:
                    cell_object["colspan"] = colspan
                if rowspan > 1:
                    cell_object["rowspan"] = rowspan

                # 이미지 링크 추출
                img_elements = col.select("img")
                if img_elements:
                    img_array = []
                    for img in img_elements:
                        img_src = img.get('src')
                        if img_src:
                            img_url = urljoin(base_url, img_src)
                            img_array.append(img_url)
                    cell_object["img_links"] = img_array

                # 링크 추출
                link_elements = col.select("a")
                if link_elements:
                    link_array = []
                    for link in link_elements:
                        href = link.get('href')
                        text = self.clean_text(link.get_text())
                        if href:
                            full_href = urljoin(base_url, href)
                            link_array.append({"href": full_href, "text": text})
                    cell_object["links"] = link_array

                # 현재 셀의 위치 정보 추가
                cell_object["row"] = current_row
                cell_object["col"] = current_col

                # cell_matrix에 셀을 추가 (rowspan과 colspan 처리)
                for i in range(rowspan):
                    for j in range(colspan):
                        if current_row + i < len(rows) and current_col + j < max_col_count:
                            cell_matrix[current_row + i][current_col + j] = cell_object

                cells_array.append(cell_object)
                current_col += colspan
            current_row += 1

        table_object["table"] = cells_array
        return table_object

    def extract_image_links(self, soup, base_url):
        images = set()
        for img in soup.find_all('img'):
            img_url = img.get('src') or img.get('data-src') or img.get('data-original')
            if img_url:
                absolute_url = urljoin(base_url, img_url)
                if re.search(r'\.(jpg|jpeg|png|gif|bmp|svg|webp|tiff|ico)$', absolute_url, re.IGNORECASE):
                    images.add(absolute_url)
        return list(images)

    def extract_file_links(self, soup, base_url):
        files = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.hwp']) or 'download' in href.lower():
                file_url = urljoin(base_url, href)
                parsed = urlparse(file_url)
                if parsed.scheme in ['http', 'https']:
                    if self.is_within_base_domain(parsed.netloc):
                        files.append(file_url)
        return files

    def extract_tables(self, soup, base_url):
        tables = []
        for table in soup.find_all('table'):
            try:
                parsed_table = self.parse_table(table, base_url)
                tables.append(parsed_table)
            except Exception as e:
                self.logger.error(f"테이블 파싱 오류 ({base_url}): {e}")
        return tables

    def extract_links(self, page_content, base_url):
        # base_url에 'www.'가 없으면 추가
        parsed_base = urlparse(base_url)
        if not parsed_base.netloc.startswith('www.'):
            base_url = f"{parsed_base.scheme}://www.{parsed_base.netloc}{parsed_base.path}"
            self.logger.debug(f"변경된 base_url: {base_url}")

        soup = BeautifulSoup(page_content, 'html.parser')
        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            self.logger.debug(f"추출된 href: {href}")
            if href.startswith('mailto:') or href.startswith('javascript:'):
                continue
            if 'download.jsp' in href.lower():
                continue
            hrefs = re.split(r'\s+', href)
            for href_part in hrefs:
                full_url = urljoin(base_url, href_part)
                self.logger.debug(f"변환된 링크: {full_url}")  # 모든 full_url을 출력
                parsed = urlparse(full_url)
                if parsed.scheme in ['http', 'https']:
                    if self.is_within_base_domain(parsed.netloc):
                        links.append(full_url)
        return links

    def is_within_base_domain(self, netloc):
        netloc = netloc.lower()
        return netloc == self.base_domain or netloc.endswith('.' + self.base_domain)

    def extract_and_merge_text(self, content, url):
        try:
            detected_encoding = chardet.detect(content)['encoding']
            if not detected_encoding:
                detected_encoding = 'utf-8'
            text = content.decode(detected_encoding, errors='replace')
        except Exception as e:
            self.logger.error(f"컨텐츠 디코딩 오류 ({url}): {e}")
            text = content.decode('utf-8', errors='replace')

        try:
            trafilatura_content = self.clean_text(trafilatura.extract(text))
        except Exception as e:
            self.logger.error(f"Trafilatura 추출 오류 ({url}): {e}")
            trafilatura_content = ""

        try:
            # HTML 정제 과정 추가
            soup = BeautifulSoup(text, 'html5lib')
            cleaned_html = soup.prettify()

            boilerpy_extractor = boilerpy_extractors.ArticleExtractor()
            boilerpy_content = self.clean_text(boilerpy_extractor.get_content(cleaned_html))
        except Exception as e:
            self.logger.error(f"BoilerPy3 추출 오류 ({url}): {e}")
            boilerpy_content = ""

        if boilerpy_content:
            merged_text = self.sliding_window_search_optimized(trafilatura_content, boilerpy_content)
        else:
            merged_text = trafilatura_content

        return merged_text

    # KMP 알고리즘 관련 함수
    def kmp_failure_function(self, pattern):
        m = len(pattern)
        pi = [0] * m
        j = 0
        for i in range(1, m):
            while (j > 0 and pattern[i] != pattern[j]):
                j = pi[j - 1]
            if pattern[i] == pattern[j]:
                j += 1
                pi[i] = j
        return pi

    def kmp_search(self, text, pattern):
        n, m = len(text), len(pattern)
        pi = self.kmp_failure_function(pattern)
        j = 0

        for i in range(n):
            while (j > 0 and text[i] != pattern[j]):
                j = pi[j - 1]
            if text[i] == pattern[j]:
                if j == m - 1:
                    return i - m + 1  # 첫 번째 매칭된 위치 반환
                j += 1
        return -1  # 패턴이 없으면 -1 반환

    def sliding_window_search_optimized(self, trafilatura_text, boilerpy_text, window_size=5):
        trafilatura_words = trafilatura_text.split()
        boilerpy_words = boilerpy_text.split()
        boilerpy_text_str = " ".join(boilerpy_words)

        for i in range(len(trafilatura_words) - window_size + 1):
            pattern = " ".join(trafilatura_words[i:i + window_size])
            pattern_pos = self.kmp_search(boilerpy_text_str, pattern)
            if pattern_pos != -1:
                extra_text = boilerpy_text_str[:pattern_pos].strip()
                trafilatura_words.insert(i, extra_text)
                break
        return " ".join(trafilatura_words)
