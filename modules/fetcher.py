# fetcher.py

import requests
import random
import time
import urllib3
from urllib.parse import urlparse, urljoin
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class Fetcher:
    def __init__(self, user_agents=None, logger=None):
        # 기본 User-Agent를 설정
        self.USER_AGENTS = user_agents or [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        self.logger = logger
        
        # 소스별 헤더 정의
        self.source_headers = {
            "RC_EDUCATION": {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "ko,en;q=0.9,en-US;q=0.8,fr;q=0.7",
                "Connection": "keep-alive",
                "Cookie" : "_ga_FTVBS928F1=GS1.1.1735906281.1.1.1735906434.20.0.0; _gid=GA1.3.1623754372.1737811808; _INSIGHT_CK_8301=dbe7d70b216aa0c433b2d18e62a96194_65369|2cb54202339961c5b3a8f79a059bc846_66952:1737868762000; _ga_E0C08CRK91=GS1.1.1737866953.48.1.1737866966.0.0.0; redirectURL=%2FsessionCheck.jsp; _ga_HW5FTJ2V8K=GS1.3.1737874386.2.1.1737874391.0.0.0; JSESSIONID=t9NLCanvKsD7U3OqIWGqStyGtrCBEWDH6pxWZKvwgNsNlZgIvgQk2aYh5fM0rguR.amV1c19kb21haW4vQ09MTEVHRV9XRUJDT04yXzE=; _ga_41NHQ5C0D6=GS1.1.1737880428.2.1.1737881929.0.0.0; _ga_YCE3MS7XVY=GS1.1.1737880853.2.1.1737882012.0.0.0; _ga_H88ES32E7W=GS1.1.1737882014.3.1.1737882024.0.0.0; AUTHN_ID=_ed3ce6e820a1b1cbce8c84a052f1a2a0; _ga=GA1.3.1393282193.1735365371; _INSIGHT_CK_8308=f6f83b21f3f1018d2348dd32ab918b33_61277|4671dc2421a4f213e1d06f2874060ebc_82317:1737884669000; _ga_D6YPRL1XE5=GS1.3.1737882317.2.1.1737882869.0.0.0; ASPSESSIONIDSSAACQRB=ABGDPMICFJPLMFJNMOBFAJEI; _INSIGHT_CK_8304=15d8d7125dcd31457256771bcd464503_97373|83324b12d1af02556ee1b90cd5fb2ccb_86062:1737888069000",
                "Host": "yicrc.yonsei.ac.kr",
                "Referer": "https://yicrc.yonsei.ac.kr/main/news.asp?mid=m06_01",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
            "POLITICAL_SCIENCE" : {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "http://politics.yonsei.ac.kr",
                "Referer": "http://politics.yonsei.ac.kr/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Host": "politics.yonsei.ac.kr",
            }
            # 다른 소스의 헤더들도 여기에 추가
        }

    def get_headers(self, source):
        """소스에 맞는 헤더 반환"""
        headers = self.source_headers.get(source, {}).copy()
        # User-Agent는 항상 랜덤하게 설정
        headers['User-Agent'] = random.choice(self.USER_AGENTS)
        return headers

    def fetch_page_content(self, session, url, source=None, retries=10, backoff_factor=2, max_backoff=100, initial_timeout=30, max_total_timeout=200):
        headers = self.get_headers(source) if source else {'User-Agent': random.choice(self.USER_AGENTS)}
        attempt = 0
        backoff = backoff_factor  # 초기 대기 시간 (초)
        timeout = initial_timeout  # 타임아웃 시간
        total_time_spent = 0  # 총 소요 시간

        while attempt < retries and total_time_spent < max_total_timeout:
            try:
                start_time = time.time()
                response = session.get(url, headers=headers, verify=False, allow_redirects=True, timeout=timeout)

                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time
                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type:
                        time.sleep(random.uniform(0.1, 0.5))  # 짧은 지연 시간 추가
                        return response.content
                    else:
                        self.logger.warning(f"비HTML 컨텐츠 ({content_type}) for URL: {url}. 스킵합니다.")
                        return None
                elif 500 <= response.status_code < 600:
                    # 서버 오류 시 재시도
                    attempt += 1
                    self.logger.warning(f"서버 오류 {response.status_code} for URL: {url}. 재시도 중... (Attempt {attempt}/{retries})")
                    if attempt >= retries:
                        break
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)  # 지수 백오프 적용
                else:
                    # 클라이언트 오류: 로깅 후 재시도하지 않음
                    self.logger.error(f"클라이언트 오류 {response.status_code} for URL: {url}. 재시도하지 않음.")
                    break
            except requests.exceptions.Timeout as e:
                # 타임아웃 예외 처리
                attempt += 1
                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time
                self.logger.warning(f"타임아웃 발생 (Attempt {attempt}/{retries}): {url} - {e}")
                if attempt >= retries or total_time_spent >= max_total_timeout:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            except requests.exceptions.RequestException as e:
                # 기타 예외 처리
                attempt += 1
                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time
                self.logger.warning(f"URL 요청 실패 (Attempt {attempt}/{retries}): {url} - {e}")
                if attempt >= retries or total_time_spent >= max_total_timeout:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        self.logger.error(f"{retries}번의 시도 또는 최대 대기 시간 {max_total_timeout}초 후에도 가져오지 못함: {url}")
        return None
        
    def fetch_with_form_data(self, session, url, source, page_param=None, no=None, retries=10, backoff_factor=2, max_backoff=100, initial_timeout=30, max_total_timeout=200):
        """
        POLITICAL_SCIENCE 소스에 대한 form-data 요청 처리 메서드.
        """

        form_data = {
            "catalogid": "politics",
            "language": "ko",
            "no": no,
            "boardcode": "com01",
            "page": page_param,
        }

        attempt = 0
        backoff = backoff_factor  # 초기 대기 시간 (초)
        timeout = initial_timeout  # 타임아웃 시간
        total_time_spent = 0  # 총 소요 시간

        headers = self.get_headers(source) if source else {'User-Agent': random.choice(self.USER_AGENTS)}
        
        while attempt < retries and total_time_spent < max_total_timeout:
            try:
                start_time = time.time()
                response = session.post(url, headers=headers, data=form_data, verify=False, allow_redirects=False, timeout=timeout)
                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time

                if response.status_code == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type:
                        time.sleep(random.uniform(0.1, 0.5))  # 짧은 지연 시간 추가
                        return response.content
                    else:
                        self.logger.warning(f"비HTML 컨텐츠 ({content_type}) for URL: {url}. 스킵합니다.")
                        return None
                elif 500 <= response.status_code < 600:
                    # 서버 오류 시 재시도
                    attempt += 1
                    self.logger.warning(f"서버 오류 {response.status_code} for URL: {url}. 재시도 중... (Attempt {attempt}/{retries})")
                    if attempt >= retries:
                        break
                    time.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)  # 지수 백오프 적용
                else:
                    # 클라이언트 오류: 로깅 후 재시도하지 않음
                    self.logger.error(f"클라이언트 오류 {response.status_code} for URL: {url}. 재시도하지 않음.")
                    break
            except requests.exceptions.Timeout as e:
                # 타임아웃 예외 처리
                attempt += 1
                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time
                self.logger.warning(f"타임아웃 발생 (Attempt {attempt}/{retries}): {url} - {e}")
                if attempt >= retries or total_time_spent >= max_total_timeout:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            except requests.exceptions.RequestException as e:
                # 기타 예외 처리
                attempt += 1
                elapsed_time = time.time() - start_time
                total_time_spent += elapsed_time
                self.logger.warning(f"URL 요청 실패 (Attempt {attempt}/{retries}): {url} - {e}")
                if attempt >= retries or total_time_spent >= max_total_timeout:
                    break
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        self.logger.error(f"{retries}번의 시도 또는 최대 대기 시간 {max_total_timeout}초 후에도 가져오지 못함: {url}")
        return None