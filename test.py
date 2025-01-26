from bs4 import BeautifulSoup
import re

# HTML 예제 (사이트에서 가져온 HTML 소스 삽입)
html = """
<a href="javascript:view(579439)">
    <font class="tabletextlist">
        <font style="FONT-WEIGHT: bold;">[필독] 사회과학대학 대관 요청 전 확인 사항 안내 (연희관, 빌링슬리관)</font>
    </font>
</a>
"""

def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    
    # "javascript:view(n)" 형태의 링크 추출
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        match = re.search(r"view\((\d+)\)", href)  # view(n) 형식의 숫자 추출
        if match:
            article_id = match.group(1)
            # 완성된 URL
            links.append(f"{base_url}?no={article_id}")
    
    return links

# 실행
base_url = "http://politics.yonsei.ac.kr/board_read.asp"
links = extract_links(html, base_url)

# 출력
print(links)
