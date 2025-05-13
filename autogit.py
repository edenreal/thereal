#!/usr/bin/env python3
import os
import json
import time
import gspread
import openai
from datetime import datetime, timedelta
from dateutil.parser import parse
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# 1) OpenAI API 키
openai.api_key = os.environ["OPENAI_API_KEY"]

# 2) GCP 서비스 계정 키 (JSON 문자열을 dict로 파싱)
creds_json_str = os.environ.get("GCP_CREDENTIALS_JSON")
if not creds_json_str:
    raise RuntimeError("❌ GCP_CREDENTIALS_JSON 환경변수가 없습니다!")
google_creds = json.loads(creds_json_str)

# 3) GPT 정보 추출 함수
def extract_listing_info(text):
    prompt = f"""
다음 글에서 아래 항목을 분석해줘. 아래와 같은 통일된 형식으로 JSON으로 출력해줘:

- 단지명
- 소재지: 반드시 '강남구 청담동 123-45'처럼 구/동/지번 형식 (지번 없으면 생략)
- 중개대상물종류
- 거래형태: '전세 보증금 10억', '월세 보증금 5억, 월세 400', '매매 45억' 등 한 줄에 통합
- 해당층/총층: 아파트/오피스텔만 '3/20', '저층/고층' 등으로, 단독주택은 '미기재'
- 공급/전용면적: '172.66㎡/151.63㎡ (52.22평/45.86평)' 형식, 없으면 '미기재'
- 룸/욕실: '5/2' (숫자/숫자)
- 주차대수: 숫자 또는 '미기재'
- 향: 남향, 남동향 등
- 입주가능일: yyyy-mm-dd 형식
- 사용승인일: yyyy-mm-dd 형식
- 관리비: 숫자 또는 '미기재'

반드시 JSON 형식으로, 키는 그대로, 순서도 지켜줘. 다음은 본문이야:
{text}
"""
    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        content = resp.choices[0].message["content"]
        return json.loads(content)
    except Exception as e:
        print(f"⚠️ GPT 파싱 오류: {e}")
        return {}

# 4) 구글 시트 인증
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
client = gspread.authorize(creds)

rss_sheet    = client.open_by_key("10lLkfTb_uf68cU2w2OAcXXN6QBiuXGnayK3nf1247tY").sheet1
result_sheet = client.open_by_key("1onQ8R2S-RaH57pel-s-cx1R1RKlagIqRpL8fIoyTnqk").sheet1

# 5) 헤더 초기화
header = [
    "업체명","URL","단지명","소재지","중개대상물종류","거래형태",
    "해당층/총층","공급/전용면적","룸/욕실","주차대수",
    "향","입주가능일","사용승인일","관리비","수집일자"
]
if not result_sheet.get_all_values():
    result_sheet.append_row(header)
existing_urls = [r[1] for r in result_sheet.get_all_values()[1:]]

# 6) RSS 시트에서 오늘·어제 글만 골라내기
rss_data = rss_sheet.get_all_records()
today     = datetime.now().date()
yesterday = today - timedelta(days=1)
new_posts = []

for row in rss_data:
    try:
        d = parse(row["포스팅 날짜"]).date()
        if d in (today, yesterday) and row["포스팅 링크"] not in existing_urls:
            new_posts.append({
                "업체명": row.get("업체명",""),
                "URL":    row["포스팅 링크"]
            })
    except:
        pass

print(f"🔍 수집 대상 포스팅 수: {len(new_posts)}")

# 7) Selenium + webdriver-manager 로드
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(
    ChromeDriverManager().install(),
    options=options
)

# 8) 크롤링 & GPT 분석 & 시트에 쓰기
for i, post in enumerate(new_posts, start=1):
    print(f"[{i}] 크롤링: {post['URL']}")
    try:
        driver.get(post["URL"])
        time.sleep(3)
        driver.switch_to.frame("mainFrame")

        content = driver.find_element(By.CLASS_NAME, "se-main-container").text
        info    = extract_listing_info(content)

        row = [post["업체명"], post["URL"]] + \
            [ info.get(c, "") for c in header[2:-1] ] + \
            [ today.strftime("%Y-%m-%d") ]
        result_sheet.append_row(row)
        print("✅ 저장 완료")
    except NoSuchElementException:
        print("❌ 본문을 찾을 수 없습니다.")
    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")

driver.quit()
