import os
import json
import time
import re

from datetime import datetime, timedelta
from dateutil.parser import parse
from oauth2client.service_account import ServiceAccountCredentials

import gspread
from openai import OpenAI

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ✅ 환경변수에서 OpenAI 키와 GCP 서비스 계정 JSON 불러오기
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

creds_json_str = os.getenv("GCP_CREDENTIALS_JSON")
if not creds_json_str:
    raise Exception("❌ GCP_CREDENTIALS_JSON 환경변수가 없습니다!")
google_creds = json.loads(creds_json_str)

# ✅ 구글 시트 인증
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
gc = gspread.authorize(creds)

# ✅ 시트 로드
rss_sheet    = gc.open_by_key("10lLkfTb_uf68cU2w2OAcXXN6QBiuXGnayK3nf1247tY").sheet1
result_sheet = gc.open_by_key("1onQ8R2S-RaH57pel-s-cx1R1RKlagIqRpL8fIoyTnqk").sheet1

# ✅ GPT 호출 + JSON 파싱 (helper)
def _call_gpt_and_parse(text: str) -> dict:
    prompt = f"""
다음 글에서 아래 항목을 분석해줘. 아래와 같은 통일된 형식으로 JSON으로 출력해줘:
- 단지명
- 소재지: 반드시 '강남구 청담동 123-45'처럼 구/동/지번 형식 (지번 없으면 생략)
- 중개대상물종류
- 거래형태: '전세 보증금 10억', '월세 보증금 5억, 월세 400', '매매 45억'
- 해당층/총층: 아파트/오피스텔만 '3/20', '저층/고층' 등으로, 단독주택은 '미기재'
- 공급/전용면적: '172.66㎡/151.63㎡ (52.22평/45.86평)' 형식, 없으면 '미기재'
- 룸/욕실: '5/2'
- 주차대수: 숫자 또는 '미기재'
- 향: 남향, 남동향 등
- 입주가능일: yyyy-mm-dd 형식
- 사용승인일: yyyy-mm-dd 형식
- 관리비: 숫자 또는 '미기재'

반드시 JSON 형식으로, 키와 순서를 지켜줘. 본문:
{text}
"""
    resp = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )
    return json.loads(resp.choices[0].message.content)

# ✅ 본문에서 Regex로 보정 후 GPT 결과에 덮어쓰기
def extract_listing_info(text: str) -> dict:
    # 1) 정규표현식 추출
    addr_match = re.search(r'([가-힣]+구\s[가-힣]+(동|읍|면)\s?\d+(?:-\d+)*)', text)
    default_addr = addr_match.group(1) if addr_match else ""

    price_match = re.search(
        r'(매매|전세|월세)\s*[:：]?\s*([\d\.]+(?:억)?(?:\s*\d+만)?)(?:원)?'
        r'(?:\s*/\s*(?:보증금\s*)?([\d\.]+(?:억)?(?:\s*\d+만)?))?', text
    )
    rent_type   = price_match.group(1) if price_match else ""
    main_price  = price_match.group(2) if price_match else ""
    second_price= price_match.group(3) if price_match and price_match.group(3) else ""

    type_match = re.search(
        r'중개대상물종류\s*[:：]?\s*([가-힣]+(?:주택|아파트|빌라|오피스텔|상가|연립주택|공동주택|근린생활시설)?)',
        text
    )
    default_type = type_match.group(1).strip() if type_match else ""

    # 2) GPT에 본문을 보내고 JSON 파싱
    info = _call_gpt_and_parse(text)

    # 3) Regex 결과로 덮어쓰기
    if default_addr:
        info["소재지"] = default_addr

    if rent_type:
        if rent_type == "매매":
            info["거래형태"] = f"매매 {main_price}"
        elif rent_type == "전세":
            info["거래형태"] = f"전세 보증금 {main_price}"
        else:  # 월세
            fee = f", 월세 {second_price}" if second_price else ""
            info["거래형태"] = f"월세 보증금 {main_price}{fee}"

    if default_type:
        info["중개대상물종류"] = default_type

    return info

# ✅ 헤더 정의
header = [
    "업체명", "URL", "단지명", "소재지", "중개대상물종류", "거래형태",
    "해당층/총층", "공급/전용면적", "룸/욕실", "주차대수",
    "향", "입주가능일", "사용승인일", "관리비", "수집일자"
]
# 시트 초기화
if not result_sheet.get_all_values():
    result_sheet.append_row(header)
existing_urls = [row[1] for row in result_sheet.get_all_values()[1:]]

# ✅ 오늘·어제 포스팅 필터링
today = datetime.now()
yesterday = today - timedelta(days=1)
new_posts = []
for row in rss_sheet.get_all_records():
    try:
        post_date = parse(str(row["포스팅 날짜"])).date()
        if post_date in (today.date(), yesterday.date()) and row["포스팅 링크"] not in existing_urls:
            new_posts.append({
                "업체명": row.get("업체명", ""),
                "URL": row["포스팅 링크"]
            })
    except:
        continue

print(f"🔍 수집 대상 포스팅 수: {len(new_posts)}")

# ✅ Selenium 드라이버 설정 (webdriver_manager 사용)
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

# ✅ 크롤링 → 파싱 → 시트 저장
for idx, post in enumerate(new_posts, start=1):
    print(f"[{idx}] 크롤링: {post['URL']}")
    try:
        driver.get(post["URL"])
        time.sleep(2)
        driver.switch_to.frame("mainFrame")
        content = driver.find_element(By.CLASS_NAME, "se-main-container").text

        info = extract_listing_info(content)
        row = [post["업체명"], post["URL"]] + [info.get(col, "") for col in header[2:-1]] + [today.strftime("%Y-%m-%d")]
        result_sheet.append_row(row)
        print("✅ 저장 완료")
    except NoSuchElementException:
        print("❌ 본문 프레임/클래스 못찾음")
    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")

driver.quit()
