import os
import json
import time
import gspread
import openai

from datetime import datetime, timedelta
from dateutil.parser import parse
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# ✅ OpenAI API 키 (환경변수 사용)
openai.api_key = os.environ["OPENAI_API_KEY"]

# GitHub Actions에서 환경변수로 받아오기
creds_json_str = os.getenv("GCP_CREDENTIALS_JSON")
if not creds_json_str:
    raise Exception("❌ GCP_CREDENTIALS_JSON 환경변수가 없습니다!")
google_creds = json.loads(creds_json_str)

# ✅ GPT 정보 추출 함수 (이스케이프 오류 방지 포함)
def extract_listing_info(text):
    try:
        # 역슬래시/유니코드 이스케이프 처리
        text = text.encode("unicode_escape").decode("utf-8")
    except Exception as e:
        print(f"⚠️ 텍스트 전처리 오류: {e}")
        return {}

    prompt = (
        "다음 글에서 아래 항목을 분석해줘. 아래와 같은 통일된 형식으로 JSON으로 출력해줘:\n"
        "- 단지명\n"
        "- 소재지: 반드시 '강남구 청담동 123-45'처럼 구/동/지번 형식 (지번 없으면 생략)\n"
        "- 중개대상물종류\n"
        "- 거래형태: '전세 보증금 10억', '월세 보증금 5억, 월세 400', '매매 45억' 등 한 줄에 통합\n"
        "- 해당층/총층: 아파트/오피스텔만 '3/20', '저층/고층' 등으로, 단독주택은 '미기재'\n"
        "- 공급/전용면적: '172.66㎡/151.63㎡ (52.22평/45.86평)' 형식, 없으면 '미기재'\n"
        "- 룸/욕실: '5/2' (숫자/숫자)\n"
        "- 주차대수: 숫자 또는 '미기재'\n"
        "- 향: 남향, 남동향 등\n"
        "- 입주가능일: yyyy-mm-dd 형식\n"
        "- 사용승인일: yyyy-mm-dd 형식\n"
        "- 관리비: 숫자 또는 '미기재'\n\n"
        "반드시 JSON 형식으로, 키는 그대로, 순서도 지켜줘. 다음은 본문이야:\n"
        f"{text}"
    )

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        result_text = response.choices[0].message["content"]
        return json.loads(result_text)
    except Exception as e:
        print(f"⚠️ GPT 파싱 오류: {e}")
        return {}

# ✅ 구글 시트 인증
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
client = gspread.authorize(creds)

# ✅ 시트 연결
rss_sheet    = client.open_by_key("10lLkfTb_uf68cU2w2OAcXXN6QBiuXGnayK3nf1247tY").sheet1
result_sheet = client.open_by_key("1onQ8R2S-RaH57pel-s-cx1R1RKlagIqRpL8fIoyTnqk").sheet1

# ✅ 헤더 정의 및 초기화
header = [
    "업체명","URL","단지명","소재지","중개대상물종류","거래형태",
    "해당층/총층","공급/전용면적","룸/욕실","주차대수",
    "향","입주가능일","사용승인일","관리비","수집일자"
]
existing_rows = result_sheet.get_all_values()
if not existing_rows:
    result_sheet.append_row(header)
existing_urls = [row[1] for row in existing_rows[1:]]

# ✅ 오늘 및 어제 기준 필터링
rss_data   = rss_sheet.get_all_records()
today      = datetime.now()
yesterday  = today - timedelta(days=1)
new_posts  = []

for row in rss_data:
    try:
        post_date = parse(str(row["포스팅 날짜"]))
        if (
            post_date.date() in [today.date(), yesterday.date()]
            and row["포스팅 링크"] not in existing_urls
        ):
            new_posts.append({
                "업체명": row.get("업체명",""),
                "URL":    row["포스팅 링크"]
            })
    except:
        continue

print(f"🔍 수집 대상 포스팅 수: {len(new_posts)}")

# ✅ 크롬드라이버 설정 (GitHub Actions 호환)
chrome_service = Service()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(
    service=chrome_service,
    options=chrome_options
)

# ✅ 본문 수집 → GPT 분석 → 시트 저장
for idx, post in enumerate(new_posts, start=1):
    print(f"[{idx}] 크롤링: {post['URL']}")
    try:
        driver.get(post["URL"])
        time.sleep(3)
        driver.switch_to.frame("mainFrame")
        content = driver.find_element(By.CLASS_NAME, "se-main-container").text
        info    = extract_listing_info(content)

        row = [post["업체명"], post["URL"]]
        for col in header[2:-1]:
            row.append(info.get(col, ""))
        row.append(today.strftime("%Y-%m-%d"))

        result_sheet.append_row(row)
        print("✅ 저장 완료")
    except NoSuchElementException:
        print("❌ 본문을 찾을 수 없습니다.")
    except Exception as e:
        print(f"⚠️ 오류 발생: {e}")

driver.quit()
