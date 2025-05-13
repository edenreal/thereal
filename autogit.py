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

# webdriver_manager 추가 import
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# ✅ OpenAI API 키 (환경변수)
openai.api_key = os.environ["OPENAI_API_KEY"]

# ✅ GCP 서비스 계정 JSON (환경변수)
creds_json_str = os.getenv("GCP_CREDENTIALS_JSON")
if not creds_json_str:
    raise Exception("❌ GCP_CREDENTIALS_JSON 환경변수가 없습니다!")
google_creds = json.loads(creds_json_str)

# ✅ GPT 정보 추출 함수
def extract_listing_info(text):
    try:
        # 역슬래시·유니코드 이스케이프 처리
        text = text.encode("unicode_escape").decode("utf-8")
    except Exception as e:
        print(f"⚠️ 전처리 오류: {e}")
        return {}

    prompt = (
        "다음 글에서 아래 항목을 분석해줘. JSON으로 한 줄씩 출력해줘:\n"
        "- 단지명\n"
        "- 소재지: '강남구 청담동 123-45' 형식\n"
        "- 중개대상물종류\n"
        "- 거래형태: '전세 보증금 10억' 등\n"
        "- 해당층/총층: '3/20' or '저층/고층'\n"
        "- 공급/전용면적: '172.66㎡/151.63㎡ (52.22평/45.86평)'\n"
        "- 룸/욕실: '5/2'\n"
        "- 주차대수\n"
        "- 향\n"
        "- 입주가능일 (yyyy-mm-dd)\n"
        "- 사용승인일 (yyyy-mm-dd)\n"
        "- 관리비\n\n"
        f"본문:\n{text}"
    )

    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0.2,
        )
        return json.loads(resp.choices[0].message["content"])
    except Exception as e:
        print(f"⚠️ GPT 파싱 오류: {e}")
        return {}

# ✅ 구글 시트 인증
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds, scope)
gc = gspread.authorize(creds)

# ✅ 시트 객체
rss_sheet    = gc.open_by_key("10lLkfTb_uf68cU2w2OAcXXN6QBiuXGnayK3nf1247tY").sheet1
result_sheet = gc.open_by_key("1onQ8R2S-RaH57pel-s-cx1R1RKlagIqRpL8fIoyTnqk").sheet1

# ✅ 헤더 초기화
header = [
    "업체명","URL","단지명","소재지","중개대상물종류","거래형태",
    "해당층/총층","공급/전용면적","룸/욕실","주차대수",
    "향","입주가능일","사용승인일","관리비","수집일자"
]
if not result_sheet.get_all_values():
    result_sheet.append_row(header)
existing_urls = [r[1] for r in result_sheet.get_all_values()[1:]]

# ✅ 오늘·어제 포스팅 필터
today, yesterday = datetime.now(), datetime.now() - timedelta(days=1)
new_posts = []
for row in rss_sheet.get_all_records():
    try:
        pd = parse(str(row["포스팅 날짜"])).date()
        if pd in (today.date(), yesterday.date()) and row["포스팅 링크"] not in existing_urls:
            new_posts.append({"업체명":row.get("업체명",""), "URL":row["포스팅 링크"]})
    except:
        continue

print(f"🔍 수집 대상 포스팅 수: {len(new_posts)}")

# ✅ 크롬 옵션 및 Service (webdriver_manager 사용)
options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# ✅ 크롤링 + GPT + 시트 저장
for idx, post in enumerate(new_posts, start=1):
    print(f"[{idx}] 크롤링: {post['URL']}")
    try:
        driver.get(post["URL"])
        time.sleep(2)
        driver.switch_to.frame("mainFrame")
        body = driver.find_element(By.CLASS_NAME, "se-main-container").text
        info = extract_listing_info(body)

        row = [post["업체명"], post["URL"]] + \
              [info.get(col, "") for col in header[2:-1]] + \
              [today.strftime("%Y-%m-%d")]
        result_sheet.append_row(row)
        print("✅ 저장 완료")
    except NoSuchElementException:
        print("❌ 본문 프레임/클래스 못찾음")
    except Exception as e:
        print(f"⚠️ 기타 오류: {e}")

driver.quit()
