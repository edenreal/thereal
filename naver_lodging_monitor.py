# -*- coding: utf-8 -*-
"""
숙박 매물 신규 감시기 — 2단계 게이트 버전 (+ 종류필터 + 중복그룹 + 건물스펙)
─────────────────────────────────────────────────────────
하는 일:
  1) '블로그목록' 탭의 블로그 RSS를 돌면서
  2) 전에 본 적 없는 새 글(=새 매물 후보)만 골라
  3) [변경] 2단계로 GPT에 태운다.
       1차(게이트): 제목 + 본문 앞 700자 → "매물이냐 아니냐"만 판정.
                    비매물이면 여기서 끝. (대부분이 여기서 걸러지므로 토큰 절약)
       2차(추출) : 매물일 때만 본문 전문(4000자)으로 전 항목 추출.
                    → 위치·종류·형태·금액·매출·객실수 + [신규] 대지면적·연면적·층수·준공연도·위반건축물
     ※ 기존엔 본문을 3000자 가져와 놓고 800자만 GPT에 줬다. 광고글은 미사여구가 앞에 오고
       스펙 블록(면적·층수·객실수·사용승인일)이 뒤에 몰려 있어서 그게 통째로 잘렸다.
       fetch는 원래 하던 그대로라 네이버 요청량은 늘지 않는다.
  4) 종류가 거주형·완전비숙박이면 시트에 안 올린다(고시원은 2026-09-23부터 포함, 종류='고시원')
  5) '매물카드' 탭에 한 줄씩 쌓는다.
  6) 다 쌓은 뒤, 매물카드 전체에서 위치+가격이 같은 도배 매물에
       중복그룹 번호(D1, D2…)를 매긴다. (행은 안 지우고 표시만)

필요 환경변수(둘 다 GitHub Secret):
  GCP_CREDENTIALS_JSON  : 구글 서비스계정 키(JSON 문자열)
  OPENAI_API_KEY        : OpenAI 키

준비물: 대상 구글시트를 서비스계정 이메일에 '편집자'로 공유해 둘 것.

※ 매물카드 칸은 '뒤에만' 늘어난다(기존 15칸 위치 그대로 + 신규 5칸).
  기존 데이터는 밀리지 않고 그대로 보존된다. 백업/재생성 안 함.
"""
import os
import re
import json
import time
import calendar
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import requests
import feedparser
import gspread
from gspread.utils import rowcol_to_a1
from openai import OpenAI

# ════════════════════════════════ 설정 ════════════════════════════════
SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
BLOG_TAB = "블로그목록"
CARD_TAB = "매물카드"
GATE_TAB = "게이트제외"   # 1차 게이트에서 '비매물'로 잘린 글의 링크 보관(재판정 방지)

# 기존 15칸은 순서 그대로 두고, 신규 5칸은 '맨 뒤'에만 붙인다(기존 데이터 안 밀림).
CARD_HEADER = ["감지일", "게시일", "블로그", "시도", "시군구", "읍면동", "종류", "형태",
               "거래금액", "매출", "객실수", "제목", "링크", "상태", "중복그룹",
               "대지면적", "연면적", "층수", "준공연도", "위반건축물"]
# 이전 형식들(뒤에서 잘라낸 모양). 헤더가 이 중 하나면 '부족한 칸만' 채워 넣는다.
PREV_HEADERS = [CARD_HEADER[:15], CARD_HEADER[:14]]

RECENT_DAYS = 14       # 이 기간 안에 올라온 새 글만 (첫 실행 폭주/놓침 방지)
SLEEP_SEC = 2.0        # 블로그 사이 간격 (천천히 돌아 차단 회피)
MODEL = "gpt-4o-mini"
RSS_TMPL = "https://rss.blog.naver.com/{}.xml"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
KST = timezone(timedelta(hours=9))

# 처음 한 번만 쓰이는 시드. '블로그목록' 탭이 비어 있으면 이걸로 채운다.
SEED_BLOGS = [
    "staybrief", "dddi570", "noble41888", "s7620mmjoe", "msoochae",
    "yoondang__", "eitting2018", "water_ah_", "gyonryoru", "spacementor",
    "korea-7942-", "7979sic", "sojwa07", "jjsskk0815", "ska6565",
]

# 비숙박 전문 블로그 — 감시에서 영구 제외(발굴기가 다시 편입해도 무시).
BLOCKLIST = {
    "stewzinnia59",   # 부동산 분양/상담
    "ksanchoi",       # 경매
    "auctionrun3988", # 경매
    "sbjjjang",       # 수익형 상가
    "jijonbpbp",      # 리조트 회원권
    "kkanglive",      # 투자일기
    "moneyschool300", # 숙박업 강의/클래스
    # 2026-07-31 추가 — 노이즈 분석에서 숙박매물 0건·전량 비숙박으로 확인
    "h3h2003",        # 코인/주식/맛집 (197건 전부 노이즈)
    "foremanenc",     # 비숙박 (숙박매물 0건)
    "sanergy_3051",   # 비숙박 (숙박매물 0건)
    "kymin0909",      # 비숙박 (숙박매물 0건)
    # 2026-09-23 추가 — 최근 30일 숙박매물 0건·잡글 5건+(자동편입분만, 사람 승인분 제외) + 회원권·달방/출장숙소
    "jps2781", "realtymasta", "nosunyee", "helloyedol", "banktown2727",
    "solacom7777", "rio8245", "gamemanian", "donghaduil", "qpmintae",
    "ksr1972", "ssy8422", "snowstorm85", "newaceno1", "ddabbs",
    "roomhub", "johnpong22", "newbongbong", "kimshuly", "jigollaid",
    "bs_gangseo", "kimvision80", "jeju5may", "zumj9bivy0if4tk", "alsthd7",
    "b9o8bfq5qsxk6", "therichpark0711", "27sjlee", "jayan_hanviet", "gunwoo_kwon6792",
    "nirana22", "codhksgml", "5304356",
    "omoidemichi",   # 호텔원 자사 블로그 — 자사 광고가 경쟁 광고로 잡히지 않게
}

GATE_BODY_CHARS = 700    # 1차 게이트: 매물이냐 아니냐만 보므로 앞부분이면 충분
FULL_BODY_CHARS = 4000   # 2차 추출: 매물일 때만. 스펙 블록이 글 뒤에 있어서 넉넉히
FETCH_HTML_WINDOW = 80000  # 본문 컨테이너에서 읽을 HTML 길이(긴 광고글 대비)
GPT_RETRIES = 3          # rate limit/timeout 시 재시도 (1→2→4초 백오프)

# ─────────────────── 종류 필터 (화이트리스트) ───────────────────
# [중요] 예전엔 블랙리스트(고시원·상가·빌딩…)였다. 그래서 목록에 없던
#   아파트·공장·창고·카페·정육점이 전부 통과해 매물카드를 오염시켰다.
#   이제는 '숙박 키워드가 없으면 무조건 제외'하는 화이트리스트로 바꾼다.
#
# 주의: 단독 단어를 넣으면 엉뚱한 게 걸린다.
#   "체류형"·"쉼터" → 농막이 통과했다.  "관광" → 관광농원.  "한옥" → 한옥주택.
#   그래서 반드시 숙박을 뜻하는 형태로만 넣는다.
LODGING_KW = [
    "모텔", "호텔", "여관", "여인숙", "호스텔", "펜션", "게스트하우스", "게하",
    "민박", "풀빌", "무인텔", "무인호텔", "레지던스", "숙박", "스테이",
    "관광숙박", "비지니스호텔", "비즈니스호텔", "캡슐호텔", "한옥스테이",
    "에어비앤비", "에어비엔비", "리조트",
    # 2026-09-23 고시원 편입(이든: 손님 수요 + 고시원 매도인이 모텔로 넘어오는 흐름). 종류는 "고시원"으로 통일 추출
    "고시원", "고시텔", "원룸텔", "리빙텔",
]


# ─────────────── 제목 사전필터 (GPT 게이트 이전, 비용 0) ───────────────
# 명백한 비숙박 글은 본문 fetch·GPT 호출 전에 제목만 보고 건너뛴다.
# 안전 규칙(둘 중 하나라도 해당하면 절대 안 버림):
#   ① 숙박 키워드(LODGING_KW)가 제목에 있음
#   ② 숙박 구조신호(객실수·매출 등)가 제목에 있음  ← '토지817㎡, 객실28개' 같은 매물 보호
# 검증(2026-08-24): 과거 정상매물 2,791건에 적용해 오탐 0건(걸린 3건은 전부 원룸/오피스텔=원래 비숙박).
TITLE_NEG = re.compile(
    r"코인|비트코인|이더리움|주식|코스피|코스닥|상장|급락|방산주|테마주|환율|"
    r"아파트|빌라|다세대|연립|원룸|투룸|쓰리룸|오피스텔|재개발|재건축|청약|분양권|"
    r"토지|임야|농지|전답|공장|창고|물류센터|"
    r"경매|타경|공매|맛집|여행후기|학원|골프연습장"
)
TITLE_RESCUE = re.compile(r"객실|\d+\s*실|월매출|매출|순익|순수익|\d+\s*룸|룸\s*\d+")   # 룸N·순익: 고시원 매물 제목("원룸14/순익300") 보호


def title_prefilter_skip(title):
    """True면 GPT를 부르지 않고 건너뛴다(명백한 비숙박)."""
    t = str(title or "")
    if any(k in t for k in LODGING_KW):
        return False
    if TITLE_RESCUE.search(t):
        return False
    return bool(TITLE_NEG.search(t))


def is_excluded_kind(kind, title=""):
    """숙박 키워드가 없으면 True(시트에 안 올림).
    종류가 비어 있으면 제목에서 한 번 더 확인한다."""
    s = str(kind or "").strip()
    if any(k in s for k in LODGING_KW):
        return False                                   # 숙박 → 통과
    if not s:                                          # 종류 빈칸 → 제목으로 판단
        return not any(k in str(title or "") for k in LODGING_KW)
    return True                                        # 숙박 키워드 없음 → 제외


# ════════════════════════════════ GPT ════════════════════════════════
# 1차 — 게이트. 매물이냐 아니냐만. 비매물이 대부분이라 여기서 대부분 끝난다.
GATE_PROMPT = """다음 블로그 글이 '숙박시설' 매물 광고인지 판단하라.

숙박시설이란: 모텔, 호텔, 여관, 여인숙, 호스텔, 펜션, 게스트하우스, 민박, 풀빌라,
무인텔, 레지던스, 관광호텔, 에어비앤비 등 '손님을 재우는 영업시설'이다.
고시원·고시텔·원룸텔·리빙텔도 영업장 자체를 사고팔거나 임대하는 광고면 대상이다.

"매물"로 판정:
- 위 숙박시설을 팔거나(매매) 임대하려고 올린 광고.
- 고시원·고시텔 영업장의 매매·임대·양도양수(시설 인수) 광고.

"비매물"로 판정 — 숙박시설이 아니면 전부 비매물이다:
- 주거: 아파트, 빌라, 다세대, 연립, 단독/다가구, 주택, 원룸, 오피스텔, 농막, 세컨하우스
- 방 한 칸 월세·달방·단기임대·한달살기 입주자 모집 글(고시원룸·원룸텔 방 포함). 영업장을 넘기는 게 아니면 비매물
- 건물/토지: 상가, 빌딩, 사옥, 공장, 창고, 물류센터, 토지, 부지, 상업지
- 점포/업종: 카페, 커피점, 식당, 정육점, 노래방, 유흥주점, 학원, 병원, 교회, 골프연습장, 농장
- 글 종류: 맛집, 여행후기, 이용후기, 정보, 상식, 일상, 강의, 세미나 홍보

중요: 숙박시설이라고 확신할 수 없으면 "비매물"로 한다. (애매하면 비매물)

제목: {title}
본문: {body}

JSON으로만 답하라: {{"매물여부":"매물 또는 비매물"}}"""


# 2차 — 추출. 매물일 때만. 본문 전문을 준다.
EXTRACT_PROMPT = """다음은 부동산 중개 블로그의 숙박시설 매물 광고다. 항목을 추출하라.

규칙:
- 글에 실제로 있는 내용만 쓴다. 절대 지어내지 않는다. 모르면 빈 문자열 "".
- "거래금액"은 보증금·월세·매매가 같은 실제 거래 가격이다. "매출"은 월매출·순수익이다. 이 둘을 절대 섞지 않는다.
- "거래금액"과 "매출"은 글에 적힌 표기 그대로 쓴다(예: "보증금 3억", "월세 600만", "매매가 25억", "월매출 4000만", "순수익 800만"). 절대 원 단위 숫자로 풀어쓰지 않는다(예: 40000000 같은 형태 금지).
- 매물 광고가 아니라 맛집·후기·정보·일상 글이면 "매물여부"를 "비매물"로 한다.
- "시도"는 광역시/도 정식명(서울특별시, 경기도, 부산광역시 등). 동·역 이름으로 분명히 알 수 있으면 채운다(예: 강동역→서울특별시, 수원→경기도). 애매하면 "".
- "시군구"는 시/군/구(예: 수원시, 강동구). "읍면동"은 동·읍·면·리(예: 구운동, 초량동, 부강면). 없으면 "".
- "형태"는 매매면 "매매", 임대면 "임대", 둘 다면 "매매/임대".
- "종류"는 글에 적힌 시설 종류 그대로 쓴다(모텔은 "모텔", 호텔은 "호텔"). 고시원·고시텔 매물일 때만 "객실수"는 방 개수로 본다.

[물건 개요] — 아래 7개는 글 뒷부분의 물건개요·상세정보 블록에 몰려 있는 경우가 많다. 반드시 끝까지 읽고 하나씩 찾아라. 광고에 있는데 빈칸으로 두는 것이 가장 큰 실수다.
- "매출": 월매출·연매출·순수익 등 영업 실적. 글에 적힌 표기 그대로(예: "월매출 4000만", "순수익 800만", "연매출 5억"). 거래금액과 절대 섞지 마라. 없으면 "".
- "객실수": 객실 개수. 글에 적힌 표기 그대로 쓴다(예: "54실", "객실 20", "20룸", "15~25실"). 억지로 형식을 바꾸지 말고, 숫자가 있으면 반드시 채운다. 없으면 "".
- "대지면적"·"연면적": 글에 적힌 표기 그대로 단위까지(예: "189.1㎡", "230평", "761.76m2"). 토지면적으로 적혀 있으면 대지면적으로 본다. 없으면 "".
- "층수": 지하·지상을 함께 적는다(예: "지하1층/지상5층", "5층", "지상4층"). 없으면 "".
- "준공연도": 사용승인일·준공일을 글에 적힌 그대로 쓴다(예: "1989.8.25", "2017.12.20", "1989"). 날짜까지 적혀 있으면 반드시 날짜까지 포함한다. 절대 연도만 남기고 자르지 마라. 없으면 "".
- "위반건축물": 위반·위법건축물 언급이 있으면 "有", 없다고 명시하면 "無", 아무 언급 없으면 "".

답하기 전에 위 7개를 하나씩 본문에서 다시 확인하라. 특히 "매출"과 "객실수"를 빠뜨리지 마라.

제목: {title}
본문: {body}

아래 JSON 형식으로만 답하라:
{{"매물여부":"매물 또는 비매물","시도":"","시군구":"","읍면동":"","종류":"","형태":"","거래금액":"","매출":"","객실수":"","대지면적":"","연면적":"","층수":"","준공연도":"","위반건축물":""}}"""


def get_openai():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다. GitHub Secret에 추가하세요.")
    return OpenAI(api_key=key)


class QuotaExhausted(RuntimeError):
    """OpenAI 크레딧/쿼터 소진. 재시도해도 소용없으니 즉시 중단시킨다."""


def _is_quota_error(e):
    """잔액 소진·쿼터 초과인지 판정(재시도 무의미한 종류)."""
    msg = str(e).lower()
    return any(k in msg for k in (
        "insufficient_quota", "credit_balance_exhausted",
        "no credits remaining", "exceeded your current quota",
        "billing_hard_limit_reached",
    ))


def _chat_json(oai, content):
    """공통 호출부. rate limit/timeout이면 1→2→4초 백오프 후 재시도.
    단 잔액 소진이면 재시도 없이 QuotaExhausted로 즉시 올린다."""
    last_err = None
    for attempt in range(GPT_RETRIES):
        try:
            resp = oai.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": content}],
                temperature=0,
                response_format={"type": "json_object"},
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            if _is_quota_error(e):
                raise QuotaExhausted(e)
            last_err = e
            if attempt < GPT_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise last_err   # 끝까지 실패하면 호출부에서 '확인필요(GPT실패)'로 기록


def gpt_gate(oai, title, body):
    """1차: 매물이냐 아니냐만 (본문 앞 700자)."""
    return _chat_json(oai, GATE_PROMPT.format(title=title, body=(body or "")[:GATE_BODY_CHARS]))


def gpt_extract(oai, title, body):
    """2차: 매물일 때만 전 항목 추출 (본문 전문)."""
    return _chat_json(oai, EXTRACT_PROMPT.format(title=title, body=(body or "")[:FULL_BODY_CHARS]))


def build_row(today, posted, bid, info, title, link, status="", dup=""):
    return [
        today, posted, bid,
        info.get("시도", ""), info.get("시군구", ""), info.get("읍면동", ""),
        info.get("종류", ""), info.get("형태", ""),
        info.get("거래금액", ""), info.get("매출", ""), info.get("객실수", ""),
        title, link, status, dup,
        info.get("대지면적", ""), info.get("연면적", ""), info.get("층수", ""),
        info.get("준공연도", ""), info.get("위반건축물", ""),
    ]


# ════════════════════════════════ 유틸 ════════════════════════════════
def strip_html(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"&[#a-zA-Z0-9]+;", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def extract_bid(s):
    """시트 칸 값이 URL이든 순수 ID든 블로그ID만 뽑는다."""
    s = (s or "").strip()
    m = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_\-]+", s):
        return s
    return ""


def canon_link(url):
    """URL 형식이 달라도 같은 글이면 같은 키가 되도록 정규화 (중복 방지)."""
    bid = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", url or "")
    logno = re.search(r"(?:logNo=|/)(\d{8,})", url or "")
    if bid and logno:
        return f"https://blog.naver.com/{bid.group(1)}/{logno.group(1)}"
    return (url or "").strip()


def entry_body(entry):
    raw = entry.get("summary") or entry.get("description") or ""
    return strip_html(raw)


def recent_ok(entry, days):
    p = entry.get("published_parsed") or entry.get("updated_parsed")
    if not p:
        return True
    dt = datetime.fromtimestamp(calendar.timegm(p), tz=timezone.utc)
    return (datetime.now(timezone.utc) - dt) <= timedelta(days=days)


def post_datetime(entry):
    """글이 실제로 올라온 시각을 한국시간 분 단위 문자열로. (없으면 빈칸)"""
    p = entry.get("published_parsed") or entry.get("updated_parsed")
    if not p:
        return ""
    dt = datetime.fromtimestamp(calendar.timegm(p), tz=timezone.utc).astimezone(KST)
    return dt.strftime("%Y-%m-%d %H:%M")


def fetch_feed(bid):
    r = requests.get(RSS_TMPL.format(bid), headers=UA, timeout=20)
    r.raise_for_status()
    return feedparser.parse(r.content)


def parse_link(url):
    """canon 링크에서 블로그ID와 글번호를 뽑는다."""
    b = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", url or "")
    n = re.search(r"(?:logNo=|/)(\d{8,})", url or "")
    return (b.group(1) if b else ""), (n.group(1) if n else "")


def fetch_post_body(bid, logno):
    """모바일 블로그 페이지에서 본문 텍스트를 가져온다. 실패하면 빈 문자열.
    ※ 요청 방식은 기존과 동일. 다만 긴 광고글이 잘리지 않게 읽는 창만 넓혔다.
      (스펙 블록이 글 뒤에 있어서, 좁게 읽으면 그 부분이 통째로 날아갔다)
    이미지 표에만 박힌 정보는 텍스트가 없으므로 여전히 못 가져온다."""
    if not (bid and logno):
        return ""
    url = f"https://m.blog.naver.com/{bid}/{logno}"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        html = r.text
        i = html.find("se-main-container")   # 본문 컨테이너 시작
        if i != -1:
            html = html[i:i + FETCH_HTML_WINDOW]
        return strip_html(html)[:FULL_BODY_CHARS]
    except Exception:
        return ""


# ════════════════════════════════ 시트 ════════════════════════════════
def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def load_blogs(ss):
    try:
        ws = ss.worksheet(BLOG_TAB)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(BLOG_TAB, rows=2000, cols=4)
        ws.append_row(["블로그ID", "메모"], value_input_option="RAW")
    raw = ws.col_values(1)[1:]
    ids = [b for b in (extract_bid(v) for v in raw) if b]
    if not ids:
        ws.append_rows([[b, "seed"] for b in SEED_BLOGS], value_input_option="RAW")
        ids = list(SEED_BLOGS)
    seen, out = set(), []
    for b in ids:
        if b in BLOCKLIST:        # 비숙박 전문 블로그는 건너뛴다
            continue
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def setup_card_tab(ss):
    """매물카드 탭을 준비한다.
      - 이미 신형식(20칸): 그대로 사용
      - 이전 형식(15칸/14칸): 부족한 칸만 '뒤에' 추가 (기존 데이터 보존, 밀리지 않음)
      - 그 외 진짜 옛 형식: 백업 후 새로 만듦
    반환: (워크시트, 중복방지용 기존 링크 리스트)"""
    try:
        ws = ss.worksheet(CARD_TAB)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(CARD_TAB, rows=3000, cols=len(CARD_HEADER) + 2)
        ws.append_row(CARD_HEADER, value_input_option="RAW")
        return ws, []

    # 칸 수가 모자라면 먼저 늘린다 (안 하면 헤더 쓰기에서 에러)
    if ws.col_count < len(CARD_HEADER):
        ws.resize(cols=len(CARD_HEADER) + 2)

    header = ws.row_values(1)
    if header == CARD_HEADER:                      # 이미 새 형식
        links = ws.col_values(CARD_HEADER.index("링크") + 1)[1:]
        return ws, links

    if header in PREV_HEADERS:                     # 부족한 칸만 뒤에 추가 (데이터 보존)
        add = CARD_HEADER[len(header):]
        rng = f"{rowcol_to_a1(1, len(header) + 1)}:{rowcol_to_a1(1, len(CARD_HEADER))}"
        ws.batch_update([{"range": rng, "values": [add]}], value_input_option="RAW")
        print(f"  매물카드에 칸 {len(add)}개 추가(기존 데이터 보존): {add}")
        links = ws.col_values(CARD_HEADER.index("링크") + 1)[1:]
        return ws, links

    # 그 외 진짜 옛 형식 → 기존 링크 수집(중복방지) 후 백업, 새 탭 생성
    old_links = ws.col_values(header.index("링크") + 1)[1:] if "링크" in header else []
    backup = CARD_TAB + "_old"
    try:
        ss.worksheet(backup)
        backup = backup + datetime.now().strftime("_%m%d%H%M")
    except gspread.WorksheetNotFound:
        pass
    ws.update_title(backup)
    new_ws = ss.add_worksheet(CARD_TAB, rows=3000, cols=len(CARD_HEADER) + 2)
    new_ws.append_row(CARD_HEADER, value_input_option="RAW")
    print(f"  기존 매물카드를 '{backup}'으로 백업하고 새 형식으로 시작합니다.")
    return new_ws, old_links


def setup_gate_tab(ss):
    """게이트제외 탭을 준비하고, 이미 '비매물'로 판정된 링크 집합을 돌려준다.

    [왜 필요한가] 예전엔 1차 게이트에서 잘린 글을 아무 데도 남기지 않았다.
    그런데 다음 실행에서 seen을 '매물카드 링크'로만 다시 만들기 때문에,
    잘린 글이 RSS에 남아 있는 동안(RECENT_DAYS=14) 매 실행마다 다시 본문을 받고
    GPT 게이트를 또 돌렸다. 하루 2회 × 14일 = 같은 글을 최대 28번 재판정.
    (실측: 게이트 컷 수가 1267·1284·1284·1292·1291로 12시간 간격에도 거의 불변)
    → 여기에 링크를 남겨 두 번 다시 판정하지 않는다.

    ※ temperature=0이라 재판정해도 결과는 같다 → 건너뛰어도 잃는 정보가 없다.
    """
    header = ["제외일", "블로그", "링크", "제목"]
    try:
        ws = ss.worksheet(GATE_TAB)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(GATE_TAB, rows=20000, cols=len(header))
        ws.append_row(header, value_input_option="RAW")
        return ws, set()
    links = ws.col_values(header.index("링크") + 1)[1:]
    return ws, {canon_link(u) for u in links if u.strip()}


def prune_gate_tab(ws, keep_days=45):
    """오래된 제외기록 정리. RSS는 최근 글만 주므로 오래된 링크는 다시 나타나지 않는다.
    (시트가 무한정 커지는 것 방지. 실패해도 감시에는 영향 없음)"""
    vals = ws.get_all_values()
    if len(vals) < 2:
        return 0
    cutoff = (datetime.now(KST) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    keep = [r for r in vals[1:] if (r[0] if r else "") >= cutoff]
    removed = len(vals) - 1 - len(keep)
    if removed <= 0:
        return 0
    ws.batch_clear([f"A2:D{len(vals)}"])
    if keep:
        ws.batch_update([{"range": f"A2:D{len(keep) + 1}", "values": keep}], value_input_option="RAW")
    return removed


def recompute_dup_groups(card_ws):
    """매물카드 전체에서 위치(시군구+읍면동)+거래금액이 같은 묶음에
    중복그룹 번호(D1, D2…)를 매긴다. 2건 이상만 번호를 받고,
    가격이 비어 있으면 묶지 않는다(빈칸). 행은 지우지 않고 '중복그룹' 칸만 갱신."""
    vals = card_ws.get_all_values()
    if len(vals) < 2:
        return 0
    header = vals[0]
    idx = {h: i for i, h in enumerate(header)}
    if any(h not in idx for h in ("시군구", "읍면동", "거래금액", "중복그룹")):
        return 0
    si, ei, pi, gi = idx["시군구"], idx["읍면동"], idx["거래금액"], idx["중복그룹"]

    def norm(x):
        return re.sub(r"[^0-9가-힣]", "", str(x or ""))

    groups = defaultdict(list)
    for r, row in enumerate(vals[1:], start=2):
        gu = row[si] if len(row) > si else ""
        dong = row[ei] if len(row) > ei else ""
        price = row[pi] if len(row) > pi else ""
        if gu.strip() and dong.strip() and price.strip():
            groups[norm(gu) + "|" + norm(dong) + "|" + norm(price)].append(r)

    labels, n = {}, 0
    for rows in groups.values():
        if len(rows) >= 2:
            n += 1
            for r in rows:
                labels[r] = f"D{n}"

    # '중복그룹' 칸 한 컬럼을 한 번의 호출로 갱신 (분당 한도 회피)
    last = len(vals)
    col = re.sub(r"\d", "", rowcol_to_a1(1, gi + 1))
    body = [[labels.get(r, "")] for r in range(2, last + 1)]
    card_ws.batch_update(
        [{"range": f"{col}2:{col}{last}", "values": body}],
        value_input_option="RAW",
    )
    return n


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    oai = get_openai()
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    blogs = load_blogs(ss)
    card_ws, known_links = setup_card_tab(ss)
    gate_ws, gate_seen = setup_gate_tab(ss)
    seen = {canon_link(u) for u in known_links} | gate_seen

    today = datetime.now(KST).strftime("%Y-%m-%d")
    new_rows = []
    gate_rows = []      # 이번에 게이트에서 잘린 글(다음 실행부터 건너뛰도록 기록)
    n_rss_fail = n_gate_skip = n_skip = n_excl = n_gpt_fail = n_spec = n_title_skip = 0
    n_tried = 0          # GPT를 실제로 부른 글 수(실패율 계산용)
    quota_err = None     # 크레딧 소진 시 여기에 담고 즉시 중단

    print(f"감시 시작 — 블로그 {len(blogs)}개(비숙박 {len(BLOCKLIST)}개 제외), "
          f"기존 매물 {len(known_links)}건 + 게이트제외 {len(gate_seen)}건 "
          f"(모델 {MODEL}, 게이트 {GATE_BODY_CHARS}자 / 추출 {FULL_BODY_CHARS}자)")
    for bid in blogs:
        try:
            feed = fetch_feed(bid)
        except Exception as e:
            n_rss_fail += 1
            print(f"  [RSS실패] {bid}: {e}")
            time.sleep(SLEEP_SEC)
            continue

        added = 0
        for entry in feed.entries:
            link = canon_link(entry.get("link", ""))
            if not link or link in seen:
                continue
            if not recent_ok(entry, RECENT_DAYS):
                continue
            seen.add(link)

            title = (entry.get("title") or "").strip()
            posted = post_datetime(entry)

            # ── 0차 제목 사전필터: 명백한 비숙박은 본문 fetch·GPT 없이 컷(비용 0) ──
            if title_prefilter_skip(title):
                n_title_skip += 1
                gate_rows.append([today, bid, link, title])
                continue

            bid_, logno_ = parse_link(link)
            body = fetch_post_body(bid_, logno_) or entry_body(entry)  # 본문 우선, 실패 시 RSS 일부
            time.sleep(0.7)   # 본문 접속 간격 (차단 회피)

            # ── 1차 게이트: 매물이냐 아니냐 (앞 700자) ─────────────────────
            n_tried += 1
            try:
                gate = gpt_gate(oai, title, body)
            except QuotaExhausted as e:
                quota_err = e
                break
            except Exception as e:
                n_gpt_fail += 1
                print(f"  [GPT실패/게이트] {link}: {e}")
                new_rows.append(build_row(today, posted, bid, {}, title, link, "확인필요(GPT실패)"))
                added += 1
                continue

            if str(gate.get("매물여부", "")).strip() == "비매물":
                n_gate_skip += 1
                gate_rows.append([today, bid, link, title])   # 다음 실행부터 재판정 안 함
                continue    # 여기서 끝 — 2차 호출 안 함(토큰 절약)

            # ── 2차 추출: 매물일 때만 전문(4000자)으로 전 항목 ────────────
            try:
                info = gpt_extract(oai, title, body)
            except QuotaExhausted as e:
                quota_err = e
                break
            except Exception as e:
                n_gpt_fail += 1
                print(f"  [GPT실패/추출] {link}: {e}")
                new_rows.append(build_row(today, posted, bid, {}, title, link, "확인필요(GPT실패)"))
                added += 1
                continue

            if str(info.get("매물여부", "")).strip() == "비매물":   # 2차에서 뒤집힌 경우
                n_skip += 1
                gate_rows.append([today, bid, link, title])   # 확정 비매물 → 재판정 안 함
                continue

            if is_excluded_kind(info.get("종류", ""), title):   # 숙박 아니면 안 올림
                n_excl += 1
                gate_rows.append([today, bid, link, title])   # 거주형/비숙박 → 재판정 안 함
                continue

            if any(info.get(k) for k in ("대지면적", "연면적", "층수", "준공연도")):
                n_spec += 1

            new_rows.append(build_row(today, posted, bid, info, title, link, ""))
            added += 1

        if added:
            print(f"  {bid}: +{added}")
        if quota_err:
            break
        time.sleep(SLEEP_SEC)

    if new_rows:
        card_ws.append_rows(new_rows, value_input_option="RAW")

    # 게이트에서 잘린 글 기록 → 다음 실행부터 fetch·GPT 둘 다 건너뛴다
    if gate_rows:
        try:
            gate_ws.append_rows(gate_rows, value_input_option="RAW")
            print(f"게이트제외 기록: +{len(gate_rows)}건 (다음 실행부터 재판정 안 함)")
        except Exception as e:
            print(f"게이트제외 기록 실패(감시에는 영향 없음): {e}")
    try:
        n_pruned = prune_gate_tab(gate_ws)
        if n_pruned:
            print(f"게이트제외 오래된 기록 정리: -{n_pruned}건")
    except Exception as e:
        print(f"게이트제외 정리 실패(무시): {e}")

    # 매물카드 전체(과거분 포함) 중복그룹 갱신
    try:
        n_dup = recompute_dup_groups(card_ws)
        print(f"중복그룹 갱신: {n_dup}개 그룹")
    except Exception as e:
        print(f"중복그룹 갱신 실패: {e}")

    print("─" * 52)
    print(f"완료 — 새 매물 {len(new_rows)}건 추가 (그중 건물스펙 확보 {n_spec}건)")
    print(f"       0차 제목컷 {n_title_skip}(GPT 안 부름) · 1차 게이트 컷 {n_gate_skip} · "
          f"2차 비매물 {n_skip} · 거주형/비숙박 제외 {n_excl} · "
          f"GPT실패 {n_gpt_fail} · RSS실패 {n_rss_fail}곳")
    print("→ '건물스펙 확보' 건수를 보세요. 이게 0에 가까우면 광고 텍스트에 스펙이 없다는 뜻입니다.")

    # ── 조용한 실패 방지: 비정상이면 워크플로우를 '실패'로 떨어뜨린다 ──
    if quota_err:
        print("!" * 52)
        print(f"중단 — OpenAI 크레딧/쿼터 소진: {quota_err}")
        print("   platform.openai.com/settings/organization/billing 에서 충전·자동충전을 확인하세요.")
        print("   남은 글은 건드리지 않았으니, 충전 후 다음 실행에서 그대로 이어집니다.")
        raise SystemExit(1)
    if n_tried >= 5 and n_gpt_fail / n_tried >= 0.5:
        print("!" * 52)
        print(f"경고 — GPT 호출 {n_tried}건 중 {n_gpt_fail}건 실패(50% 이상). 위 [GPT실패] 로그를 보세요.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
