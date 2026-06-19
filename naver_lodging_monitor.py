# -*- coding: utf-8 -*-
"""
숙박 매물 신규 감시기 (감시기 단독 버전)
─────────────────────────────────────────────────────────
하는 일:
  1) '블로그목록' 탭의 블로그 RSS를 돌면서
  2) 전에 본 적 없는 새 글(=새 매물)만 골라
  3) 제목에서 동네·종류·형태·금액을 뽑고
     (네 개가 다 있으면 거기서 끝, 하나라도 없으면 본문 앞부분에서 빠진 것 + 객실수 보충)
  4) '매물카드' 탭에 한 줄씩 쌓는다.

안 하는 일: 표 이미지 판독, GPT 호출, 본문 전체 크롤링. (전부 의도적으로 제외)

필요 환경변수:
  GCP_CREDENTIALS_JSON  : 구글 서비스계정 키(JSON 문자열). 기존 것 그대로 사용.

준비물(중요): 대상 구글시트를 서비스계정 이메일에 '편집자'로 공유해야 함.
"""
import os
import re
import json
import time
import calendar
from datetime import datetime, timezone, timedelta

import requests
import feedparser
import gspread

# ════════════════════════════════ 설정 ════════════════════════════════
SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
BLOG_TAB = "블로그목록"
CARD_TAB = "매물카드"
CARD_HEADER = ["감지일", "블로그", "종류", "동네", "형태", "금액", "객실수", "제목", "링크"]

RECENT_DAYS = 14      # 이 기간 안에 올라온 새 글만 (첫 실행 폭주 방지 + 놓침 방지)
SLEEP_SEC = 2.0       # 블로그 사이 간격 (천천히 돌아 차단 회피)
RSS_TMPL = "https://rss.blog.naver.com/{}.xml"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
KST = timezone(timedelta(hours=9))

# 처음 한 번만 쓰이는 시드. '블로그목록' 탭이 비어 있으면 이걸로 채운다.
SEED_BLOGS = [
    "staybrief", "dddi570", "noble41888", "s7620mmjoe", "msoochae",
    "yoondang__", "eitting2018", "water_ah_", "gyonryoru", "spacementor",
    "korea-7942-", "7979sic", "sojwa07", "jjsskk0815", "ska6565",
]

# ════════════════════════════════ 파서 ════════════════════════════════
METRO = {
    "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천", "광주": "광주",
    "대전": "대전", "울산": "울산", "세종": "세종", "경기": "경기", "강원": "강원",
    "충북": "충북", "충남": "충남", "전북": "전북", "전남": "전남",
    "경북": "경북", "경남": "경남", "제주": "제주",
}
CITY = [
    "수원", "성남", "의정부", "안양", "부천", "광명", "평택", "동두천", "안산", "고양",
    "과천", "구리", "남양주", "오산", "시흥", "군포", "의왕", "하남", "용인", "파주",
    "이천", "안성", "김포", "화성", "양주", "포천", "여주",
    "춘천", "원주", "강릉", "동해", "태백", "속초", "삼척",
    "청주", "충주", "제천", "천안", "공주", "보령", "아산", "서산", "논산", "계룡", "당진",
    "전주", "군산", "익산", "정읍", "남원", "김제", "목포", "여수", "순천", "나주", "광양",
    "포항", "경주", "김천", "안동", "구미", "영주", "영천", "상주", "문경", "경산",
    "창원", "진주", "통영", "사천", "김해", "밀양", "거제", "양산", "서귀포",
]
TYPES = [
    ("호스텔", "호스텔"), ("게스트하우스", "게스트하우스"), ("게하", "게스트하우스"),
    ("무인텔", "무인텔"), ("모텔", "모텔"), ("호텔", "호텔"),
    ("고시원", "고시원"), ("고시텔", "고시원"), ("리빙텔", "고시원"),
    ("펜션", "펜션"), ("여인숙", "여관"), ("여관", "여관"),
    ("민박", "민박"), ("숙박시설", "숙박시설"), ("숙박업", "숙박시설"),
]
MONEY = r"(\d[\d,]*(?:\.\d+)?\s*억(?:\s*\d[\d,]*\s*만원?)?|\d[\d,]*\s*천만?원?|\d[\d,]*\s*만원?)"
PRICE_BAD_CTX = ["매출", "수익", "평단", "융자", "대출", "관리비", "이상", "이하", "도보"]
NON_REGION = {
    "무권리", "권리", "관리", "처리", "정리", "분리", "수리", "거리",
    "우선", "만실", "마무리", "유리", "셀프", "서리", "온리", "프리",
}


def parse_region(text):
    metro = next((v for k, v in METRO.items() if k in text), "")
    city = next((c for c in CITY if c in text), "")
    sub = ""
    for m in re.finditer(r"([가-힣]{2,4}(?:구|동|읍|면|리|역))", text):
        if m.group(1) not in NON_REGION:
            sub = m.group(1)
            break
    parts = []
    head = metro or city
    if head:
        parts.append(head)
    if sub and sub != head and head not in sub:
        parts.append(sub)
    if not parts and city:
        parts.append(city)
    return " ".join(parts)


def parse_type(text):
    for kw, label in TYPES:
        if kw in text:
            return label
    return ""


def parse_deal(text):
    sale = any(k in text for k in ["매매", "매각"])
    lease = any(k in text for k in ["임대", "전세", "월세", "무권리"])
    if sale and lease:
        return "매매/임대"
    if sale:
        return "매매"
    if lease:
        return "임대"
    return ""


def parse_price(text):
    out = []
    m = re.search(r"보증금\s*[:：]?\s*" + MONEY, text)
    if m:
        out.append("보증금 " + re.sub(r"\s+", "", m.group(1)))
    m = re.search(r"(?:월세|임대료)\s*[:：]?\s*" + MONEY, text)
    if m:
        out.append("월세 " + re.sub(r"\s+", "", m.group(1)))
    if out:
        return " / ".join(out)
    m = re.search(r"(?:매매가|매매금액|매매)\s*[:：]?\s*" + MONEY, text)
    if m:
        return "매매 " + re.sub(r"\s+", "", m.group(1))
    for m in re.finditer(MONEY, text):
        ctx = text[max(0, m.start() - 6):m.end() + 3]
        if any(bad in ctx for bad in PRICE_BAD_CTX):
            continue
        return re.sub(r"\s+", "", m.group(1))
    return ""


def parse_rooms(text):
    m = re.search(r"(\d+)\s*객실", text)
    if m:
        return m.group(1) + "실"
    m = re.search(r"객실\s*[은는이가]?\s*(?:및\s*욕실\s*)?수?\s*[:：]?\s*(\d+)", text)
    if m:
        return m.group(1) + "실"
    m = re.search(r"(\d+)\s*개의?\s*객실", text)
    if m:
        return m.group(1) + "실"
    m = re.search(r"방\s*(?:갯수|개수)?\s*[:：]?\s*(\d+)\s*개", text)
    if m:
        return m.group(1) + "실"
    m = re.search(r"(?<![가-힣])(\d+)\s*실(?![장비무])", text)
    if m:
        return m.group(1) + "실"
    return ""


def parse_card(title, body_head=""):
    """제목 먼저. 동네·종류·형태·금액 4개가 다 있으면 본문 안 봄.
    하나라도 없으면 본문 앞부분에서 빠진 것 + 객실수 보충."""
    region = parse_region(title)
    typ = parse_type(title)
    deal = parse_deal(title)
    price = parse_price(title)
    rooms = parse_rooms(title)        # 제목 파싱은 공짜 → 객실수도 제목에선 항상 시도
    used_body = False
    if not (region and typ and deal and price):
        used_body = True
        head = (body_head or "")[:700]
        region = region or parse_region(head)
        typ = typ or parse_type(head)
        deal = deal or parse_deal(head)
        price = price or parse_price(head)
        rooms = rooms or parse_rooms(head)
    return {"종류": typ, "동네": region, "형태": deal,
            "금액": price, "객실수": rooms, "_body": used_body}


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
        return True  # 날짜를 모르면 일단 통과
    dt = datetime.fromtimestamp(calendar.timegm(p), tz=timezone.utc)
    return (datetime.now(timezone.utc) - dt) <= timedelta(days=days)


def fetch_feed(bid):
    r = requests.get(RSS_TMPL.format(bid), headers=UA, timeout=20)
    r.raise_for_status()
    return feedparser.parse(r.content)


# ════════════════════════════════ 시트 ════════════════════════════════
def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def get_or_create_ws(ss, title, header=None):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=2000, cols=max(20, len(header or [])))
        if header:
            ws.append_row(header, value_input_option="RAW")
        return ws


def load_blogs(ss):
    ws = get_or_create_ws(ss, BLOG_TAB, header=["블로그ID", "메모"])
    raw = ws.col_values(1)[1:]  # 헤더 제외, 1열
    ids = [b for b in (extract_bid(v) for v in raw) if b]
    if not ids:  # 비었으면 시드 주입
        ws.append_rows([[b, "seed"] for b in SEED_BLOGS], value_input_option="RAW")
        ids = list(SEED_BLOGS)
    seen, out = set(), []
    for b in ids:
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    blogs = load_blogs(ss)
    card_ws = get_or_create_ws(ss, CARD_TAB, header=CARD_HEADER)

    existing = card_ws.col_values(len(CARD_HEADER))[1:]  # 링크 열
    seen = {canon_link(u) for u in existing}

    today = datetime.now(KST).strftime("%Y-%m-%d")
    new_rows = []
    n_fail = n_body = 0

    print(f"감시 시작 — 블로그 {len(blogs)}개, 기존 매물 {len(seen)}건")
    for bid in blogs:
        try:
            feed = fetch_feed(bid)
        except Exception as e:
            n_fail += 1
            print(f"  [RSS 실패] {bid}: {e}")
            time.sleep(SLEEP_SEC)
            continue

        added = 0
        for entry in feed.entries:
            link = canon_link(entry.get("link", ""))
            if not link or link in seen:
                continue
            if not recent_ok(entry, RECENT_DAYS):
                continue
            title = (entry.get("title") or "").strip()
            card = parse_card(title, entry_body(entry))
            if card["_body"]:
                n_body += 1
            new_rows.append([today, bid, card["종류"], card["동네"], card["형태"],
                             card["금액"], card["객실수"], title, link])
            seen.add(link)
            added += 1
        if added:
            print(f"  {bid}: 새 매물 {added}건")
        time.sleep(SLEEP_SEC)

    if new_rows:
        card_ws.append_rows(new_rows, value_input_option="RAW")

    print("─" * 50)
    print(f"완료 — 새 매물 {len(new_rows)}건 추가 "
          f"(본문보충 {n_body}건 · RSS실패 {n_fail}곳)")


if __name__ == "__main__":
    main()
