# -*- coding: utf-8 -*-
"""
숙박 매물 신규 감시기 — GPT 추출 버전
─────────────────────────────────────────────────────────
하는 일:
  1) '블로그목록' 탭의 블로그 RSS를 돌면서
  2) 전에 본 적 없는 새 글(=새 매물 후보)만 골라
  3) 제목+본문(RSS 일부)을 GPT에게 주고
       시도·시군구·종류·형태·거래금액·매출·객실수를 뽑게 하고
       매물이 아니면(맛집·정보·일상 글) 걸러내고
  4) '매물카드' 탭에 한 줄씩 쌓는다.

필요 환경변수(둘 다 GitHub Secret):
  GCP_CREDENTIALS_JSON  : 구글 서비스계정 키(JSON 문자열)
  OPENAI_API_KEY        : OpenAI 키 (기존 autogit에서 쓰던 것 재사용)

준비물: 대상 구글시트를 서비스계정 이메일에 '편집자'로 공유해 둘 것.

※ 기존 '매물카드' 탭이 옛 형식이면 자동으로 '매물카드_old'로 백업하고
  새 형식 탭을 새로 만든다. 기존 데이터는 보존되고 중복도 안 쌓인다.
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
from openai import OpenAI

# ════════════════════════════════ 설정 ════════════════════════════════
SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
BLOG_TAB = "블로그목록"
CARD_TAB = "매물카드"
CARD_HEADER = ["감지일", "블로그", "시도", "시군구", "읍면동", "종류", "형태",
               "거래금액", "매출", "객실수", "제목", "링크", "상태"]

RECENT_DAYS = 14       # 이 기간 안에 올라온 새 글만 (첫 실행 폭주/놓침 방지)
SLEEP_SEC = 2.0        # 블로그 사이 간격 (천천히 돌아 차단 회피)
MODEL = "gpt-4o-mini"  # 접근이 안 되면 "gpt-3.5-turbo" 로 바꿔도 됨
RSS_TMPL = "https://rss.blog.naver.com/{}.xml"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
KST = timezone(timedelta(hours=9))

# 처음 한 번만 쓰이는 시드. '블로그목록' 탭이 비어 있으면 이걸로 채운다.
SEED_BLOGS = [
    "staybrief", "dddi570", "noble41888", "s7620mmjoe", "msoochae",
    "yoondang__", "eitting2018", "water_ah_", "gyonryoru", "spacementor",
    "korea-7942-", "7979sic", "sojwa07", "jjsskk0815", "ska6565",
]

# ════════════════════════════════ GPT ════════════════════════════════
EXTRACT_PROMPT = """다음은 부동산 중개 블로그 글이다. 숙박시설(모텔·호텔·호스텔·고시원·펜션·게스트하우스·여관 등)의 매매 또는 임대 매물 광고인지 판단하고, 매물이면 항목을 추출하라.

규칙:
- 글에 실제로 있는 내용만 쓴다. 절대 지어내지 않는다. 모르면 빈 문자열 "".
- "거래금액"은 보증금·월세·매매가 같은 실제 거래 가격이다. "매출"은 월매출·순수익이다. 이 둘을 절대 섞지 않는다.
- "거래금액"과 "매출"은 글에 적힌 표기 그대로 쓴다(예: "보증금 3억", "월세 3500만", "매매가 25억", "월매출 4000만", "순수익 800만"). 절대 원 단위 숫자로 풀어쓰지 않는다(예: 40000000 같은 형태 금지).
- 맛집·후기·정보·상식·일상 글이면 "매물여부"를 "비매물"로 한다.
- "시도"는 광역시/도 정식명(서울특별시, 경기도, 부산광역시 등). 동·역 이름으로 분명히 알 수 있으면 채운다(예: 강동역→서울특별시, 수원→경기도). 애매하면 "".
- "시군구"는 시/군/구(예: 수원시, 강동구). "읍면동"은 동·읍·면·리(예: 구운동, 초량동, 부강면). 없으면 "".
- "형태"는 매매면 "매매", 임대면 "임대", 둘 다면 "매매/임대".
- "객실수"는 숫자+실 형식(예: 54실). 없으면 "".

제목: {title}
본문: {body}

아래 JSON 형식으로만 답하라:
{{"매물여부":"매물 또는 비매물","시도":"","시군구":"","읍면동":"","종류":"","형태":"","거래금액":"","매출":"","객실수":""}}"""


def get_openai():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다. GitHub Secret에 추가하세요.")
    return OpenAI(api_key=key)


def gpt_extract(oai, title, body):
    content = EXTRACT_PROMPT.format(title=title, body=(body or "")[:1500])
    resp = oai.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": content}],
        temperature=0,
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


def build_row(today, bid, info, title, link, status=""):
    return [
        today, bid,
        info.get("시도", ""), info.get("시군구", ""), info.get("읍면동", ""),
        info.get("종류", ""), info.get("형태", ""),
        info.get("거래금액", ""), info.get("매출", ""), info.get("객실수", ""),
        title, link, status,
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
    이미지 표에 박힌 정보는 텍스트가 없으므로 못 가져온다(그건 비전 영역)."""
    if not (bid and logno):
        return ""
    url = f"https://m.blog.naver.com/{bid}/{logno}"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        html = r.text
        i = html.find("se-main-container")   # 본문 컨테이너 시작
        if i != -1:
            html = html[i:i + 20000]
        return strip_html(html)[:3000]
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
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def setup_card_tab(ss):
    """매물카드 탭을 준비한다. 옛 형식이면 백업 후 새로 만든다.
    반환: (새 워크시트, 중복방지용 기존 링크 리스트)"""
    try:
        ws = ss.worksheet(CARD_TAB)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(CARD_TAB, rows=3000, cols=len(CARD_HEADER) + 2)
        ws.append_row(CARD_HEADER, value_input_option="RAW")
        return ws, []

    header = ws.row_values(1)
    if header == CARD_HEADER:                      # 이미 새 형식
        links = ws.col_values(CARD_HEADER.index("링크") + 1)[1:]
        return ws, links

    # 옛 형식 → 기존 링크 수집(중복방지) 후 백업, 새 탭 생성
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


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    oai = get_openai()
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    blogs = load_blogs(ss)
    card_ws, known_links = setup_card_tab(ss)
    seen = {canon_link(u) for u in known_links}

    today = datetime.now(KST).strftime("%Y-%m-%d")
    new_rows = []
    n_rss_fail = n_skip = n_gpt_fail = 0

    print(f"감시 시작 — 블로그 {len(blogs)}개, 기존 {len(seen)}건 (모델 {MODEL})")
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
            bid_, logno_ = parse_link(link)
            body = fetch_post_body(bid_, logno_) or entry_body(entry)  # 본문 우선, 실패 시 RSS 일부
            time.sleep(0.7)   # 본문 접속 간격 (차단 회피)

            try:
                info = gpt_extract(oai, title, body)
            except Exception as e:
                n_gpt_fail += 1
                new_rows.append(build_row(today, bid, {}, title, link, "확인필요(GPT실패)"))
                added += 1
                continue

            if str(info.get("매물여부", "")).strip() == "비매물":
                n_skip += 1
                continue

            new_rows.append(build_row(today, bid, info, title, link, ""))
            added += 1

        if added:
            print(f"  {bid}: +{added}")
        time.sleep(SLEEP_SEC)

    if new_rows:
        card_ws.append_rows(new_rows, value_input_option="RAW")

    print("─" * 52)
    print(f"완료 — 새 매물 {len(new_rows)}건 추가  "
          f"(비매물 제외 {n_skip} · GPT실패 {n_gpt_fail} · RSS실패 {n_rss_fail}곳)")


if __name__ == "__main__":
    main()
