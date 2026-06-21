# -*- coding: utf-8 -*-
"""
숙박 매물 신규 감시기 — GPT 추출 버전 (+ 종류필터 + 중복그룹)
─────────────────────────────────────────────────────────
하는 일:
  1) '블로그목록' 탭의 블로그 RSS를 돌면서
  2) 전에 본 적 없는 새 글(=새 매물 후보)만 골라
  3) 제목+본문(RSS 일부)을 GPT에게 주고
       시도·시군구·종류·형태·거래금액·매출·객실수를 뽑게 하고
       매물이 아니면(맛집·정보·일상 글) 걸러내고
  4) [신규] 종류가 고시원·거주형·완전비숙박이면 시트에 안 올린다
  5) '매물카드' 탭에 한 줄씩 쌓는다.
  6) [신규] 다 쌓은 뒤, 매물카드 전체에서 위치+가격이 같은 도배 매물에
       중복그룹 번호(D1, D2…)를 매긴다. (행은 안 지우고 표시만)

필요 환경변수(둘 다 GitHub Secret):
  GCP_CREDENTIALS_JSON  : 구글 서비스계정 키(JSON 문자열)
  OPENAI_API_KEY        : OpenAI 키 (기존 autogit에서 쓰던 것 재사용)

준비물: 대상 구글시트를 서비스계정 이메일에 '편집자'로 공유해 둘 것.

※ 기존 '매물카드'가 '중복그룹' 칸이 없던 형식이면, 데이터를 그대로 두고
  '중복그룹' 칸만 자동으로 추가한다. (백업/재생성 안 함, 중복도 안 쌓임)
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
CARD_HEADER = ["감지일", "게시일", "블로그", "시도", "시군구", "읍면동", "종류", "형태",
               "거래금액", "매출", "객실수", "제목", "링크", "상태", "중복그룹"]
PREV_CARD_HEADER = CARD_HEADER[:-1]   # '중복그룹' 칸이 없던 이전 버전(=마이그레이션 대상)

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

# 비숙박 전문 블로그 — 감시에서 영구 제외(발굴기가 다시 편입해도 무시).
# 여기에 블로그ID만 추가하면 그 블로그는 다시 안 긁는다.
BLOCKLIST = {
    "stewzinnia59",   # 부동산 분양/상담
    "ksanchoi",       # 경매
    "auctionrun3988", # 경매
    "sbjjjang",       # 수익형 상가
    "kj-4848",        # 고시원
    "jijonbpbp",      # 리조트 회원권
    "kkanglive",      # 투자일기
    "moneyschool300", # 숙박업 강의/클래스
}

EXTRACT_BODY_CHARS = 800   # GPT에 보낼 본문 길이(글자). 매물 정보는 앞부분에 몰려 있어 800이면 충분.
GPT_RETRIES = 3            # rate limit/timeout 시 재시도 횟수(1→2→4초 백오프)

# ─────────────────── 종류 필터 (고시원·거주형·비숙박 제거) ───────────────────
# 숙박 키워드: 종류에 이게 들어가면 '무조건 통과'.
#   (민박주택·한옥스테이·관광용 호텔처럼 거주/비숙박 단어가 섞여도 숙박이면 구제)
LODGING_KW = [
    "모텔", "호텔", "여관", "여인숙", "호스텔", "펜션", "게스트하우스", "게하",
    "민박", "풀빌", "무인텔", "무인호텔", "관광", "비지니스", "비즈니스",
    "레지던스", "캡슐", "한옥", "숙박", "스테이", "단기숙소", "체류형", "쉼터", "세컨하우스",
]
# 명백한 거주형·완전비숙박: 위 숙박 키워드가 '없으면서' 이게 들어가면 제외.
EXCLUDE_KW = [
    "고시원", "고시텔", "원룸텔", "원룸", "주택", "주거시설", "오피스텔",
    "상가", "빌딩", "사옥", "건물", "토지", "대지", "임야",
    "병원", "요양병원", "의료시설", "체육", "PC방", "피씨방",
    "이자카야", "회원권", "근린생활시설", "위락시설", "캠핑장",
]


def is_excluded_kind(kind):
    """고시원·거주형·완전비숙박이면 True(시트에 안 올림).
    종류가 비어 있거나(=GPT가 못 뽑음) 애매하면 False(그대로 남김)."""
    s = str(kind or "").strip()
    if not s:
        return False
    if any(k in s for k in LODGING_KW):
        return False
    if any(k in s for k in EXCLUDE_KW):
        return True
    return False


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
    content = EXTRACT_PROMPT.format(title=title, body=(body or "")[:EXTRACT_BODY_CHARS])
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
            last_err = e
            if attempt < GPT_RETRIES - 1:
                time.sleep(2 ** attempt)   # 1초 → 2초 → 4초 대기 후 재시도
    raise last_err   # 끝까지 실패하면 호출부에서 '확인필요(GPT실패)'로 기록


def build_row(today, posted, bid, info, title, link, status="", dup=""):
    return [
        today, posted, bid,
        info.get("시도", ""), info.get("시군구", ""), info.get("읍면동", ""),
        info.get("종류", ""), info.get("형태", ""),
        info.get("거래금액", ""), info.get("매출", ""), info.get("객실수", ""),
        title, link, status, dup,
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
        if b in BLOCKLIST:        # 비숙박 전문 블로그는 건너뛴다
            continue
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def setup_card_tab(ss):
    """매물카드 탭을 준비한다.
      - 이미 신형식(중복그룹 칸 있음): 그대로 사용
      - 중복그룹 칸만 없는 이전 형식: 칸만 추가(데이터 보존)
      - 그 외 진짜 옛 형식: 백업 후 새로 만듦
    반환: (워크시트, 중복방지용 기존 링크 리스트)"""
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

    if header == PREV_CARD_HEADER:                 # '중복그룹' 칸만 추가 (데이터 보존)
        ws.update_cell(1, len(CARD_HEADER), "중복그룹")
        print("  매물카드에 '중복그룹' 칸을 추가했습니다(기존 데이터 보존).")
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


def recompute_dup_groups(card_ws):
    """매물카드 전체에서 위치(시군구+읍면동)+거래금액이 같은 묶음에
    중복그룹 번호(D1, D2…)를 매긴다. 2건 이상만 번호를 받고,
    가격이 비어 있으면 묶지 않는다(빈칸). 행은 지우지 않고 '중복그룹' 칸만 갱신.
    반환: 중복 그룹 개수."""
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
    col = re.sub(r"\d", "", rowcol_to_a1(1, gi + 1))   # 컬럼 문자(예: O)
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
    seen = {canon_link(u) for u in known_links}

    today = datetime.now(KST).strftime("%Y-%m-%d")
    new_rows = []
    n_rss_fail = n_skip = n_excl = n_gpt_fail = 0

    print(f"감시 시작 — 블로그 {len(blogs)}개(비숙박 {len(BLOCKLIST)}개 제외), 기존 {len(seen)}건 (모델 {MODEL})")
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
            bid_, logno_ = parse_link(link)
            body = fetch_post_body(bid_, logno_) or entry_body(entry)  # 본문 우선, 실패 시 RSS 일부
            time.sleep(0.7)   # 본문 접속 간격 (차단 회피)

            try:
                info = gpt_extract(oai, title, body)
            except Exception as e:
                n_gpt_fail += 1
                new_rows.append(build_row(today, posted, bid, {}, title, link, "확인필요(GPT실패)"))
                added += 1
                continue

            if str(info.get("매물여부", "")).strip() == "비매물":
                n_skip += 1
                continue

            if is_excluded_kind(info.get("종류", "")):   # 고시원·거주형·비숙박이면 안 올림
                n_excl += 1
                continue

            new_rows.append(build_row(today, posted, bid, info, title, link, ""))
            added += 1

        if added:
            print(f"  {bid}: +{added}")
        time.sleep(SLEEP_SEC)

    if new_rows:
        card_ws.append_rows(new_rows, value_input_option="RAW")

    # 매물카드 전체(과거분 포함) 중복그룹 갱신
    try:
        n_dup = recompute_dup_groups(card_ws)
        print(f"중복그룹 갱신: {n_dup}개 그룹")
    except Exception as e:
        print(f"중복그룹 갱신 실패: {e}")

    print("─" * 52)
    print(f"완료 — 새 매물 {len(new_rows)}건 추가  "
          f"(비매물 {n_skip} · 거주형/비숙박 제외 {n_excl} · "
          f"GPT실패 {n_gpt_fail} · RSS실패 {n_rss_fail}곳)")


if __name__ == "__main__":
    main()
