# -*- coding: utf-8 -*-
"""
경쟁 블로그 발굴기 — 자동편입 버전
─────────────────────────────────────────────────────────
하는 일 (주 1회):
  1) 네이버 검색 API로 숙박 매물 키워드를 검색
  2) 결과에서 블로그ID를 모아, 이미 감시 중·이미 후보·BLOCKLIST인 건 빼고
  3) 숙박 키워드에 여러 번 걸린 블로그를 GPT로 '숙박/비숙박' 판정
  4) [변경] GPT가 '숙박'이라고 하면 → 사람 승인 없이 바로 '블로그목록'에 편입(자동)
            '비숙박'·'확인필요'는 → '블로그후보' 탭에 기록만 (편입 안 함)

  ※ 그래서 너는 발굴기를 신경 쓸 필요가 없다. 주 1회 알아서 알짜를 주워 담는다.
    비숙박이 잘못 편입된 게 보이면, 감시기 BLOCKLIST에 그 블로그ID만 추가하면 끝.
  ※ 후보 탭에서 직접 '승인' 칸에 O를 쳐도 여전히 편입된다(수동 보강용).

필요 환경변수(GitHub Secret):
  GCP_CREDENTIALS_JSON / NAVER_CLIENT_ID / NAVER_CLIENT_SECRET / OPENAI_API_KEY
"""
import os
import re
import json
import time
import html
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import requests
import gspread
from gspread.utils import rowcol_to_a1
from openai import OpenAI

# ════════════════════════════════ 설정 ════════════════════════════════
SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
BLOG_TAB = "블로그목록"
CAND_TAB = "블로그후보"
CAND_HEADER = ["발견일", "블로그ID", "블로거명", "등장횟수", "샘플제목",
               "GPT판정", "GPT이유", "블로그주소", "승인"]
KST = timezone(timedelta(hours=9))

SEARCH_URL = "https://openapi.naver.com/v1/search/blog.json"
SEARCH_KEYWORDS = [
    "모텔 매매", "모텔 임대", "호텔 매매", "호텔 임대",
    "호스텔 매매", "고시원 매매", "숙박시설 매매", "숙박시설 임대",
    "모텔 매물", "여관 매매",
]
DISPLAY = 100      # 키워드당 가져올 검색 결과 수 (최대 100)
MIN_HITS = 2       # 이 횟수 이상 검색에 걸린 블로그만 후보로 (노이즈 제거)
MODEL = "gpt-4o-mini"
APPROVE_MARKS = {"O", "Y", "ㅇ", "승인", "예"}

# 비숙박 전문 블로그 — 후보로도 올리지 않는다(감시기 BLOCKLIST와 동일하게 유지).
BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
    "kkanglive", "moneyschool300",
}


# ════════════════════════════════ 유틸 ════════════════════════════════
def strip_tags(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    return html.unescape(re.sub(r"\s+", " ", s)).strip()


def bid_from(url):
    m = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", url or "")
    return m.group(1) if m else ""


def get_naver_keys():
    cid = os.getenv("NAVER_CLIENT_ID")
    sec = os.getenv("NAVER_CLIENT_SECRET")
    if not (cid and sec):
        raise SystemExit("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 없습니다.")
    return cid, sec


def search_blog(kw, cid, sec, display=DISPLAY):
    headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": sec}
    params = {"query": kw, "display": display, "sort": "date"}
    r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json().get("items", [])


# ════════════════════════════════ GPT 판정 ════════════════════════════════
JUDGE_PROMPT = """다음은 네이버 블로그의 이름과 최근 글 제목 몇 개다. 이 블로그가 '숙박시설(모텔·호텔·호스텔·여관·펜션·게스트하우스 등) 매물을 중개·소개하는 블로그'인지 판단하라.

"숙박"으로 판정:
- 모텔·호텔·호스텔·여관·펜션·게스트하우스 등의 매매·임대 매물을 실제로 올리는 공인중개사·중개 블로그.

"비숙박"으로 판정 (아래는 전부 비숙박):
- 인테리어·시공·리모델링 업체, 대출·금융, 청소·방역
- 경매 물건을 나열하는 경매 전문 블로그(제목에 "타경"·사건번호 위주)
- 상가·빌딩·공장·토지·사무실 등 숙박이 아닌 부동산 위주
- 투자일기·재테크·부동산 일상/일기 블로그
- 강의·원데이클래스·세미나·교육·컨설팅 홍보 블로그
- 회원권·콘도·분양·분양상담
- 숙박 이용후기·여행기·맛집·기타 업종

판단이 애매하면 "비숙박"으로 한다. (이 판정으로 자동 편입되므로 보수적으로 판단하라.)

블로그명: {name}
최근 글 제목:
{titles}

JSON으로만 답하라: {{"판정":"숙박 또는 비숙박","이유":"15자 이내로 짧게"}}"""


def get_openai():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다.")
    return OpenAI(api_key=key)


def gpt_judge(oai, name, titles):
    content = JUDGE_PROMPT.format(name=name, titles="\n".join(f"- {t}" for t in titles))
    resp = oai.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": content}],
        temperature=0,
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


# ════════════════════════════════ 시트 ════════════════════════════════
def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def get_or_create(ss, title, header):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title, rows=3000, cols=max(10, len(header)))
        ws.append_row(header, value_input_option="RAW")
        return ws


def current_blog_ids(ss):
    try:
        ws = ss.worksheet(BLOG_TAB)
    except gspread.WorksheetNotFound:
        ws = get_or_create(ss, BLOG_TAB, ["블로그ID", "메모"])
        return set(), ws
    raw = ws.col_values(1)[1:]
    ids = {(bid_from(v) or v.strip()) for v in raw if v.strip()}
    return ids, ws


def existing_candidates(cand_ws):
    raw = cand_ws.col_values(2)[1:]   # 블로그ID 열
    return {v.strip() for v in raw if v.strip()}


def promote_approved(cand_ws, blog_ws, blog_ids):
    """블로그후보에서 승인(O)된 행을 블로그목록으로 옮긴다(수동 보강용).
    자동편입과 별개로, 사람이 직접 O를 친 것도 계속 처리한다."""
    vals = cand_ws.get_all_values()
    if len(vals) < 2:
        return []
    header = vals[0]
    if "승인" not in header or "블로그ID" not in header:
        return []
    ai, bi = header.index("승인"), header.index("블로그ID")

    promoted, mark_rows = [], []
    for i, row in enumerate(vals[1:], start=2):
        mark = row[ai].strip().upper() if len(row) > ai else ""
        bid = row[bi].strip() if len(row) > bi else ""
        is_approved = mark in APPROVE_MARKS
        is_done = (mark == "등록완료" or mark == "자동편입")
        if (is_approved or is_done) and bid and bid not in blog_ids:
            promoted.append(bid)
            blog_ids.add(bid)
            if is_approved:
                mark_rows.append(i)

    if promoted:
        blog_ws.append_rows([[b, "후보승인"] for b in promoted], value_input_option="RAW")
        if mark_rows:
            col = re.sub(r"\d", "", rowcol_to_a1(1, ai + 1))
            cand_ws.batch_update(
                [{"range": f"{col}{r}", "values": [["등록완료"]]} for r in mark_rows]
            )
    return promoted


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    cid, sec = get_naver_keys()
    oai = get_openai()
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    blog_ids, blog_ws = current_blog_ids(ss)
    cand_ws = get_or_create(ss, CAND_TAB, CAND_HEADER)

    # 1) 사람이 직접 O 친 후보도 편입(수동 보강)
    promoted = promote_approved(cand_ws, blog_ws, blog_ids)
    if promoted:
        print(f"수동승인 반영: {len(promoted)}개 — {promoted}")

    # 2) 발굴 (감시 중 + 후보 + BLOCKLIST 제외)
    already = blog_ids | existing_candidates(cand_ws) | BLOCKLIST
    cand = defaultdict(lambda: {"name": "", "hits": 0, "titles": [], "link": ""})

    print(f"발굴 시작 — 키워드 {len(SEARCH_KEYWORDS)}개, 제외 {len(already)}개(감시+후보+차단 {len(BLOCKLIST)})")
    for kw in SEARCH_KEYWORDS:
        try:
            items = search_blog(kw, cid, sec)
        except Exception as e:
            print(f"  [검색실패] {kw}: {e}")
            continue
        for it in items:
            b = bid_from(it.get("bloggerlink", ""))
            if not b or b in already:
                continue
            c = cand[b]
            c["name"] = strip_tags(it.get("bloggername", ""))
            c["link"] = it.get("bloggerlink", "")
            c["hits"] += 1
            t = strip_tags(it.get("title", ""))
            if t and len(c["titles"]) < 3:
                c["titles"].append(t)
        print(f"  '{kw}': 누적 신규후보 {len(cand)}개")
        time.sleep(0.3)

    # 3) MIN_HITS 이상만 GPT 판정 → '숙박'이면 자동 편입
    today = datetime.now(KST).strftime("%Y-%m-%d")
    survivors = [(b, c) for b, c in sorted(cand.items(), key=lambda x: -x[1]["hits"])
                 if c["hits"] >= MIN_HITS]
    print(f"후보 {len(survivors)}개 GPT 판정 중...")

    rows, auto = [], []
    n_yes = n_judgefail = 0
    for b, c in survivors:
        try:
            j = gpt_judge(oai, c["name"], c["titles"])
            verdict = str(j.get("판정", "")).strip()
            reason = str(j.get("이유", "")).strip()
        except Exception:
            verdict, reason = "확인필요", "GPT판정실패"
            n_judgefail += 1

        if verdict == "숙박":
            auto.append(b)
            blog_ids.add(b)
            mark = "자동편입"
            n_yes += 1
        else:
            mark = ""    # 비숙박·확인필요는 후보 탭에 기록만(편입 안 함)
        rows.append([today, b, c["name"], c["hits"], " / ".join(c["titles"]),
                     verdict, reason, c["link"], mark])

    if rows:
        cand_ws.append_rows(rows, value_input_option="RAW")
    if auto:
        blog_ws.append_rows([[b, "발굴자동편입"] for b in auto], value_input_option="RAW")

    print("─" * 52)
    print(f"완료 — 새 후보 {len(rows)}개 · GPT '숙박' {n_yes}개 자동편입 · "
          f"판정실패 {n_judgefail} · 수동승인 {len(promoted)}")
    print("→ 비숙박이 잘못 편입된 게 보이면, 감시기와 발굴기의 BLOCKLIST에 그 블로그ID만 추가하세요.")


if __name__ == "__main__":
    main()
