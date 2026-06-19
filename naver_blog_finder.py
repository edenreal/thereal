# -*- coding: utf-8 -*-
"""
경쟁 블로그 발굴기
─────────────────────────────────────────────────────────
하는 일 (주 1회):
  1) '블로그후보' 탭에서 네가 승인(O)한 블로그를 → '블로그목록'으로 옮긴다 (감시 시작)
  2) 네이버 검색 API로 숙박 매물 키워드를 검색
  3) 결과에서 블로그ID를 모아, 이미 감시 중이거나 이미 후보인 건 빼고
  4) 숙박 키워드에 여러 번 걸린 순서로 '블로그후보' 탭에 올린다
  5) 너는 후보를 보고 '승인' 칸에 O만 치면 된다 (다음 실행 때 감시 시작)

핵심: 발굴기는 후보까지만 올린다. 감시 명단 등록은 네 승인(O)을 거친다.
       (검색은 인테리어·대출 업체 블로그 같은 쓰레기도 물어오기 때문)

필요 환경변수(GitHub Secret):
  GCP_CREDENTIALS_JSON
  NAVER_CLIENT_ID
  NAVER_CLIENT_SECRET
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
MIN_HITS = 2       # 이 횟수 이상 검색에 걸린 블로그만 후보로 (노이즈 제거). 더 넓게 보려면 1로.
MODEL = "gpt-4o-mini"   # 후보 판정용. 접근 안 되면 "gpt-3.5-turbo"로.
APPROVE_MARKS = {"O", "Y", "ㅇ", "승인", "예"}


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
JUDGE_PROMPT = """다음은 네이버 블로그의 이름과 최근 글 제목 몇 개다. 이 블로그가 '숙박시설(모텔·호텔·호스텔·고시원·여관 등) 매물을 중개·소개하는 블로그'인지 판단하라.

- 모텔·호텔 등의 매매·임대 매물을 올리는 공인중개사·중개 블로그면 "숙박".
- 인테리어·시공·리모델링 업체, 대출·금융, 청소·방역, 숙박 이용후기·여행기, 기타 업종이면 "비숙박".

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
    """감시 명단(블로그목록)의 ID 집합과 워크시트를 돌려준다."""
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
    """블로그후보에서 승인(O)된 행을 블로그목록으로 옮긴다."""
    vals = cand_ws.get_all_values()
    if len(vals) < 2:
        return []
    header = vals[0]
    if "승인" not in header or "블로그ID" not in header:
        return []
    ai, bi = header.index("승인"), header.index("블로그ID")
    promoted = []
    for i, row in enumerate(vals[1:], start=2):
        mark = row[ai].strip().upper() if len(row) > ai else ""
        if mark in APPROVE_MARKS:
            bid = row[bi].strip() if len(row) > bi else ""
            if bid and bid not in blog_ids:
                promoted.append(bid)
                blog_ids.add(bid)
                cand_ws.update_cell(i, ai + 1, "등록완료")
    if promoted:
        blog_ws.append_rows([[b, "발굴기승인"] for b in promoted], value_input_option="RAW")
    return promoted


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    cid, sec = get_naver_keys()
    oai = get_openai()
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    blog_ids, blog_ws = current_blog_ids(ss)
    cand_ws = get_or_create(ss, CAND_TAB, CAND_HEADER)

    # 1) 승인된 후보 → 감시목록
    promoted = promote_approved(cand_ws, blog_ws, blog_ids)
    if promoted:
        print(f"승인 반영: {len(promoted)}개 감시목록에 추가 — {promoted}")

    # 2) 발굴 (이미 감시 중이거나 이미 후보인 건 제외 대상)
    already = blog_ids | existing_candidates(cand_ws)
    cand = defaultdict(lambda: {"name": "", "hits": 0, "titles": [], "link": ""})

    print(f"발굴 시작 — 키워드 {len(SEARCH_KEYWORDS)}개, 기존(감시+후보) {len(already)}개 제외")
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

    # 3) MIN_HITS 이상만, GPT로 숙박/비숙박 판정 후 기록
    today = datetime.now(KST).strftime("%Y-%m-%d")
    survivors = [(b, c) for b, c in sorted(cand.items(), key=lambda x: -x[1]["hits"])
                 if c["hits"] >= MIN_HITS]
    print(f"후보 {len(survivors)}개 GPT 판정 중...")

    rows = []
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
            n_yes += 1
        rows.append([today, b, c["name"], c["hits"], " / ".join(c["titles"]),
                     verdict, reason, c["link"], ""])

    if rows:
        cand_ws.append_rows(rows, value_input_option="RAW")

    print("─" * 52)
    print(f"완료 — 새 후보 {len(rows)}개 (GPT '숙박' {n_yes}개 · 판정실패 {n_judgefail}) · 승인반영 {len(promoted)}개")
    print("→ '블로그후보' 탭에서 GPT판정이 '숙박'인 줄을 보고, 맞으면 '승인' 칸에 O 를 치세요.")
    print("  ('비숙박' 줄은 평소 무시. GPT가 잘못 거른 게 있나 가끔만 확인하면 됩니다.)")


if __name__ == "__main__":
    main()
