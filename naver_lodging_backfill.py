# -*- coding: utf-8 -*-
"""
매물카드 백필 v2 — 사용승인 '월일' 재추출
─────────────────────────────────────────────────────────
왜 다시 도나:
  스팟체크(사람 로드뷰 채점 20건, 정답 18건)에서 광고 원문에 사용승인이
  월일까지(예: 1989.8.25) 적힌 게 19건이었다. 그런데 예전 프롬프트가
  "연도 4자리만" 뽑게 시켜서 월일을 버렸다.
  같은 동네에 같은 해 준공 건물은 흔하지만, 같은 '날짜' 승인은 사실상
  유일하다 → 월일은 옆건물 오답을 가르는 최강 변별 키다.
  게다가 면적이 오염된(광고≠대장) 매물도 날짜로는 잡힐 수 있다.

무엇을 하나:
  '매물카드'에서 아래 행만 골라 본문을 다시 가져와 재추출한다:
    ① 준공연도가 연도 4자리만 있는 행 (예: "1989")  → 월일로 업그레이드 시도
    ② 준공연도는 빈칸인데 다른 스펙(대지·연·층수)은 있는 행 → 승인일만 놓쳤을 가능성
  ※ 스펙이 전무한 행(≈293건)은 이미 "원문에 없음"으로 판정 끝 → 재처리 안 함.

갱신 정책 — 기존 데이터를 절대 파괴하지 않는다:
  준공연도·위반건축물 : 새 값이 '비어있지 않으면' 덮어씀 (월일 업그레이드)
  나머지 필드          : '기존이 빈칸일 때만' 채움 (보너스)
  새 값이 빈칸이면     : 기존 값 유지 (GPT 변동성으로 데이터가 지워지는 것 방지)
  비매물 재판정        : 무시. 이 행들은 이미 매물로 확정된 행이다.
                        GPT가 이번에 비매물이라 해도 행을 뒤집지 않는다(카운트만).

환경변수:
  GCP_CREDENTIALS_JSON, OPENAI_API_KEY
  BACKFILL_LIMIT : 이번에 처리할 최대 건수 (기본 200, 0이면 전량)
"""
import os
import re
import json
import time

import requests
import gspread
from gspread.utils import rowcol_to_a1
from openai import OpenAI

# ════════════════════════════════ 설정 ════════════════════════════════
SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
CARD_TAB = "매물카드"
MODEL = "gpt-4o-mini"
FULL_BODY_CHARS = 4000
FETCH_HTML_WINDOW = 80000
GPT_RETRIES = 3
BATCH = 100
LIMIT = int(os.getenv("BACKFILL_LIMIT", "200"))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

OTHER_SPECS = ("대지면적", "연면적", "층수")
SKIP_STATUS = {"비매물(백필)", "본문없음(백필)", "비매물(재처리)",
               "비매물(거주형/비숙박)", "비숙박(정리)"}

BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
    "kkanglive", "moneyschool300",
}

# ════════ GPT — 감시기의 2차 추출 프롬프트와 반드시 동일하게 유지 ════════
EXTRACT_PROMPT = """다음은 부동산 중개 블로그의 숙박시설 매물 광고다. 항목을 추출하라.

규칙:
- 글에 실제로 있는 내용만 쓴다. 절대 지어내지 않는다. 모르면 빈 문자열 "".
- "거래금액"은 보증금·월세·매매가 같은 실제 거래 가격이다. "매출"은 월매출·순수익이다. 이 둘을 절대 섞지 않는다.
- "거래금액"과 "매출"은 글에 적힌 표기 그대로 쓴다(예: "보증금 3억", "월세 600만", "매매가 25억", "월매출 4000만", "순수익 800만"). 절대 원 단위 숫자로 풀어쓰지 않는다(예: 40000000 같은 형태 금지).
- 매물 광고가 아니라 맛집·후기·정보·일상 글이면 "매물여부"를 "비매물"로 한다.
- "시도"는 광역시/도 정식명(서울특별시, 경기도, 부산광역시 등). 동·역 이름으로 분명히 알 수 있으면 채운다(예: 강동역→서울특별시, 수원→경기도). 애매하면 "".
- "시군구"는 시/군/구(예: 수원시, 강동구). "읍면동"은 동·읍·면·리(예: 구운동, 초량동, 부강면). 없으면 "".
- "형태"는 매매면 "매매", 임대면 "임대", 둘 다면 "매매/임대".

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

ALWAYS_UPDATE = ("준공연도", "위반건축물")            # 새 값이 있으면 덮어씀
FILL_IF_EMPTY = ("시도", "시군구", "읍면동", "종류", "형태", "거래금액",
                 "매출", "객실수", "대지면적", "연면적", "층수")


def get_openai():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다.")
    return OpenAI(api_key=key)


def gpt_extract(oai, title, body):
    content = EXTRACT_PROMPT.format(title=title, body=(body or "")[:FULL_BODY_CHARS])
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
                time.sleep(2 ** attempt)
    raise last_err


# ════════════════════════════════ 유틸 ════════════════════════════════
def strip_html(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"&[#a-zA-Z0-9]+;", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_link(url):
    b = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", url or "")
    n = re.search(r"(?:logNo=|/)(\d{8,})", url or "")
    return (b.group(1) if b else ""), (n.group(1) if n else "")


def fetch_post_body(bid, logno):
    if not (bid and logno):
        return ""
    url = f"https://m.blog.naver.com/{bid}/{logno}"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        html = r.text
        i = html.find("se-main-container")
        if i != -1:
            html = html[i:i + FETCH_HTML_WINDOW]
        return strip_html(html)[:FULL_BODY_CHARS]
    except Exception:
        return ""


def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def has_month_day(s):
    """'1989.8.25'처럼 연도 뒤에 뭔가 더 있으면 True."""
    s = str(s or "").strip()
    return bool(s) and not re.fullmatch(r"\d{4}", s)


# ════════════════════════════════ 메인 ════════════════════════════════
def main():
    oai = get_openai()
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)
    ws = ss.worksheet(CARD_TAB)

    vals = ws.get_all_values()
    if len(vals) < 2:
        print("매물카드가 비어 있습니다.")
        return

    header = vals[0]
    idx = {h: i for i, h in enumerate(header)}
    need = ("블로그", "제목", "링크", "상태", "준공연도", "위반건축물") + OTHER_SPECS
    missing = [h for h in need if h not in idx]
    if missing:
        raise SystemExit(f"매물카드에 없는 칸: {missing}")

    Bi, Ti, Li, Si, Ji = idx["블로그"], idx["제목"], idx["링크"], idx["상태"], idx["준공연도"]
    last_col = re.sub(r"\d", "", rowcol_to_a1(1, len(header)))

    def cell(row, col):
        i = idx[col]
        return (row[i].strip() if len(row) > i else "")

    # ── 대상 선정 ──────────────────────────────────────────────
    #  ① 준공연도가 연도 4자리만  → 월일 업그레이드 시도
    #  ② 준공연도 빈칸 + 다른 스펙은 있음 → 승인일만 놓쳤을 가능성
    #  (스펙 전무 행은 이미 "원문에 없음" 판정 → 제외)
    targets = []
    n_year_only = n_missing = 0
    for r, row in enumerate(vals[1:], start=2):
        status = cell(row, "상태")
        blog = cell(row, "블로그")
        if status in SKIP_STATUS or status.startswith("본문없음") or blog in BLOCKLIST:
            continue
        j = cell(row, "준공연도")
        if has_month_day(j):
            continue                                   # 이미 월일 있음
        if re.fullmatch(r"\d{4}", j):
            targets.append((r, row)); n_year_only += 1
        elif not j and any(cell(row, c) for c in OTHER_SPECS):
            targets.append((r, row)); n_missing += 1

    total = len(targets)
    if LIMIT > 0:
        targets = targets[:LIMIT]
    print(f"대상 {total}건 (연도만 {n_year_only} · 승인일결측+스펙有 {n_missing}) "
          f"중 이번 실행 {len(targets)}건 (BACKFILL_LIMIT={LIMIT or '전량'})")
    print("→ 월일이 채워진 행은 자동으로 대상에서 빠집니다. 나눠 돌려도 이어집니다.\n")

    updates = []
    n_upgraded = n_year_kept = n_viol = n_bonus = n_nobody = n_nonprop = n_fail = 0

    def flush():
        nonlocal updates
        if updates:
            ws.batch_update(updates, value_input_option="RAW")
            updates = []

    for i, (r, row) in enumerate(targets, start=1):
        link = cell(row, "링크")
        title = cell(row, "제목")
        bid, logno = parse_link(link)

        body = fetch_post_body(bid, logno)
        time.sleep(0.6)
        if not body:
            n_nobody += 1
            continue                    # 표시 없이 넘어감(행은 이미 유효한 매물)

        try:
            info = gpt_extract(oai, title, body)
        except Exception:
            n_fail += 1
            continue

        if str(info.get("매물여부", "")).strip() == "비매물":
            n_nonprop += 1
            continue                    # 이미 매물로 확정된 행 — 뒤집지 않는다

        new_row = (list(row) + [""] * len(header))[:len(header)]
        changed = False

        for k in ALWAYS_UPDATE:         # 준공연도·위반건축물: 새 값 있으면 덮어씀
            v = str(info.get(k, "")).strip()
            if v and v != cell(row, k):
                new_row[idx[k]] = v
                changed = True
                if k == "준공연도":
                    if has_month_day(v):
                        n_upgraded += 1
                    else:
                        n_year_kept += 1
                else:
                    n_viol += 1

        for k in FILL_IF_EMPTY:         # 나머지: 빈칸만 채움 (기존 값 보호)
            v = str(info.get(k, "")).strip()
            if v and not cell(row, k):
                new_row[idx[k]] = v
                changed = True
                n_bonus += 1

        if changed:
            updates.append({"range": f"A{r}:{last_col}{r}", "values": [new_row]})
        if len(updates) >= BATCH:
            flush()
        if i % 50 == 0:
            print(f"  {i}/{len(targets)} … 월일확보 {n_upgraded} · 연도유지 {n_year_kept} "
                  f"· 위반갱신 {n_viol} · 보너스 {n_bonus} · 실패 {n_fail}")

    flush()
    print("─" * 56)
    print(f"완료 — 처리 {len(targets)}건")
    print(f"  ★ 월일 확보(예: 1989.8.25) : {n_upgraded}건")
    print(f"    연도만 재확인            : {n_year_kept}건 (광고에 날짜가 없는 것)")
    print(f"    위반건축물 갱신          : {n_viol}건")
    print(f"    다른 빈칸 보너스 채움    : {n_bonus}건")
    print(f"  본문없음 {n_nobody} · 비매물재판정(무시) {n_nonprop} · GPT실패 {n_fail}")
    print(f"\n남은 대상: 약 {max(total - len(targets), 0)}건 → 다시 실행하면 이어서 진행됩니다.")
    print("→ '월일 확보' 비율이 스팟체크(18/20)만큼 나오면 resolve_b에 월일 키를 붙일 재료가 완성된 것입니다.")


if __name__ == "__main__":
    main()
