# -*- coding: utf-8 -*-
"""
매물카드 소급 백필 — 과거 행을 새 프롬프트로 다시 추출
─────────────────────────────────────────────────────────
왜 필요한가:
  예전 감시기는 본문을 800자만 GPT에 줬다. 숙박 광고는 미사여구가 앞에 오고
  물건개요(면적·층수·객실수·매출·사용승인일)가 뒤에 몰려 있어서, 그 블록이
  통째로 잘려 나갔다. 글은 아직 블로그에 살아 있으므로 소급 재추출이 가능하다.

무엇을 하나:
  '매물카드'에서 [대지면적·연면적·층수·준공연도가 모두 빈] 행을 골라
  링크로 본문을 다시 가져와(4000자) 감시기와 동일한 프롬프트로 재추출한다.
    매물         → 위치·종류·형태·거래금액·매출·객실수 + 스펙 5개를 채운다
    비매물       → 상태에 '비매물(백필)' 표시만 (행은 안 지운다)
    본문 못 가져옴 → 상태에 '본문없음(백필)' 표시 (다음 실행 때 재시도 안 함)
    GPT 실패     → 손대지 않음 (다음 실행 때 자동 재시도)

  ※ 스펙이 하나라도 채워지면 대상에서 빠지므로, 중단했다가 다시 돌려도
    이어서 진행된다. 여러 번 나눠 돌려도 안전하다.

환경변수:
  GCP_CREDENTIALS_JSON, OPENAI_API_KEY
  BACKFILL_LIMIT : 이번에 처리할 최대 건수 (기본 200). 0이면 전량.
                   처음엔 100~200으로 돌려서 결과를 확인한 뒤 늘려라.
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
BATCH = 100                       # 이 행 수마다 시트에 한 번 기록 (분당 한도 회피)
LIMIT = int(os.getenv("BACKFILL_LIMIT", "200"))   # 0이면 전량
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}

SPEC_COLS = ("대지면적", "연면적", "층수", "준공연도")   # 이게 전부 비어 있으면 백필 대상
SKIP_STATUS = {"비매물(백필)", "본문없음(백필)", "비매물(재처리)",
               "비매물(거주형/비숙박)", "비숙박(정리)"}

BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
    "kkanglive", "moneyschool300",
}

# 종류 필터 — 화이트리스트. (예전 블랙리스트는 아파트·공장·카페가 다 통과했다)
LODGING_KW = [
    "모텔", "호텔", "여관", "여인숙", "호스텔", "펜션", "게스트하우스", "게하",
    "민박", "풀빌", "무인텔", "무인호텔", "레지던스", "숙박", "스테이",
    "관광숙박", "비지니스호텔", "비즈니스호텔", "캡슐호텔", "한옥스테이",
    "에어비앤비", "에어비엔비", "리조트",
]


def is_excluded_kind(kind, title=""):
    """숙박 키워드가 없으면 True(제외). 종류가 비면 제목으로 한 번 더 본다."""
    s = str(kind or "").strip()
    if any(k in s for k in LODGING_KW):
        return False
    if not s:
        return not any(k in str(title or "") for k in LODGING_KW)
    return True


def kind_is_nonlodging(kind):
    """종류가 '적혀 있는데' 숙박이 아니면 True. (빈칸은 판단 보류 → False)"""
    s = str(kind or "").strip()
    if not s:
        return False
    return not any(k in s for k in LODGING_KW)


# ════════ GPT — 감시기의 2차 추출 프롬프트와 동일하게 유지할 것 ════════
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
- "준공연도": 사용승인일·준공일의 연도 4자리만(예: "1989"). 없으면 "".
- "위반건축물": 위반·위법건축물 언급이 있으면 "有", 없다고 명시하면 "無", 아무 언급 없으면 "".

답하기 전에 위 7개를 하나씩 본문에서 다시 확인하라. 특히 "매출"과 "객실수"를 빠뜨리지 마라.

제목: {title}
본문: {body}

아래 JSON 형식으로만 답하라:
{{"매물여부":"매물 또는 비매물","시도":"","시군구":"","읍면동":"","종류":"","형태":"","거래금액":"","매출":"","객실수":"","대지면적":"","연면적":"","층수":"","준공연도":"","위반건축물":""}}"""

FILL_KEYS = ("시도", "시군구", "읍면동", "종류", "형태", "거래금액", "매출", "객실수",
             "대지면적", "연면적", "층수", "준공연도", "위반건축물")


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
    need = ("블로그", "제목", "링크", "상태") + SPEC_COLS + FILL_KEYS
    missing = [h for h in need if h not in idx]
    if missing:
        raise SystemExit(f"매물카드에 없는 칸: {missing}\n→ 감시기(신버전)를 한 번 돌려 칸을 먼저 늘리세요.")

    Bi, Ti, Li, Si, Ki = idx["블로그"], idx["제목"], idx["링크"], idx["상태"], idx["종류"]
    last_col = re.sub(r"\d", "", rowcol_to_a1(1, len(header)))
    col_S = re.sub(r"\d", "", rowcol_to_a1(1, Si + 1))

    def blank(row, col):
        i = idx[col]
        return not (row[i].strip() if len(row) > i else "")

    # ── 0단계: 비숙박 정리 (GPT 안 씀 = 공짜) ────────────────────────────
    # 종류가 아파트·공장·카페 등 숙박이 아닌 행에 표시만 단다. 행은 지우지 않는다.
    purge = []
    for r, row in enumerate(vals[1:], start=2):
        status = row[Si] if len(row) > Si else ""
        kind = row[Ki] if len(row) > Ki else ""
        if status in SKIP_STATUS:
            continue
        if kind_is_nonlodging(kind):
            purge.append({"range": f"{col_S}{r}", "values": [["비숙박(정리)"]]})
            vals[r - 1][Si] = "비숙박(정리)"     # 아래 대상 선정에도 즉시 반영
    if purge:
        for i in range(0, len(purge), 200):
            ws.batch_update(purge[i:i + 200], value_input_option="RAW")
        print(f"0단계 — 비숙박 {len(purge)}건에 '비숙박(정리)' 표시 (GPT 미사용, 비용 0)")
        print("  → 매물카드에서 상태 필터로 숨기면 됩니다. 행은 지우지 않았습니다.\n")

    # 대상: 스펙 4개가 전부 비어 있고, 이미 판정된 상태가 아니고, BLOCKLIST가 아닌 행
    targets = []
    for r, row in enumerate(vals[1:], start=2):
        status = row[Si] if len(row) > Si else ""
        blog = row[Bi] if len(row) > Bi else ""
        if status in SKIP_STATUS or blog in BLOCKLIST:
            continue
        if all(blank(row, c) for c in SPEC_COLS):
            targets.append((r, row))

    total = len(targets)
    if LIMIT > 0:
        targets = targets[:LIMIT]
    print(f"백필 대상 {total}건 중 이번 실행 {len(targets)}건 "
          f"(BACKFILL_LIMIT={LIMIT or '전량'})")
    print("→ 스펙이 채워진 행은 자동으로 대상에서 빠집니다. 나눠 돌려도 이어서 진행됩니다.\n")

    updates = []
    n_ok = n_spec = n_room = n_rev = n_non = n_nobody = n_fail = 0

    def flush():
        nonlocal updates
        if updates:
            ws.batch_update(updates, value_input_option="RAW")
            updates = []

    for i, (r, row) in enumerate(targets, start=1):
        link = row[Li] if len(row) > Li else ""
        title = row[Ti] if len(row) > Ti else ""
        bid, logno = parse_link(link)

        new_row = (list(row) + [""] * len(header))[:len(header)]

        body = fetch_post_body(bid, logno)
        time.sleep(0.6)   # 차단 회피

        if not body:                       # 글 삭제·비공개 등
            new_row[Si] = "본문없음(백필)"
            n_nobody += 1
            updates.append({"range": f"A{r}:{last_col}{r}", "values": [new_row]})
        else:
            try:
                info = gpt_extract(oai, title, body)
            except Exception:
                n_fail += 1                # 손대지 않음 → 다음 실행 때 재시도
                continue

            if str(info.get("매물여부", "")).strip() == "비매물":
                new_row[Si] = "비매물(백필)"
                n_non += 1
            elif is_excluded_kind(info.get("종류", ""), title):
                new_row[Si] = "비숙박(정리)"
                n_non += 1
            else:
                for k in FILL_KEYS:        # 빈칸이든 아니든 새 값으로 갱신
                    new_row[idx[k]] = info.get(k, "")
                new_row[Si] = ""
                n_ok += 1
                if any(info.get(c) for c in SPEC_COLS):
                    n_spec += 1
                if info.get("객실수"):
                    n_room += 1
                if info.get("매출"):
                    n_rev += 1
            updates.append({"range": f"A{r}:{last_col}{r}", "values": [new_row]})

        if len(updates) >= BATCH:
            flush()
        if i % 25 == 0:
            print(f"  {i}/{len(targets)} … 매물 {n_ok} (스펙 {n_spec} · 객실수 {n_room} · 매출 {n_rev}) "
                  f"/ 비매물 {n_non} / 본문없음 {n_nobody} / GPT실패 {n_fail}")

    flush()

    def pct(x):
        return f"{x / n_ok * 100:.1f}%" if n_ok else "-"

    print("─" * 56)
    print(f"완료 — 처리 {len(targets)}건")
    print(f"  매물 갱신 {n_ok}건")
    print(f"    ├ 스펙 확보  {n_spec}건 ({pct(n_spec)})")
    print(f"    ├ 객실수 확보 {n_room}건 ({pct(n_room)})")
    print(f"    └ 매출 확보  {n_rev}건 ({pct(n_rev)})")
    print(f"  비매물 {n_non} · 본문없음 {n_nobody} · GPT실패 {n_fail}(다음 실행 때 재시도)")
    print(f"\n남은 대상: 약 {max(total - len(targets), 0)}건 → 다시 실행하면 이어서 진행됩니다.")


if __name__ == "__main__":
    main()
