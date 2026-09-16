# -*- coding: utf-8 -*-
"""
매물카드 GPT실패 재처리 — 전 항목(20필드) 완전복구 버전
─────────────────────────────────────────────────────────
'매물카드' 탭에서 상태가 '확인필요(GPT실패)'인 행을 다시 GPT로 추출해서
진짜 매물(예: 대천 해수욕장 모텔 임대)을 살린다.

[2026-07-31 업그레이드] 예전 recheck는 8필드(시도~객실수)만 뽑고 본문을
  800자만 GPT에 줬다. 그래서 건물스펙(대지면적·연면적·층수·준공연도·
  위반건축물)이 통째로 빠졌다 — 특히 '준공연도(사용승인 월일)'는 옆건물
  오답을 가르는 최강 변별 키인데 이게 빈칸으로 복구됐다.
  → 감시기(naver_lodging_monitor.py)의 최신 EXTRACT 프롬프트·본문처리를
    그대로 이식했다. 이제 감시기와 100% 동일하게 20필드 전부 복구한다.
    (스펙 블록이 광고글 '뒤'에 몰려 있어서 본문 4000자·HTML창 80000자로 넓힘)

  - BLOCKLIST(비숙박 전문) 블로그 행은 건너뛴다(재처리 안 함).
  - 결과:
      매물            → 전 항목 채우고 상태를 빈칸으로 (정상 매물로 전환)
      비매물          → 상태 '비매물(재처리)'
      거주형/비숙박   → 상태 '비매물(거주형/비숙박)'
      그래도 실패     → 그대로 둠(상태 유지)
  - 이어달리기: 복구된 행은 상태가 빈칸이 되므로, 중간에 끊겨도 다시 실행하면
    남은 '확인필요(GPT실패)' 행만 이어서 처리한다.
  - 데이터 보호: GPT가 이번에 빈 값을 줘도 기존 값을 지우지 않는다(있는 값만 덮음).

환경변수: GCP_CREDENTIALS_JSON, OPENAI_API_KEY
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
FULL_BODY_CHARS = 4000       # 감시기와 동일 — 스펙 블록이 글 뒤에 있어 넉넉히
FETCH_HTML_WINDOW = 80000    # 감시기와 동일 — 긴 광고글 대비
GPT_RETRIES = 3
FAIL_STATUS = "확인필요(GPT실패)"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
BATCH = 50   # 이 행 수마다 시트에 기록(구글 분당 한도 회피 + 중간 크래시 대비 진행 보존)
SLEEP_SEC = 0.6   # 본문 접속 간격 (네이버 차단 회피)

# 감시기와 동일한 비숙박 블로그 목록 — 여기 글은 재처리하지 않는다.
BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
    "kkanglive", "moneyschool300",
    # 2026-07-31 추가 — 숙박매물 0건·전량 비숙박(코인/주식/맛집/공장 등)
    "h3h2003", "foremanenc", "sanergy_3051", "kymin0909",
}

# 감시기(naver_lodging_monitor.py)와 동일한 종류 화이트리스트.
# 숙박 키워드가 없으면 제외(시트에 안 남김). 단독 단어는 오탐이 있어 형태로만 넣는다.
LODGING_KW = [
    "모텔", "호텔", "여관", "여인숙", "호스텔", "펜션", "게스트하우스", "게하",
    "민박", "풀빌", "무인텔", "무인호텔", "레지던스", "숙박", "스테이",
    "관광숙박", "비지니스호텔", "비즈니스호텔", "캡슐호텔", "한옥스테이",
    "에어비앤비", "에어비엔비", "리조트",
]


def is_excluded_kind(kind, title=""):
    """숙박 키워드가 없으면 True(시트에 안 올림).
    종류가 비어 있으면 제목에서 한 번 더 확인한다. (감시기와 동일 로직)"""
    s = str(kind or "").strip()
    if any(k in s for k in LODGING_KW):
        return False                                   # 숙박 → 통과
    if not s:                                          # 종류 빈칸 → 제목으로 판단
        return not any(k in str(title or "") for k in LODGING_KW)
    return True                                        # 숙박 키워드 없음 → 제외


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

# 성공 시 채울 필드(전 항목). 시트에 있는 칸만 채운다(없는 칸은 건너뜀).
FILL_FIELDS = ("시도", "시군구", "읍면동", "종류", "형태", "거래금액", "매출",
               "객실수", "대지면적", "연면적", "층수", "준공연도", "위반건축물")


def get_openai():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다.")
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
            if _is_quota_error(e):
                raise QuotaExhausted(e)
            last_err = e
            if attempt < GPT_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise last_err


# ════════════════════════════════ 유틸 (감시기와 동일) ════════════════════════════════
def strip_html(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"&[#a-zA-Z0-9]+;", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_link(url):
    b = re.search(r"blog\.naver\.com/([A-Za-z0-9_\-]+)", url or "")
    n = re.search(r"(?:logNo=|/)(\d{8,})", url or "")
    return (b.group(1) if b else ""), (n.group(1) if n else "")


def fetch_post_body(bid, logno):
    """모바일 블로그에서 본문 텍스트를 가져온다. 실패하면 빈 문자열.
    (감시기와 동일 — 스펙 블록이 글 뒤에 있어 읽는 창을 넓게 잡는다)"""
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
    need = ("블로그", "제목", "링크", "상태")
    if any(h not in idx for h in need):
        print("매물카드 헤더가 예상과 다릅니다:", header)
        return
    Bi, Si, Li, Ti = idx["블로그"], idx["상태"], idx["링크"], idx["제목"]
    # 시트에 실제로 있는 칸만 채운다(구형 14/15칸 시트여도 안전)
    fill_fields = [f for f in FILL_FIELDS if f in idx]
    missing = [f for f in FILL_FIELDS if f not in idx]
    if missing:
        print(f"⚠ 이 시트에 없는 칸은 건너뜀: {missing}")
    last_col = re.sub(r"\d", "", rowcol_to_a1(1, len(header)))   # 마지막 컬럼 문자

    # 재처리 대상 선별
    targets = []
    for r, row in enumerate(vals[1:], start=2):
        status = row[Si] if len(row) > Si else ""
        blog = row[Bi] if len(row) > Bi else ""
        if status == FAIL_STATUS and blog not in BLOCKLIST:
            targets.append((r, row))
    print(f"재처리 대상 {len(targets)}건 (BLOCKLIST 제외) — 채울 필드 {len(fill_fields)}개")

    updates = []
    n_rescued = n_spec = n_nonprop = n_excl = n_nobody = n_fail = 0
    n_tried = 0
    quota_err = None

    def flush():
        nonlocal updates
        if updates:
            ws.batch_update(updates, value_input_option="RAW")
            updates = []

    for i, (r, row) in enumerate(targets, start=1):
        link = row[Li] if len(row) > Li else ""
        title = row[Ti] if len(row) > Ti else ""
        bid, logno = parse_link(link)
        body = fetch_post_body(bid, logno)
        time.sleep(SLEEP_SEC)
        if not body:
            n_nobody += 1
            continue                    # 본문 못 가져옴 — 상태 유지(다음 실행에 재시도)

        n_tried += 1
        try:
            info = gpt_extract(oai, title, body)
        except QuotaExhausted as e:
            quota_err = e      # 지금까지 처리분은 아래 flush로 저장, 나머지는 상태 유지
            break
        except Exception as e:
            n_fail += 1
            print(f"  [GPT실패] 행{r} {link}: {e}")
            continue   # 상태 유지(다음 실행에 재시도)

        new_row = (list(row) + [""] * len(header))[:len(header)]   # 헤더 칸수로 패딩
        if str(info.get("매물여부", "")).strip() == "비매물":
            new_row[Si] = "비매물(재처리)"
            n_nonprop += 1
        elif is_excluded_kind(info.get("종류", ""), title):
            new_row[Si] = "비매물(거주형/비숙박)"
            n_excl += 1
        else:
            for key in fill_fields:
                v = str(info.get(key, "")).strip()
                if v:                       # 있는 값만 덮음(빈 값으로 기존 데이터 지우지 않음)
                    new_row[idx[key]] = v
            new_row[Si] = ""                # 정상 매물로 전환
            n_rescued += 1
            if any(str(info.get(k, "")).strip() for k in ("대지면적", "연면적", "층수", "준공연도")):
                n_spec += 1

        updates.append({"range": f"A{r}:{last_col}{r}", "values": [new_row]})
        if len(updates) >= BATCH:
            flush()
        if i % 50 == 0:
            print(f"  진행 {i}/{len(targets)} … 복구 {n_rescued}(스펙有 {n_spec}) "
                  f"· 비매물 {n_nonprop + n_excl} · 본문없음 {n_nobody} · 실패 {n_fail}")

    flush()
    print("─" * 52)
    print(f"완료 — 매물 복구 {n_rescued}건 (그중 건물스펙 확보 {n_spec}건)")
    print(f"       비매물(재처리) {n_nonprop} · 거주형/비숙박 {n_excl} · "
          f"본문없음 {n_nobody} · GPT실패 {n_fail}")
    print("→ 본문없음·GPT실패는 상태가 유지되니, 다시 실행하면 그 행만 이어서 재시도합니다.")

    # ── 조용한 실패 방지: 비정상이면 워크플로우를 '실패'로 떨어뜨린다 ──
    if quota_err:
        print("!" * 52)
        print(f"중단 — OpenAI 크레딧/쿼터 소진: {quota_err}")
        print("   처리분은 저장됐고 나머지는 상태 유지 — 충전 후 다시 실행하면 이어집니다.")
        raise SystemExit(1)
    if n_tried >= 5 and n_fail / n_tried >= 0.5:
        print("!" * 52)
        print(f"경고 — GPT 호출 {n_tried}건 중 {n_fail}건 실패(50% 이상). 위 [GPT실패] 로그를 보세요.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
