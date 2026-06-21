# -*- coding: utf-8 -*-
"""
매물카드 GPT실패 재처리 — 1회용 스크립트
─────────────────────────────────────────────────────────
'매물카드' 탭에서 상태가 '확인필요(GPT실패)'인 행을 다시 GPT로 추출해서
진짜 매물(예: 대천 해수욕장 모텔 임대)을 살린다.

  - BLOCKLIST(비숙박 전문) 블로그 행은 건너뛴다(재처리 안 함).
  - 결과:
      매물            → 시도~객실수 채우고 상태를 빈칸으로 (정상 매물로 전환)
      비매물          → 상태 '비매물(재처리)'
      거주형/비숙박   → 상태 '비매물(거주형/비숙박)'
      그래도 실패     → 그대로 둠(상태 유지)

한 번만 돌리면 된다. 환경변수: GCP_CREDENTIALS_JSON, OPENAI_API_KEY
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
EXTRACT_BODY_CHARS = 800
GPT_RETRIES = 3
FAIL_STATUS = "확인필요(GPT실패)"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
BATCH = 100   # 구글 분당 한도 회피용: 이 행 수마다 한 번에 기록

# 감시기와 동일한 비숙박 블로그 목록 — 여기 글은 재처리하지 않는다.
BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
}

# 감시기와 동일한 종류 필터
LODGING_KW = [
    "모텔", "호텔", "여관", "여인숙", "호스텔", "펜션", "게스트하우스", "게하",
    "민박", "풀빌", "무인텔", "무인호텔", "관광", "비지니스", "비즈니스",
    "레지던스", "캡슐", "한옥", "숙박", "스테이", "단기숙소", "체류형", "쉼터", "세컨하우스",
]
EXCLUDE_KW = [
    "고시원", "고시텔", "원룸텔", "원룸", "주택", "주거시설", "오피스텔",
    "상가", "빌딩", "사옥", "건물", "토지", "대지", "임야",
    "병원", "요양병원", "의료시설", "체육", "PC방", "피씨방",
    "이자카야", "회원권", "근린생활시설", "위락시설", "캠핑장",
]


def is_excluded_kind(kind):
    s = str(kind or "").strip()
    if not s:
        return False
    if any(k in s for k in LODGING_KW):
        return False
    if any(k in s for k in EXCLUDE_KW):
        return True
    return False


# ════════════════════════════════ GPT (감시기와 동일) ════════════════════════════════
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
        raise SystemExit("OPENAI_API_KEY 환경변수가 없습니다.")
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
    if not (bid and logno):
        return ""
    url = f"https://m.blog.naver.com/{bid}/{logno}"
    try:
        r = requests.get(url, headers=UA, timeout=15)
        r.raise_for_status()
        html = r.text
        i = html.find("se-main-container")
        if i != -1:
            html = html[i:i + 20000]
        return strip_html(html)[:3000]
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
    need = ("블로그", "종류", "형태", "거래금액", "매출", "객실수",
            "시도", "시군구", "읍면동", "제목", "링크", "상태")
    if any(h not in idx for h in need):
        print("매물카드 헤더가 예상과 다릅니다:", header)
        return
    Bi, Si, Li, Ti = idx["블로그"], idx["상태"], idx["링크"], idx["제목"]
    last_col = re.sub(r"\d", "", rowcol_to_a1(1, len(header)))   # 마지막 컬럼 문자

    # 재처리 대상 선별
    targets = []
    for r, row in enumerate(vals[1:], start=2):
        status = row[Si] if len(row) > Si else ""
        blog = row[Bi] if len(row) > Bi else ""
        if status == FAIL_STATUS and blog not in BLOCKLIST:
            targets.append((r, row))
    print(f"재처리 대상 {len(targets)}건 (BLOCKLIST 제외)")

    updates = []
    n_rescued = n_nonprop = n_excl = n_fail = 0

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
        time.sleep(0.5)

        try:
            info = gpt_extract(oai, title, body)
        except Exception:
            n_fail += 1
            continue   # 그대로 둠

        new_row = (list(row) + [""] * len(header))[:len(header)]   # 15칸으로 패딩
        if str(info.get("매물여부", "")).strip() == "비매물":
            new_row[Si] = "비매물(재처리)"
            n_nonprop += 1
        elif is_excluded_kind(info.get("종류", "")):
            new_row[Si] = "비매물(거주형/비숙박)"
            n_excl += 1
        else:
            for key in ("시도", "시군구", "읍면동", "종류", "형태", "거래금액", "매출", "객실수"):
                new_row[idx[key]] = info.get(key, "")
            new_row[Si] = ""        # 정상 매물로 전환
            n_rescued += 1

        updates.append({"range": f"A{r}:{last_col}{r}", "values": [new_row]})
        if len(updates) >= BATCH:
            flush()
        if i % 50 == 0:
            print(f"  진행 {i}/{len(targets)} … 복구 {n_rescued}, 비매물 {n_nonprop + n_excl}, 실패 {n_fail}")

    flush()
    print("─" * 52)
    print(f"완료 — 매물 복구 {n_rescued} · 비매물 {n_nonprop} · "
          f"거주형/비숙박 {n_excl} · 여전히 실패 {n_fail}")
    print("→ 매물카드에서 상태가 '비매물…'인 행은 필요하면 정렬해서 지우면 됩니다.")


if __name__ == "__main__":
    main()
