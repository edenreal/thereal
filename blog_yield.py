# -*- coding: utf-8 -*-
"""
블로그별 실적 분석 — 읽기 전용(GPT 호출 없음, 비용 0)
─────────────────────────────────────────────────────────
'블로그목록'의 감시 대상 블로그가 실제로 '숙박 매물'을 얼마나 만들어냈는지 센다.
목적: 글만 많이 쓰고 매물은 0건인 블로그를 찾아 감시에서 빼기(= GPT 게이트 호출 절감).

출력: 블로그별 (정상매물 / 노이즈 / 총행) + 매물 0건 블로그 목록.
아무것도 수정하지 않는다. 환경변수: GCP_CREDENTIALS_JSON
"""
import os
import json
from collections import defaultdict

import gspread

SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
CARD_TAB = "매물카드"
BLOG_TAB = "블로그목록"

BLOCKLIST = {
    "stewzinnia59", "ksanchoi", "auctionrun3988", "sbjjjang", "kj-4848", "jijonbpbp",
    "kkanglive", "moneyschool300",
    "h3h2003", "foremanenc", "sanergy_3051", "kymin0909",
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


def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def main():
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)

    # 감시 대상 블로그
    bw = ss.worksheet(BLOG_TAB)
    watched = [v.strip() for v in bw.col_values(1)[1:] if v.strip()]
    watched = [w.split("/")[-1] if "/" in w else w for w in watched]
    seen, uniq = set(), []
    for b in watched:
        if b not in seen:
            seen.add(b)
            uniq.append(b)
    watched = uniq

    # 매물카드 집계
    ws = ss.worksheet(CARD_TAB)
    vals = ws.get_all_values()
    header = vals[0]
    idx = {h: i for i, h in enumerate(header)}
    Bi, Si = idx["블로그"], idx["상태"]

    listing = defaultdict(int)   # 정상매물
    noise = defaultdict(int)     # 비매물/비숙박
    other = defaultdict(int)     # 실패/본문없음 등
    for row in vals[1:]:
        b = (row[Bi] if len(row) > Bi else "").strip()
        s = (row[Si] if len(row) > Si else "").strip()
        if not b:
            continue
        if not s:
            listing[b] += 1
        elif s.startswith("비매물") or s.startswith("비숙박"):
            noise[b] += 1
        else:
            other[b] += 1

    print(f"감시 대상 블로그 {len(watched)}개 (BLOCKLIST {len(BLOCKLIST)}개 별도)")
    print(f"매물카드 {len(vals)-1}행 집계\n")

    rows = []
    for b in watched:
        if b in BLOCKLIST:
            continue
        rows.append((listing.get(b, 0), noise.get(b, 0), other.get(b, 0), b))
    rows.sort(key=lambda x: (-x[0], -x[1]))

    print("=" * 62)
    print("매물 생산 상위 20")
    print("=" * 62)
    for L, N, O, b in rows[:20]:
        print(f"  매물{L:4}  노이즈{N:4}  기타{O:4}   {b}")

    zero = [r for r in rows if r[0] == 0]
    zero_with_rows = [r for r in zero if (r[1] + r[2]) > 0]
    zero_silent = [r for r in zero if (r[1] + r[2]) == 0]

    print("\n" + "=" * 62)
    print(f"★ 매물 0건 블로그: {len(zero)}개 / 감시중 {len(rows)}개")
    print("=" * 62)
    print(f"\n[A] 노이즈만 생산 ({len(zero_with_rows)}개) — 확실한 제거 대상")
    for L, N, O, b in sorted(zero_with_rows, key=lambda x: -(x[1] + x[2]))[:40]:
        print(f"  노이즈{N:4} 기타{O:4}   {b}")

    print(f"\n[B] 시트에 한 줄도 없음 ({len(zero_silent)}개) — 게이트에서 전량 컷(순수 비용) 또는 신규")
    print("  " + ", ".join(b for _, _, _, b in zero_silent[:60]))

    total_listing = sum(r[0] for r in rows)
    print(f"\n요약: 매물 생산 블로그 {len([r for r in rows if r[0]>0])}개가 총 {total_listing}건 생산")
    print(f"      매물 0건 블로그 {len(zero)}개는 게이트 호출만 소비 중")


if __name__ == "__main__":
    main()
