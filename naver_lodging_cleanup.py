# -*- coding: utf-8 -*-
"""
매물카드 노이즈 정리 — 비매물/비숙박 행 삭제
─────────────────────────────────────────────────────────
'매물카드' 탭에서 상태가 '비매물…' 또는 '비숙박…'인 행을 삭제한다.
(recheck/backfill/정리 과정에서 비매물·비숙박으로 확정된 노이즈)

안전장치:
  1) 삭제 전 반드시 '매물카드_백업_YYYYMMDD_HHMMSS' 탭으로 전체 복제(원본 보존).
  2) 기본은 PREVIEW(미리보기) — 몇 건 지워질지 개수·샘플만 출력하고 아무것도 안 지운다.
  3) 실제 삭제는 CLEANUP_MODE=delete AND CLEANUP_CONFIRM=DELETE 둘 다 있어야 실행.
  4) 삭제는 원본 좌표 기준으로 아래→위(내림차순) 한 번의 batch_update로 처리(인덱스 밀림 방지).

지우는 대상(상태가 아래로 시작):
  "비매물"  (예: 비매물(재처리), 비매물(거주형/비숙박), 비매물(백필))
  "비숙박"  (예: 비숙박(정리))
지우지 않는 것: 빈칸(정상매물), '확인필요(GPT실패)', '본문없음(백필)' 등.

환경변수:
  GCP_CREDENTIALS_JSON, (OPENAI 불필요)
  CLEANUP_MODE    : preview(기본) | delete
  CLEANUP_CONFIRM : delete 모드일 때 반드시 "DELETE" 여야 실제 삭제
"""
import os
import re
import json
from datetime import datetime, timezone, timedelta

import gspread

SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
CARD_TAB = "매물카드"
DELETE_PREFIXES = ("비매물", "비숙박")   # 이 접두사로 시작하는 상태 = 삭제 대상
KST = timezone(timedelta(hours=9))

MODE = os.getenv("CLEANUP_MODE", "preview").strip().lower()
CONFIRM = os.getenv("CLEANUP_CONFIRM", "").strip()


def get_client():
    raw = os.getenv("GCP_CREDENTIALS_JSON")
    if not raw:
        raise SystemExit("GCP_CREDENTIALS_JSON 환경변수가 없습니다.")
    return gspread.service_account_from_dict(json.loads(raw))


def is_delete_target(status):
    s = str(status or "").strip()
    return any(s.startswith(p) for p in DELETE_PREFIXES)


def contiguous_ranges(rows):
    """정렬된 1-based 행번호 리스트 → [(start,end), ...] 연속구간(둘 다 포함)."""
    rows = sorted(rows)
    ranges = []
    start = prev = None
    for r in rows:
        if start is None:
            start = prev = r
        elif r == prev + 1:
            prev = r
        else:
            ranges.append((start, prev))
            start = prev = r
    if start is not None:
        ranges.append((start, prev))
    return ranges


def main():
    gc = get_client()
    ss = gc.open_by_key(SHEET_KEY)
    ws = ss.worksheet(CARD_TAB)

    vals = ws.get_all_values()
    if len(vals) < 2:
        print("매물카드가 비어 있습니다.")
        return
    header = vals[0]
    idx = {h: i for i, h in enumerate(header)}
    if "상태" not in idx:
        raise SystemExit(f"'상태' 칸이 없습니다. 헤더: {header}")
    Si = idx["상태"]
    Ti = idx.get("제목", -1)

    # 삭제 대상 수집 (2-based 행번호 = 시트 실제 행)
    targets = []
    by_status = {}
    for r, row in enumerate(vals[1:], start=2):
        status = row[Si] if len(row) > Si else ""
        if is_delete_target(status):
            targets.append(r)
            by_status[status.strip()] = by_status.get(status.strip(), 0) + 1

    total_rows = len(vals) - 1
    print(f"매물카드 데이터 {total_rows}행 중 삭제 대상 {len(targets)}행 "
          f"(남을 행 {total_rows - len(targets)}행)")
    print("상태별 삭제 대상:")
    for s, c in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {c:5}  {s}")

    if not targets:
        print("삭제할 행이 없습니다.")
        return

    # 샘플 몇 개 미리보기
    print("\n삭제 대상 샘플 5건:")
    for r in targets[:5]:
        row = vals[r - 1]
        title = row[Ti] if (Ti >= 0 and len(row) > Ti) else ""
        print(f"  행{r} [{row[Si]}] {title[:44]}")

    if MODE != "delete":
        print("\n※ PREVIEW 모드 — 아무것도 지우지 않았습니다.")
        print("  실제로 지우려면 CLEANUP_MODE=delete + CLEANUP_CONFIRM=DELETE 로 다시 실행하세요.")
        return

    if CONFIRM != "DELETE":
        print("\n⛔ CLEANUP_MODE=delete 이지만 CLEANUP_CONFIRM 이 'DELETE'가 아닙니다. 중단.")
        return

    # ── 실제 삭제 ──
    # 1) 백업 탭으로 전체 복제
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    backup_name = f"{CARD_TAB}_백업_{stamp}"
    ss.duplicate_sheet(ws.id, new_sheet_name=backup_name)
    print(f"\n① 백업 생성: '{backup_name}' (원본 {len(vals)}행 그대로 보존)")

    # 2) 원본 좌표 기준 연속구간 → 아래에서 위로 deleteDimension (인덱스 밀림 방지)
    ranges = contiguous_ranges(targets)          # 1-based, 둘 다 포함
    requests = []
    for start, end in sorted(ranges, key=lambda x: -x[0]):   # 큰 행부터
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": start - 1,     # 0-based inclusive
                    "endIndex": end,             # 0-based exclusive
                }
            }
        })
    ss.batch_update({"requests": requests})
    print(f"② 삭제 완료: {len(targets)}행 삭제 ({len(ranges)}개 구간, 한 번에 처리)")
    print(f"③ 남은 데이터 {total_rows - len(targets)}행")
    print(f"→ 문제 있으면 백업 탭 '{backup_name}'에서 복구하세요.")


if __name__ == "__main__":
    main()
