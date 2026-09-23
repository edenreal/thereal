# -*- coding: utf-8 -*-
"""1회용: 2026-09-23 추출 프롬프트 실수로 모텔·펜션 등이 종류='고시원'으로 찍힌 행 바로잡기.
종류='고시원'인데 제목에 고시원 계열 단어가 없으면 제목에서 종류를 다시 뽑는다(없으면 빈칸).
FIX_MODE=preview(기본)면 목록만, apply면 실제 수정."""
import os, json, re
import gspread
from gspread.utils import rowcol_to_a1

SHEET_KEY = "1nQuvBD99FafPYnIKDyvSugNnDZhUbrkbX7hoFDWOiCY"
GOSI = re.compile(r"고시원|고시텔|원룸텔|리빙텔|미니룸")
ORDER = [("무인텔", "모텔"), ("모텔", "모텔"), ("여관", "여관"), ("호스텔", "호스텔"),
         ("게스트하우스", "게스트하우스"), ("풀빌라", "풀빌라"), ("펜션", "펜션"),
         ("호텔", "호텔"), ("민박", "민박"), ("숙박", "숙박시설")]

gc = gspread.service_account_from_dict(json.loads(os.environ["GCP_CREDENTIALS_JSON"]))
ws = gc.open_by_key(SHEET_KEY).worksheet("매물카드")
vals = ws.get_all_values()
h = vals[0]; ki, ti = h.index("종류"), h.index("제목")
col = re.sub(r"\d", "", rowcol_to_a1(1, ki + 1))
ups = []
for i, row in enumerate(vals[1:], start=2):
    if row[ki].strip() != "고시원" or GOSI.search(row[ti]):
        continue
    new = next((v for k, v in ORDER if k in row[ti]), "")
    ups.append({"range": f"{col}{i}", "values": [[new]]})
    print(f"{i}행: 고시원 → {new or '(빈칸)'} | {row[ti][:50]}")
print(f"대상 {len(ups)}행")
if os.getenv("FIX_MODE") == "apply" and ups:
    ws.batch_update(ups, value_input_option="RAW")
    print("적용 완료")
