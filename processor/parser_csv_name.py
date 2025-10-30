import os
import re
from typing import Optional, Tuple

_cn_map = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
    '六': 6, '七': 7, '八': 8, '九': 9
}

def _chinese_to_int(s: str) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    if s.isdigit():
        return int(s)
    if len(s) == 1 and s in _cn_map:
        return _cn_map[s]
    total = 0
    for ch in s:
        if ch in _cn_map:
            total = total * 10 + _cn_map[ch]
        else:
            return None
    return total if total != 0 else None

def parse_drag_and_cells(path: str) -> Tuple[Optional[int], Optional[int]]:
    basename = os.path.splitext(os.path.basename(path))[0]

    drag_match = re.search(r'一拖\s*([一二三四五六七八九\d])', basename)
    drag_count: Optional[int] = None
    if drag_match:
        token = drag_match.group(1)
        drag_count = _chinese_to_int(token)

    cells_match = re.search(r'\d+[Pp](\d+)[Ss]', basename)
    cells: Optional[int] = None
    if cells_match:
        try:
            cells = int(cells_match.group(1))
        except Exception:
            cells = None

    return drag_count, cells

def parse_drag_and_cells_for_kelie(path: str) -> Tuple[Optional[int], Optional[int]]:
    basename = os.path.splitext(os.path.basename(path))[0]

    drag_match = re.search(r'一拖\s*([一二三四五六七八九\d])', basename)
    drag_count: Optional[int] = None
    if drag_match:
        token = drag_match.group(1)
        drag_count = _chinese_to_int(token)

    cells_match = re.search(r'科列(?:\d+[Pp])?(\d+)[Ss]', basename)
    cells: Optional[int] = None
    if cells_match:
        try:
            cells = int(cells_match.group(1))
        except Exception:
            cells = None

    return drag_count, cells

if __name__ == "__main__":
    fn = r"D:\jz_pack_data\01\2025-09-17\备份\锐能@DT2459A-F9G-0000006@03HPB0DA0001BWF9G0000081@330阶梯充一拖四1P102S DCR@20250916183907@20250917001729@通道1@@工步层.csv"
    drag, cells = parse_drag_and_cells(fn)
    print("drag_count:", drag)
    print("cells:", cells)
