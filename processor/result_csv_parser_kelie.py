import re
import os
import json
from typing import List, Optional
import pandas as pd
import numpy as np

def detect_encoding(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            f.readline()
        return "utf-8"
    except Exception:
        return "gbk"

def clean_cols(cols: List[str]) -> List[str]:
    return [c.strip().replace('\ufeff','') for c in cols]

def clean_str(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    if isinstance(s, str):
        t = re.sub(r'[\t\r\n]+', '', s)
        return t.strip()
    try:
        return str(s).strip()
    except Exception:
        return s

def compute_partition_sizes(total_len: int, n_parts: int) -> List[int]:
    base = total_len // n_parts
    rem = total_len % n_parts
    return [base + (1 if i < rem else 0) for i in range(n_parts)]

def split_pack_codes_from_cell(val: Optional[str], n: int) -> List[Optional[str]]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return [None]*n
    s = str(val).strip()
    for sep in [",",";","|","，"]:
        if sep in s:
            parts = [clean_str(p) for p in s.split(sep) if p is not None and str(p).strip()!='']
            if len(parts) >= n:
                return parts[:n]
            else:
                return parts + [None]*(n-len(parts))
    res = [None]*n
    res[0] = clean_str(s)
    return res

def compute_cell_stats(cell_list: List[Optional[float]]):
    vals = [(v, idx) for idx, v in enumerate(cell_list) if v is not None]
    if not vals:
        return None, None, None, None
    max_pair = max(vals, key=lambda x: x[0])
    min_pair = min(vals, key=lambda x: x[0])
    max_val, max_idx0 = float(max_pair[0]), int(max_pair[1])
    min_val, min_idx0 = float(min_pair[0]), int(min_pair[1])
    return max_val, max_idx0 + 1, min_val, min_idx0 + 1

FIELD_MAP = {
    "电池包码": "pack_code",
    "车辆码": "vehicle_code",
    "设备IP": "device_ip",
    "获取时间": "acquire_time",
    "通道号": "channel_id",
    "工步号": "step_id",
    "工步名称": "step_name",
    "电压": "voltage",
    "电流": "current",
    "功率": "power",
    "阶段充电能量": "stage_charge_energy",
    "阶段放电能量": "stage_discharge_energy",
    "阶段充电容量": "stage_charge_capacity",
    "阶段放电容量": "stage_discharge_capacity",
    "充电能量": "charge_energy",
    "放电能量": "discharge_energy",
    "充电容量": "charge_capacity",
    "放电容量": "discharge_capacity",
    "阶段时间": "stage_time",
    "累计时间": "total_time",
    "循环体": "cycle_block",
    "循环次数": "cycle_count",
    "循环统计": "cycle_stats",
    "WorkingConditionStep": "working_condition_step",
    "BatVoltage": "bms_pack_voltage",
}

KEEP_FIELDS_CH = [
    '电池包码', '车辆码', '设备IP', '获取时间', '通道号', '工步号', '工步名称', '电压', '电流', '功率',
    '阶段充电能量', '阶段放电能量', '阶段充电容量', '阶段放电容量', '充电能量', '放电能量',
    '充电容量', '放电容量', '阶段时间', '累计时间', '循环体', '循环次数', '循环统计',
    'WorkingConditionStep', 'BatVoltage'
]
KEEP_FIELDS = KEEP_FIELDS_CH

def get_first_nonnull(csv_path: str, colname: str, chunksize: int = 2000, encoding: Optional[str] = None) -> Optional[str]:

    if encoding is None:
        encoding = detect_encoding(csv_path)
    try:
        for chunk in pd.read_csv(csv_path, usecols=[colname], encoding=encoding, iterator=True, chunksize=chunksize, low_memory=False):
            chunk.columns = clean_cols(list(chunk.columns))
            if colname not in chunk.columns:
                return None
            srs = chunk[colname].dropna().astype(str).apply(clean_str)
            for v in srs:
                if v not in (None, ""):
                    return v
    except Exception:
        return None
    return None

def result_csv_to_json_kelie(
    csv_path: str,
    out_jsonl_path: str,
    n_packs: int = 4,
    chunksize: int = 2000,
    encoding: Optional[str] = None
):
    if encoding is None:
        encoding = detect_encoding(csv_path)

    header = pd.read_csv(csv_path, nrows=0, encoding=encoding)
    all_cols = clean_cols(list(header.columns))

    temp_cols = [c for c in all_cols if re.match(r'^CellTemp\d+$', c, re.I)]
    if not temp_cols:
        temp_cols = [c for c in all_cols if c.startswith("CellTemp")]
    cell_cols = [c for c in all_cols if re.match(r'^CellVolt\d+$', c, re.I)]
    if not cell_cols:
        cell_cols = [c for c in all_cols if c.startswith("CellVolt")]

    def idx_key(col):
        nums = re.findall(r'\d+', col)
        return int(nums[0]) if nums else 0
    temp_cols = sorted(temp_cols, key=idx_key)
    cell_cols = sorted(cell_cols, key=idx_key)

    keep_fields = [c for c in KEEP_FIELDS if c in all_cols]

    pack_codes_global: List[Optional[str]] = []
    if "电池包码" in all_cols:
        try:
            for chunk_pack in pd.read_csv(csv_path, encoding=encoding, usecols=["电池包码"], iterator=True, chunksize=chunksize, low_memory=False):
                chunk_pack.columns = clean_cols(list(chunk_pack.columns))
                vals = chunk_pack["电池包码"].dropna().astype(str).tolist()
                for v in vals:
                    cv = clean_str(v)
                    if cv not in (None, "") and cv not in pack_codes_global:
                        pack_codes_global.append(cv)
                        if len(pack_codes_global) >= n_packs:
                            break
                if len(pack_codes_global) >= n_packs:
                    break
        except Exception:
            pass
    if len(pack_codes_global) < n_packs:
        pack_codes_global = pack_codes_global + [None]*(n_packs - len(pack_codes_global))

    vehicle_first = None
    deviceip_first = None
    if "车辆码" in keep_fields:
        vehicle_first = get_first_nonnull(csv_path, "车辆码", chunksize=chunksize, encoding=encoding)
    if "设备IP" in keep_fields:
        deviceip_first = get_first_nonnull(csv_path, "设备IP", chunksize=chunksize, encoding=encoding)

    temp_total = len(temp_cols)
    cell_total = len(cell_cols)
    temp_sizes = compute_partition_sizes(temp_total, n_packs) if temp_total>0 else [0]*n_packs
    cell_sizes = compute_partition_sizes(cell_total, n_packs) if cell_total>0 else [0]*n_packs

    temp_ranges, cell_ranges = [], []
    s = 0
    for sz in temp_sizes:
        temp_ranges.append((s, s+sz)); s += sz
    s = 0
    for sz in cell_sizes:
        cell_ranges.append((s, s+sz)); s += sz

    cols_to_read = keep_fields + temp_cols + cell_cols
    seen = set(); cols_to_read_unique = []
    for c in cols_to_read:
        if c not in seen:
            cols_to_read_unique.append(c)
            seen.add(c)

    out_dir = os.path.dirname(out_jsonl_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    fout = open(out_jsonl_path, "w", encoding="utf-8")

    rows_in = 0
    rows_out = 0
    src_idx = 0

    reader = pd.read_csv(csv_path, encoding=encoding, usecols=cols_to_read_unique, iterator=True, chunksize=chunksize, low_memory=False)
    for chunk in reader:
        chunk.columns = clean_cols(list(chunk.columns))
        chunk.dropna(axis=1, how='all', inplace=True)

        for c in temp_cols:
            if c in chunk.columns:
                chunk[c] = pd.to_numeric(chunk[c], errors="coerce")
        for c in cell_cols:
            if c in chunk.columns:
                chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

        for _, row in chunk.iterrows():
            rows_in += 1

            if any(pack_codes_global):
                pack_codes = pack_codes_global
            else:
                raw = row.get("电池包码", None) if "电池包码" in row.index else None
                pack_codes = split_pack_codes_from_cell(raw, n_packs)

            duplicated = {}
            for fld in keep_fields:
                if fld in row.index:
                    v = row.get(fld, None)
                    if isinstance(v, str):
                        v_clean = clean_str(v)
                        if v_clean == "":
                            if fld == "车辆码":
                                duplicated[fld] = vehicle_first
                            elif fld == "设备IP":
                                duplicated[fld] = deviceip_first
                            else:
                                duplicated[fld] = None
                        else:
                            duplicated[fld] = v_clean
                    else:
                        if (isinstance(v, float) and np.isnan(v)):
                            if fld == "车辆码":
                                duplicated[fld] = vehicle_first
                            elif fld == "设备IP":
                                duplicated[fld] = deviceip_first
                            else:
                                duplicated[fld] = None
                        else:
                            duplicated[fld] = v
                else:
                    # 列不存在 -> None
                    duplicated[fld] = None

            temp_values = [ None if (c not in chunk.columns) or pd.isna(row.get(c, None)) else float(row.get(c, None)) for c in temp_cols ]
            cell_values = [ None if (c not in chunk.columns) or pd.isna(row.get(c, None)) else float(row.get(c, None)) for c in cell_cols ]

            for i in range(n_packs):
                tstart, tend = temp_ranges[i]
                cstart, cend = cell_ranges[i]
                batt_temp_chunk = temp_values[tstart:tend] if temp_total>0 else []
                cell_volt_chunk  = cell_values[cstart:cend] if cell_total>0 else []

                if len(batt_temp_chunk) < temp_sizes[i]:
                    batt_temp_chunk = batt_temp_chunk + [None]*(temp_sizes[i]-len(batt_temp_chunk))
                if len(cell_volt_chunk) < cell_sizes[i]:
                    cell_volt_chunk = cell_volt_chunk + [None]*(cell_sizes[i]-len(cell_volt_chunk))

                max_val, max_idx_1b, min_val, min_idx_1b = compute_cell_stats(cell_volt_chunk)

                out_obj = {}

                pk = pack_codes[i] if i < len(pack_codes) else None
                out_obj[FIELD_MAP.get("电池包码","电池包码")] = clean_str(pk) if isinstance(pk, str) else pk

                for fld in keep_fields:
                    if fld == "电池包码":
                        continue
                    english_key = FIELD_MAP.get(fld, fld)
                    val = duplicated.get(fld)
                    if isinstance(val, str):
                        out_obj[english_key] = clean_str(val)
                    else:
                        out_obj[english_key] = None if (isinstance(val, float) and np.isnan(val)) else val

                for j, val in enumerate(batt_temp_chunk, start=1):
                    key = f"BMS_BattTemp{j}"
                    out_obj[key] = None if val is None else float(val)

                for j, val in enumerate(cell_volt_chunk, start=1):
                    key = f"BMS_CellVolt{j}"
                    out_obj[key] = None if val is None else float(val) / 1000

                out_obj["max_voltage"] = None if max_val is None else float(max_val) / 1000
                out_obj["max_voltage_cell_index"] = max_idx_1b
                out_obj["min_voltage"] = None if min_val is None else float(min_val) / 1000
                out_obj["min_voltage_cell_index"] = min_idx_1b

                if (max_val is None) or (min_val is None):
                    out_obj["pack_voltage_range"] = None
                else:
                    out_obj["pack_voltage_range"] = round(float(max_val) - float(min_val), 4) / 1000

                # out_obj["_source_row_idx"] = int(src_idx)
                out_obj['test_device_name'] = '科列'
                src_idx += 1

                fout.write(json.dumps(out_obj, ensure_ascii=False) + "\n")
                rows_out += 1

    fout.close()
    return {"rows_in": rows_in, "rows_out": rows_out, "outfile": out_jsonl_path}


if __name__ == "__main__":
    csv_path = r'C:\Users\HP\PycharmProjects\ftp2kafka_project\data\incoming\data_ftp_upload_pack_电测_02_2025-09-28_锐能_DT2528A-F9V-0000207_03HPB0DA0001BWF9V0000025_330阶梯充一拖四-科列1P102S_DCR_20250927222956_20250928041456_通道2\锐能@DT2528A-F9V-0000207@03HPB0DA0001BWF9V0000025@330阶梯充一拖四-科列1P102S DCR@20250927222956@20250928041456@通道2@@工步层.csv'
    out_jsonl = csv_path.replace(".csv", "_processed.jsonl")
    res = result_csv_to_json(csv_path, out_jsonl, n_packs=4, chunksize=2000)
    print("done:", res)
