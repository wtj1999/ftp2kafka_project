import pandas as pd
import json
import numpy as np
from typing import List, Optional
from processor.parser_csv_name import *

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

# 中文字段 -> 英文字段
FIELD_MAP = {
    '获取时间': 'acquire_time',
    '通道号': 'channel_id',
    '工步号': 'step_id',
    '工步名称': 'step_name',
    '电压': 'voltage',
    '电流': 'current',
    '功率': 'power',
    '阶段充电能量': 'stage_charge_energy',
    '阶段放电能量': 'stage_discharge_energy',
    '阶段充电容量': 'stage_charge_capacity',
    '阶段放电容量': 'stage_discharge_capacity',
    '充电能量': 'charge_energy',
    '放电能量': 'discharge_energy',
    '充电容量': 'charge_capacity',
    '放电容量': 'discharge_capacity',
    '阶段时间': 'stage_time',
    '累计时间': 'total_time',
    '循环体': 'cycle_block',
    '循环次数': 'cycle_count',
    '循环统计': 'cycle_stats',
    'WorkingConditionStep': 'working_condition_step',
    'BMS_BattVol': 'bms_pack_voltage',
    '电池包码': 'pack_code',
}

KEEP_COLS_CH = [
    '获取时间', '通道号', '工步号', '工步名称', '电压', '电流', '功率',
    '阶段充电能量', '阶段放电能量', '阶段充电容量', '阶段放电容量',
    '充电能量', '放电能量', '充电容量', '放电容量',
    '阶段时间', '累计时间', '循环体', '循环次数', '循环统计',
    'WorkingConditionStep', 'BMS_BattVol'
]

def get_pack_codes_from_step_file(record_file: str) -> List[str]:

    step_file = record_file.replace("记录层", "工步层")
    if not os.path.exists(step_file):
        raise FileNotFoundError(f"找不到工步层文件: {step_file}")
    df_step = pd.read_csv(step_file, dtype=str, low_memory=False)
    drag, cells_num = parse_drag_and_cells(step_file)

    if "电池包码" not in df_step.columns:
        raise ValueError("工步层文件缺少 '电池包码' 列")
    if drag is None or cells_num is None:
        raise ValueError("文件名无法解析，无法判定电芯数目以及电测拖数")
    pack_codes = (
        df_step["电池包码"]
        .dropna()
        .astype(str)
        .map(clean_str)
        .map(lambda x: x if x != "" else None)
        .dropna()
        .unique()
        .tolist()
    )
    vehicle_code = (
        df_step["车辆码"]
        .dropna()
        .astype(str)
        .map(clean_str)
        .map(lambda x: x if x != "" else None)
        .dropna()
        .unique()
        .tolist()
    )

    if len(pack_codes) != drag:
        raise ValueError(f"电池包码数量与电测拖数不一致")
    if len(vehicle_code) != 1:
        raise ValueError(f"车辆码数量不是1个: {vehicle_code}")
    return pack_codes, vehicle_code, drag, cells_num

def process_csv_to_json(csv_path: str,
                        out_jsonl_path: str):
    pack_codes, vehicle_code, drag, cells_num = get_pack_codes_from_step_file(csv_path)
    print("电池包码:", pack_codes)

    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    df.columns = [clean_str(c) for c in df.columns]
    df = df.applymap(lambda x: clean_str(x) if isinstance(x, str) else x)

    def col_all_empty(col_series):
        return col_series.replace('', np.nan).isna().all()

    drop_cols = [c for c in df.columns if col_all_empty(df[c])]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    temp_cols = [c for c in df.columns if c and c.startswith("BMS_BattTemp")]
    volt_cols = [c for c in df.columns if c and c.startswith("BMS_CellVolt")][:drag*cells_num]

    def idx_key(col):
        nums = re.findall(r'\d+', col)
        return int(nums[0]) if nums else 0
    temp_cols = sorted(temp_cols, key=idx_key)
    volt_cols = sorted(volt_cols, key=idx_key)

    if len(temp_cols) % len(pack_codes) != 0 or len(volt_cols) != cells_num * drag:
        raise ValueError(f"温度列数={len(temp_cols)}, 电压列数={len(volt_cols)}, 错误")

    temps_num = int(len(temp_cols) // len(pack_codes))

    other_cols = [c for c in KEEP_COLS_CH if c in df.columns]

    set_cols = [
    '电压', '电流', '功率',
    '阶段充电能量', '阶段放电能量', '阶段充电容量', '阶段放电容量',
    '充电能量', '放电能量', '充电容量', '放电容量','BMS_BattVol'
    ]

    json_records = []

    for c in temp_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    for c in volt_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    for c in set_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    for idx, row in df.iterrows():
        other_data = {}
        for ch_col in other_cols:
            eng_col = FIELD_MAP.get(ch_col, ch_col)
            val = row.get(ch_col, None)
            if isinstance(val, str):
                val2 = clean_str(val)
                other_data[eng_col] = val2 if val2 != "" else None
            else:
                if pd.isna(val):
                    other_data[eng_col] = None
                else:
                    try:
                        other_data[eng_col] = float(val)
                    except Exception:
                        other_data[eng_col] = val

        temps = row[temp_cols].to_numpy(dtype=float)  # length 32
        volts = row[volt_cols].to_numpy(dtype=float)  # length 408

        for i in range(drag):
            pack_data = {}
            pack_data['pack_code'] = clean_str(pack_codes[i])
            pack_data['vehicle_code'] = clean_str(vehicle_code[0])

            for k, v in other_data.items():
                pack_data[k] = v

            temp_chunk = temps[i*temps_num:(i+1)*temps_num]
            volt_chunk = volts[i*cells_num:(i+1)*cells_num]

            for j in range(temps_num):
                key = f"BMS_BattTemp{j+1}"
                val = temp_chunk[j] if j < len(temp_chunk) else np.nan
                pack_data[key] = None if pd.isna(val) else float(val)

            for j in range(cells_num):
                key = f"BMS_CellVolt{j+1}"
                val = volt_chunk[j] if j < len(volt_chunk) else np.nan
                pack_data[key] = None if pd.isna(val) else float(val)

            volt_list = [v for v in (volt_chunk.tolist()) if not pd.isna(v)]
            if len(volt_list) == 0:
                max_v = min_v = None
                max_idx = min_idx = None
            else:
                arr = np.array(volt_chunk, dtype=float)
                valid_mask = ~np.isnan(arr)
                valid_idx = np.where(valid_mask)[0]
                valid_vals = arr[valid_mask]
                max_rel_idx = int(np.argmax(valid_vals))
                min_rel_idx = int(np.argmin(valid_vals))
                max_v = float(valid_vals[max_rel_idx])
                min_v = float(valid_vals[min_rel_idx])
                max_idx = int(valid_idx[max_rel_idx]) + 1
                min_idx = int(valid_idx[min_rel_idx]) + 1

            pack_data["max_voltage"] = None if max_v is None else float(max_v)
            pack_data["max_voltage_cell_index"] = max_idx
            pack_data["min_voltage"] = None if min_v is None else float(min_v)
            pack_data["min_voltage_cell_index"] = min_idx

            if (max_v is None) or (min_v is None):
                pack_data["pack_voltage_range"] = None
            else:
                pack_data["pack_voltage_range"] = round(float(max_v) - float(min_v), 4)

            # pack_data["_source_row_idx"] = int(idx)
            pack_data['test_device_name'] = '锐能'
            pack_data['acquire_time'] = str(pd.to_datetime(pack_data['acquire_time'].replace('/', ' '), format='%Y-%m-%d %H:%M:%S.%f'))
            pack_data['vehicle_to_pack_num'] = f'1拖{drag}'
            json_records.append(pack_data)

    # out_file = record_file.replace(".csv", "_processed.jsonl")
    with open(out_jsonl_path, "w", encoding="utf-8") as f:
        for rec in json_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"已生成: {out_jsonl_path}, 共 {len(json_records)} 条记录")


if __name__ == "__main__":
    record_file = r"D:\jz_pack_data\14\锐能@DT2480A-FAP-0000022@03HPB0DA0004BWFAP0000040@330阶梯充一拖三1P102S DCR@20251023192634@20251024011409@通道2@@记录层.csv"
    out_file = record_file.replace(".csv", "_processed.jsonl")
    process_csv_to_json(record_file, out_file)

