import pandas as pd
from typing import Dict
import math
import numpy as np
import re
import os
from typing import Optional, Tuple
from processor.pred_data import pred_func

def read_jsonl_to_df(file_path):
    try:
        df = pd.read_json(file_path, lines=True)
        return df
    except Exception as e:
        print(f"[ERROR] 读取文件失败: {e}")
        return None

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

def parse_drag_and_cells(basename: str) -> Tuple[Optional[int], Optional[int]]:

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

def process_tree_data(file_path):
    df = read_jsonl_to_df(file_path)
    pack_num, cell_num = parse_drag_and_cells(df['vehicle_to_pack_num'].iloc[0])
    if pack_num is None or cell_num is None:
        return
    cols_to_check = [f"BMS_CellVolt{i}" for i in range(1, 103)]
    req_steps = set(int(x) for x in range(1, 12))
    df_filtered = df.dropna(subset=cols_to_check)  # 去除空值
    df = df_filtered[~(df_filtered[cols_to_check] == 0).any(axis=1)]  # 去除值为 0 的行

    pack_code_list = df['pack_code'].unique()
    pack_code_list.sort()
    if len(pack_code_list) != pack_num:
        return

    vehicle_code = df['vehicle_code'].unique()[0]

    pack_dfs: Dict[str, pd.DataFrame] = {}
    for p in pack_code_list:
        sub = df[df['pack_code'] == p].copy()
        sub = sub.sort_values('acquire_time').reset_index(drop=True)
        step_nums = pd.to_numeric(sub['step_id'], errors='coerce').dropna().astype(int).unique()
        step_set = set(step_nums)
        missing_steps = sorted(list(req_steps - step_set))
        if missing_steps:
            return
        else:
            pack_dfs[p] = sub

    lengths = [len(g) for g in pack_dfs.values()]
    if len(set(lengths)) != 1:
        return

    volt_concat_list = []
    temp_concat_list = []
    volt_cols_per_pack = [f"BMS_CellVolt{i}" for i in range(1, 103)]
    temp_cols_per_pack = [f"BMS_BattTemp{i}" for i in range(1, 9)]
    for idx, p in enumerate(pack_code_list):
        g = pack_dfs[p].reset_index(drop=True)

        offset = idx * len(volt_cols_per_pack)
        rename_map = {old: f'BMS_BattVolt{offset + i + 1}' for i, old in enumerate(volt_cols_per_pack)}
        tdf_renamed = g[volt_cols_per_pack].rename(columns=rename_map).reset_index(drop=True)
        volt_concat_list.append(tdf_renamed)

        offset_temp = idx * len(temp_cols_per_pack)
        rename_map_temp = {old: f'BMS_BattTemp{offset_temp + i + 1}' for i, old in enumerate(temp_cols_per_pack)}
        tdf_renamed_temp = g[temp_cols_per_pack].rename(columns=rename_map_temp).reset_index(drop=True)
        temp_concat_list.append(tdf_renamed_temp)

    meta_frames = g[['step_id', 'acquire_time', 'voltage', 'current', 'power', 'charge_energy', 'discharge_energy',
                     'charge_capacity', 'discharge_capacity']].reset_index(drop=True)

    volt_df = pd.concat(volt_concat_list, axis=1)
    volt_df = pd.concat([meta_frames, volt_df], axis=1)

    temp_df = pd.concat(temp_concat_list, axis=1)
    temp_df = pd.concat([meta_frames, temp_df], axis=1)

    META_COLS = ['step_id', 'acquire_time', 'voltage', 'current', 'power',
                 'charge_energy', 'discharge_energy', 'charge_capacity', 'discharge_capacity']

    volt_feature_cols = [c for c in volt_df.columns if c not in META_COLS]
    temp_feature_cols = [c for c in temp_df.columns if c not in META_COLS]

    features = {}
    features['vehicle_code'] = str(vehicle_code)

    input_volt = volt_df.iloc[:11].reset_index(drop=True)
    input_temp = temp_df.iloc[:11].reset_index(drop=True)
    meta_input = input_volt[META_COLS].reset_index(drop=True) if all(
        col in input_volt.columns for col in META_COLS) else pd.DataFrame(columns=META_COLS)

    for i in range(len(input_volt)):
        try:
            sid = int(meta_input.loc[i, 'step_id'])
        except Exception:
            sid = i + 1

        for col in ['voltage', 'current', 'power', 'charge_energy', 'discharge_energy', 'charge_capacity',
                    'discharge_capacity']:
            colname = f"{col}_{sid}"
            if col in meta_input.columns:
                val = meta_input.loc[i, col]
                features[colname] = float(val) if (
                        pd.notna(val) and not (isinstance(val, str) and val == 'nan')) else np.nan
            else:
                features[colname] = np.nan

        volt_vals = input_volt.loc[i, volt_feature_cols].to_numpy(dtype=float)
        features[f"volt_mean_{sid}"] = np.nanmean(volt_vals) if np.any(~np.isnan(volt_vals)) else np.nan
        features[f"volt_var_{sid}"] = np.nanvar(volt_vals) if np.any(~np.isnan(volt_vals)) else np.nan
        features[f"volt_min_{sid}"] = np.nanmin(volt_vals) if np.any(~np.isnan(volt_vals)) else np.nan
        features[f"volt_max_{sid}"] = np.nanmax(volt_vals) if np.any(~np.isnan(volt_vals)) else np.nan
        features[f"volt_range_{sid}"] = (features[f"volt_max_{sid}"] - features[f"volt_min_{sid}"]) if (
                not math.isnan(features[f"volt_max_{sid}"]) and not math.isnan(
            features[f"volt_min_{sid}"])) else np.nan

        temp_vals = input_temp.loc[i, temp_feature_cols].to_numpy(dtype=float)
        features[f"temp_mean_{sid}"] = np.nanmean(temp_vals) if np.any(~np.isnan(temp_vals)) else np.nan
        features[f"temp_var_{sid}"] = np.nanvar(temp_vals) if np.any(~np.isnan(temp_vals)) else np.nan
        features[f"temp_min_{sid}"] = np.nanmin(temp_vals) if np.any(~np.isnan(temp_vals)) else np.nan
        features[f"temp_max_{sid}"] = np.nanmax(temp_vals) if np.any(~np.isnan(temp_vals)) else np.nan
        features[f"temp_range_{sid}"] = (features[f"temp_max_{sid}"] - features[f"temp_min_{sid}"]) if (
                not math.isnan(features[f"temp_max_{sid}"]) and not math.isnan(
            features[f"temp_min_{sid}"])) else np.nan

    discharge_energy_series = df.loc[df['step_id'] == 14, 'discharge_energy']
    discharge_capacity_series = df.loc[df['step_id'] == 14, 'discharge_capacity']

    discharge_energy = float(discharge_energy_series.iloc[0]) if not discharge_energy_series.empty else None
    discharge_capacity = float(discharge_capacity_series.iloc[0]) if not discharge_capacity_series.empty else None

    voltage_step_11 = df.loc[df['step_id'] == 11, 'voltage']
    voltage_step_12 = df.loc[df['step_id'] == 12, 'voltage']
    vehicle_dcr = (
        float(voltage_step_11.iloc[0] - voltage_step_12.iloc[0]) / 297.0
        if not voltage_step_11.empty and not voltage_step_12.empty
        else None
    )

    volt_data_14 = volt_df.loc[volt_df['step_id'] == 14, volt_feature_cols]
    volt_range_14 = (
        float(volt_data_14.iloc[0].max() - volt_data_14.iloc[0].min())
        if not volt_data_14.empty
        else None
    )

    volt_data_15 = volt_df.loc[volt_df['step_id'] == 15, volt_feature_cols]
    volt_range_15 = (
        float(volt_data_15.iloc[0].max() - volt_data_15.iloc[0].min())
        if not volt_data_15.empty
        else None
    )

    input_df = pd.DataFrame([features])
    pred_df = pred_func(input_df, pack_num, cell_num)

    result = {
        'discharge_energy_ground_truth': discharge_energy,
        'discharge_capacity_ground_truth': discharge_capacity,
        'vehicle_dcr_ground_truth': vehicle_dcr,
        'volt_range_14_ground_truth': volt_range_14,
        'volt_range_15_ground_truth': volt_range_15,
        'discharge_energy_pred': pred_df.iloc[0]['target_discharge_energy'],
        'discharge_capacity_pred': pred_df.iloc[0]['target_discharge_capacity'],
        'vehicle_dcr_pred': pred_df.iloc[0]['target_vehicle_dcr'],
        'volt_range_14_pred': pred_df.iloc[0]['target_volt_range_14'],
        'volt_range_15_pred': pred_df.iloc[0]['target_volt_range_15'],
        'pred_time': str(volt_df['acquire_time'].iloc[-1]),
        'vehicle_code': vehicle_code,
        'pack_code': list(pack_code_list)
    }

    return result





# 示例调用
if __name__ == "__main__":
    file_path = r"C:\Users\HP\PycharmProjects\PACK_PRED_v1\data\锐能@DT24102D-G24-0000024@03HPB0DA0001BWG240000029@330阶梯充一拖四1P102S DCR@20260205212835@20260206031845@通道1@@工步层_processed.jsonl"
    process_tree_data(file_path)