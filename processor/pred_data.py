import os
import pandas as pd
import numpy as np
import joblib
from catboost import CatBoostRegressor

def pred_func(df: pd.DataFrame, pack_num: int, cell_num: int):
    OUT_DIR = f"catboost_models_packnum_{pack_num}_cellnum_{cell_num}"
    FEATURE_COLS_FILE = os.path.join(OUT_DIR, "feature_cols.joblib")
    IMPUTER_FILE = os.path.join(OUT_DIR, "imputer.joblib")

    TARGETS = [
        "target_discharge_energy",
        "target_discharge_capacity",
        "target_vehicle_dcr",
        "target_volt_range_14",
        "target_volt_range_15"
    ]

    # ----------------- 载入预处理器与特征列 -----------------
    if not os.path.exists(FEATURE_COLS_FILE) or not os.path.exists(IMPUTER_FILE):
        raise FileNotFoundError("feature_cols.joblib or imputer.joblib not found in OUT_DIR")

    feature_cols = joblib.load(FEATURE_COLS_FILE)
    imputer = joblib.load(IMPUTER_FILE)

    missing_cols = [c for c in feature_cols if c not in df.columns]
    if missing_cols:
        print(f"[WARN] new data missing {len(missing_cols)} columns, filling with NaN (examples: {missing_cols[:5]})")
        for c in missing_cols:
            df[c] = np.nan

    X_new = df[feature_cols].copy()
    X_new = X_new.apply(pd.to_numeric, errors='coerce')

    # ----------------- 用训练时的 imputer 填充 -----------------
    X_new_imputed = pd.DataFrame(imputer.transform(X_new), columns=feature_cols, index=X_new.index)
    print(f"[INFO] Imputed new data shape: {X_new_imputed.shape}")

    # ----------------- 逐目标加载模型并预测 -----------------
    preds_df = pd.DataFrame(index=X_new_imputed.index)
    for target in TARGETS:
        model_path = os.path.join(OUT_DIR, f"catboost_{target}.cbm")
        if not os.path.exists(model_path):
            print(f"[WARN] model file not found for {target}: {model_path} -- skipping")
            preds_df[target] = np.nan
            continue

        model = CatBoostRegressor()
        model.load_model(model_path)
        preds = model.predict(X_new_imputed)
        preds_df[target] = preds

    return preds_df
