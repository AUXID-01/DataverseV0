import os
import time
import json
import pandas as pd
from .file_handlers import READERS


# ============================================================
# 1️⃣ Universal JSON Flattener (Recursive)
# ============================================================
def deep_flatten_value(prefix, value, out):

    # skip empty / null prefixes
    if prefix in ("", None):
        return

    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}_{k}".strip("_")
            deep_flatten_value(key, v, out)

    elif isinstance(value, list):
        for i, v in enumerate(value):
            key = f"{prefix}_{i}".strip("_")
            deep_flatten_value(key, v, out)

    else:
        out[prefix] = value if value is not None else ""


def deep_flatten_row(row: dict):
    flat = {}
    for key, value in row.items():
        deep_flatten_value(key, value, flat)
    return flat


# ============================================================
# 2️⃣ Smart JSON Extractor (handles ANY JSON shape)
# ============================================================
def flatten_json(data, prefix=""):
    """
    Safely flatten JSON.
    Guarantees keys are never '', None, or invalid.
    """
    out = {}

    def normalize_key(key):
        if key is None or str(key).strip() == "":
            return "unknown"
        return str(key)

    def recurse(value, key_prefix=""):
        key_prefix = normalize_key(key_prefix)

        if isinstance(value, dict):
            for k, v in value.items():
                nk = normalize_key(k)
                recurse(v, f"{key_prefix}_{nk}")

        elif isinstance(value, list):
            for i, item in enumerate(value):
                recurse(item, f"{key_prefix}_{i}")

        else:
            final_key = normalize_key(key_prefix)
            out[final_key] = value if value is not None else ""

    recurse(data, prefix)
    return out


def extract_json_safely(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []

    # Case 1: root = LIST
    if isinstance(data, list):
        return pd.DataFrame([flatten_json(item) for item in data])

    # Case 2: root = DICT
    if isinstance(data, dict):
        list_candidates = [(k, v) for k, v in data.items() if isinstance(v, list)]

        if list_candidates:
            root_key, largest_list = max(list_candidates, key=lambda x: len(x[1]))

            for item in largest_list:
                flat = flatten_json(item)

                # add non-list fields
                for k, v in data.items():
                    if not isinstance(v, list):
                        flat[k] = v

                rows.append(flat)

            return pd.DataFrame(rows)

        # If dict with no lists → flatten entire dict as one row
        return pd.DataFrame([flatten_json(data)])

    # Case 3: primitive
    return pd.DataFrame([{"value": data}])


# ============================================================
# 3️⃣ Patch-2: Normalize irregular list columns
# ============================================================
def normalize_list_columns(df):
    for col in df.columns:
        series = df[col]

        if series.apply(lambda x: isinstance(x, list)).any():
            max_len = series.apply(lambda x: len(x) if isinstance(x, list) else 1).max()

            df[col] = series.apply(
                lambda x:
                    x + [None] * (max_len - len(x))
                    if isinstance(x, list)
                    else [x] + [None] * (max_len - 1)
            )
    return df


# ============================================================
# 4️⃣ Main extract_data() — FULLY UPDATED
# ============================================================
def detect_file_type(file_path):
    return os.path.splitext(file_path)[1].lower().replace(".", "")


def extract_data(file_path):
    """Universal extractor with FULL deep flattening included."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_type = detect_file_type(file_path)
        print(f"\n📂 Detected file type: {file_type.upper()}")

        start_time = time.time()

        # ---- JSON special handling ----
        if file_type == "json":
            df = extract_json_safely(file_path)
        else:
            reader = READERS.get(file_type)
            if not reader:
                print(f"⚠️ Unsupported file type: {file_type}")
                return pd.DataFrame()
            df = reader(file_path)

        # ---- Patch-2 (rectangular list normalization) ----
        df = normalize_list_columns(df)

        # ====================================================
        # ⭐️ FINAL STEP: FULL DEEP FLATTENING ⭐️
        # ====================================================
        records = df.to_dict(orient="records")
        flat_records = [deep_flatten_row(r) for r in records]
        df = pd.DataFrame(flat_records)

        duration = time.time() - start_time
        print(f"✅ Extracted {len(df)} rows (fully structured) from {file_path} in {duration:.2f}s")

        return df

    except Exception as e:
        print(f"❌ Extraction error: {e}")
        return pd.DataFrame()
