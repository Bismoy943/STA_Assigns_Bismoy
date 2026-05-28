import os
import glob
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR           = os.path.dirname(os.path.abspath(__file__))
OLA_TRACKER_PATH   = os.path.join(BASE_DIR, "ADM_OLA_tracker.csv")
PARENT_METADATA_PATH = os.path.join(
    BASE_DIR, "ingestion_metadata", "parent_metadata.csv"
)
DAG_CONFIG_DIR     = os.path.join(BASE_DIR, "dag_config", "ingestion")
OUTPUT_PATH        = os.path.join(BASE_DIR, "weight_analysis_report.csv")

BASE_SCORE = 2400




def parse_arrival_minutes(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    match = re.match(r'\(T\+(\d+)\)\s+(\d+):(\d+):(\d+)', str(val).strip())
    if not match:
        return None
    t_offset = int(match.group(1))
    h, m, s  = int(match.group(2)), int(match.group(3)), int(match.group(4))
    return t_offset * 1440 + h * 60 + m + s / 60


def minutes_to_str(total_minutes):
    if total_minutes is None:
        return "N/A"
    t_offset  = int(total_minutes // 1440)
    remainder = total_minutes % 1440
    h = int(remainder // 60)
    m = int(remainder % 60)
    s = int((remainder % 1) * 60)
    return f"(T+{t_offset}) {h:02d}:{m:02d}:{s:02d}"


def get_expected_band(avg_arrival_mins):
    """
    Derive expected weight band from AVG ARRIVAL TIME (T-offset ignored).

    Formula:
        expected_weight = (HH * 100) + MM   → minimum 1 if result is 0
        expected_min    = expected_weight - 100
        expected_max    = expected_weight + 100
    """
    if avg_arrival_mins is None:
        return (None, None)

    remainder = avg_arrival_mins % 1440          # strip T-offset, keep time-of-day
    hh = int(remainder // 60)
    mm = int(remainder % 60)

    expected_weight = max(1, hh * 100 + mm)      # minimum weight = 1
    exp_min = expected_weight - 100
    exp_max = expected_weight + 100

    return (exp_min, exp_max)


def alignment_flag(dag_weight, exp_min, exp_max):
    if dag_weight is None or exp_min is None:
        return "❓ UNKNOWN"
    if exp_min <= dag_weight <= exp_max:
        return "✅ OK"
    elif dag_weight < exp_min:
        # DAG weight TOO LOW → actual weight HIGH → higher priority → runs EARLIER than expected
        return "🔴 TOO LOW  (runs too early — increase weight)"
    else:
        # DAG weight TOO HIGH → actual weight LOW → lower priority → runs LATER than expected
        return "🔴 TOO HIGH (runs too late — decrease weight)"


def load_parent_metadata():
    """
    Load parent_metadata.csv and build a lookup:
        FILE_NAME (source_table_file_name) → table_file_id
    Multiple rows may share the same file name (different countries/TPs),
    so we store them as a list for fallback matching.
    """
    df = pd.read_csv(PARENT_METADATA_PATH, sep="|", dtype=str, on_bad_lines="skip")
    df.columns = [c.strip().lower() for c in df.columns]
    logger.info("Loaded parent_metadata.csv: %d rows", len(df))
    return df

def normalize_filename(fn):
    """Strip .gz suffix for comparison purposes."""
    fn = fn.strip().upper()
    if fn.endswith('.GZ'):
        fn = fn[:-3]
    return fn




def resolve_table_file_id(pm_df, file_name, country, tp_sys):
    """
    Match FILE_NAME against source_table_file_name in parent_metadata.
    Normalizes .gz suffix on both sides before comparing.
    """
    fn_normalized = normalize_filename(file_name)

    # Build normalized column once for efficiency
    if "_fn_norm" not in pm_df.columns:
        pm_df["_fn_norm"] = pm_df["source_table_file_name"].apply(
            lambda x: normalize_filename(str(x)) if pd.notna(x) else ""
        )

    mask_fn = pm_df["_fn_norm"] == fn_normalized

    if not mask_fn.any():
        return None, "NOT FOUND IN PARENT METADATA"

    candidates = pm_df[mask_fn]

    # Level 1: file + tp + country
    m1 = candidates[
        (candidates["tp"].str.strip().str.upper()      == tp_sys.upper()) &
        (candidates["country"].str.strip().str.upper() == country.upper())
    ]
    if not m1.empty:
        return str(m1.iloc[0]["table_file_id"]).strip(), None

    # Level 2: file + tp
    m2 = candidates[candidates["tp"].str.strip().str.upper() == tp_sys.upper()]
    if not m2.empty:
        logger.debug("L2 match (no country): FILE=%s TP=%s", file_name, tp_sys)
        return str(m2.iloc[0]["table_file_id"]).strip(), None

    # Level 3: file name only
    logger.warning("L3 fallback (file name only): FILE=%s TP=%s COUNTRY=%s",
                   file_name, tp_sys, country)
    return str(candidates.iloc[0]["table_file_id"]).strip(), None


def load_ingestion_config_map():
    result = {}
    for fpath in glob.glob(os.path.join(DAG_CONFIG_DIR, "*_ingestion_config.csv")):
        fname  = os.path.basename(fpath)
        tp_sys = fname.replace("_ingestion_config.csv", "").upper()
        try:
            df = pd.read_csv(fpath, sep="|", dtype=str, on_bad_lines="skip")
            df.columns = [c.strip().lower() for c in df.columns]
            tid_map = {}
            for _, row in df.iterrows():
                tid = str(row.get("table_id", "")).strip()
                wt  = str(row.get("weights",  "")).strip()
                if tid and wt:
                    try:
                        tid_map[tid] = int(wt)
                    except ValueError:
                        pass
            result[tp_sys] = tid_map
            logger.info("Loaded ingestion config: %-20s  %d entries", tp_sys, len(tid_map))
        except Exception as e:
            logger.warning("Failed to load %s: %s", fname, e)
    return result

def resolve_all_table_file_ids(pm_df, file_name, country, tp_sys):
    """
    Returns ALL matching table_file_ids for a given file_name/country/tp combination.
    Priority order: exact match (file+tp+country), then (file+tp), then (file only).
    Returns list of table_file_ids (best matches first).
    """
    fn_normalized = normalize_filename(file_name)

    if "_fn_norm" not in pm_df.columns:
        pm_df["_fn_norm"] = pm_df["source_table_file_name"].apply(
            lambda x: normalize_filename(str(x)) if pd.notna(x) else ""
        )

    mask_fn = pm_df["_fn_norm"] == fn_normalized

    if not mask_fn.any():
        return [], "NOT FOUND IN PARENT METADATA"

    candidates = pm_df[mask_fn]

    # Level 1: file + tp + country
    m1 = candidates[
        (candidates["tp"].str.strip().str.upper()      == tp_sys.upper()) &
        (candidates["country"].str.strip().str.upper() == country.upper())
    ]

    # Level 2: file + tp
    m2 = candidates[candidates["tp"].str.strip().str.upper() == tp_sys.upper()]

    # Build ordered list: L1 first, then L2 extras, then all remaining
    seen = set()
    ordered_ids = []

    for df_subset in [m1, m2, candidates]:
        for tid in df_subset["table_file_id"].astype(str).str.strip():
            if tid and tid not in seen:
                seen.add(tid)
                ordered_ids.append(tid)

    return ordered_ids, None


def find_dag_weight_any_config(tid_list, primary_tp_sys, ingestion_cfg_map):
    """
    Given a list of table_file_ids (priority ordered), search for a DAG weight.
    Step 1: Search primary TP config for any of the IDs.
    Step 2: If not found, search ALL configs for any of the IDs.
    Returns (table_file_id_used, config_tp_used, dag_weight) or (None, None, None).
    """
    primary_cfg = ingestion_cfg_map.get(primary_tp_sys, {})

    # Step 1: Try primary TP config with all IDs
    for tid in tid_list:
        if tid in primary_cfg:
            return tid, primary_tp_sys, primary_cfg[tid]

    # Step 2: Search ALL configs (TP name mismatch scenario)
    for cfg_tp, cfg_map in ingestion_cfg_map.items():
        for tid in tid_list:
            if tid in cfg_map:
                logger.debug(
                    "Cross-TP match: TID=%s found in %s (OLA TP=%s)",
                    tid, cfg_tp, primary_tp_sys
                )
                return tid, cfg_tp, cfg_map[tid]

    return None, None, None

def parse_ola_time_minutes(ola_time_str):
    """
    Parse OLA_TIME string like '2:00:00' or '13:15:00' into total minutes from midnight.
    """
    if pd.isna(ola_time_str) or str(ola_time_str).strip() in ("", "nan"):
        return None
    match = re.match(r'(\d+):(\d+):(\d+)', str(ola_time_str).strip())
    if not match:
        return None
    h, m, s = int(match.group(1)), int(match.group(2)), int(match.group(3))
    return h * 60 + m + s / 60


def alignment_flag_ola(dag_weight, ola_time_str):
    """
    Compare DAG weight against expected band derived from OLA_TIME.
    OLA time is treated as the DEADLINE — the DAG weight should reflect this time.
    """
    ola_mins = parse_ola_time_minutes(ola_time_str)
    if ola_mins is None:
        return "❓ OLA TIME MISSING"

    # Reuse the same band formula as get_expected_band but with OLA time minutes
    exp_min, exp_max = get_expected_band(ola_mins)

    if dag_weight is None or exp_min is None:
        return "❓ UNKNOWN"
    if exp_min <= dag_weight <= exp_max:
        return "✅ OK (vs OLA)"
    elif dag_weight < exp_min:
        return "🔴 TOO LOW  (runs before OLA deadline — increase weight)"
    else:
        return "🔴 TOO HIGH (runs after OLA deadline — decrease weight)"






def main():
    logger.info("Loading OLA tracker  : %s", OLA_TRACKER_PATH)
    ola_df = pd.read_csv(OLA_TRACKER_PATH, dtype=str)
    ola_df.columns = [c.strip() for c in ola_df.columns]

    date_cols = [c for c in ola_df.columns if re.match(r'\d+/\d+/\d{4}', c.strip())]
    logger.info("Found %d date columns: %s … %s", len(date_cols), date_cols[0], date_cols[-1])

    pm_df             = load_parent_metadata()
    ingestion_cfg_map = load_ingestion_config_map()

    records = []

    for _, row in ola_df.iterrows():
        tp_sys       = str(row.get("TP_SYS",             "")).strip().upper()
        country      = str(row.get("COUNTRY",            "")).strip()
        target_table = str(row.get("TARGET_TABLE_NAME",  "")).strip().upper()
        file_name    = str(row.get("FILE_NAME",          "")).strip()
        ola_time     = str(row.get("OLA_TIME",           "")).strip()
        earliest     = str(row.get("EARLIST_ARRIVAL_TIME","")).strip()
        most_delayed = str(row.get("MOST_DELAYED_TIME",  "")).strip()
        frequency    = str(row.get("FREQUENCY",          "")).strip()

        arrival_minutes = [
            m for dc in date_cols
            if (m := parse_arrival_minutes(row.get(dc, ""))) is not None
        ]
        data_days       = len(arrival_minutes)
        avg_mins        = sum(arrival_minutes) / data_days if data_days else None
        avg_arrival_str = minutes_to_str(avg_mins)
        exp_min, exp_max = get_expected_band(avg_mins)

        base = {
            "TP_SYS":              tp_sys,
            "COUNTRY":             country,
            "TARGET_TABLE_NAME":   target_table,
            "FILE_NAME":           file_name,
            "OLA_TIME":            ola_time,
            "EARLIEST_ARRIVAL":    earliest,
            "MOST_DELAYED":        most_delayed,
            "FREQUENCY":           frequency,
            "DATA_DAYS":           data_days,
            "AVG_ARRIVAL_TIME":    avg_arrival_str,
            "EXPECTED_WEIGHT_MIN": exp_min,
            "EXPECTED_WEIGHT_MAX": exp_max,
        }

        # ── Step 1: Get ALL matching table_file_ids ───────────────────────────
        tid_list, err = resolve_all_table_file_ids(pm_df, file_name, country, tp_sys)

        if not tid_list:
            records.append({**base,
                "TABLE_FILE_ID":   err,
                "MATCHED_CFG_TP":  None,
                "DAG_WEIGHT":      None,
                "ACTUAL_WEIGHT":   None,
                "ALIGNMENT_FLAG":  f"❓ {err}",
                "ALIGNMENT_FLAG_OLA": "❓ N/A",  # ← ADD THIS
            })
            continue

        # ── Step 2: Find DAG weight across configs ────────────────────────────
        tid_used, cfg_tp_used, dag_weight = find_dag_weight_any_config(
            tid_list, tp_sys, ingestion_cfg_map
        )

        if dag_weight is None:
            # Weight not found in any config
            records.append({**base,
                "TABLE_FILE_ID":   ", ".join(tid_list),   # show all IDs tried
                "MATCHED_CFG_TP":  None,
                "DAG_WEIGHT":      None,
                "ACTUAL_WEIGHT":   None,
                "ALIGNMENT_FLAG":  "❓ UNKNOWN (no weight in any config)",
                "ALIGNMENT_FLAG_OLA": "❓ N/A",  # ← ADD THIS
            })
            continue

        actual_weight = BASE_SCORE - dag_weight
        flag          = alignment_flag(dag_weight, exp_min, exp_max)
        flag_ola = alignment_flag_ola(dag_weight, ola_time)  # ← ADD THIS LINE

        records.append({**base,
            "TABLE_FILE_ID":  tid_used,
            "MATCHED_CFG_TP": cfg_tp_used,
            "DAG_WEIGHT":     dag_weight,
            "ACTUAL_WEIGHT":  actual_weight,
            "ALIGNMENT_FLAG": flag,
            "ALIGNMENT_FLAG_OLA": flag_ola,  # ← ADD THIS
        })

    # ── Build & save report ───────────────────────────────────────────────────
    report_df = pd.DataFrame(records, columns=[
        "TP_SYS", "COUNTRY", "TARGET_TABLE_NAME", "FILE_NAME",
        "OLA_TIME", "EARLIEST_ARRIVAL", "MOST_DELAYED", "FREQUENCY",
        "DATA_DAYS", "AVG_ARRIVAL_TIME",
        "TABLE_FILE_ID", "MATCHED_CFG_TP", "DAG_WEIGHT", "ACTUAL_WEIGHT",
        "EXPECTED_WEIGHT_MIN", "EXPECTED_WEIGHT_MAX", "ALIGNMENT_FLAG","ALIGNMENT_FLAG_OLA",
    ])

    total     = len(report_df)
    ok        = (report_df["ALIGNMENT_FLAG"] == "✅ OK").sum()
    too_high  = report_df["ALIGNMENT_FLAG"].str.contains("TOO HIGH", na=False).sum()
    too_low   = report_df["ALIGNMENT_FLAG"].str.contains("TOO LOW",  na=False).sum()
    not_found = report_df["ALIGNMENT_FLAG"].str.contains("NOT FOUND", na=False).sum()
    unknown   = report_df["ALIGNMENT_FLAG"].str.contains("UNKNOWN",   na=False).sum()

    logger.info("=" * 60)
    logger.info("REPORT SUMMARY")
    logger.info("  Total rows           : %d", total)
    logger.info("  ✅ Weight OK          : %d", ok)
    logger.info("  🔴 Too high (early)   : %d", too_high)
    logger.info("  🔴 Too low  (late)    : %d", too_low)
    logger.info("  ❓ Not found          : %d", not_found)
    logger.info("  ❓ Unknown (no weight): %d", unknown)
    logger.info("=" * 60)

    report_df.to_csv(OUTPUT_PATH, index=False)
    logger.info("Report saved → %s", OUTPUT_PATH)


'''
def main():
    logger.info("Loading OLA tracker  : %s", OLA_TRACKER_PATH)
    ola_df = pd.read_csv(OLA_TRACKER_PATH, dtype=str)
    ola_df.columns = [c.strip() for c in ola_df.columns]

    date_cols = [c for c in ola_df.columns if re.match(r'\d+/\d+/\d{4}', c.strip())]
    logger.info("Found %d date columns: %s … %s", len(date_cols), date_cols[0], date_cols[-1])

    pm_df             = load_parent_metadata()
    ingestion_cfg_map = load_ingestion_config_map()

    records = []

    for _, row in ola_df.iterrows():
        tp_sys       = str(row.get("TP_SYS",             "")).strip().upper()
        country      = str(row.get("COUNTRY",            "")).strip()
        target_table = str(row.get("TARGET_TABLE_NAME",  "")).strip().upper()
        file_name    = str(row.get("FILE_NAME",          "")).strip()
        ola_time     = str(row.get("OLA_TIME",           "")).strip()
        earliest     = str(row.get("EARLIST_ARRIVAL_TIME","")).strip()
        most_delayed = str(row.get("MOST_DELAYED_TIME",  "")).strip()
        frequency    = str(row.get("FREQUENCY",          "")).strip()

        # ── Avg arrival from date columns ─────────────────────────────────────
        arrival_minutes = [
            m for dc in date_cols
            if (m := parse_arrival_minutes(row.get(dc, ""))) is not None
        ]
        data_days       = len(arrival_minutes)
        avg_mins        = sum(arrival_minutes) / data_days if data_days else None
        avg_arrival_str = minutes_to_str(avg_mins)

        # ── Expected weight band from AVG ARRIVAL time ────────────────────────
        exp_min, exp_max = get_expected_band(avg_mins)

        base = {
            "TP_SYS":              tp_sys,
            "COUNTRY":             country,
            "TARGET_TABLE_NAME":   target_table,
            "FILE_NAME":           file_name,
            "OLA_TIME":            ola_time,
            "EARLIEST_ARRIVAL":    earliest,
            "MOST_DELAYED":        most_delayed,
            "FREQUENCY":           frequency,
            "DATA_DAYS":           data_days,
            "AVG_ARRIVAL_TIME":    avg_arrival_str,
            "EXPECTED_WEIGHT_MIN": exp_min,
            "EXPECTED_WEIGHT_MAX": exp_max,
        }

        # ── Step 1: Match FILE_NAME → table_file_id via parent_metadata ───────
        tid, err = resolve_table_file_id(pm_df, file_name, country, tp_sys)

        if tid is None:
            records.append({**base,
                "TABLE_FILE_ID":  err,
                "DAG_WEIGHT":     None,
                "ACTUAL_WEIGHT":  None,
                "ALIGNMENT_FLAG": f"❓ {err}",
            })
            continue

        # ── Step 2: Lookup DAG weight from ingestion config ───────────────────
        cfg_map       = ingestion_cfg_map.get(tp_sys, {})
        dag_weight    = cfg_map.get(tid)
        actual_weight = (BASE_SCORE - dag_weight) if dag_weight is not None else None
        flag          = alignment_flag(actual_weight, exp_min, exp_max)

        records.append({**base,
            "TABLE_FILE_ID":  tid,
            "DAG_WEIGHT":     dag_weight,
            "ACTUAL_WEIGHT":  actual_weight,
            "ALIGNMENT_FLAG": flag,
        })

    # ── Build & save report ───────────────────────────────────────────────────
    report_df = pd.DataFrame(records, columns=[
        "TP_SYS", "COUNTRY", "TARGET_TABLE_NAME", "FILE_NAME",
        "OLA_TIME", "EARLIEST_ARRIVAL", "MOST_DELAYED", "FREQUENCY",
        "DATA_DAYS", "AVG_ARRIVAL_TIME",
        "TABLE_FILE_ID", "DAG_WEIGHT", "ACTUAL_WEIGHT",
        "EXPECTED_WEIGHT_MIN", "EXPECTED_WEIGHT_MAX", "ALIGNMENT_FLAG",
    ])

    total     = len(report_df)
    ok        = (report_df["ALIGNMENT_FLAG"] == "✅ OK").sum()
    too_high  = report_df["ALIGNMENT_FLAG"].str.contains("TOO HIGH", na=False).sum()
    too_low   = report_df["ALIGNMENT_FLAG"].str.contains("TOO LOW",  na=False).sum()
    not_found = report_df["ALIGNMENT_FLAG"].str.contains("NOT FOUND", na=False).sum()
    unknown   = report_df["ALIGNMENT_FLAG"].str.contains("UNKNOWN",   na=False).sum()

    logger.info("=" * 60)
    logger.info("REPORT SUMMARY")
    logger.info("  Total rows           : %d", total)
    logger.info("  ✅ Weight OK          : %d", ok)
    logger.info("  🔴 Too high (early)   : %d", too_high)
    logger.info("  🔴 Too low  (late)    : %d", too_low)
    logger.info("  ❓ Not found          : %d", not_found)
    logger.info("  ❓ Unknown (no weight): %d", unknown)
    logger.info("=" * 60)

    report_df.to_csv(OUTPUT_PATH, index=False)
    logger.info("Report saved → %s", OUTPUT_PATH)
'''

if __name__ == "__main__":
    main()