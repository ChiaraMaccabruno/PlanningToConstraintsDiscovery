import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import datetime

def puliziaEventLog(csvInput, csvOutput, xesOutput, cleaning_conf=None):
    # Initialize configuration and cleaning options
    if cleaning_conf is None:
        cleaning_conf = {}

    options = cleaning_conf["options"]

    sep = cleaning_conf["csv_separator"]
    remove_empty_columns = options["remove_empty_columns"]
    remove_redundant_columns = options["remove_redundant_columns"]
    remove_constant_columns = options["remove_constant_columns"]
    plan_col_conf = cleaning_conf["plan_column"]
    activity_col_conf = cleaning_conf["activity_column"]
    timestamp_col_conf = cleaning_conf["timestamp_column"]


    # Load CSV file and handle missing values
    df = pd.read_csv(csvInput, sep=sep, dtype=str,
                    keep_default_na=False, na_values=["nan", "NaN", ""])
    # replace missing values with empty string
    df = df.fillna("")  

    plan_col = "case:concept:name" if "case:concept:name" in df.columns else plan_col_conf
    activity_col = "concept:name" if "concept:name" in df.columns else activity_col_conf
    timestamp_col = "time:timestamp" if "time:timestamp" in df.columns else timestamp_col_conf

    rename_map = {}

    # Standardize the name columns for PM4Py compatibility
    if plan_col in df.columns and plan_col != "case:concept:name":
            rename_map[plan_col] = "case:concept:name"

    if activity_col in df.columns and activity_col != "concept:name":
        rename_map[activity_col] = "concept:name"
    
    if timestamp_col in df.columns and timestamp_col != "time:timestamp":
        rename_map[timestamp_col] = "time:timestamp"

    if rename_map:
        df = df.rename(columns=rename_map)

    if 'event_id' not in df.columns:
        df['event_id'] = range(1, len(df)+1)

    # Remove columns that contain only empty values
    if remove_empty_columns:
        empty_cols = [c for c in df.columns if df[c].eq("").all()]
        if empty_cols:
            print(f"Removing empty columns: {empty_cols}")
            df.drop(columns=empty_cols, inplace=True)


    # Remove redundant columns (different columns with identical values)
    if remove_redundant_columns:
        cols = list(df.columns)
        redundant = set()

        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                c1, c2 = cols[i], cols[j]
                if c1 not in redundant and c2 not in redundant:
                    if df[c1].fillna("").equals(df[c2].fillna("")):
                        redundant.add(c2)

        if redundant:
            print(f"Removing redundant columns: {sorted(redundant)}")
            df.drop(columns=list(redundant), inplace=True)

    # Remove columns with a unique value
    if remove_constant_columns:
        constant_cols = []
        for col in df.columns:
            unique_vals = set(df[col].astype(str).str.strip().unique()) - {""}
            if len(unique_vals) <= 1:
                constant_cols.append(col)

        if constant_cols:
            print(f"Removing columns with constant values: {constant_cols}")
            df.drop(columns=constant_cols, inplace=True)

    # Save the cleaned dataframe to a CSV file
    df.to_csv(csvOutput, sep=";", index=False, encoding="utf-8")

    df = dataframe_utils.convert_timestamp_columns_in_df(df)  

    required = ["case:concept:name", "concept:name", "time:timestamp"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise RuntimeError(
            f"[CLEANING ERROR] Missing required PM4Py columns: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    log = log_converter.apply(df)                              
    xes_exporter.apply(log, xesOutput)           

    print("\nCleaning completed.")
