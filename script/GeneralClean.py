import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import datetime

CLEANING_OPTIONS = {
    "remove_empty_columns": True,      
    "remove_sporadic_columns": False,  
    "remove_redundant_columns": True,  
    "remove_constant_columns": True,   
}

# Threshold for the minimum presence of values in a column
global_presence_threshold = 0.20  

# Minimum number of occurrences of an action to consider it significant
min_action_occurrences = 2            

def puliziaEventLog(csvInput, csvOutput, xesOutput):
    # Load CSV file
    df = pd.read_csv(csvInput, sep=";", dtype=str,
                    keep_default_na=False, na_values=["nan", "NaN", ""])
    # replace missing values with empty string
    df = df.fillna("")  

    # Rename the case ID column to standard PM4Py format if present
    if "case_id" in df.columns:
        df.rename(columns={"case_id": "case:concept:name"}, inplace=True)

    # Remove columns with all empty values
    if CLEANING_OPTIONS["remove_empty_columns"]:
        empty_cols = [c for c in df.columns if df[c].eq("").all()]
        if empty_cols:
            print(f"Removing empty columns: {empty_cols}")
            df.drop(columns=empty_cols, inplace=True)

    # Remove columns with mostly missing values
    if CLEANING_OPTIONS["remove_sporadic_columns"]:
        cols_to_consider = list(df.columns)
        cols_to_delete = []
        n_rows = len(df)

        for col in cols_to_consider:
            # Create a mask to identify which row contain empty-values in the column
            non_empty = df[col].astype(str).str.strip() != ""
            # Calculate the proportion of non-empty values in the column
            global_presence = non_empty.sum() / max(1, n_rows)

            # Count occurrences per activity:
            # Count the number of rows per activity
            action_counts = df.groupby("activity").size()
            # Count the number of non-empty values per activity for this column
            action_nonempty = df[non_empty].groupby("activity").size()

            # Check if there is at least one action where this column is always present when the action occurs
            has_action_always_present = False
            for action, total in action_counts.items():
                # Get the count of non-empty values for this activity in this column
                ne = int(action_nonempty.get(action, 0))
                # If the activity occurs enough times and all of its rows have non-empty values, then the column is considered "always present" for this action
                if total >= min_action_occurrences and ne == total:
                    has_action_always_present = True
                    break

            # Decide whether to delete column
            if not has_action_always_present or global_presence < global_presence_threshold:
                cols_to_delete.append(col)

        if cols_to_delete:
            print(f"Removing columns with mostly empty or non-useful values: {cols_to_delete}")
            df.drop(columns=cols_to_delete, inplace=True)

    # Remove redundant columns (columns with same values)
    if CLEANING_OPTIONS["remove_redundant_columns"]:
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
    if CLEANING_OPTIONS["remove_constant_columns"]:
        constant_cols = []
        for col in df.columns:
            unique_vals = set(df[col].astype(str).str.strip().unique()) - {""}
            if len(unique_vals) <= 1:
                constant_cols.append(col)

        if constant_cols:
            print(f"Removing columns with constant values: {constant_cols}")
            df.drop(columns=constant_cols, inplace=True)


    df.to_csv(csvOutput, sep=";", index=False, encoding="utf-8")

    df = dataframe_utils.convert_timestamp_columns_in_df(df)  
    log = log_converter.apply(df)                              
    xes_exporter.apply(log, xesOutput)           

    print("\nCleaning completed.")
