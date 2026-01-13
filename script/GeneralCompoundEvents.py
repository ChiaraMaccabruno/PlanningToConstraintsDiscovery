import pandas as pd
import re
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def merge_generic_events(df, manual_cols=None):
    
    # Normalize column names
    original_cols = df.columns.tolist()
    
    key_cols = ["case:concept:name", "concept:name"]
    

    numbered_cols = {}

    # User specified manual columns (e.g., ["loc_from", "loc_to"])
    if manual_cols and len(manual_cols) >= 2:
        # Create a "manual" group that contains exactly these columns in the given order
        print(f"[COMPOUND] Using manual columns: {manual_cols}")
        numbered_cols["manual_group"] = manual_cols
    else:
        # Automatic detection of numbered columns using Regex (e.g., _1, _2)
        print("[COMPOUND] Rilevamento automatico colonne numerate (es. _1, _2)...")
        temp_numbered = {}
        for col in df.columns:
            m = re.match(r"(.+?)[_]?(\d+)$", col)
            if m:
                base, idx = m.groups()
                temp_numbered.setdefault(base, []).append((int(idx), col))
        
        # Keep only bases that have exactly 2 columns to define a start -> end relationship
        for base in temp_numbered:
            sorted_cols = [c for _, c in sorted(temp_numbered[base])]
            if len(sorted_cols) == 2:
                numbered_cols[base] = sorted_cols

    if not numbered_cols:
        print("[COMPOUND] No valid columns found for compounding. Returning original log.")
        return df


    merged_rows = []

    # Group by Case and Activity to find sequential chains within the same process instance
    for keys, group in df.groupby(key_cols):
        # Sort by timestamp to ensure chronological order
        group = group.sort_values("time:timestamp").reset_index(drop=True)
        used = set()

        for i in range(len(group)):
            if i in used:
                continue
            
            chain = [i]
            
            # Build the chain: columns[1] of current row == column[0] of next row
            progress = True
            while progress:
                progress = False
                # Search for a subsequent event that 'fits' the chain
                for j in range(i + 1, len(group)):
                    if j in used or j in chain:
                        continue
                    
                    match_all = True
                    for base, cols in numbered_cols.items():
                        val_prev_dest = group.loc[chain[-1], cols[1]]
                        val_curr_src = group.loc[j, cols[0]]
                        
                        if val_prev_dest != val_curr_src:
                            match_all = False
                            break
                    
                    if match_all:
                        chain.append(j)
                        progress = True
                        break 
            

            # Create a single merged row from the detected chain
            first_idx, last_idx = chain[0], chain[-1]
            
            row_data = group.loc[first_idx].to_dict()
            
            # Update destination columns with the values from the LAST event in the chain
            # E.g., if chain is A->B, B->C, C->D, the final row represents A->D
            for base, cols in numbered_cols.items():
                dest_col = cols[1] 
                row_data[dest_col] = group.loc[last_idx, dest_col]
            
            # Keep the start timestamp of the first event
            row_data["time:timestamp"] = group.loc[first_idx, "time:timestamp"]
            
            merged_rows.append(row_data)
            used.update(chain)

    # Reconstruct the DataFrame with merged data
    df_merged = pd.DataFrame(merged_rows)
    
    if df_merged.empty:
         print("[WARNING] Compounding resulted in an empty dataframe.")
         return df

    # Final sorting
    sort_cols = key_cols + ["time:timestamp"]
    df_merged = df_merged.sort_values(by=sort_cols).reset_index(drop=True)
    
    # Regenerate sequential event IDs
    df_merged["event_id"] = df_merged.groupby(key_cols[0]).cumcount() + 1

    order = ["case:concept:name", "event_id", "time:timestamp", "concept:name"]
    other_cols = [c for c in df_merged.columns if c not in order]
    df_merged = df_merged[order + other_cols]

    return df_merged


def compoundEvents(csvInput, csvOutput, xesOutput, compound_conf=None):
    # Initialize configuration
    if compound_conf is None:
        compound_conf = {}


    sep = compound_conf.get("csv_separator", ";")

    input_case = compound_conf.get("case_column", "case_id")
    input_time = compound_conf.get("timestamp_column", "timestamp")
    input_act  = compound_conf.get("activity_column", "activity")
    
    target_columns = compound_conf.get("columns", [])

    print(f"[COMPOUND] Reading {csvInput}...")

    try:
        df = pd.read_csv(
            csvInput,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_values=["nan", "NaN", ""]
        )
    except FileNotFoundError:
        print(f"[ERROR] File not found: {csvInput}")
        return

    df = df.fillna("")


    rename_map = {
        input_case: "case:concept:name",
        input_time: "time:timestamp",
        input_act:  "concept:name"
    }
    
    actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=actual_rename)

    required = ["case:concept:name", "time:timestamp", "concept:name"]
    if not all(col in df.columns for col in required):
        print(f"[ERROR] Missing columns after renaming. Expected: {required}. Found: {df.columns.tolist()}")
        print(f"       Check the 'compound' section in config.yaml.")
        return


    manual_cols_param = target_columns if target_columns else None
    
    df_merged = merge_generic_events(df, manual_cols=manual_cols_param)


    df_merged.to_csv(csvOutput, sep=sep, index=False)
    print(f"[COMPOUND] CSV saved to: {csvOutput}")


    print("[COMPOUND] Converting to XES...")
    df_xes = df_merged.copy()
    try:
        df_xes = dataframe_utils.convert_timestamp_columns_in_df(df_xes)
        
        # log_converter richiede esplicitamente quale colonna è il Case ID
        log = log_converter.apply(df_xes, parameters={
            log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: "case:concept:name"
        })

        xes_exporter.apply(log, xesOutput)
        print(f"[COMPOUND] XES saved to: {xesOutput}")
    except Exception as e:
        print(f"[ERROR] XES generation failed: {e}")

    return df_merged