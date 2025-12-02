import pandas as pd
import re
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def merge_generic_events(df, key_cols=None):
    df.columns = df.columns.str.strip().str.lower()
    
    if key_cols is None:
        key_cols = ["case:concept:name", "concept:name"]  

    # Identify columns with same base (e.g., col_1, col_2)
    numbered_cols = {}
    for col in df.columns:
        # Match columns ending with a number
        m = re.match(r"(.+?)[_]?(\d+)$", col)
        if m:
            base, idx = m.groups()
            numbered_cols.setdefault(base, []).append((int(idx), col))
    
    # Sort columns by number (e.g., col_1 before col_2)
    for base in numbered_cols:
        numbered_cols[base] = [col for _, col in sorted(numbered_cols[base])]
    
    # Keep only bases with max 2 numerated columns
    # Keep only bases with exactly 2 numerated columns
    numbered_cols = {base: cols for base, cols in numbered_cols.items() if len(cols) == 2}


    merged_rows = []

    for _, group in df.groupby(key_cols):
        group = group.reset_index(drop=True)
        used = set()  

        for i in range(len(group)):
            if i in used:
                continue
            chain = [i] 

            # Build chain of rows where col_(n+1) in current row = col_n in next row
            progress = True
            while progress:
                progress = False
                for j in range(i+1, len(group)):
                    if j in used or j in chain:
                        continue
                    
                    match_all = True
                    for base, cols in numbered_cols.items():
                        if len(cols) == 2:
                            if group.loc[chain[-1], cols[1]] != group.loc[j, cols[0]]:
                                match_all = False
                                break
                    if match_all:
                        chain.append(j)
                        progress = True
                        break

            # Create merged row using the first row of the chain 
            first, last = chain[0], chain[-1]
            row_data = {col: group.loc[first, col] for col in df.columns if col not in ["event_id", "timestamp"]}
            # keep timestamp of first event
            row_data["time:timestamp"] = group.loc[first, "time:timestamp"]  
            merged_rows.append(row_data)
            used.update(chain)  

    df_merged = pd.DataFrame(merged_rows)
    df_merged = df_merged.sort_values(by=key_cols + ["time:timestamp"]).reset_index(drop=True)
    # Assign sequential event IDs per case
    df_merged["event_id"] = df_merged.groupby(key_cols[0]).cumcount() + 1

    # Reorder columns
    order = ["case:concept:name", "event_id", "time:timestamp", "concept:name"]
    other_cols = [c for c in df_merged.columns if c not in order]
    df_merged = df_merged[order + other_cols]


    return df_merged


def compoundEvents(csvInput, csvOutput, xesOutput):
    df = pd.read_csv(
        csvInput,
        sep=';',
        dtype=str,
        keep_default_na=False,
        na_values=["nan", "NaN", ""]
    )
    df = df.fillna("")

    df_merged = merge_generic_events(df)

    df_merged.to_csv(csvOutput, sep=';', index=False)
    print(f"CSV file saved in: {csvOutput}")

    # Conversione per XES
    df_xes = df_merged.copy()
    df_xes = dataframe_utils.convert_timestamp_columns_in_df(df_xes)
    df_xes = df_xes.rename(columns={
        "case_id": "case:concept:name",
        "activity": "concept:name",
        "timestamp": "time:timestamp"
    })

    log = log_converter.apply(df_xes, parameters={
        log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: "case:concept:name"
    })

    xes_exporter.apply(log, xesOutput)
    print(f"XES file saved in: {xesOutput}")

    return df_merged
