import pandas as pd
import re
from datetime import datetime, timedelta
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def aggregateColumns(
    input_csv,
    output_csv="output_event_log.csv",
    output_xes="output_event_log.xes",
#   columns,
    sep=";"
):
    
    df = pd.read_csv(input_csv, sep=sep, dtype=str, keep_default_na=False)

    df = df.fillna("")

    # Rename some columns for PM4Py/XES compatibility
    df = df.rename(columns={
        "case_id": "case:concept:name",
        "activity": "concept:name",
        "timestamp": "time:timestamp"
    })

    # Display columns available for aggregation
    print("\nColumns available in the file:")
    print(list(df.columns))


    coll = list(df.columns)

    columns_to_aggregate = []
    columns_to_exclude = {"case:concept:name", "time:timestamp", "event_id"}

    print("\nEnter the columns to aggregate (separated by commas)")
    print(f"Example: {coll[4]},{coll[5]}")

    # Ask user which columns to aggregate
    col_string = input("\nColumns to aggregate: ").strip()

    for c in col_string.split(","):
        col = c.strip()
        if col in df.columns:
            columns_to_aggregate.append(col)


    print("\nColumns chosen for aggregation:", columns_to_aggregate)


    # Generate new column by concatenating chosen columns, separated by "_"
    new_column_name = "_".join(columns_to_aggregate)

    df[new_column_name] = df.apply(
        lambda row: "_".join(
            [str(row[col]) for col in columns_to_aggregate if str(row[col]).strip() != ""]
        ),
        axis=1
    )

    # Insert the new column at the position of the first aggregated column
    first_col_index = df.columns.get_loc(columns_to_aggregate[0])

    df.insert(first_col_index, new_column_name, df.pop(new_column_name))

    # Ask user if they want to delete original columns after aggregation
    nodrop_columns = columns_to_exclude

    print("\nDo you want to remove the original columns after aggregation? (y/n))")
    delete_choice = input().lower().strip()

    if delete_choice == "y":
        columns_to_delete = [
            c for c in columns_to_aggregate 
            if c not in nodrop_columns
        ]
        df = df.drop(columns=columns_to_delete)

    if "concept:name" in columns_to_aggregate:
        df.rename(columns={new_column_name: "concept:name"}, inplace=True)


    order = ["case:concept:name", "event_id", "time:timestamp", "concept:name"]

    other_cols = [c for c in df.columns if c not in order and c != "concept:name"]

    df = df[order + other_cols]

    df.to_csv(output_csv, sep=sep, index=False, encoding="utf-8")
    print(f"\nCSV generato: {output_csv}")

    df = dataframe_utils.convert_timestamp_columns_in_df(df)
    log = log_converter.apply(df)
    xes_exporter.apply(log, output_xes)

    print(f"XES generato: {output_xes}")
    print("\nOperazione completata!")


#aggregateColumns("event_log.csv")
