import pandas as pd
import re
import os
from datetime import datetime, timedelta
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def aggregateColumns(
    input_csv,
    output_prefix,
    grounding_conf = None
):
    if grounding_conf is None:
        grounding_conf = {}

    # ---------------------------
    # LETTURA CONFIG DA YAML
    # ---------------------------
    sep = grounding_conf["csv_separator"]
    drop_original = grounding_conf["drop_original_columns"]
    plan_col = grounding_conf["plan_column"]
    timestamp_col = grounding_conf["timestamp_column"]
    activity_col = grounding_conf["activity_column"]
    aggregations = grounding_conf["aggregations"]

    # ---------------------------
    # CARICAMENTO CSV
    # ---------------------------
    original_df = pd.read_csv(input_csv, sep=sep, dtype=str, keep_default_na=False)
    original_df = original_df.fillna("")

    # ---------------------------
    # RINOMINA PER PM4PY
    # ---------------------------
    rename_map = {
        plan_col: "case:concept:name",
        activity_col: "concept:name",
        timestamp_col: "time:timestamp"
    }

    original_df = original_df.rename(columns=rename_map)

    # Aggiorna anche le aggregazioni
    for agg in aggregations:
        agg["columns"] = [rename_map.get(c, c) for c in agg["columns"]]

 

    # ---------------------------
    # ESECUZIONE DI TUTTE LE AGGREGAZIONI
    # ---------------------------
    for agg in aggregations:
        name = agg["name"]
        cols = agg["columns"]

        df = original_df.copy()

        missing = [c for c in cols if c not in df.columns]
        if missing:
            print(f"[WARNING] Aggregation '{name}' skipped. Missing columns: {missing}")
            continue

        print(f"[GROUNDING] Aggregating: {name} -> {cols}")

        if "concept:name" in cols:
            name = "concept:name"
        # Nuova colonna aggregata
        df[name] = df.apply(
            lambda row: "_".join(
                str(row[c]).strip() for c in cols if str(row[c]).strip() != ""
            ),
            axis=1
        )
        

        # Posizioniamo la nuova colonna vicino alla prima colonna aggregata
        first_idx = df.columns.get_loc(cols[0])
        col_values = df.pop(name)
        df.insert(first_idx, name, col_values)

        # Rimozione colonne originali (opzionale da YAML)
        if drop_original:
            for c in cols:
                if c not in ("case:concept:name", "time:timestamp", "event_id", "concept:name"):
                    if c in df.columns:
                        df.drop(columns=[c], inplace=True)


        # ---------------------------
        # ORDINAMENTO COLONNE: XES-FRIENDLY
        # ---------------------------
        order = ["case:concept:name", "event_id", "time:timestamp", "concept:name"]
        other_cols = [c for c in df.columns if c not in order]
        df = df[order + other_cols]

        base, ext = os.path.splitext(output_prefix)
        output_csv = f"{base}_{name}.csv"
        output_xes = f"{base}_{name}.xes"

        # ---------------------------
        # SALVATAGGIO CSV + XES
        # ---------------------------
        df.to_csv(output_csv, sep=sep, index=False, encoding="utf-8")
        print(f"[GROUNDING] CSV generato: {output_csv}")

        df = dataframe_utils.convert_timestamp_columns_in_df(df)
        log = log_converter.apply(df)
        xes_exporter.apply(log, output_xes)
        print(f"[GROUNDING] XES generato: {output_xes}")

    print("[GROUNDING] Operazione completata.")

