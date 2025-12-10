import pandas as pd
import re
import os
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def merge_generic_events(df, manual_cols=None):
    
    # Normalizziamo i nomi colonne 
    original_cols = df.columns.tolist()
    
    key_cols = ["case:concept:name", "concept:name"]
    

    numbered_cols = {}

    if manual_cols and len(manual_cols) >= 2:
        # CASO 1: L'utente ha specificato le colonne (Es. loc_from, loc_to)
        # Creiamo un gruppo "manual" che contiene esattamente queste colonne nell'ordine dato
        print(f"[COMPOUND] Utilizzo colonne manuali: {manual_cols}")
        numbered_cols["manual_group"] = manual_cols
    else:
        # CASO 2: Automatico (Regex)
        print("[COMPOUND] Rilevamento automatico colonne numerate (es. _1, _2)...")
        temp_numbered = {}
        for col in df.columns:
            # Cerca colonne che finiscono con un numero (es. waypoint_1, waypoint_2)
            m = re.match(r"(.+?)[_]?(\d+)$", col)
            if m:
                base, idx = m.groups()
                temp_numbered.setdefault(base, []).append((int(idx), col))
        
        # Ordina per indice e mantieni solo basi con esattamente 2 colonne (start -> end)
        for base in temp_numbered:
            sorted_cols = [c for _, c in sorted(temp_numbered[base])]
            if len(sorted_cols) == 2:
                numbered_cols[base] = sorted_cols

    if not numbered_cols:
        print("[COMPOUND] Nessuna colonna valida trovata per il compound. Restituisco il log originale.")
        return df


    merged_rows = []

    # Raggruppa per Caso e Attività
    for keys, group in df.groupby(key_cols):
        # Ordina per timestamp per garantire sequenzialità temporale
        group = group.sort_values("time:timestamp").reset_index(drop=True)
        used = set()

        for i in range(len(group)):
            if i in used:
                continue
            
            chain = [i]
            
            # Costruisce la catena: colonna[1] riga corrente == colonna[0] riga successiva
            progress = True
            while progress:
                progress = False
                # Cerca nel resto del gruppo un evento che 'combacia'
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
            

            # Prendi la prima riga della catena come base
            first_idx, last_idx = chain[0], chain[-1]
            
            row_data = group.loc[first_idx].to_dict()
            
            # Aggiorna le colonne di destinazione con quelle dell'ultimo evento della catena
            # Esempio: se ho fatto A->B, B->C, C->D. La riga finale deve essere A->D.
            # Quindi tengo 'from' del primo e prendo 'to' dell'ultimo.
            for base, cols in numbered_cols.items():
                dest_col = cols[1] # La colonna di destinazione
                row_data[dest_col] = group.loc[last_idx, dest_col]
            
            # Manteniamo il timestamp dell'inizio
            row_data["time:timestamp"] = group.loc[first_idx, "time:timestamp"]
            
            merged_rows.append(row_data)
            used.update(chain)

    # Ricostruzione DataFrame
    df_merged = pd.DataFrame(merged_rows)
    
    if df_merged.empty:
         print("[WARNING] Il compound ha prodotto un dataframe vuoto. Controllare i dati.")
         return df

    # Ordinamento finale
    sort_cols = key_cols + ["time:timestamp"]
    df_merged = df_merged.sort_values(by=sort_cols).reset_index(drop=True)
    
    # Rigenerazione event_id sequenziale
    df_merged["event_id"] = df_merged.groupby(key_cols[0]).cumcount() + 1

    # Riordino colonne pulito
    order = ["case:concept:name", "event_id", "time:timestamp", "concept:name"]
    other_cols = [c for c in df_merged.columns if c not in order]
    df_merged = df_merged[order + other_cols]

    return df_merged


def compoundEvents(csvInput, csvOutput, xesOutput, compound_conf=None):
    if compound_conf is None:
        compound_conf = {}


    sep = compound_conf.get("csv_separator", ";")
    

    input_case = compound_conf.get("case_column", "case_id")
    input_time = compound_conf.get("timestamp_column", "timestamp")
    input_act  = compound_conf.get("activity_column", "activity")
    
    # Colonne su cui fare compound (opzionale)
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
        print(f"[ERROR] File non trovato: {csvInput}")
        return

    df = df.fillna("")


    rename_map = {
        input_case: "case:concept:name",
        input_time: "time:timestamp",
        input_act:  "concept:name"
    }
    
    # Rinomina solo se le colonne esistono (per evitare key error se il file è già processato)
    actual_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=actual_rename)

    # Verifica colonne essenziali
    required = ["case:concept:name", "time:timestamp", "concept:name"]
    if not all(col in df.columns for col in required):
        print(f"[ERROR] Colonne mancanti dopo la rinomina. Attese: {required}. Trovate: {df.columns.tolist()}")
        print(f"       Controllare 'compound' section nel config.yaml.")
        return


    manual_cols_param = target_columns if target_columns else None
    
    df_merged = merge_generic_events(df, manual_cols=manual_cols_param)


    df_merged.to_csv(csvOutput, sep=sep, index=False)
    print(f"[COMPOUND] CSV salvato in: {csvOutput}")


    print("[COMPOUND] Conversione in XES...")
    df_xes = df_merged.copy()
    try:
        df_xes = dataframe_utils.convert_timestamp_columns_in_df(df_xes)
        
        # log_converter richiede esplicitamente quale colonna è il Case ID
        log = log_converter.apply(df_xes, parameters={
            log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: "case:concept:name"
        })

        xes_exporter.apply(log, xesOutput)
        print(f"[COMPOUND] XES salvato in: {xesOutput}")
    except Exception as e:
        print(f"[ERROR] Fallita generazione XES: {e}")

    return df_merged