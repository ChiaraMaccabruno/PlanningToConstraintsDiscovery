import csv
import re
import os
from pathlib import Path

# Pulisce la stringa da parentesi e apici
def clean_field(s):
    if not s:
        return ""
    s = s.replace("[", "").replace("]", "").replace("'", "").strip()
    if len(s) == 1 and s.lower() in "abcdefghijklmnopqrstuvwxyz":
        return "" 
    return s

# Mappa i template dichiarativi in sintassi PDDL/LTL
def map_constraint(template, activation, target):
    A = clean_field(activation)
    B = clean_field(target)

    if not A: return []
    binary_templates = ("Response", "Precedence", "Succession", "RespondedExistence", "CoExistence", "ChainResponse")
    if template in binary_templates and not B: return []

    # --- VINCOLI UNARI 
    if template in ("Existence", "AtLeast1", "AtLeastOnce"):
        # "sometime" corrisponde a ST nello pseudocodice 
        return [f"(sometime {A})"]

    if template == "AtMostOnce":
        # "at-most-once" corrisponde a AO nello pseudocodice
        return [f"(at-most-once {A})"]

    if template == "ExactlyOne":
        return [f"(sometime {A})", f"(at-most-once {A})"]

    if template == "Absence":
        # Absence è "always not A" 
        return [f"(always (not {A}))"]

    # --- VINCOLI BINARI ---
    if template == "Response":
        # Response(A,B) = se A, allora dopo B. Corrisponde a sometime-after
        return [f"(sometime-after {A} {B})"]

    if template == "Precedence":
        # Precedence(A,B) = se B, allora prima A. 
        # In PAC sometime-before vuole PRIMA il trigger (B) POI il necessario (A) 
        return [f"(sometime-before {A} {B})"] 

    if template == "Succession":
        # Succession = Response + Precedence
        return [f"(sometime-after {A} {B})", f"(sometime-before {B} {A})"]

    if template == "RespondedExistence":
        # PAC non ha RespondedExistence puro, usiamo Response come approssimazione
        return [f"(sometime-after {A} {B})"]

    if template == "CoExistence":
        return [f"(sometime-after {A} {B})", f"(sometime-after {B} {A})"]

    if template == "ChainResponse":
        # ChainResponse = immediately followed. Corrisponde a always-next
        return [f"(always-next {A} {B})"]

    return [f"; UNEXPRESSIBLE {template}"]

# Legge il CSV e genera la lista dei vincoli
def read_constraints_from_csv(csv_path):
    tc_list = []
    if not os.path.exists(csv_path):
        return []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=';', quotechar="'")
        for i, row in enumerate(reader):
            template_raw = row.get("Template")
            activation_raw = row.get("Activation")
            target_raw = row.get("Target")
            
            constraints = map_constraint(template_raw, activation_raw, target_raw)
            
            # Debug: Stampa solo i primi 5 vincoli generati
            if constraints and i < 5: 
                 print(f"    Riga {i}: {template_raw}({activation_raw}, {target_raw}) -> {constraints}")

            tc_list.extend(constraints)
            
    return tc_list

# Inserisce i nuovi vincoli nel file PDDL rimuovendo quelli vecchi
def insert_constraints_into_pddl(pddl_path, output_path, tc_list):
    with open(pddl_path, "r", encoding="utf-8") as f:
        text = f.read()


    # Cerca il goal per capire se è un file problema
    match = re.search(r'\(:goal', text, re.IGNORECASE)
    
    if not match:
        # Se non c'è il goal, è probabilmente un DOMINIO
        print(f"{pddl_path.name} è un dominio (niente :goal).")
        with open(output_path, "w", encoding="utf-8") as out:
            out.write(text)
        return

    # Se non ci sono nuovi vincoli, salva solo il file pulito
    if not tc_list:
        with open(output_path, "w", encoding="utf-8") as out:
            out.write(text)
        return

    # Formatta il nuovo blocco
    constraints_content = "\n        ".join(tc for tc in tc_list)
    constraints_block = f"(:constraints\n    (and\n        {constraints_content}\n    )\n)"

    text = re.sub(r"\(:constraints[\s\S]*?\)(?=\s*\(|$)", "", text, flags=re.IGNORECASE)

    last_paren = text.rfind(")")
    
    if last_paren != -1:
        # Inserisce PRIMA dell'ultima parentesi
        new_text = text[:last_paren] + "\n\n    " + constraints_block + "\n" + text[last_paren:]
    else:
        # Fallback estremo (file malformato senza parentesi finale?)
        new_text = text + "\n" + constraints_block

    with open(output_path, "w", encoding="utf-8") as out:
        out.write(new_text)

# Elabora tutti i file PDDL nella cartella
def batch_convert(csv_path, pddl_dir, output_dir):
    csv_path = Path(csv_path)
    pddl_dir = Path(pddl_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        print(f"CSV non trovato: {csv_path}")
        return

    print(f"Uso CSV: {csv_path.name}")
    tc_list = read_constraints_from_csv(csv_path)
    print(f"{len(tc_list)} vincoli generati.")

    pddl_files = list(pddl_dir.glob("*.pddl"))
    if not pddl_files:
        print(f"Nessun file .pddl trovato in {pddl_dir}")
        return

    for pddl_file in pddl_files:
        print(f"Check file {pddl_file.name}...")
        out_path = output_dir / pddl_file.name
        insert_constraints_into_pddl(pddl_file, out_path, tc_list)
        
    print("Finito.")

if __name__ == "__main__":
    batch_convert(
        csv_path="results/driverlog/run_1/minerful/event_log_DriverLog_minerful.csv",
        pddl_dir="DriverLog/",
        output_dir="problems_with_constraints/"
    )