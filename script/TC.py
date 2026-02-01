import csv
import re
import os
from pathlib import Path

# Removes unwanted characters like brackets and quotes, and strips leading/trailing whitespace
def clean_field(s):
    if not s:
        return ""
    s = s.replace("[", "").replace("]", "").replace("'", "").replace(" ", "_").strip()
    return s

# Converts declarative templates from CSV into PAC 
def map_constraint(template, activation, target):
    raw_A = clean_field(activation)
    raw_B = clean_field(target)

    A = raw_A if raw_A else raw_B
    B = raw_B

    if not A: return []
    binary_templates = ("Response", "Precedence", "Succession", "ExclusiveChoice", "ChainResponse")
    if template in binary_templates and not B: return []

    existence_match = re.match(r'Existence\((\d+)\)', template, re.IGNORECASE)
    if existence_match:
        n = int(existence_match.group(1))
        if n > 1:
            # Crea la sequenza A A A...
            repeated_A = " ".join([A] * n)
            return [f"(pattern {repeated_A})"]
        else:
            return [f"(sometime {A})"]

    # UNARY CONSTRAINTS
    # Event A must occur at least once in the process
    if template in ("Existence", "AtLeastOnce", "AtLeast1", "AtLeastOne", "Participation"):
        return [f"(sometime {A})"]

    # Event A must occur at most once
    if template in ("AtMostOnce", "AtMost1"):
        return [f"(at-most-once {A})"]

    # Event A must occur exactly once
    if template == "ExactlyOne":
        return [f"(sometime {A})", f"(at-most-once {A})"]

    #if template == "Absence":
    #    # Absence è "always not A" 
    #   return [f"(always (not {A}))"]

    # BINARY CONSTRAINTS
    # If a occurs, then b occurs after a
    if template == "Response":
        return [f"(sometime-after {A} {B})"]

    # b occurs only if preceded by a
    if template == "Precedence":
        return [f"(sometime-before {B} {A})"] 

    # a occurs if and only if it is followed by b
    if template == "Succession":
        # Succession = Response + Precedence
        return [f"(sometime-after {A} {B})", f"(sometime-before {B} {A})"]

    #if template == "RespondedExistence":
    #    return [f"(sometime-after {A} {B})"]

    #  If b occurs, then a occurs, and viceversa
    #if template == "CoExistence":
    #    return [f"(sometime-after {A} {B})", f"(sometime-after {B} {A})"]

    if template == "ExclusiveChoice":
        return [f"(sometime (or {A} {B}))", f"(at-most-once (or {A} {B}))"]

    # Each time a occurs, then b occurs immediately afterwards
    if template == "ChainResponse":
        return [f"(always-next {A} {B})"]

    # At least one of A or B must occur
    if template == "Choice":
        return [f"(sometime (or {A} {B}))"]

    return [f"UNEXPRESSIBLE {template}"]

def is_expressible(tc):
    return not tc.startswith("UNEXPRESSIBLE")

# Reads a CSV file containing declarative templates and generates a csv file with TC
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
            
            for con in constraints:
                if is_expressible(con):
                    tc_list.append({
                        "template": template_raw,
                        "activation": activation_raw,
                        "target": target_raw,
                        "tc": con
                    })
            
    return tc_list


def write_tc_csv(tc_list, output_csv):
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["template", "activation", "target", "tc"],
            delimiter=";"
        )
        writer.writeheader()
        writer.writerows(tc_list)

def write_tc_pddl(tc_list, output_pddl):
    with open(output_pddl, "w", encoding="utf-8") as f:
        f.write("(:constraints\n  (and\n")
        for c in tc_list:
            if is_expressible(c["tc"]):
                f.write(f"    {c['tc']}\n")
        f.write("  )\n)\n")

# Updates a PDDL file by inserting constraints, removing any existing (:constraints ...) blocks
def insert_constraints_into_pddl(pddl_path, output_path, tc_list):
    with open(pddl_path, "r", encoding="utf-8") as f:
        text = f.read()


    # Check if the file contains a goal (problem file) or is a domain
    match = re.search(r'\(:goal', text, re.IGNORECASE)
    
    if not match:
        # Domain file: no constraints to insert
        print(f"{pddl_path.name} is a domain (no :goal found).")
        with open(output_path, "w", encoding="utf-8") as out:
            out.write(text)
        return

    # If no new constraints, save original file
    if not tc_list:
        with open(output_path, "w", encoding="utf-8") as out:
            out.write(text)
        return

    # Build the new constraints block
    expressible_tc = [
        c for c in tc_list
        if is_expressible(c["tc"])
    ]

    constraints_content = "\n        ".join(c['tc'] for c in expressible_tc)
    constraints_block = f"(:constraints\n    (and\n        {constraints_content}\n    )\n)"

    # Remove existing (:constraints ...) blocks
    text = re.sub(r"\(:constraints[\s\S]*?\)(?=\s*\(|$)", "", text, flags=re.IGNORECASE)

    last_paren = text.rfind(")")
    
    if last_paren != -1:
        new_text = text[:last_paren] + "\n\n    " + constraints_block + "\n" + text[last_paren:]
    else:
        new_text = text + "\n" + constraints_block

    with open(output_path, "w", encoding="utf-8") as out:
        out.write(new_text)

# Process all PDDL files in a directory
def batch_convert(csv_path, pddl_dir, output_dir):
    csv_path = Path(csv_path)
    pddl_dir = Path(pddl_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return

    print(f"Using CSV: {csv_path.name}")
    tc_list = read_constraints_from_csv(csv_path)
    print(f"{len(tc_list)} constraints generated.")

    stem_name = csv_path.stem.replace("_minerful", "")
    if not stem_name.endswith("_tc"):
        stem_name += "_tc"
    
    tc_csv_output = output_dir / f"{stem_name}.csv"
    write_tc_csv(tc_list, tc_csv_output)
    print(f"Created constraint CSV file: {tc_csv_output.name}")

    #tc_pddl_output = output_dir / f"{stem_name}.pddl"
    #write_tc_pddl(tc_list, tc_pddl_output)
    #print(f"Created PDDL constraints file: {tc_pddl_output.name}")

    #pddl_files = list(pddl_dir.glob("*.pddl"))
    #if not pddl_files:
    #    print(f"No .pddl files found in {pddl_dir}")
    #    return

    #for pddl_file in pddl_files:
        #print(f"Check file {pddl_file.name}...")
    #    out_path = output_dir / pddl_file.name
    #    insert_constraints_into_pddl(pddl_file, out_path, tc_list)
        
    print("Finished processing.")

def apply_trajectory_constraints(csv_path, pddl_dir, output_dir):
    batch_convert(csv_path, pddl_dir, output_dir)
