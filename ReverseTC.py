import csv
from pathlib import Path
import re
from collections import defaultdict

# Mapping semplice one-to-one
TC_TO_DECLARE_SINGLE = {
    "sometime-after": "Response",
    "sometime-before": "Precedence",
    "sometime": "AtLeast1",
    "at-most-once": "AtMostOnce",
    "always-next": "ChainResponse",
}

def parse_tc(tc):
    tc = tc.strip()

    m = re.match(r"\(sometime-after ([^\s\)]+) ([^\s\)]+)\)", tc)
    if m:
        return {"type": "sometime-after", "A": m.group(1), "B": m.group(2)}

    m = re.match(r"\(sometime-before ([^\s\)]+) ([^\s\)]+)\)", tc)
    if m:
        return {"type": "sometime-before", "A": m.group(2), "B": m.group(1)}

    m = re.match(r"\(sometime ([^\s\)]+)\)", tc)
    if m:
        return {"type": "sometime", "A": m.group(1), "B": ""}

    m = re.match(r"\(at-most-once ([^\s\)]+)\)", tc)
    if m:
        return {"type": "at-most-once", "A": m.group(1), "B": ""}

    m = re.match(r"\(always-next ([^\s\)]+) ([^\s\)]+)\)", tc)
    if m:
        return {"type": "always-next", "A": m.group(1), "B": m.group(2)}

    m = re.match(r"\(sometime \(or ([^\s\)]+) ([^\s\)]+)\)\)", tc)
    if m:
        return {"type": "sometime-or", "A": m.group(1), "B": m.group(2)}

    m = re.match(r"\(at-most-once \(or ([^\s\)]+) ([^\s\)]+)\)\)", tc)
    if m:
        return {"type": "at-most-once-or", "A": m.group(1), "B": m.group(2)}

    return None

def tc_to_declare(tc_list):
    declare = []
    # Strutture per combinazioni
    unary = defaultdict(set)        # {A: set(["sometime","at-most-once"])}
    binary = defaultdict(set)       # {(A,B): set(["sometime-after","sometime-before"]) }
    binary_or = {}                  # {(A,B): {"sometime-or": True, "at-most-once-or": True}}

    # Prima pass: riempi le strutture
    for tc in tc_list:
        parsed = parse_tc(tc)
        if not parsed:
            continue

        t = parsed["type"]
        A = parsed["A"]
        B = parsed["B"]

        if t in {"sometime", "at-most-once"}:
            unary[A].add(t)
        elif t in {"sometime-after", "sometime-before", "always-next"}:
            binary[(A,B)].add(t)
        elif t in {"sometime-or", "at-most-once-or"}:
            key = tuple(sorted([A,B]))
            if key not in binary_or:
                binary_or[key] = {}
            binary_or[key][t] = True

    # Genera Declare vincoli singoli
    for (A,B), types in binary.items():
        if "sometime-after" in types and "sometime-before" in types:
            declare.append({"Template": "Succession", "Activation": A, "Target": B})
        elif "sometime-after" in types:
            declare.append({"Template": "Response", "Activation": A, "Target": B})
        elif "sometime-before" in types:
            declare.append({"Template": "Precedence", "Activation": A, "Target": B})
        elif "always-next" in types:
            declare.append({"Template": "ChainResponse", "Activation": A, "Target": B})

    for A, types in unary.items():
        if "sometime" in types and "at-most-once" in types:
            declare.append({"Template": "ExactlyOne", "Activation": A, "Target": ""})
        elif "sometime" in types:
            declare.append({"Template": "AtLeast1", "Activation": A, "Target": ""})
        elif "at-most-once" in types:
            declare.append({"Template": "AtMostOnce", "Activation": A, "Target": ""})

    for (A,B), types in binary_or.items():
        if "sometime-or" in types and "at-most-once-or" in types:
            declare.append({"Template": "ExclusiveChoice", "Activation": A, "Target": B})
        elif "sometime-or" in types:
            declare.append({"Template": "Choice", "Activation": A, "Target": B})

    return declare

def apply_reverse_mapping(tc_csv_path, output_dir):
    tc_csv_path = Path(tc_csv_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tc_list = []
    with open(tc_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if row.get("tc"):
                tc_list.append(row["tc"])

    declare_list = tc_to_declare(tc_list)

    output_csv = output_dir / f"recovered_declare_{tc_csv_path.stem}.csv"
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["DeclareConstraint", "Template", "Activation", "Target"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()

        for d in declare_list:
            if d["Target"]:
                constraint = f"{d['Template']}({d['Activation']}, {d['Target']})"
            else:
                constraint = f"{d['Template']}({d['Activation']})"
            d["DeclareConstraint"] = constraint
            writer.writerow(d)

    print(f"Reverse mapping completed: {output_csv.name}")
