import csv
import re
from pathlib import Path
from collections import defaultdict

def parse_tc(tc):
    tc = tc.strip()

    # sometime(or A B) -> Choice
    m = re.match(r"\(sometime \(or ([^\s\)]+) ([^\s\)]+)\)\)", tc)
    if m:
        return {"type": "choice", "A": m.group(1), "B": m.group(2)}

    # sometime-after A B -> Response
    m = re.match(r"\(sometime-after ([^\s\)]+) ([^\s\)]+)\)", tc)
    if m:
        return {"type": "after", "A": m.group(1), "B": m.group(2)}

    # sometime-before A B -> Precedence
    m = re.match(r"\(sometime-before ([^\s\)]+) ([^\s\)]+)\)", tc)
    if m:
        return {"type": "before", "A": m.group(1), "B": m.group(2)}

    # sometime A -> Existence
    m = re.match(r"\(sometime ([^\s\)]+)\)", tc)
    if m:
        return {"type": "sometime", "A": m.group(1)}

    # at-most-once A -> AtMostOnce
    m = re.match(r"\(at-most-once ([^\s\)]+)\)", tc)
    if m:
        return {"type": "atmostonce", "A": m.group(1)}

    return None


def tc_to_declare(tc_list):
    unary = defaultdict(list)            
    binary = defaultdict(set)            
    declare = []

    for tc in tc_list:
        parsed = parse_tc(tc)
        if not parsed:
            continue

        if parsed["type"] in {"sometime", "atmostonce"}:
            unary[parsed["A"]].append(parsed["type"])

        elif parsed["type"] == "choice":
            declare.append({
                "Template": "Choice",
                "Activation": parsed["A"],
                "Target": parsed["B"]
            })

        elif parsed["type"] in {"after", "before"}:
            binary[(parsed["A"], parsed["B"])].add(parsed["type"])

    # Applying Unary rules

    for A, types in unary.items():
        sometime_count = types.count("sometime")
        has_atmost = "atmostonce" in types

        # sometime(A) + at-most-once(A)  -> ExactlyOne(A)
        if sometime_count == 1 and has_atmost:
            declare.append({
                "Template": "ExactlyOne",
                "Activation": A,
                "Target": ""
            })

        # Multiple sometime(A) -> Existence(n, A)
        elif sometime_count > 1 and not has_atmost:
            declare.append({
                "Template": f"Existence({sometime_count})",
                "Activation": A,
                "Target": ""
            })
        # Single sometime(A) -> Existence(A)
        elif sometime_count == 1:
            declare.append({
                "Template": "Existence",
                "Activation": A,
                "Target": ""
            })
        # at-most-once(A) -> AtMostOnce(A)
        elif has_atmost:
            declare.append({
                "Template": "AtMostOnce",
                "Activation": A,
                "Target": ""
            })

    # Applying Binary rules

    for (A, B), types in binary.items():
        # sometime-after(A,B) + sometime-before(A,B) -> Succession(A,B)
        if "after" in types and "before" in types:
            declare.append({
                "Template": "Succession",
                "Activation": A,
                "Target": B
            })
        # sometime-after(A,B) only -> Response(A,B)
        elif "after" in types:
            declare.append({
                "Template": "Response",
                "Activation": A,
                "Target": B
            })
        # sometime-before(A,B) only -> Precedence(A,B)
        elif "before" in types:
            declare.append({
                "Template": "Precedence",
                "Activation": A,
                "Target": B
            })

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
            # Format the readable constraint string: Template(A, B) or Template(A)
            if d["Target"]:
                constraint = f"{d['Template']}({d['Activation']}, {d['Target']})"
            else:
                constraint = f"{d['Template']}({d['Activation']})"

            d["DeclareConstraint"] = constraint
            writer.writerow(d)

    print(f"Reverse mapping completed: {output_csv.name}")
