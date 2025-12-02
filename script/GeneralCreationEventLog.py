import os
import re
import csv
from datetime import datetime, timedelta
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


def is_generic_name(name):
    return (len(name) <= 2 and name.isalpha()) or re.match(r"(obj|var|p)\d*", name)

# Parse PDDL domain to extract actions and their parameters
def parse_domain(domain_file):
    with open(domain_file, "r", encoding="utf-8") as f:
        text = f.read().lower()

    actions = {}

    pattern = r"\(:action\s+([\w-]+).*?:parameters\s*\((.*?)\)"
    for action, params_block in re.findall(pattern, text, re.DOTALL):
        params_block = params_block.strip()
        param_groups = re.findall(r"((?:\?\w+\s*)+)-\s*([\w-]+)", params_block)

        extracted_params = []
        for group, ptype in param_groups:
            names = re.findall(r"\?([\w-]+)", group)
            for name in names:
                # If parameter name is generic, rename it with its type
                if is_generic_name(name):
                    name = ptype.replace("-", "_")
                extracted_params.append((name, ptype))


        final_params = []

        # Count how many times each parameter name appears 
        count = {}
        for name, ptype in extracted_params:
            base = name
            count[base] = count.get(base, 0) + 1

        # Track occurrences to handle repeated parameters
        occurrence = {}
        for name, ptype in extracted_params:
            base = name
            occurrence[base] = occurrence.get(base, 0) + 1

            # If parameter occurs only once, keep its name as is
            if count[base] == 1:
                new_name = base
            else:
                # If multiple parameters have the same base, append _1, _2, ...
                new_name = f"{base}_{occurrence[base]}"

            final_params.append((new_name, ptype))


        actions[action] = final_params

    return actions


def parse_plan_line(line, actions_def):
    tokens = line.strip("() ").lower().split()
    activity, values = tokens[0], tokens[1:]

    if activity not in actions_def:
        raise ValueError(f"Action '{activity}' not found in the domain")

    expected_params = actions_def[activity]
    # Check if the number of parameters matches domain definition
    if len(values) != len(expected_params):
        raise ValueError(
            f"Parameter mismatch in '{activity}': expected {len(expected_params)}, "
            f"found {len(values)} → {values}"
        )

    data = {"activity": activity}
    for (name, _), val in zip(expected_params, values):
        data[name] = val

    return data



def generate_event_log(domain_file, root_plans_dir, output_csv, output_xes):
    print("\nParsing domain...")
    actions_def = parse_domain(domain_file)

    print("\nActions extracted:")
    for a, p in actions_def.items():
        print(f"  - {a}: {p}")

    rows = []
    timestamp = datetime(2025, 1, 1)
    event_id, case_id = 1, 1

    print("\nReading plans...")

    for root, dirs, files in os.walk(root_plans_dir):
        dirs.sort()
        for file in sorted(files):
            if "plan" not in file.lower():
                continue

            plan_path = os.path.join(root, file)
            print(f"  Found plan: {plan_path}  (case_id = plan_{case_id})")

            with open(plan_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith(";"):
                        continue

                    data = parse_plan_line(line, actions_def)
                    data.update({
                        "case_id": f"plan_{case_id}",
                        "event_id": event_id,
                        "timestamp": timestamp.isoformat()
                    })

                    rows.append(data)
                    event_id += 1
                    timestamp += timedelta(seconds=1)

            case_id += 1

            # If there exist columns base, base_1, base_2, ..., the value of base is copied in base_1 and base is dropped
            all_keys = set().union(*rows)

            for key in list(all_keys):
                base = key
                numbered_1 = f"{base}_1"

                if base in all_keys and numbered_1 in all_keys and "_" not in base:
                    for row in rows:
                        if base in row:
                            if numbered_1 not in row or not row[numbered_1]:
                                row[numbered_1] = row[base]
                            del row[base]


    order = ["case_id", "event_id", "timestamp", "activity"]
    other_cols = sorted(set().union(*rows) - set(order))
    fieldnames = order + other_cols

    print("\nWriting CSV...")
    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


    print(f"CSV generated: {output_csv}")
    print(f"Number of events: {len(rows)}")

    print("\nConverting to XES...")
    df = pd.read_csv(output_csv, sep=";", dtype=str, keep_default_na=False, na_values=["nan", "NaN", ""])
    df = df.fillna("")
    df = dataframe_utils.convert_timestamp_columns_in_df(df)

    df = df.rename(columns={
        "case_id": "case:concept:name",
        "activity": "concept:name",
        "timestamp": "time:timestamp"
    })

    log = log_converter.apply(df, parameters={
        log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: "case:concept:name"
    })

    xes_exporter.apply(log, output_xes)
    print(f"XES generated: {output_xes}")
    

def createEventLog(domainPath, planDirectory, csvOutput, xesOutput):
    return generate_event_log(
        domain_file=domainPath,
        root_plans_dir=planDirectory,
        output_csv=csvOutput,
        output_xes=xesOutput
    )


if __name__ == "__main__":
    generate_event_log(
        domain_file="DriverLog/driverlog.pddl",
        root_plans_dir="DriverLogPlans",
        output_csv="event_log3.csv",
        output_xes="event_log3.xes"
    )
