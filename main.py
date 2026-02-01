import os
import time
import yaml
import shutil
import csv
from pathlib import Path

from script.GeneralCreationPlan import createPlans
from script.RemoveDuplicatePlans import removeDuplicatePlans
from script.GeneralCreationEventLog import createEventLog
from script.GeneralClean import puliziaEventLog
from script.GeneralGrounding import aggregateColumns
from script.GeneralCompoundEvents import compoundEvents
from script.GeneralExtraction import extraction

from script.TC import apply_trajectory_constraints
from script.ReverseTC import apply_reverse_mapping

# ----------------- Utility -----------------

# Creates a directory if it doesn't exist
def ensure_dir(d):
    os.makedirs(d, exist_ok=True)
    return d

# Generates a unique filename by appending an index if the file already exists
def unique_file(path):
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    i = 0
    new_path = path
    while os.path.exists(new_path):
        new_path = os.path.join(directory, f"{name}_{i}{ext}")
        i += 1
    return new_path

# Helper to validate if a file path is provided and exists
def file_exists_and_not_none(value):
    return value is not None and value != "" and os.path.exists(value)

# ----------------- Pipeline -----------------
def pipeline(config, exp, rep_index, base_output_dir):
    timings = {}
    t0 = time.perf_counter()

    # Create output subdirectories for each stage of the process
    plans_output_dir = ensure_dir(os.path.join(base_output_dir, "plans"))
    eventlog_dir = ensure_dir(os.path.join(base_output_dir, "eventlog"))
    cleaned_dir = ensure_dir(os.path.join(base_output_dir, "cleaned"))
    grounded_dir = ensure_dir(os.path.join(base_output_dir, "grounded"))
    compound_dir = ensure_dir(os.path.join(base_output_dir, "compound"))
    minerful_dir = ensure_dir(os.path.join(base_output_dir, "minerful"))

    fd_path = config.get("fast_downward", {}).get("path")
    override = exp.get("input_override", config.get("input_override", {}))

    domain_file = exp["domain_file"]
    problems_dir = exp["problems_dir"]

    pipeline_opts = exp.get("pipeline_options", {})

    # ----------------- PLAN GENERATION -----------------
    run_create_plans = pipeline_opts.get("run_create_plans", True)
    run_event_log = pipeline_opts.get("run_event_log", True)
    if run_create_plans or run_event_log:
        if run_create_plans:
            print("1) PLAN GENERATION")
            start = time.perf_counter()
            planning_conf = exp.get("planning", {})
            createPlans(domain_file, 
                        problems_dir, 
                        plans_output_dir, 
                        fast_downward_path=fd_path,
                        planning_conf=planning_conf
            )
            print(f"Time for plan generation: {time.perf_counter() - start:.2f} sec")
            elapsed = time.perf_counter() - start
            timings["plan_generation"] = elapsed
        elif file_exists_and_not_none(override.get("plans_dir")):
            plans_output_dir = override.get("plans_dir")
            print(f"Plan generation skipped; using override: {plans_output_dir}")
        else:
            raise RuntimeError("No plans available to continue pipeline.")
    else:
        plans_output_dir = None
        print("Plans not needed; skipping plan phase entirely.")

    # ----------------- DUPLICATE PLAN REMOVAL -----------------
    if pipeline_opts.get("run_remove_duplicates", False):
        print("1.1) DUPLICATE PLAN REMOVAL")
        start = time.perf_counter()
        removeDuplicatePlans(plans_output_dir)
        print(f"Time for duplicate removal: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["duplicate_removal"] = elapsed
    else:
        print("Duplicate removal skipped.")

    # ----------------- EVENT LOG -----------------
    run_event_log = pipeline_opts.get("run_event_log", True)
    event_csv = unique_file(os.path.join(eventlog_dir, f"event_log_{Path(problems_dir).name}.csv"))
    event_xes = unique_file(os.path.join(eventlog_dir, f"event_log_{Path(problems_dir).name}.xes"))

    if run_event_log:
        print("2) EVENT LOG")
        start = time.perf_counter()
        eventlog_conf = exp.get("eventlog", {})
        createEventLog(domain_file, 
                       plans_output_dir, 
                       event_csv, 
                       event_xes,
                       eventlog_conf=eventlog_conf)
        print(f"Time for event log: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["event_log"] = elapsed
    elif file_exists_and_not_none(override.get("event_log_csv")):
        event_csv = override.get("event_log_csv")
        event_xes = override.get("event_log_xes")
        print(f"Event log skipped; using override: {event_csv}")
    else:
        raise RuntimeError("No event log available.")

    # ----------------- CLEANING -----------------
    run_cleaning = pipeline_opts.get("run_cleaning", False)
    cleaned_csv = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{Path(problems_dir).name}.csv"))
    cleaned_xes = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{Path(problems_dir).name}.xes"))

    if run_cleaning:
        print("3) CLEANING")
        start = time.perf_counter()
        cleaning_conf = exp.get("cleaning", {})


        puliziaEventLog(
            csvInput=event_csv,
            csvOutput=cleaned_csv,
            xesOutput=cleaned_xes,
            cleaning_conf=cleaning_conf
        )
        print(f"Time for cleaning: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["cleaning"] = elapsed
    elif file_exists_and_not_none(override.get("cleaned_csv")):
        cleaned_csv = override.get("cleaned_csv")
        cleaned_xes = override.get("cleaned_xes")
        print(f"Cleaning skipped; using override: {cleaned_csv}")
    else:
        cleaned_csv = event_csv
        cleaned_xes = event_xes
        print("Cleaning disabled; using raw event log.")

    # ----------------- GROUNDING -----------------
    run_grounding = pipeline_opts.get("run_grounding", False)
    grounding_conf = exp.get("grounding", {})
    output_prefix_config = grounding_conf.get("output_prefix", "grounded_event_log")

    output_prefix = os.path.join(grounded_dir, output_prefix_config)

    grounded_csv_list = []
    grounded_xes_list = []

    grounded_csv = None 
    grounded_xes = None

    if run_grounding:
        print("4) GROUNDING")
        start = time.perf_counter()
        aggregateColumns(cleaned_csv, 
                         output_prefix,
                         grounding_conf=grounding_conf) 

        grounded_csv_list = sorted(str(p) for p in Path(grounded_dir).glob("*.csv"))
        grounded_xes_list = sorted(str(p) for p in Path(grounded_dir).glob("*.xes"))

        if not grounded_csv_list:
            print("No grounding files found, falling back to cleaned log.")
            grounded_csv_list = [cleaned_csv]
            grounded_xes_list = [cleaned_xes]

        grounded_csv = grounded_csv_list[0]
        grounded_xes = grounded_xes_list[0]

        print(f"Grounding generated {len(grounded_csv_list)} aggregations.")

        print(f"Time for grounding: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["grounding"] = elapsed
    elif file_exists_and_not_none(override.get("grounded_csv")):
        grounded_csv = override.get("grounded_csv")
        grounded_xes = override.get("grounded_xes")
        print(f"Grounding skipped; using override: {grounded_csv}")
    else:
        grounded_csv = cleaned_csv
        grounded_xes = cleaned_xes
        print("Grounding disabled; using cleaned event log.")

    # ----------------- COMPOUND -----------------
    run_compound = pipeline_opts.get("run_compound", False)

    compound_csv_list = []
    compound_xes_list = []

    if run_compound:
        print("5) COMPOUND")
        start = time.perf_counter()
        compound_conf = exp.get("compound", {})

        for g_csv, g_xes in zip(grounded_csv_list, grounded_xes_list):

            stem = Path(g_csv).stem.replace(".csv", "")
            out_csv = os.path.join(compound_dir, f"compound_{stem}.csv")
            out_xes = os.path.join(compound_dir, f"compound_{stem}.xes")

            compoundEvents(g_csv, out_csv, out_xes, compound_conf=compound_conf)

            compound_csv_list.append(out_csv)
            compound_xes_list.append(out_xes)

        print(f"Time for compound: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["compound"] = elapsed
        print(f"Compound generated for {len(compound_csv_list)} aggregations.")
    else:
        compound_csv_list = grounded_csv_list
        compound_xes_list = grounded_xes_list


    # ----------------- MINERful -----------------
    run_minerful = pipeline_opts.get("run_minerful", True)
    minerful_conf = exp.get("minerful", {})
    minerful_dir = ensure_dir(os.path.join(base_output_dir, "minerful"))

    explicit_file = minerful_conf.get("input_file")
    explicit_dir  = minerful_conf.get("input_directory")

    xes_files = []

    if explicit_file:
        if not os.path.isfile(explicit_file):
            raise FileNotFoundError(f"input_file not found: {explicit_file}")
        xes_files = [explicit_file]

    elif explicit_dir:
        if not os.path.isdir(explicit_dir):
            raise NotADirectoryError(f"input_directory not valid: {explicit_dir}")
        xes_files = sorted(
            str(p) for p in Path(explicit_dir).glob("*.xes")
        )
        if not xes_files:
            raise ValueError(f"No .xes files found in {explicit_dir}")

    else:
        if run_compound:
            xes_files = compound_xes_list

        elif run_grounding:
            xes_files = grounded_xes_list

        elif run_cleaning:
            xes_files = [cleaned_xes]

        else:
            xes_files = [event_xes]

    if not xes_files:
        raise ValueError(f"No .xes files found")

    print("Files that will be used for MINERful:")
    for f in xes_files:
        print("  -", f)

    minerful_csv = []
    minerful_json = []

    if run_minerful:
        print("6) MINERful")
        start = time.perf_counter()

        for input_xes in xes_files:

            input_csv = override.get("event_log_csv")  


            if not file_exists_and_not_none(input_csv):
                potential_csv = str(Path(input_xes).with_suffix(".csv"))
                
                if os.path.exists(potential_csv):
                    input_csv = potential_csv
                else:
                    print(f"[WARN] Specific CSV not found for {Path(input_xes).name}. Fallback back on {cleaned_csv}")
                    input_csv = cleaned_csv

            
            stem = Path(input_xes).stem

            output_xes_with_classifier = unique_file(
                os.path.join(minerful_dir, f"classified_{stem}.xes")
            )
            output_csv = unique_file(
                os.path.join(minerful_dir, f"{stem}{minerful_conf.get('output_csv_suffix', '_minerful.csv')}")
            )
            output_json = unique_file(
                os.path.join(minerful_dir, f"{stem}{minerful_conf.get('output_json_suffix', '_minerful.json')}")
            )

            csv_out, json_out = extraction(
                input_xes=input_xes,
                input_csv=input_csv,
                output_xes_with_classifier=output_xes_with_classifier,
                output_csv=output_csv,
                output_json=output_json,
                minerful_conf=minerful_conf
            )

            minerful_csv.append(csv_out)
            minerful_json.append(json_out)

        print(f"Time for MINERful: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["minerful"] = elapsed

    elif file_exists_and_not_none(override.get("minerful_dir")):
        minerful_dir = override.get("minerful_dir")
        print(f"MINERful skipped; using override: {minerful_dir}")
    else:
        print("MINERful skipped.")


    # ----------------- TRAJECTORY CONSTRAINTS -----------------
    tc_conf = exp.get("trajectory_constraints", {})
    run_tc = pipeline_opts.get("run_traj_constraint", False)

    if run_tc:
        print("7) TRAJECTORY CONSTRAINTS")
        start = time.perf_counter()

        explicit_file = tc_conf.get("input_file")
        explicit_dir  = tc_conf.get("input_directory")

        tc_csv_files = []

        if explicit_file:
            tc_csv_files = [explicit_file]

        elif explicit_dir:
            tc_csv_files = sorted(
                str(p) for p in Path(explicit_dir).glob("*.csv")
            )

        else:
            tc_csv_files = minerful_csv 

        if not tc_csv_files:
            raise ValueError("No CSV available for TC")

        base_tc_output_dir = ensure_dir(os.path.join(base_output_dir, "problems_constraints"))

        for csv_tc in tc_csv_files:
            stem = Path(csv_tc).stem.replace("_minerful", "")
            current_output_dir = ensure_dir(os.path.join(base_tc_output_dir, stem))

            print(f"Applying constraints from: {Path(csv_tc).name}")
            print(f"Output folder: {current_output_dir}")

            apply_trajectory_constraints(
                csv_path=csv_tc,
                pddl_dir=exp["problems_dir"],
                output_dir=current_output_dir
            )
        print(f"Time for trajectory constraints: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["trajectory_constraints"] = elapsed

    # ----------------- REVERSE MAPPING (NEW) -----------------
    rev_conf = exp.get("reverse_mapping", {})
    run_reverse = pipeline_opts.get("run_reverse_mapping", False)

    if run_reverse:
        print("8) REVERSE MAPPING (TC -> Declare)")
        start = time.perf_counter()
        
        explicit_file = rev_conf.get("input_file")
        reverse_output_dir = ensure_dir(os.path.join(base_output_dir, "reverse_mapping"))

        if explicit_file and os.path.exists(explicit_file):
            print(f"Using explicit input file: {explicit_file}")
            apply_reverse_mapping(
                tc_csv_path=explicit_file,
                output_dir=reverse_output_dir
            )
        
        else:
            base_tc_dir = Path(base_output_dir) / "problems_constraints"
            tc_files = list(base_tc_dir.rglob("*.csv"))
            
            for tc_csv in tc_files:
                if "recovered_declare" in tc_csv.name: continue
                apply_reverse_mapping(
                    tc_csv_path=tc_csv,
                    output_dir=reverse_output_dir
                )
        print(f"Time for reverse mapping: {time.perf_counter() - start:.2f} sec")
        elapsed = time.perf_counter() - start
        timings["reverse_mapping"] = elapsed

    

    #-----------------------------------------------


    print(f"Pipeline completed for run {rep_index}. Results in: {base_output_dir}\n")

    timing_file = os.path.join(base_output_dir, "timings.csv")

    with open(timing_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["phase", "time_seconds"])
        for phase, t in timings.items():
            writer.writerow([phase, t])
            
    return {
        "plans_dir": plans_output_dir,
        "event_csv": event_csv,
        "event_xes": event_xes,
        "cleaned_csv": cleaned_csv,
        "cleaned_xes": cleaned_xes,
        "grounded_csv": grounded_csv,
        "grounded_xes": grounded_xes,
        "compound_csv": compound_csv_list,
        "compound_xes": compound_xes_list,
        "minerful_csv": minerful_csv,
        "minerful_json": minerful_json
    }

# ----------------- Main -----------------
def main():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    base_results = config.get("output_dirs", {}).get("base_dir", "results/")
    ensure_dir(base_results)
    experiments = config.get("experiments", [])

    for exp in experiments:
        exp_name = exp.get("name", Path(exp.get("problems_dir", "unnamed")).name)
        repeat = int(exp.get("repeat", 1))
        for r in range(1, repeat + 1):
            run_dir = os.path.join(base_results, exp_name, f"run_{r}")
            ensure_dir(run_dir)
          
            shutil.copy("config.yaml", os.path.join(run_dir, "config_used.yaml"))
            print(f"Starting experiment: {exp_name} run {r}/{repeat}")
            try:
                pipeline(config=config, exp=exp, rep_index=r, base_output_dir=run_dir)
                print(f"Experiment {exp_name} run {r} finished successfully.\n")
            except Exception as e:
                print(f"Experiment {exp_name} run {r} failed: {str(e)}\nContinuing with next experiment.\n")
                continue

if __name__ == "__main__":
    main()
