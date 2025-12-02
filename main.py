import os
import time
import yaml
from pathlib import Path

from script.GeneralCreationPlan import createPlans
from script.RemoveDuplicatePlans import removeDuplicatePlans
from script.GeneralCreationEventLog import createEventLog
from script.GeneralClean import puliziaEventLog
from script.GeneralGrounding import aggregateColumns
from script.GeneralCompoundEvents import compoundEvents
from script.GeneralExtraction import extraction

# ----------------- Utility -----------------
def ensure_dir(d):
    os.makedirs(d, exist_ok=True)
    return d

def unique_file(path):
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    i = 0
    new_path = path
    while os.path.exists(new_path):
        new_path = os.path.join(directory, f"{name}_{i}{ext}")
        i += 1
    return new_path

def file_exists_and_not_none(value):
    return value is not None and value != "" and os.path.exists(value)

# ----------------- Pipeline -----------------
def pipeline(config, exp, rep_index, base_output_dir):
    t0 = time.perf_counter()

    # Creazione cartelle output
    plans_output_dir = ensure_dir(os.path.join(base_output_dir, "plans"))
    eventlog_dir = ensure_dir(os.path.join(base_output_dir, "eventlog"))
    cleaned_dir = ensure_dir(os.path.join(base_output_dir, "cleaned"))
    grounded_dir = ensure_dir(os.path.join(base_output_dir, "grounded"))
    compound_dir = ensure_dir(os.path.join(base_output_dir, "compound"))
    minerful_dir = ensure_dir(os.path.join(base_output_dir, "minerful"))

    fd_path = config.get("fast_downward", {}).get("path")
    override = config.get("input_override", {})

    domain_file = exp["domain_file"]
    problems_dir = exp["problems_dir"]

    # ----------------- PLAN GENERATION -----------------
    run_create_plans = config["pipeline_options"].get("run_create_plans", True)
    if run_create_plans:
        print("1) PLAN GENERATION")
        start = time.perf_counter()
        createPlans(domain_file, problems_dir, plans_output_dir, fast_downward_path=fd_path)
        print(f"Time for plan generation: {time.perf_counter() - start:.2f} sec")
    elif file_exists_and_not_none(override.get("plans_dir")):
        plans_output_dir = override.get("plans_dir")
        print(f"Plan generation skipped; using override: {plans_output_dir}")
    else:
        raise RuntimeError("No plans available to continue pipeline.")

    # ----------------- DUPLICATE PLAN REMOVAL -----------------
    if config["pipeline_options"].get("run_remove_duplicates", False):
        print("1.1) DUPLICATE PLAN REMOVAL")
        removeDuplicatePlans(plans_output_dir, simulate=False)
    else:
        print("Duplicate removal skipped.")

    # ----------------- EVENT LOG -----------------
    run_event_log = config["pipeline_options"].get("run_event_log", True)
    event_csv = unique_file(os.path.join(eventlog_dir, f"event_log_{Path(problems_dir).name}.csv"))
    event_xes = unique_file(os.path.join(eventlog_dir, f"event_log_{Path(problems_dir).name}.xes"))

    if run_event_log:
        print("2) EVENT LOG")
        start = time.perf_counter()
        createEventLog(domain_file, plans_output_dir, event_csv, event_xes)
        print(f"Time for event log: {time.perf_counter() - start:.2f} sec")
    elif file_exists_and_not_none(override.get("event_log_csv")):
        event_csv = override.get("event_log_csv")
        event_xes = override.get("event_log_xes")
        print(f"Event log skipped; using override: {event_csv}")
    else:
        raise RuntimeError("No event log available.")

    # ----------------- CLEANING -----------------
    run_cleaning = config["pipeline_options"].get("run_cleaning", False)
    cleaned_csv = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{Path(problems_dir).name}.csv"))
    cleaned_xes = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{Path(problems_dir).name}.xes"))

    if run_cleaning:
        print("3) CLEANING")
        start = time.perf_counter()
        puliziaEventLog(event_csv, cleaned_csv, cleaned_xes)
        print(f"Time for cleaning: {time.perf_counter() - start:.2f} sec")
    elif file_exists_and_not_none(override.get("cleaned_csv")):
        cleaned_csv = override.get("cleaned_csv")
        cleaned_xes = override.get("cleaned_xes")
        print(f"Cleaning skipped; using override: {cleaned_csv}")
    else:
        cleaned_csv = event_csv
        cleaned_xes = event_xes
        print("Cleaning disabled; using raw event log.")

    # ----------------- GROUNDING -----------------
    run_grounding = config["pipeline_options"].get("run_grounding", False)
    grounded_csv = unique_file(os.path.join(grounded_dir, f"grounded_event_log_{Path(problems_dir).name}.csv"))
    grounded_xes = unique_file(os.path.join(grounded_dir, f"grounded_event_log_{Path(problems_dir).name}.xes"))

    if run_grounding:
        print("4) GROUNDING")
        start = time.perf_counter()
        cols = config.get("grounding", {}).get("columns") if config.get("grounding", {}).get("enabled") else None
        aggregateColumns(cleaned_csv, grounded_csv, grounded_xes)  # aggiungere cols se necessario
        print(f"Time for grounding: {time.perf_counter() - start:.2f} sec")
    elif file_exists_and_not_none(override.get("grounded_csv")):
        grounded_csv = override.get("grounded_csv")
        grounded_xes = override.get("grounded_xes")
        print(f"Grounding skipped; using override: {grounded_csv}")
    else:
        grounded_csv = cleaned_csv
        grounded_xes = cleaned_xes

    # ----------------- COMPOUND -----------------
    run_compound = config["pipeline_options"].get("run_compound", False)
    compound_csv = unique_file(os.path.join(compound_dir, f"compound_event_log_{Path(problems_dir).name}.csv"))
    compound_xes = unique_file(os.path.join(compound_dir, f"compound_event_log_{Path(problems_dir).name}.xes"))

    if run_compound:
        print("5) COMPOUND")
        start = time.perf_counter()
        compoundEvents(grounded_csv, compound_csv, compound_xes)
        print(f"Time for compound: {time.perf_counter() - start:.2f} sec")
    elif file_exists_and_not_none(override.get("compound_csv")):
        compound_csv = override.get("compound_csv")
        compound_xes = override.get("compound_xes")
        print(f"Compound skipped; using override: {compound_csv}")
    else:
        compound_csv = grounded_csv
        compound_xes = grounded_xes

    # ----------------- MINERful -----------------
    run_minerful = config["pipeline_options"].get("run_minerful", True)
    minerful_csv = unique_file(os.path.join(minerful_dir, f"minerful_output_{Path(problems_dir).name}.csv"))
    result_json = unique_file(os.path.join(minerful_dir, f"minerful_output_{Path(problems_dir).name}.json"))
    classified_xes = unique_file(os.path.join(minerful_dir, f"classified_event_log_{Path(problems_dir).name}.xes"))

    if run_minerful:
        print("6) MINERful")
        start = time.perf_counter()
        minerful_conf = config.get("minerful", {})
        extraction(
            input_xes=compound_xes,
            output_xes_with_classifier=classified_xes,
            output_csv=minerful_csv,
            output_json=result_json,
            input_csv=compound_csv,
            use_classifier=minerful_conf.get("use_classifier", False)
        )
        print(f"Time for MINERful: {time.perf_counter() - start:.2f} sec")
    else:
        print("MINERful skipped.")

    print(f"Pipeline completed for run {rep_index}. Results in: {base_output_dir}\n")
    return {
        "plans_dir": plans_output_dir,
        "event_csv": event_csv,
        "event_xes": event_xes,
        "cleaned_csv": cleaned_csv,
        "cleaned_xes": cleaned_xes,
        "grounded_csv": grounded_csv,
        "grounded_xes": grounded_xes,
        "compound_csv": compound_csv,
        "compound_xes": compound_xes,
        "minerful_csv": minerful_csv,
        "minerful_json": result_json
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
            print(f"Starting experiment: {exp_name} run {r}/{repeat}")
            try:
                pipeline(config=config, exp=exp, rep_index=r, base_output_dir=run_dir)
                print(f"Experiment {exp_name} run {r} finished successfully.\n")
            except Exception as e:
                print(f"Experiment {exp_name} run {r} failed: {str(e)}\nContinuing with next experiment.\n")
                continue

if __name__ == "__main__":
    main()
