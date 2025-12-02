import os
import time
from script.GeneralCreationPlan import createPlans
from script.RemoveDuplicatePlans import removeDuplicatePlans
from script.GeneralCreationEventLog import createEventLog
from script.GeneralClean import puliziaEventLog
from script.GeneralGrounding import aggregateColumns
from script.GeneralCompoundEvents import compoundEvents
from script.GeneralExtraction import extraction

def unique_file(path):
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    
    i = 0
    new_path = path
    while os.path.exists(new_path):
        new_path = os.path.join(directory, f"{name}_{i}{ext}")
        i += 1

    return new_path


def pipeline(
    domain_file,
    problems_dir,
    plans_output_dir,
    eventlog_dir,
    cleaned_dir,
    grounded_dir,
    compound_dir,
    minerful_dir,
    columns_to_aggregate=None,
    create_compound=True,
    run_create_plans=False,
    run_remove_duplicates=False,
    run_event_log=True,
    run_cleaning=True,
    run_grounding=True,
    run_compound=True,
    run_minerful=True
):

#    plans_output_dir = unique_dir(plans_output_dir, problems_dir)+6
#    eventlog_dir = unique_dir(eventlog_dir, problems_dir)
#    cleaned_dir = unique_dir(cleaned_dir, problems_dir)
#    grounded_dir = unique_dir(grounded_dir, problems_dir)
#    compound_dir = unique_dir(compound_dir, problems_dir)
#    minerful_dir = unique_dir(minerful_dir, problems_dir)
    domain_name = os.path.basename(os.path.normpath(problems_dir)).lower()

    # Create output folders if they don't exist
    for d in [plans_output_dir, eventlog_dir, cleaned_dir, grounded_dir, compound_dir, minerful_dir]:
        os.makedirs(d, exist_ok=True)

    # Generation of the plans with Fast-downward
    if run_create_plans:
        start = time.perf_counter()
        print("\n1) PLAN GENERATION")
        createPlans(
            domain=domain_file,
            problems_dir=problems_dir,
            output_dir=plans_output_dir,
            fast_downward_path="/home/chiara_maccabruno/progetto/downward/fast-downward.py"
        )
        end = time.perf_counter()
        print(f"Time for plan generation: {end - start:.2f} seconds")
    else:
        print("Plans already generated, skipping this step")

        # Duplicate plan removal
    if run_remove_duplicates:
        print("\n1.1) DUPLICATE PLAN REMOVAL")
        removeDuplicatePlans(plans_output_dir, simulate=False)
    else:
        print("Duplicate plan removal disabled, skipping this step")


    # Generation of the Event Log
    event_csv = unique_file(os.path.join(eventlog_dir, f"event_log_{domain_name}.csv"))
    event_xes = unique_file(os.path.join(eventlog_dir, f"event_log_{domain_name}.xes"))

    if run_event_log:
        start = time.perf_counter()
        print("\n2)EVENT LOG")
        createEventLog(
            domainPath=domain_file,
            planDirectory=plans_output_dir,
            csvOutput=event_csv,
            xesOutput=event_xes
        )
        end = time.perf_counter()
        print(f"Time for event log generation: {end - start:.2f} seconds")
    else:
        print("Event log already created, skipping this step")

    # Cleaning of the Event Log
    cleaned_csv = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{domain_name}.csv"))
    cleaned_xes = unique_file(os.path.join(cleaned_dir, f"cleaned_event_log_{domain_name}.xes"))

    if run_cleaning:
        start = time.perf_counter()
        print("\n3) EVENT LOG CLEANING")
        puliziaEventLog(
            csvInput=event_csv,
            csvOutput=cleaned_csv,
            xesOutput=cleaned_xes
        )
        end = time.perf_counter()
        print(f"Time for cleaning: {end - start:.2f} seconds")
    else:
        print("Cleaning already done, skipping this step")

    # Column grounding
    grounded_csv = unique_file(os.path.join(grounded_dir, f"grounded_event_log_{domain_name}.csv"))
    grounded_xes = unique_file(os.path.join(grounded_dir, f"grounded_event_log_{domain_name}.xes"))

    if run_grounding:
        start = time.perf_counter()
        print("\n4)COLUMN AGGREGATION")
        if columns_to_aggregate:
            aggregateColumns(
                input_csv=cleaned_csv,
                output_csv=grounded_csv,
                output_xes=grounded_xes,
              # columns=columns_to_aggregate
            )
            #print(" Aggregated columns:", columns_to_aggregate)
        else:
            print(" No columns to aggregate, skipping")
            grounded_csv = cleaned_csv
            grounded_xes = cleaned_xes
        end = time.perf_counter()
        print(f"Time for column aggregation: {end - start:.2f} seconds")
    else:
        print("Grounding already done, skipping this step")
        grounded_csv = cleaned_csv
        grounded_xes = cleaned_xes

    compound_csv = unique_file(os.path.join(compound_dir, f"compound_event_log_{domain_name}.csv"))
    compound_xes = unique_file(os.path.join(compound_dir, f"compound_event_log_{domain_name}.xes"))


    # Compound events (OPTIONAL)
    if run_compound and create_compound:
        start = time.perf_counter()
        print("\n5)COMPOUND EVENTS")

        compoundEvents(
            csvInput=grounded_csv,
            csvOutput=compound_csv,
            xesOutput=compound_xes
        )
        final_input_xes = compound_xes
        final_input_csv = compound_csv
        end = time.perf_counter()
        print(f"Time for compound events: {end - start:.2f} seconds")
        print(" Compound events created")
    else:
        final_input_xes = grounded_xes
        final_input_csv = grounded_csv
        if not create_compound:
            print("Compound events disabled")
        else:
            print("Compound events already created, skipping this step")

    # MINERful
    result_json = unique_file(os.path.join(minerful_dir, f"minerful_output_{domain_name}.json"))
    classified_xes = unique_file(os.path.join(minerful_dir, f"classified_event_log_{domain_name}.xes"))
    minerful_csv = unique_file(os.path.join(minerful_dir, f"minerful_output_{domain_name}.csv"))


    if run_minerful:
        start = time.perf_counter()
        print("\n6)MINERFUL")
        extraction(
            input_xes=final_input_xes,
            output_xes_with_classifier=classified_xes,
            output_csv=minerful_csv,
            output_json=result_json,
            input_csv=final_input_csv,
            use_classifier=False
        )
        end = time.perf_counter()
        print(f"Time for MINERful: {end - start:.2f} seconds")
    else:
        print("MINERful already executed, skipping this step")

    print("\nPIPELINE COMPLETED")
    print("MINERful result:", result_json)


if __name__ == "__main__":
    pipeline(
        domain_file="Rovers/StripsRover.pddl",
        problems_dir="Rovers/",
        plans_output_dir="plans/",
        eventlog_dir="eventlog/",
        cleaned_dir="cleaned/",
        grounded_dir="grounded/",
        compound_dir="compound/",
        minerful_dir="minerful/",
        columns_to_aggregate=["agent", "location"],  
        create_compound=True,
        run_create_plans=False,
        run_remove_duplicates=False,
        run_event_log=True,
        run_cleaning=True,
        run_grounding=True,
        run_compound=True,
        run_minerful=True
    )
# ----- SISTEMARE TIMEOUT E FILE DA SALVARE ----------#
