import xml.etree.ElementTree as ET
import subprocess
import os
import pandas as pd
from pathlib import Path

# Convertion of numeric attributes (float/int) to strings and replace 'NaN' or null values with an empty string
def clean_numeric_fields_in_xes(input_file, output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()

    ET.register_namespace('', "http://www.xes-standard.org/")
    ET.register_namespace('concept', "http://www.xes-standard.org/concept.xesext")

    for elem in root.iter():
        if elem.tag.endswith('float') or elem.tag.endswith('int'):
            val = elem.get('value')

            if val is None or val.lower() == 'nan':
                elem.set('value', '')

            elem.tag = elem.tag.replace('float', 'string').replace('int', 'string')

    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    return output_file

# Injects a classifier into the XES log
def add_classifier_to_xes(input_file, output_file, name, keys):
    # Parse the original XES file into an XML tree
    tree = ET.parse(input_file)
    # get root element (log)
    root = tree.getroot()

    ET.register_namespace('', "http://www.xes-standard.org/")
    ET.register_namespace('concept', "http://www.xes-standard.org/concept.xesext")
    
    # Create new classifier element
    classifier_elem = ET.Element("{http://www.xes-standard.org/}classifier")
    # set the classifier name attribute
    classifier_elem.set("name", name)
    # set the keys attribute
    classifier_elem.set("keys", keys)

    # Insert classifier as the first child of the root element
    root.insert(0, classifier_elem)

    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"New XES file created with classifier: {output_file}")
    return output_file



def extraction(
    input_xes,
    input_csv,
    output_xes_with_classifier,
    output_csv,
    output_json,
    minerful_conf
):

    # Input Validation
    if not os.path.exists(input_xes):
        raise FileNotFoundError(f"[Extraction ERROR] Input XES not found: {input_xes}")

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"[Extraction ERROR] Input CSV not found: {input_csv}")

    # Configuration Setup
    sep = minerful_conf.get("csv_separator", ";")
    use_classifier = minerful_conf.get("use_classifier", False)
    classifier_name = minerful_conf.get("classifier_name", "activityClassifier")
    classifier_keys = minerful_conf.get("classifier_keys", "objective")

    support = minerful_conf.get("support", 0.06)
    confidence = minerful_conf.get("confidence", 1.0)
    coverage = minerful_conf.get("coverage", 0.06)
    pruning = minerful_conf.get("pruning_strategy", "hierarchyconflictredundancy")

    xmx_memory = minerful_conf.get("xmx_memory", "4g")

    jar_path = minerful_conf.get("minerful_jar")
    lib_path = minerful_conf.get("minerful_lib")

    output_dir = minerful_conf.get("output_dir", "minerful")
    os.makedirs(output_dir, exist_ok=True)

    # XES Normalization
    cleaned_xes = os.path.join(
        output_dir,
        os.path.basename(input_xes).replace(".xes", "_cleaned.xes")
    )

    clean_numeric_fields_in_xes(input_xes, cleaned_xes)

    # If enabled, use the XES classifier to group events by custom keys
    if use_classifier:
        final_xes = output_xes_with_classifier
        add_classifier_to_xes(cleaned_xes, final_xes, classifier_name, classifier_keys)
        classifier_flag = ["-iLClassif", "logspec"]
    else:
        final_xes = cleaned_xes
        classifier_flag = []

    # Load CSV
    df = pd.read_csv(input_csv, sep=sep, dtype=str, keep_default_na=False)
    df = df.fillna("")

    # # Build the MINERful Java command to run the MINERful JAR file
    if not os.path.exists(jar_path):
        raise FileNotFoundError(f"MINERful.jar not found at: {jar_path}")

    classpath = f"{jar_path}:{lib_path}"

    cmd = [
        "java",
        f"-Xmx{xmx_memory}",
        "-cp", classpath,
        "minerful.MinerFulMinerStarter",
        "-iLF", os.path.abspath(final_xes),
        "-s", str(support),
        "-c", str(confidence),
        "-g", str(coverage),
        "-prune", pruning,
        "-oCSV", os.path.abspath(output_csv),
        "-oJSON", os.path.abspath(output_json),
    ] + classifier_flag

    print("Running MINERful:\n", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)

    print("\n--- MINERful stdout ---")
    print(result.stdout)
    print("\n--- MINERful stderr ---")
    print(result.stderr)

    print(f"\n[Extraction completed]\nCSV: {output_csv}\nJSON: {output_json}")

    return output_csv, output_json