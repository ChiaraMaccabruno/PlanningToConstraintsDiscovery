import xml.etree.ElementTree as ET
import subprocess
import os
import pandas as pd

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

    # Sostituisci tutti i NaN nei float/int con stringa vuota
    #for elem in root.iter():
        # controlla solo float e int
   #     if elem.tag.endswith('float') or elem.tag.endswith('int'):
    #        val = elem.get('value')
    #        if val is None or val.lower() == 'nan':
                # se è NaN o vuoto, sostituisci con stringa vuota
    #            elem.set('value', '')
            # cambia il tag da float/int a string
    #        elem.tag = elem.tag.replace('float', 'string').replace('int', 'string')


    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"New XES file created with classifier: {output_file}")
    return output_file



def extraction(
    input_xes,
    output_xes_with_classifier,
    output_csv,
    output_json,
    input_csv,
    sep=";",
    # Flag to decide whether to add classifier to XES
    use_classifier = False,
    classifier_name = "activityClassifier",
    classifier_keys = "objective",
    # support threshold
    s = 0.06,
    # confidence threshold
    c = 1.0,
    # coverage threshold
    g = 0.06,
    # pruning strategy to remove redundancy
    prune1 = "hierarchyconflictredundancy",
): 
    if not os.path.exists(input_xes):
        raise FileNotFoundError(f"[Extraction ERROR] Input XES not found: {input_xes}")  
    else:
        print(f"{input_xes}")  

    cleaned_xes = os.path.join("minerful", os.path.basename(input_xes).replace(".xes", "_cleaned.xes"))
    print("c")
    clean_numeric_fields_in_xes(input_xes, cleaned_xes)
    print("d")

    if use_classifier:
        # If classifier is enabled, create a new XES with classifier
        final_xes = output_xes_with_classifier
        add_classifier_to_xes(cleaned_xes, final_xes, classifier_name, classifier_keys)
        # Set flags for MINERful to use the classifier
        classifier_flag = ["-iLClassif", "logspec"]
    else:
        final_xes = cleaned_xes
        print("a")
        # If classifier is not used, keep the original XES
        #final_xes = input_xes
        # MINERful default behavior: no classifier
        classifier_flag = [] 
        print("b")

    # directory of this file (script/)
    script_dir = os.path.dirname(__file__)
    print("e")

    # go to directory progetto/
    project_dir = os.path.dirname(script_dir)
    print("f")

    # path script MINERful
    minerful_dir = os.path.join(project_dir, "MINERful")
    print("g")

    if not os.path.exists(os.path.join(minerful_dir, "MINERful.jar")):
        raise FileNotFoundError("MINERful.jar non trovato nella cartella MINERful")


    df = pd.read_csv(input_csv, sep=sep, dtype=str, keep_default_na=False)
    df = df.fillna("")
    print("h")

    classpath = os.path.join(minerful_dir, "MINERful.jar") + ":" + \
            os.path.join(minerful_dir, "lib/*")

    print("CLASSPATH =", classpath)

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print("i")

    output_dir = os.path.join(project_dir, "minerful")
    os.makedirs(output_dir, exist_ok=True)

    output_csv = os.path.join(project_dir, "minerful", "minerful_output_rovers.csv")
    print("j")
    output_json = os.path.join(project_dir, "minerful", "minerful_output_rovers.json")
    print("k")

    cmd = [
        "java",
        "-Xmx8096m",
        "-cp", classpath,
        "minerful.MinerFulMinerStarter",
        "-iLF", os.path.abspath(final_xes),
        "-s", str(s),
        "-c", str(c),
        "-g", str(g),
        "-prune", prune1,
        "-oCSV", output_csv,
        "-oJSON", output_json
    ] + classifier_flag
    
    print("Running MINERful with command:", " ".join(cmd))
    result = subprocess.run(cmd, cwd=minerful_dir, capture_output=True, text=True)

    print("Result:")
    print(result.stdout)
    print("Result:")
    print(result.stderr)

    print(f"Completed. CSV: {output_csv}  JSON: {output_json}")
    print(f"Completed. CSV: {os.path.abspath(output_csv)}  JSON: {os.path.abspath(output_json)}")

    return output_csv, output_json
