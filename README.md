# T

# T – Planning & Process Mining Pipeline

This repository contains an **integrated pipeline** for:

- generating **PDDL plans**,
- converting them into **Event Logs**,
- extracting **declarative constraints (Declare)** and **Trajectory Constraints (TC)**.

---

## Main Features

- **Plan Generation**  
  Solving PDDL planning problems using **Fast Downward**.

- **Event Log Creation**  
  Conversion of plans into **CSV** and **XES** event logs.

- **Data Processing**  
  Log cleaning, activity grounding, and creation of compound events.

- **Constraint Discovery**  
  Automatic discovery of declarative constraints using **MINERful**.

- **Trajectory Constraints (TC)**  
  Mapping of Declare constraints into Trajectory Constraints.

- **Reverse Mapping**  
  Reconstruction of Declare models from Trajectory Constraints.

---

## Requirements and Installation

### 1. Clone the Repository
```bash
git clone <repo-url>
```
```bash
cd <repository-folder>
```

### 2. Install External Dependencies

The project requires:
- Fast Downward (planning solver)

- MINERful (declarative constraint discovery)

Clone them into the project root:
# Planning solver
```bash
git clone https://github.com/aibasel/downward.git
```

# Constraint discovery
```bash
git clone https://github.com/Process-in-Chains/MINERful.git
```

### 3. Install Python Dependencies
```bash
pip install pandas pm4py pyyaml
```

### 4. Configuration Guide (`config.yaml`)

The `config.yaml` file is the only file that users need to modify in order to customize the system behavior.

### Main sections to configure:
- **Experiments**: Defines the domain file, the problems directory, and the number of repetitions.
- **Pipeline Options**: Allows enabling (`true`) or disabling (`false`) each individual stage of the pipeline.
- **Input Override**: Allows skipping initial stages by manually providing paths to existing CSV/XES files.
- **Event Log & Activity Mapping**: Defines the structure of the Event Log.
- **Grounding & Compound**: Configures columns aggregation and the merging of consecutive events.
- **Minerful**: Controls Support, Confidence, and Coverage thresholds to ensure the quality of the discovered constraints.

### 5. Execution and Results

To run the pipeline:
```bash
python main.py
```



