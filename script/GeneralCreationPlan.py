import subprocess
import os
import shutil
import time
import signal
import threading
from concurrent.futures import ProcessPoolExecutor


def translate_problem(domain_file, problem_file, base_output_dir, fast_downward_path):
    base = os.path.basename(problem_file).replace(".pddl", "")
    problem_dir = os.path.join(base_output_dir, base)
    os.makedirs(problem_dir, exist_ok=True)
    sas_file = os.path.join(problem_dir, "output.sas")

    if os.path.exists(sas_file):
        print(f"{base}: output.sas already exists.")
        return sas_file

    domain_abs = os.path.abspath(domain_file)
    problem_abs = os.path.abspath(problem_file)

    # Fast Downward translation command
    cmd = [
        fast_downward_path, "--translate",
        domain_abs, problem_abs,
        "--translate-memory-limit", "4096M"
    ]

    print(f"\nTranslating {problem_file}")
    result = subprocess.run(cmd, cwd=problem_dir)

    if result.returncode != 0 or not os.path.exists(sas_file):
        print(f"Translation error for {problem_file}")
        return None

    print(f"Translation completed: {sas_file}")
    return sas_file


def run_search_for_problem(
    problem_name,
    sas_file,
    base_output_dir,
    commands,
    fast_downward_path,
    run_alias,
    run_non_alias,
    time_limit_alias,
    time_limit_non_alias
):
    problem_dir = os.path.join(base_output_dir, problem_name)
    plan_dir = os.path.join(problem_dir, "plans")
    os.makedirs(plan_dir, exist_ok=True)

    print(f"\nStarting searches for {problem_name}\n")

    # Iterate over all search commands
    for idx, cmd_str in enumerate(commands, 1):
        is_alias = "--alias" in cmd_str

        # Skip commands depending on user settings
        if is_alias and not run_alias:
            print(f"[CMD {idx}] Alias skipped")
            continue
        if not is_alias and not run_non_alias:
            print(f"[CMD {idx}] Non-alias skipped")
            continue

        cmd_dir = os.path.join(problem_dir, f"cmd_{idx}")
        os.makedirs(cmd_dir, exist_ok=True)

        local_sas = os.path.join(cmd_dir, "output.sas")
        shutil.copy(sas_file, local_sas)

        # Build command differently for alias vs non-alias searches:
        # alias can use --overall-time-limit; non-alias uses older r.113 method
        if is_alias:
            cmd_parts = [
                fast_downward_path
            ] + cmd_str.split() + [
                "--overall-time-limit", time_limit_alias,
                "output.sas"
            ]
        else:
            cmd_parts = [
                fast_downward_path,
                "output.sas"
            ] + cmd_str.split()

        print(f"[{problem_name} / CMD {idx}] command: {' '.join(cmd_parts)}")

        proc = subprocess.Popen(
            cmd_parts,
            cwd=cmd_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            preexec_fn=os.setsid
        )

        # Thread for streaming output
        def stream_output(pipe):
            for line in iter(pipe.readline, ''):
                print(f"[{problem_name} / CMD {idx}] {line.strip()}")
            pipe.close()

        t = threading.Thread(target=stream_output, args=(proc.stdout,))
        t.daemon = True
        t.start()

        # Timeout management: stop and go to the next command after around 800s
        start_time = time.time()
        timeout = time_limit_non_alias if not is_alias else None

        while t.is_alive() and proc.poll() is None:
            if timeout and (time.time() - start_time > timeout):
                print(f"Timeout for {problem_name} CMD {idx}")
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                break
            time.sleep(1)

        t.join(timeout=5)
        proc.wait(timeout=5)

        # Move generated plans to the plan directory
        for entry in os.scandir(cmd_dir):
            if entry.name.startswith("sas_plan"):
                new_name = f"{problem_name}_cmd{idx}_{entry.name}"
                shutil.move(entry.path, os.path.join(plan_dir, new_name))
                print(f"Plan saved: {plan_dir}/{new_name}")

        shutil.rmtree(cmd_dir, ignore_errors=True)
        print(f"[{problem_name} / CMD {idx}] finished (exit {proc.returncode})")

    print(f"All searches for {problem_name} completed.\n")


def createPlans(
    domain,
    problems_dir,
    output_dir,
    fast_downward_path="fast-downward.py",
    commands=None,
    run_alias=True,
    run_non_alias=True,
    time_limit_alias="800s",
    time_limit_non_alias=800,
    max_workers=6
):

    if commands is None:
        commands = [
            '--search astar(hmax())',
            '--search astar(lmcut())',
            '--search eager_greedy([ff()])',
            '--search eager_greedy([add()])',
            '--search eager_wastar([add()],w=5)',
            '--search eager_wastar([ff()],w=5)',
            '--search lazy_greedy([ff()])',
            '--search lazy_greedy([cg()])',
            '--search lazy_wastar([ff()],w=5)',
            '--search lazy_wastar([cg()],w=5)',
            '--search ehc(ff())',
            '--alias lama',
            '--alias seq-sat-lama-2011',
            '--alias seq-sat-fdss-2023',
            '--alias seq-sat-fdss-2018',
            '--alias seq-sat-fd-autotune-1',
            '--alias seq-sat-fd-autotune-2',
        ]

    os.makedirs(output_dir, exist_ok=True)

    # List all PDDL problem files excluding domain files
    problems = [
        os.path.join(problems_dir, f)
        for f in os.listdir(problems_dir)
        if f.endswith(".pddl") and "domain" not in f.lower()
    ]
    problems.sort()

    # Translate all problems to SAS
    sas_files = {}
    for prob in problems:
        sas = translate_problem(domain, prob, output_dir, fast_downward_path)
        if sas:
            name = os.path.basename(prob).replace(".pddl", "")
            sas_files[name] = sas

    # Parallel execution of searches using ProcessPoolExecutor
    print(f"\nStarting parallel searches on {len(sas_files)} problems ({max_workers} workers)...")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for name, sas in sas_files.items():
            futures.append(executor.submit(
                run_search_for_problem,
                name,
                sas,
                output_dir,
                commands,
                fast_downward_path,
                run_alias,
                run_non_alias,
                time_limit_alias,
                time_limit_non_alias
            ))

        for f in futures:
            f.result()

    print("\nAll executions completed!")
