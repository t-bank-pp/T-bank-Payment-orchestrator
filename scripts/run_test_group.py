"""Run one group, keep pytest's exit code, and fail acceptance on skipped tests."""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

from qa_groups import GROUPS, inventory

parser = argparse.ArgumentParser()
parser.add_argument("group", choices=GROUPS)
parser.add_argument("--collect-only", action="store_true")
args = parser.parse_args()
files = inventory()[args.group]
if not files:
    raise SystemExit("Empty test group")
Path("reports").mkdir(exist_ok=True)
command = [sys.executable, "-m", "pytest", "-q", *files]
if args.collect_only:
    raise SystemExit(subprocess.call([*command, "--collect-only"]))
report = Path("reports") / (args.group + ".xml")
command += ["--junitxml=" + str(report)]
with (Path("reports") / (args.group + ".log")).open("w") as log:
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        log.write(line)
    result = process.wait()
if result:
    raise SystemExit(result)
suites = list(ET.parse(report).getroot().iter("testsuite"))
if any(int(s.get("skipped", "0")) for s in suites):
    raise SystemExit("Acceptance requires zero skips; inspect the test environment")
if not sum(int(s.get("tests", "0")) for s in suites):
    raise SystemExit("Acceptance requires collected tests")
print(f"PASS: {GROUPS[args.group]} | commit {os.getenv('GITHUB_SHA', 'local')}")
