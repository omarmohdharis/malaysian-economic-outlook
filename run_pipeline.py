"""
Local pipeline runner — runs the full stack in sequence.
Use this to test the pipeline on your machine before pushing.

Usage:
    python run_pipeline.py            # full run
    python run_pipeline.py --extract  # extraction only
    python run_pipeline.py --cdc      # CDC only
    python run_pipeline.py --dbt      # dbt only
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def run(cmd: list[str], cwd: Path = ROOT) -> None:
    print(f"\n>> {' '.join(cmd)}  (cwd: {cwd})\n")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"\nFAILED Command failed: {' '.join(cmd)}", file=sys.stderr)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--extract", action="store_true")
    parser.add_argument("--cdc",     action="store_true")
    parser.add_argument("--dbt",     action="store_true")
    args = parser.parse_args()

    run_all = not any([args.extract, args.cdc, args.dbt])

    if run_all:
        # Restore CDC state first (no-op locally if the warehouse already has it)
        run([sys.executable, "state.py", "import"], cwd=ROOT / "ingestion")

    if run_all or args.extract:
        run([sys.executable, "extract.py"], cwd=ROOT / "ingestion")

    if run_all or args.cdc:
        run([sys.executable, "cdc.py"], cwd=ROOT / "ingestion")
        # Snapshot CDC state so it can be committed and restored by CI
        run([sys.executable, "state.py", "export"], cwd=ROOT / "ingestion")

    if run_all or args.dbt:
        run(["dbt", "run"],  cwd=ROOT / "transforms")
        run(["dbt", "test"], cwd=ROOT / "transforms")

    print("\nOK Pipeline complete.\n")


if __name__ == "__main__":
    main()
