#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd -P)
reference_dir=${WEEK4_REFERENCE_DIR:-}

case "$#" in
  0) ;;
  2)
    if [ "$1" != "--reference" ]; then
      printf 'Usage: %s --reference /path/to/week4-reference\n' "$0" >&2
      exit 2
    fi
    reference_dir=$2
    ;;
  *)
    printf 'Usage: %s --reference /path/to/week4-reference\n' "$0" >&2
    exit 2
    ;;
esac

if [ -z "$reference_dir" ]; then
  printf 'Set --reference or WEEK4_REFERENCE_DIR to the separate Week 4 checker checkout.\n' >&2
  exit 2
fi
if [ ! -f "$reference_dir/check.sh" ]; then
  printf 'Week 4 checker entrypoint is missing.\n' >&2
  exit 2
fi
reference_dir=$(CDPATH= cd "$reference_dir" && pwd -P)
cd "$script_dir"

report_path=$script_dir/week-4-public-report.json
if [ -e "$report_path" ] || [ -L "$report_path" ]; then
  printf 'Report path already exists; move it before running the checker: %s\n' "$report_path" >&2
  exit 2
fi
trap 'rm -f "$report_path"' 0

printf 'RUN | official Week 4 checker\n'
(cd "$reference_dir" && ./check.sh --repo "$script_dir")

printf 'RUN | Release build and host unit/static checks\n'
git diff --check
sh -n check.sh
dotnet build ModuleDev.sln --configuration Release
dotnet run --project tests/Workflow.Worker.LogicTests/Workflow.Worker.LogicTests.csproj --configuration Release
python3 -m unittest discover -s src/PythonPerimeter -p tests.py -v
python3 -B - <<'PY'
import ast
import json
from pathlib import Path
for path in sorted(Path('tests').glob('*.py')):
    ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
for directory in (Path('contracts/course-1'), Path('manifests')):
    for path in sorted(directory.glob('*.json')):
        json.loads(path.read_text(encoding='utf-8'))
print('PASS | Python syntax and contract/manifest JSON')
PY

for harness in \
  tests/week4_observability.py \
  tests/week4_sql_regression.py \
  tests/week4_stalled.py \
  tests/week4_trace.py \
  tests/week4_crash_matrix.py \
  tests/week3_e2e.py \
  tests/week2_regression.py \
  tests/signal_process_concurrency.py
do
  printf 'RUN | %s\n' "$harness"
  python3 "$harness"
done

printf 'PASS | official Week 4 checker, host checks, Week 4 harness, Week 2/3 regressions and signal concurrency\n'
