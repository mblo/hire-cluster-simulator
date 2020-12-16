#!/usr/bin/env bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

PLOTS=(
  "evaluate_paper_cluster_load.py"
  "evaluate_paper_inc_success.py"
  "evaluate_paper_inc_success_tg.py"
  "evaluate_paper_latency_cdf.py"
  "evaluate_paper_locality.py"
  "evaluate_paper_solver.py"
)

for plot in ${PLOTS[@]}; do
  echo "start $plot"
  python3 "$DIR/$plot" "$@" >"./$plot.out" 2>"./$plot.err"
done

wait
