#!/usr/bin/env bash


ALIBABA_DIR="traces/alibaba_trace_2018"
mkdir -p "$ALIBABA_DIR"
if [[ ! -f "$ALIBABA_DIR/hire_workload_jobs.csv" ]]; then
    echo "process alibaba 2018 traces"
    if [[ ! -f "$ALIBABA_DIR/batch_task.csv" || ! -f "$ALIBABA_DIR/container_meta.csv" || ! -f "$ALIBABA_DIR/machine_meta.csv" ]]; then
      echo "missing alibaba cluster trace (machine_meta.csv, container_meta.csv, batch_task.csv),
      please download from https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/trace_2018.md
      to $ALIBABA_DIR/batch_task.csv ... "
      exit 1
    fi

    python3 src/main/python/workload/traceAlibaba.py  --parse-machines --parse-jobs
    echo "expected MD5 (traces/alibaba_trace_2018/hire_cluster.csv) = 3bf468ad14564be34bd52aaae0384c62"
    md5sum "$ALIBABA_DIR/hire_cluster.csv"
    echo "expected MD5 (traces/alibaba_trace_2018/hire_workload_jobs.csv) = 4a708425f5ffd5fab2ea9050e90f8acc"
    md5sum "$ALIBABA_DIR/hire_workload_jobs.csv"
else
    echo "nothing to do for alibaba 2018 traces"
fi
