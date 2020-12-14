![ScalaTest](https://github.com/mblo/hire-cluster-simulator/workflows/ScalaTest/badge.svg)

# HIRE cluster simulator

This repository contains the code for the paper
> **Switches for HIRE: Resource Scheduling for Data Center In-Network Computing**  
> Marcel Blöcher, Lin Wang, Patrick Eugster, and Max Schmidt  
> ACM ASPLOS 2021

## Contents

This repository holds the code used in the paper.

- ```src/main/evaluation/evals``` holds the plotting scripts
- ```src/main/evaluation/experiments``` holds the experiment configurations
- ```src/main/scala``` holds the simulator codebase

## Setup

1. **Build docker image**

   This prepares Scala, JDK, Python3.
   ```
   docker build -t asplos21-hire/runner:latest .
   ```
2. **Build jar**
   ```
   docker run -it -v $PWD:/app --rm asplos21-hire/runner sbt assembly
   ```
3. **Prepare cluster trace**
    - Download (`batch_task.csv`, `container_meta.csv`, `machine_meta.csv`)
      from https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/trace_2018.md
      and save them in ./traces/alibaba_trace_2018
        - http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/batch_task.tar.gz
        - http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/machine_meta.tar.gz
        - http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/container_meta.tar.gz
    - Prepare trace for simulator:
   ```
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./bootstrap-traces.sh  
   ```

## Paper evaluation

We run all experiments on an AMD EPYC 7542 with 512GB RAM
using `OpenJDK 64-Bit Server VM GraalVM CE 20.1.0 (build 11.0.7+10-jvmci-20.1-b02, mixed mode, sharing)`. Most
simulations use less than 25gb memory (depends on the JVM used). You can set the number of parallel simulations in each
script (--worker XX).

1. **Setup**

   Prepare jar files, prepare trace files as described above

2. **Run quick paper evaluation**

   For a quick paper evaluation, use the experiment ```exp-asplos-quick-test.sh```, which runs only a small subset of
   all simulations.
   ```
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-quick-test.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/evals/run-paper-eval.sh  \
       -e exp-rerun-asplos-quick-test \   
       -o . --sweep mu-inp &
   ```

3. **Run all simulations.**

   Each experiment script runs a set of parallel simulations; Check the script for parallel worker threads (--worker XX)
   . Each experiment script writes output files to `./exp-rerun-asplos-XXXXXX`.
   ```
   # pass "--dry" to check configuration first   
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-baselines-yarn.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-baselines-coco.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-baselines-k8.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-baselines-sparrow.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-hire.sh --dry
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/experiments/exp-asplos-speed-benchmark.sh --dry
   ```
4. **Plotting**

   All simulation results (`exp-rerun-asplos-XXXXXX`) must be stored on a single server for plotting.
   ```
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./src/main/evaluation/evals/run-paper-eval.sh  \
       -e exp-rerun-asplos-hire  \
       -e exp-rerun-asplos-baselines-k8 \   
       -e exp-rerun-asplos-baselines-sparrow \   
       -e exp-rerun-asplos-baselines-yarn \   
       -e exp-rerun-asplos-baselines-coco \   
       -o .. --sweep mu-inp &
   
   python3 src/main/evaluation/evals/evaluate_paper_solver.py  \
       -e exp-rerun-asplos-speed-bench \   
       -o .. --sweep mu-inp &
   ```

### Notes

- ```evaluate_paper_cluster_load.py``` --> creates the violin plots Fig 6f, Fig 6i
- ```evaluate_paper_inc_success.py``` --> creates the plots Fig 6a, Fig 6f
- ```evaluate_paper_inc_success_tg.py``` --> creates the plots Fig 6b, Fig 6g
  (Note this only works for HIRE experiments)
- ```evaluate_paper_latency_cdf.py``` --> creates the plots Fig 6e, Fig 6j
- ```evaluate_paper_locality.py``` --> creates the plots Fig 6c, Fig 6h
- ```evaluate_paper_solver.py``` --> creates the plot Fig 7
  (Note this only works for HIRE experiments)

## Customization

You can easily modify the behavior of the scheduler (```/src/main/scala/hiresim/scheduler/```), the
workload (```/src/main/scala/hiresim/workload/```), and the cluster configuration (```/src/main/scala/hiresim/cell/```).
Furthermore, the experiment configurations (```/src/main/evaluation/experiments/```) provide many parameters and flags
for customization.

# Code Contributors

- [@Marcel Blöcher](https://github.com/mblo), [@Max Schmidt](https://github.com/max-schmidt-university)
  , [@Marco Micera](https://github.com/marcomicera)
- Adapted scheduler logic (```/src/main/scala/hiresim/scheduler/```)
  partially taken from [Kubernetes](https://github.com/kubernetes/kubernetes),
  [Sparrow](https://dl.acm.org/doi/10.1145/2517349.2522716),
  [Yarn](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html),
  [CoCo](https://github.com/camsas/firmament) -- as described in the source code. (Apache License 2.0)
- MCMF solver, parts inspired from [ICGog/Flowlessly](https://github.com/ICGog/Flowlessly/tree/master/src/solvers) (Apache License 2.0), relaxation solver based on [https://stuff.mit.edu/people/dimitrib/BT_Relax_1988.pdf](https://stuff.mit.edu/people/dimitrib/BT_Relax_1988.pdf)
