![ScalaTest](https://github.com/mblo/hire-cluster-simulator/workflows/ScalaTest/badge.svg)

# HIRE cluster simulator

This repository contains the source code implementation of the ASPLOS paper
> **Switches for HIRE: Resource Scheduling for Data Center In-Network Computing**  
> Marcel Blöcher, Lin Wang, Patrick Eugster, and Max Schmidt  
> ACM ASPLOS 2021

## Directory Structure

### `src/main/scala` 
Code for the simulator, including the schedulers (Yarn++, K8++, CoCo++, Sparrow++, HIRE), the workload, and all other components of the simulation.

### `src/main/evaluation/experiments`
Experiment configurations for all experiments in the paper.

### `src/main/evaluation/evals` 
Plotting and post-processing code for all plots in the paper.

### `src/test/scala` 
Test cases including simple unit tests and regression test cases.

## Setup

The simulator is implemented in Scala and the plotting scripts are implemented in Python.
We have tested the simulator on Ubuntu 20.04 with Python 3.7.5, Scala 2.13.4, SBT 1.4.4, and OpenJDK GraalVM CE 20.1.0.

**Required software dependencies can be installed either using Docker or manually**

### Docker

**Build docker image**

This prepares Scala, JDK, Python3 and builds the jar:
```bash
docker build --build-arg HOST_USER=$(id -u -n) --build-arg HOST_UID=$(id -u) -t asplos21-hire/runner:latest .
```

When you modify the simulator codebase, you must rebuild the jar:
```bash
docker run -it -v $PWD:/app --rm asplos21-hire/runner sbt assembly
```

### Manual Setup

 1. Install Python3, JDK 11, and SBT as required
 2. Install Python3 dependencies
  ```bash
  pip3 install -r requirements.txt
  # or using a virtual environment...
  python3 -m venv ./py-env
  source "./py-env/bin/activate"
  pip3 install -r requirements.txt
  ```
 3. Build `jar`
  
  (You must rebuild the jar when modifying the simulator codebase)
  ```bash
  sbt assembly
  ```

## Prepare Cluster Trace

Before you can run any of the experiments, you must prepare the cluster trace. 
You can either download the pre-compiled traces files or build them manually:

### Pre-compiled cluster trace
Download zip archive from [https://zenodo.org/record/4419041/files/traces.zip?download=1](https://zenodo.org/record/4419041/files/traces.zip?download=1).
Extract archive into this directory. This creates the following nested folder: `THIS_REPO/traces/alibaba_trace_2018/`

### Manually prepare cluster trace
This requires appx. 10GB RAM.

   1. Download (`batch_task.csv`, `container_meta.csv`, `machine_meta.csv`)
        from [https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/](https://github.com/alibaba/clusterdata/blob/master/cluster-trace-v2018/)
        and save them in `THIS_REPO/traces/alibaba_trace_2018`
        - [http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/batch_task.tar.gz](http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/batch_task.tar.gz)
        - [http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/machine_meta.tar.gz](http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/machine_meta.tar.gz)
        - [http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/container_meta.tar.gz](http://clusterdata2018pubus.oss-us-west-1.aliyuncs.com/container_meta.tar.gz)
    
   2. Prepare trace for simulator:
    
   ```bash
   # with Docker setup:
   docker run -it -v $PWD:/app --rm asplos21-hire/runner ./bootstrap-traces.sh  
   # wihtout Docker:
   ./bootstrap-traces.sh  
   ```

## Experiment workflow explained
**All following commands use the Docker setup. If you don't use Docker, simply remove `docker run -it -v $PWD:/app --rm asplos21-hire/runner` from each of the commands.**


 - When you update the simulator, rebuild the jar (`docker run -it -v $PWD:/app --rm asplos21-hire/runner sbt assembly`)

 - Experiment bash scripts are stored in `src/main/evaluation/experiments`. You may want to configure 
   - the number of parallel worker threads: `--worker XX`
   - the memory limit for each worker: `--memory 30`
 
   You may want to change the parameter sweeps for an experiment. 
   E.g., `seed=0:1:2` defines 3 values for parameter `seed`

 - The experiment bash scripts invoke the Python `ExpRunner.py` which manages the JVM workers.
   
   When you pass the `--dry` argument to the experiment runner, you will enter an interactive bash.
   Press `[up] [down]` for checking all experiment configurations and press `p` to print the command line arguments for the selected configuration.
   This is intended to serve as a sanity check before starting all experiments.

   E.g., 

   ```bash
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-k8.sh --dry
   ```

 - To start the experiments, invoke the experiment bash script without the `--dry` option.

   This will create the output directory as defined by `--output` (in the bash script), relative to the current working directory.

   E.g., 

   ```bash
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-k8.sh
   ```

 - When all experiments are done, you can run post-processing and plotting.

   E.g.,

   ```bash
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/evals/run-paper-eval.sh  \
       -e exp-rerun-asplos-hire  \
       -e exp-rerun-asplos-baselines-k8  \
       -o . --sweep mu-inp \
       --ignore time-it:shared-resource-mode:useSimpleTwoStateInpServerFlavorOptions 
   ```

   You have to pass each experiment folder `-e EXP_FOLDER` and set the output directory of the plot `-o` (relative to the 1st experiment folder).
   Furthermore, we set the sweep variable (not tested with others) and set the variables that should be ignored when running post processing `--ignore` (this combines all schedulers into the same plot).

   If you want to create only a single plot, please check the `run-paper-eval.sh` for more details:
     - ```evaluate_paper_cluster_load.py``` --> creates the violin plots Fig 6f, Fig 6i
     - ```evaluate_paper_inc_success.py``` --> creates the plots Fig 6a, Fig 6f
     - ```evaluate_paper_inc_success_tg.py``` --> creates the plots Fig 6b, Fig 6g
       (Note this only works for HIRE experiments)
     - ```evaluate_paper_latency_cdf.py``` --> creates the plots Fig 6e, Fig 6j
     - ```evaluate_paper_locality.py``` --> creates the plots Fig 6c, Fig 6h
     - ```evaluate_paper_solver.py``` --> creates the plot Fig 7
       (Note this only works for HIRE experiments)

## Paper evaluation
Each experiment script writes output files to `THIS_DIR/exp-rerun-asplos-XXXXXX`.
Each experiment script runs a set of parallel simulations; Check the script for parallel worker threads (`--worker XX`).
On a server with 64 cores and 512GB RAM we use `--worker 15`. If your server has less RAM available, please adjust --worker XX accordingly in the experiment scripts `./src/main/evaluation/experiments`.
Most simulations use less than 25gb memory (depends on the JVM used).
We run all experiments on an AMD EPYC 7542 with 512GB RAM
using `OpenJDK 64-Bit Server VM GraalVM CE 20.1.0 (build 11.0.7+10-jvmci-20.1-b02, mixed mode, sharing)`. 

## Run partial/quick paper experiments

If you want to run a small subset of all experiments (only `seed=0` and `µ={0.05, 0.25, 0.5, 0.75, 1.0}`),
use the experiment `exp-asplos-quick-test.sh`.

**All following commands use the Docker setup. If you don't use Docker, simply remove `docker run -it -v $PWD:/app --rm asplos21-hire/runner` from each of the commands.**


   ```bash
   # run experiments
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
       ./src/main/evaluation/experiments/exp-asplos-quick-test.sh 
   # post processing, plot Fig 6
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
       ./src/main/evaluation/evals/run-paper-eval.sh  \
         -e exp-rerun-asplos-quick-test \   
         -o . --sweep mu-inp --ignore time-it:shared-resource-mode:useSimpleTwoStateInpServerFlavorOptions 
   # post processing, plot Fig 7
   docker run -it -v $PWD:/app --rm asplos21-hire/runner  \
        python3  src/main/evaluation/evals/evaluate_paper_solver.py   \
        -e  exp-rerun-asplos-quick-test -o . --sweep mu-inp
   ```

## Run all paper experiments
**All following commands use the Docker setup. If you don't use Docker, simply remove `docker run -it -v $PWD:/app --rm asplos21-hire/runner` from each of the commands.**

These experiments write appx. 150GB of logfiles.

   ### Run experiments
   
   ```bash
   # pass "--dry" to check configuration first   
   # run yarn experiments, takes some hours with `--worker 15`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-yarn.sh
   # run coco experiments, takes appx. 14 days with `--worker 15`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-coco.sh
   # run k8 experiments, takes some hours with `--worker 15`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-k8.sh
   # run sparrow experiments, takes some hours with `--worker 15`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-baselines-sparrow.sh
   # run hire experiments, takes appx. 6 days with `--worker 15`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-hire.sh
   # run hire speed benchmark experiments, takes some hours with `--worker 6`
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/experiments/exp-asplos-speed-benchmark.sh
   ```

   ### Plotting

   All simulation results (`exp-rerun-asplos-XXXXXX`) must be located in the local working directory for plotting.
   This is the default case when you run all experiments on the same machine.

   ```bash
   # create plots Fig 6 (post processing runs appx. 1 hour)
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
     ./src/main/evaluation/evals/run-paper-eval.sh  \
       -e exp-rerun-asplos-hire  \
       -e exp-rerun-asplos-baselines-k8 \   
       -e exp-rerun-asplos-baselines-sparrow \   
       -e exp-rerun-asplos-baselines-yarn \   
       -e exp-rerun-asplos-baselines-coco \   
       -o .. --sweep mu-inp --ignore time-it:shared-resource-mode:useSimpleTwoStateInpServerFlavorOptions 
   
   # create plots of hire speed benchmark Fig 7
   docker run -it -v $PWD:/app --rm asplos21-hire/runner \
      python3 src/main/evaluation/evals/evaluate_paper_solver.py  \
       -e exp-rerun-asplos-hire-speed-benchmark \   
       -o .. --sweep mu-inp 
   ```


## Customization

You can easily modify the behavior of the scheduler (`/src/main/scala/hiresim/scheduler/`), the
workload (`/src/main/scala/hiresim/workload/`), and the cluster configuration (`/src/main/scala/hiresim/cell/`).
Furthermore, the experiment configurations (`/src/main/evaluation/experiments/`) provide many parameters and flags
for customization.

You may also want to check the `main class` of the simulator, `/src/main/scala/hiresim/hxperiments/SimRunnerFromCmdArguments.scala`, for all available command line arguments.

## Code Contributors

- [@Marcel Blöcher](https://github.com/mblo), [@Max Schmidt](https://github.com/max-schmidt-university)
  , [@Marco Micera](https://github.com/marcomicera)
- Adapted scheduler logic (```/src/main/scala/hiresim/scheduler/```)
  partially taken from [Kubernetes](https://github.com/kubernetes/kubernetes),
  [Sparrow](https://dl.acm.org/doi/10.1145/2517349.2522716),
  [Yarn](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html),
  [CoCo](https://github.com/camsas/firmament) -- as described in the source code. (Apache License 2.0)
- MCMF solver, parts inspired from [ICGog/Flowlessly](https://github.com/ICGog/Flowlessly/tree/master/src/solvers) (Apache License 2.0), relaxation solver based on [https://stuff.mit.edu/people/dimitrib/BT_Relax_1988.pdf](https://stuff.mit.edu/people/dimitrib/BT_Relax_1988.pdf)
