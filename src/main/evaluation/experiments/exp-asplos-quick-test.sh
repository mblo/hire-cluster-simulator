#!/usr/bin/env bash

CMD='python3 src/main/python/ExpRunner.py --jar target/hire.jar --memory 50 --worker 15 --output exp-rerun-asplos-quick-test --set
cell-k=26
cell-max-active-inp=3
create-inp-starting-time=1
flowSchedulerPostponeSchedulingIfStuck=250
hireInpServerPenaltyCost=3.0
hireInpServerPenaltyWaitingLower=500
hireInpServerPenaltyWaitingUpper=2000
hireShortcutsMaxSearchSpace=50
hireShortcutsMaxSelection=50
inp-types=netchain,sharp,incbricks,netcache,distcache,harmonia,hovercraft,r2p2
kappa-draw-random=true
kappa-runtime=0.9
kappa-tasks=0.9
max-server-capacity=1000000
max-switch-capacity=1000000
maxInpFlavorDecisionsPerRound=250
maxServerPressure=0.98
minQueuingTimeBeforePreemption=2000
seed=0
mu-inp=0.05:0.5:1.0:0.25:0.75
precision=100
cellSwitchHomogeneous=random2:homogeneous
ratioOfIncTaskGroups=0.35
scale-cell-servers=4.5,3.0
scale-cell-switches=1,1,1
scheduler=hire,parallel-0-relax-cost-ssp:coco,parallel-0-relax-cost-ssp,resubmit,-10:kubernetes#,greedy:kubernetes,resubmit,-10:sparrow,2,200,0.5,true,greedy:sparrow,2,200,0.5,false,resubmit,-10:yarn#,greedy:yarn,resubmit,-10
shared-resource-mode=once:foreach
useSimpleTwoStateInpServerFlavorOptions=false:true
sim-time=129600000
workload-time=259200000
softLimitProducersInGraph=800
sspMaxSearchTimeSeconds=600000
statistics-start=0
status-report-message=600
think-time-scaling=1
time-it=true
verbose=0
disableLimits=0 '

export CMD
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
"$DIR/run-exp.sh" "$@"
