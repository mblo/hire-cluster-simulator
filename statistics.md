## Controlling statistics collection and dumping

* To enable solver statistics: ``--statistics-solver true``
* To enable tenant statistics per job: ``--statistics-tenant-job  true``
* To enable tenant statistics per tg: ``--statistics-tenant-tg  true``
* To enable scheduler statistics: ``--statistics-scheduler  true``
* To enable cell statistics: `--statistics-cell  true`  
  => By default all statistics are enabled.

* To set the statistics dump interval: `--statistics-interval XYZ` where
  ``XYZ`` is the interval to use
* To set the statistics target directory: `--output-dir DIR` where `DIR` is the directory to dump to
* To set the starting point of statistics collection: `--statistics-start T`
  where `T` is the starting point  
  => Note that statistics for the solver are not affected by the starting time.
* To enable extra snapshot hours for tenant: `--statistics-tenant-extra-snapshots-hours A,B,C`
  where `A`, `B` and `C` are the timestamps

## Statistics

### Flow Solver Statistics

The following data will be dumped for each run of the solver:

| Field | Description |
| ---- | ------------ |
| RunID | Unique id for each run within a experiment |
| Solver | Name of solver |
| ParallelRun | Boolean if this was a parallel solver run |
| SimulationTime | The simulation time this run was invoked at |
| StartTimeNanos | Start time of this run (in ns) |
| EndTimeNanos | End time of this run (in ns) |
| ProducerCnt | The amount of producer nodes in the graph |
| NonProducerCnt | The amount of non producer nodes in the graph |
| Supply | The total supply to be distributed |
| ServerLoad | The avg load of the server nodes |
| SwitchLoad | The avg load of the switch nodes |
| CleanUpTime | The time needed for cleanup the graph for this run |

### Tenant Statistics

The following data will be dumped for each TaskGroup:

| Field | Description |
| ---- | ------------ |
| JobID | The id of the job the task group belongs to |
| TaskGroupID | The id of the task group itself |
| ValidForJob | If the task group might be part of the job or not |
| SubmissionTime | The timestamp this TaskGroup was submitted to the scheduler |
| TotalTasks | The total amount of tasks within this TaskGroup |
| TaskGroupType | The type of this TaskGroup. I if switch, S otherwise |
| TasksStarted | The total amount of tasks started |
| AvgQueueing | The average queueing time of all the tasks |
| Duration | The duration of one task to complete (with considering INC boost) |
| OriginalDuration | The original duration according to the trace |
| DetourInc | Number of additional switch levels that for outgoing communications  |
| InvolvedRacks | Number of involved racks for server task groups |
| MaxDiameter | Max diameter of any tg communication of communicating groups |

The following data will be dumped for each Job:

| Field | Description |
| ---- | ------------ |
| JobID | The id of the job |
| InvolvedTGCnt | The amount of task groups that are active (TG's within selected flavor) |
| TasksTotal | The total amount of tasks that are involved in this job |
| TasksStarted | The total amount of tasks started | 
| AvgTaskQueueing | The average queueing time of all tasks in this job |
| AvgTaskDuration | The average task duration within this job |
| AvgTaskDurationOriginal | The average original task duration within this job |
| InpStatus | S: if the job has no flavor / Y: If the inp flavor has been chosen / N: If the inp flavor has not been chosen |
| detourInc | Number of additional switch levels that for outgoing communications |
| involvedRacks | Number of involved racks for server task groups |
| outgoingConnectionMaxDiameter | Max diameter of any tg communication of communicating groups |

### Cell Statistics

The following data will be dumped periodically:

| Field | Description |
| ---- | ------------ |
| Time | The simulation timestep this sample was taken at |
| UtilizationSwitchINP | The overall utilization of inp types for switches |
| UtilizationSwitchesDim1 | The overall switch utilization in dimension 1 |
| UtilizationSwitchesDim2 | The overall switch utilization in dimension 2 |
| UtilizationSwitchesDim{X} | The overall switch utilization in dimension X |
| ActualUtilizationSwitchesDim1 | The actual overall switch utilization in dimension 1 |
| ActualUtilizationSwitchesDim2 | The actual overall switch utilization in dimension 2 |
| ActualUtilizationSwitchesDim{X} | The actual overall switch utilization in dimension X |
| UtilizationServerDim1 | The overall server utilization in dimension 1 |
| UtilizationServerDim2 | The overall server utilization in dimension 2 |
| UtilizationServerDim{X} | The overall server utilization in dimension X |

### Scheduler Statistics

The following data will be dumped periodically:

| Field | Description |
| ---- | ------------ |
| Time | The simulation timestamp this sample was taken from |
| Attempts | How many scheduling attemps were already made |
| Allocations | How many allocations were already made |
| ScheduledTasks | How many tasks have already been scheduled |
| ThinkTime | The total think time the scheduler needed so far |
| ThinkTimeWasted | The wasted total think time (counting rounds with 0 allocations). |
| JobsInQueue | The amount of jobs still awaiting placement |
| TotalJobsFullyScheduled | The amount of jobs that have been fully scheduled |
| TotalJobsFullyScheduledINP | The amount of INP jobs that have been fully scheduled |
| TotalJobQueueTime | The total accumulated job queueing time |
| CountJobs | The total amount of jobs added |
| CountJobsWithInp | The total amount of jobs with INP added |
| CountJobsFinished | The total amount of jobs finished |
| CountJobsFinishedINP | The total amount of INP jobs finished |
| CountPreemptions | The total amount of preemtions occured |
| PendingJobsNotFullyScheduled | The amount of jobs not fully scheduled |
| PendingJobsFullyScheduled | The amount of jobs fully scheduled |
| PendingJobsNotFullyScheduledPlusFutureTgSubmissions | The amount of jobs not fully scheduled plus the amount of pending jobs |
| TimesServerFlavorTaken | How often the server flavor has been chosen so far |
| TimesSwitchFlavorTaken | How often the switch flavor has been chosen so far |