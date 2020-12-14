package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.shared.TimeIt
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.{EmptyEvent, SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.{immutable, mutable}

protected class KubeSchedulingItem(val taskGroup: TaskGroup,
                                   val timeAdded: SimTypes.simTime,
                                   var attempts: Int = 0,
                                   var backOffConsiderAfter: SimTypes.simTime = 0) {
}

protected class KubeMaschineCandidate(val machineId: Int,
                                      val score: Long = 0,
                                      val tasksToStart: Int = 0)

class KubernetesScheduler(override val name: String,
                          implicit override val cell: Cell,
                          implicit override val simulator: Simulator,
                          override val thinkTimeScaling: Double = 1.0,
                          val flavorSelector: FlavorSelector,
                          val minimumPercentageOfNodesToScore: Double = 0.1,
                          val backoffTime: SimTypes.simTime = 10000, // start with 10 seconds according to Kubernetes config
                          val backoffTimeMax: SimTypes.simTime = 6 * 60000, // 6 minutes according to Kubernetes config
                          val maxSchedulingAttempts: Int = 10,
                          override val schedulingComplexityPer100ThinkTime: Int = 5000,
                          val startAtMost1Task: Boolean = false,
                          val takeServerFallbackBeforeUnsched: Boolean = false,
                          val takeServerFallbackBeforeBackoff: Boolean = false,
                         ) extends Scheduler(name = name, cell = cell, simulator = simulator,
  thinkTimeScaling = thinkTimeScaling,
  schedulingComplexityPer100ThinkTime = schedulingComplexityPer100ThinkTime)
  with SchedulerWithFlavorSelector with QueueBasedScheduler {

  assert(minimumPercentageOfNodesToScore <= 1.0)
  assert(schedulingComplexityPer100ThinkTime > 0)

  private implicit val _this: SchedulerWithFlavorSelector = this

  //  https://kubernetes.io/docs/tasks/administer-cluster/topology-manager/
  // Note, we do not implement the topology-manager (introduced in 1.16, but deactivated in default config),
  //  which allows to consider some topology related constraints. However, it is not intended to serve as a topology
  //  aware scheduling manager so that pods belonging to the same job will be scheduled nearby

  // https://github.com/kubernetes/enhancements/blob/master/keps/sig-scheduling/20180409-scheduling-framework.md
  // https://kubernetes.io/docs/concepts/configuration/scheduling-framework/#


  // two queues, for high and low prio jobs, and for each queue we consider the submission time
  val activeQueueHigh: mutable.PriorityQueue[KubeSchedulingItem] = mutable.PriorityQueue()(Ordering.by[KubeSchedulingItem, SimTypes.simTime](_.timeAdded).reverse)
  val activeQueueLow: mutable.PriorityQueue[KubeSchedulingItem] = mutable.PriorityQueue()(Ordering.by[KubeSchedulingItem, SimTypes.simTime](_.timeAdded).reverse)

  // holds all task groups, that are backed off (if scheduling failed), sorted by the timely spoken "next" pod to be considered again
  val backoffQueue: mutable.PriorityQueue[KubeSchedulingItem] = mutable.PriorityQueue()(Ordering.by[KubeSchedulingItem, SimTypes.simTime](_.backOffConsiderAfter).reverse)

  // all scheduling items which cannot be scheduled
  val unschedulableQueue: mutable.Queue[KubeSchedulingItem] = mutable.Queue()

  var previousOffsetServers: Int = 0
  var previousOffsetSwitches: Int = 0

  override def getNumberOfPendingJobs: Int = allNotFinishedJobs.size

  override def addJobAndSchedule(newJob: Job): Unit = {
    flavorSelector.jobSubmitted(newJob)
    notifyMeForeachTaskGroupWhenReady(newJob)
    allNotFinishedJobs.add(newJob)
    allNotScheduledJobs.add(newJob)

    newJob.taskGroups.foreach(tg => {
      if (tg.submitted <= simulator.currentTime()) {
        logVerbose(s"add taskGroup to the pending list")
        if (newJob.isHighPriority) {
          activeQueueHigh.enqueue(new KubeSchedulingItem(tg, timeAdded = simulator.currentTime()))
        } else {
          activeQueueLow.enqueue(new KubeSchedulingItem(tg, timeAdded = simulator.currentTime()))
        }
      }
    })
    if (!isScheduling) {
      schedule()
    }
  }

  override def isPendingJobQueueEmpty: Boolean = activeQueueHigh.isEmpty &&
    activeQueueLow.isEmpty &&
    backoffQueue.isEmpty &&
    unschedulableQueue.isEmpty

  /**
   * will be triggerend only! if notifyMeForeachTaskGroupWhenReady is called for a job
   */
  override def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit = {
    assert(taskGroup.submitted == simulator.currentTime())
    if (job.isHighPriority) {
      activeQueueHigh.enqueue(new KubeSchedulingItem(taskGroup, timeAdded = simulator.currentTime()))
    } else {
      activeQueueLow.enqueue(new KubeSchedulingItem(taskGroup, timeAdded = simulator.currentTime()))
    }
    if (!isScheduling) {
      schedule()
    }
  }

  def scoreMachine(machine: Int, taskGroup: TaskGroup, totalTasksOfJob: Int): Long = {
    /**
     * kubernetes/pkg/scheduler/algorithmprovider/defaults/defaults.go
     * After the optional NormalizeScore, the scheduler will combine node scores from
     * all plugins according to the configured plugin weights.
     */
    val weightedScores: mutable.ListBuffer[Long] = mutable.ListBuffer()
    val highScore = 1000
    val weightSelectorSpread: Int = 1
    val weightLeastRequestedPrioMap: Int = 1
    val weightBalancedResourceAllocation: Int = 1
    // weight is set to 1 for all, except priorities.NodePreferAvoidPodsPriority
    var totalWeights: Int = 0
    val affectedCelLBackref: mutable.TreeSet[Allocation] = if (taskGroup.isSwitch)
      cell.backrefAllocationsSwitches(machine) else
      cell.backrefAllocationsServer(machine)
    val job = taskGroup.job.get

    // we do not implement priorities.NodePreferAvoidPodsPriority, # weight 10000
    // we do not implement priorities.TaintTolerationPriority,
    // we do not implement priorities.ImageLocalityPriority,

    val tasksOfSameJob: Int = affectedCelLBackref.count(a => a.job == job)
    val scoreSelectorSpread: Long = (tasksOfSameJob * highScore) / totalTasksOfJob

    assert(scoreSelectorSpread >= 0)
    assert(scoreSelectorSpread <= highScore)
    weightedScores.addOne(scoreSelectorSpread * weightSelectorSpread)
    totalWeights += weightSelectorSpread

    // priorities.LeastRequestedPriority,
    // LeastRequestedPriorityMap is a priority function that favors nodes with fewer requested resources.
    //	// It calculates the percentage of memory and CPU requested by pods scheduled on the node, and
    //	// prioritizes based on the minimum of the average of the fraction of requested to capacity.
    //	//
    //	// Details:
    //	// (cpu((capacity-sum(requested))*10/capacity) + memory((capacity-sum(requested))*10/capacity))/2
    val machineDimLoad: immutable.Seq[Long] = if (taskGroup.isSwitch) {
      cell.switches(machine).numericalResources.indices.map(i => {
        val t = 10 * cell.switches(machine).numericalResources(i) / cell.totalMaxSwitchCapacity(machine)(i)
        assert(t >= 0)
        t
      })
    } else {
      cell.servers(machine).indices.map(i => {
        val t = 10 * cell.servers(machine)(i) / cell.totalMaxServerCapacity(machine)(i)
        assert(t >= 0)
        t
      })
    }
    val scoreLeastRequestedPrioMap: Long = highScore * machineDimLoad.sum / (machineDimLoad.size * 10) // /10 for scaling
    assert(scoreLeastRequestedPrioMap >= 0)
    assert(scoreLeastRequestedPrioMap <= highScore)
    totalWeights += weightLeastRequestedPrioMap
    weightedScores.addOne(scoreLeastRequestedPrioMap * weightLeastRequestedPrioMap)

    // priorities.BalancedResourceAllocation,
    //  BalancedResourceAllocationMap favors nodes with balanced resource usage rate.
    //  // BalancedResourceAllocationMap should **NOT** be used alone, and **MUST** be used together
    //	// with LeastRequestedPriority. It calculates the difference between the cpu and memory fraction
    //	// of capacity, and prioritizes the host based on how close the two metrics are to each other.
    //	// Detail: score = core = 10 - abs(cpuFraction-memoryFraction)*10. The algorithm is partly inspired by:
    //	// "Wei Huang et al. An Energy Efficient Virtual Machine Placement Algorithm with Balanced
    //	// Resource Utilization"
    val fractions: immutable.Seq[Long] =
    if (taskGroup.isSwitch) {

      val switch_resources = taskGroup.resources.asInstanceOf[SwitchResource].numericalResources

      cell.switches(machine).numericalResources.indices.map(i => {
        val available = cell.switches(machine).numericalResources(i)

        if (available > 0)
          switch_resources(i) / available
        else
          0

      })
    } else {

      val server_resources = taskGroup.resources.asInstanceOf[ServerResource].numericalResources

      cell.servers(machine).indices.map(i => {
        val available = cell.servers(machine)(i)

        if (available > 0)
          server_resources(i) / available
        else
          0

      })
    }

    val balance_score = {
      10 * highScore - math.abs(fractions.max - fractions.min) * 10 * highScore
    }
    totalWeights += weightBalancedResourceAllocation
    weightedScores.addOne(balance_score * weightBalancedResourceAllocation)

    logVerbose(s"score of machine $machine for $taskGroup is ${weightedScores.sum / totalWeights}")

    weightedScores.sum / totalWeights
  }

  override protected def schedule(): Unit = {
    // Just enqueue a empty event. This will enforce that the post timestep hooks are called after this round
    simulator.scheduleEvent(new EmptyEvent(simulator.currentTime()))
  }

  // Register scheduling as a post timestep hook at this point.
  simulator.registerPostSimulationStepHook(_ => onEndOfTimeStampCheckScheduling())

  private def onEndOfTimeStampCheckScheduling(): Unit = {
    if (!isScheduling && cell.currentServerLoadMax().toDouble <= SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE)
      runSchedulingLogic()
  }

  @inline private def pushToActiveQueue(item: KubeSchedulingItem) = {
    if (item.taskGroup.job.get.isHighPriority) {
      activeQueueHigh.enqueue(item)
    } else {
      activeQueueLow.enqueue(item)
    }
  }

  /**
   * // Nodes in a cluster that meet the scheduling requirements of a Pod are called feasible Nodes for the Pod.
   * // The scheduler finds feasible Nodes for a Pod and then runs a set of functions to score the feasible Nodes,
   * // picking a Node with the highest score among the feasible ones to run the Pod.
   * // The scheduler then notifies the API server about this decision in a process called Binding.
   *
   * // percentageOfNodesToScore
   * // The formula yields 10% for a 5000-node cluster
   *
   * // The scheduler starts from the start of the array and checks feasibility of the nodes until it finds enough
   * // Nodes as specified by percentageOfNodesToScore. For the next Pod, the scheduler continues from the point in
   * // the Node array that it stopped at when checking feasibility of Nodes for the previous Pod.
   *
   */
  private def runSchedulingLogic(): Unit = {
    assert(!isScheduling)
    isScheduling = true

    val timeIt = TimeIt("K8 schedule")

    val now = simulator.currentTime()

    var maxDecisionsToBeDone: Int = schedulingComplexityPer100ThinkTime
    val allocations: mutable.ListBuffer[Allocation] = mutable.ListBuffer()
    var nextPodToScheduleOption: Option[KubeSchedulingItem] = None

    val utilServers = cell.currentServerLoads().map(_.toDouble)
    val utilSwitches = cell.currentSwitchLoads().map(_.toDouble)

    var actualAllocatedTasks = 0

    var forceConsumingFromBackoff: Boolean =
      if (activeQueueLow.isEmpty && activeQueueHigh.isEmpty && unschedulableQueue.isEmpty && backoffQueue.nonEmpty
        && SimulationConfiguration.KUBERNETES_ENABLE_AGGRESSIVE_BACKOFF_CONSUME) {
        logVerbose("Force at least one of the backoff to come to front, so that we do scheduling again")
        backoffQueue.head.backOffConsiderAfter = now
        true
      } else false

    while (maxDecisionsToBeDone > 0) {
      maxDecisionsToBeDone -= 1

      if (nextPodToScheduleOption.isEmpty) {

        // check backOff, if there is something to be considered
        while (backoffQueue.nonEmpty && (backoffQueue.head.backOffConsiderAfter <= now || forceConsumingFromBackoff)) {
          val poppedFromBackOff: KubeSchedulingItem = backoffQueue.dequeue()
          pushToActiveQueue(poppedFromBackOff)
        }

        // check unscheduled queue
        if (unschedulableQueue.nonEmpty && activeQueueHigh.isEmpty && activeQueueLow.isEmpty) {
          // there are jobs which are marked as unscheduled, but there is nothing else to do,
          // so start considering them again
          while (unschedulableQueue.nonEmpty) {
            val popped = unschedulableQueue.dequeue()
            popped.attempts = 0
            pushToActiveQueue(popped)
          }
        }

        if (activeQueueLow.isEmpty && activeQueueHigh.isEmpty) {
          maxDecisionsToBeDone = 0
          if (allocations.isEmpty) {
            logVerbose("nothing to do, pause scheduling")
            isScheduling = false

            // is there something in the backoff?
            if (backoffQueue.nonEmpty) {
              val waitTill = backoffQueue.head.backOffConsiderAfter
              assert(waitTill > simulator.currentTime())
              simulator.scheduleActionWithDelay(s => {
                // schedule delayed.. will be triggered as post step action
              }, waitTill - simulator.currentTime())
              logVerbose(s".. there is something in the backlog.. so simply make delayed scheduling event in ${waitTill - simulator.currentTime()}")
            }
          }
        } else {
          // get next item to be scheduled
          nextPodToScheduleOption = Some({
            if (activeQueueHigh.nonEmpty) {
              activeQueueHigh.dequeue()
            } else {
              activeQueueLow.dequeue()
            }
          })
        }
      }

      if (nextPodToScheduleOption.isDefined) {
        val nextPodToSchedule = nextPodToScheduleOption.get

        nextPodToSchedule.attempts += 1
        val tg = nextPodToSchedule.taskGroup
        val job = tg.job.get

        val in_flavor =
          if (job.flavorHasBeenChosen)
            job.checkIfTaskGroupIsDefinitelyInFlavorSelection(tg)
          else
            job.checkIfTaskGroupMightBeInFlavorSelection(tg)

        // check if this guy is still valid
        if (tg.notStartedTasks == 0 || job.isDone || !in_flavor) {
          // tg is outdated, nothing to do
          // simply go to the next scheduling attempt (using a new event, so that we cannot hit a recursion error)
          nextPodToScheduleOption = None
          maxDecisionsToBeDone += 1
        } else {

          val startedWithOffset: Int = if (tg.isSwitch) previousOffsetSwitches else previousOffsetServers
          var toBeChecked: Int = {
            val total = if (tg.isSwitch) cell.numSwitches else cell.numServers
            val ratio = Math.max(minimumPercentageOfNodesToScore, total match {
              case x if x <= 100 =>.5
              case x if (x > 100 && x <= 5000) =>.1
              case _ =>.05
            })
            (minimumPercentageOfNodesToScore * ratio).ceil.toInt
          }
          assert(toBeChecked > 0)

          var checkedMachines = 0

          // cache the number of tasks of this job flavor
          val totalTasksOfJob = job.taskGroups.map(tg => {
            if (job.checkIfTaskGroupMightBeInFlavorSelection(tg)) tg.numTasks else 0
          }).sum

          // check up to percentageOfNodesToScore*total candidates
          val bestCandidate: Option[KubeMaschineCandidate] = if (
            (tg.isSwitch && (0 until cell.resourceDimSwitches).exists(i => tg.resources.asInstanceOf[SwitchResource].numericalResources(i) > 0 && utilSwitches(i) >= pauseSchedulingWhenUtilizationAbove)) ||
              (!tg.isSwitch && (0 until cell.resourceDimServer).exists(i => tg.resources.asInstanceOf[ServerResource](i) > 0 && utilServers(i) >= pauseSchedulingWhenUtilizationAbove)))
            None
          else {
            var intermediateBestCandidate: Option[KubeMaschineCandidate] = None
            while (toBeChecked > 0) {
              val machine = if (tg.isSwitch) {
                previousOffsetSwitches += 1
                previousOffsetSwitches %= cell.numSwitches
                previousOffsetSwitches
              } else {
                previousOffsetServers += 1
                previousOffsetServers %= cell.numServers
                previousOffsetServers
              }
              // get next feasible machine
              val maxToLaunch = cell.checkMaxTasksToAllocate(tg, machine)
              checkedMachines += 1
              if (maxToLaunch > 0) {
                val currentScore: Long = scoreMachine(
                  machine = machine,
                  taskGroup = tg,
                  totalTasksOfJob = totalTasksOfJob)
                if (intermediateBestCandidate.isEmpty || intermediateBestCandidate.get.score < currentScore) {
                  intermediateBestCandidate = Some(new KubeMaschineCandidate(
                    machineId = machine,
                    score = currentScore,
                    tasksToStart = if (startAtMost1Task) 1 else maxToLaunch))
                }
                toBeChecked -= 1
              } else {
                // go to next machine
              }

              // check if the currently checked machine is the very same machine where we started the check
              if (startedWithOffset == machine) {
                // yes, so stop checking, and if we were not able to find any possible machine, we have to backoff this pod
                toBeChecked = 0
              }
            }
            intermediateBestCandidate
          }

          if (bestCandidate.isEmpty) {
            // failed, trigger backoff
            var plannedBackoffTime = Math.pow(2, nextPodToSchedule.attempts - 1).toInt * backoffTime
            assert(plannedBackoffTime > 0)
            if (plannedBackoffTime > backoffTimeMax) {
              plannedBackoffTime = backoffTimeMax
            }
            if (nextPodToSchedule.attempts >= maxSchedulingAttempts) {
              logVerbose(s"I cannot schedule task group $tg, tried ${nextPodToSchedule.attempts}" +
                s" times, but ${tg.notStartedTasks} tasks not running, push to ")

              if (job.statisticsInpChosen && takeServerFallbackBeforeUnsched && tg.isSwitch && flavorSelector.allowSchedulerToWithdrawJobAndResubmit) {
                logVerbose(s"$this failed to schedule a tg ($tg) for long time," +
                  s"apply server fallback")

                flavorSelector.applyServerFallback(job, replaceJobAndResubmitIfNecessary = true)
              } else {
                logVerbose(s"$this failed to schedule a tg ($tg) for long time," +
                  s"mark this tg as unschedulable")
                unschedulableQueue.enqueue(nextPodToSchedule)
              }
            } else {
              // check if we should choose the server fallback
              val jobWasUpdated = if
              (job.statisticsInpChosen && takeServerFallbackBeforeUnsched && takeServerFallbackBeforeBackoff
                  && tg.isSwitch && flavorSelector.allowSchedulerToWithdrawJobAndResubmit) {
                flavorSelector.applyServerFallback(job, replaceJobAndResubmitIfNecessary = false)
              } else false

              if (!jobWasUpdated) {
                logVerbose(s"not able to find a machine for taskgroup $tg, so apply backoff of $plannedBackoffTime " +
                  s" considering this is the ${nextPodToSchedule.attempts} attempt")

                nextPodToSchedule.backOffConsiderAfter = simulator.currentTime() + plannedBackoffTime
                backoffQueue.enqueue(nextPodToSchedule)
              } else {
                logVerbose(s"not able to find a machine for taskgroup $tg, but we updated/replaced the job with a server fallback")
              }
            }
            // track this as wasted scheduling time
            nextPodToScheduleOption = None
            maxDecisionsToBeDone = 0
            applyAllocationAfterThinkTime(allocations = Nil,
              thinkTime = thinkTimeForCheckedMachines(checkedMachines, tg.isSwitch),
              andRunScheduleAfterwards = true)
          } else {
            // we found something, great! so lets starts some guys
            logVerbose(s"found a machine ${
              bestCandidate.get.machineId
            } to start ${
              bestCandidate.get.tasksToStart
            } " +
              s" task(s) for task group $tg ")

            val thinkTime = thinkTimeForCheckedMachines(checkedMachines, tg.isSwitch)
            val taskStartTime = now + thinkTime

            val jobWasChosenBeforeNewAllocation = job.flavorHasBeenChosen
            val allocation: Allocation = job.doResourceAllocation(
              taskGroup = tg,
              numberOfTasksToStart = bestCandidate.get.tasksToStart,
              machine = bestCandidate.get.machineId,
              cell = cell,
              startTime = taskStartTime)

            actualAllocatedTasks += bestCandidate.get.tasksToStart
            maxDecisionsToBeDone = 0

            if (job.flavorHasBeenChosen && !jobWasChosenBeforeNewAllocation) {
              if (job.statisticsInpChosen)
                statisticsFlavorTakeInp += 1
              else
                statisticsFlavorTakeServer += 1
            }

            // we reset the attempts counter, since we successfully scheduled something
            nextPodToSchedule.attempts = 0
            // if task group has some tasks remaining, simple put back to the queue
            if (tg.notStartedTasks > 0) {
              if (maxDecisionsToBeDone <= 0) {
                logVerbose(s"task group $tg has pending tasks, put back to scheduling queue")
                if (job.isHighPriority) {
                  activeQueueHigh.enqueue(nextPodToSchedule)
                } else {
                  activeQueueLow.enqueue(nextPodToSchedule)
                }
                nextPodToScheduleOption = None
              } else {
                // simply schedule the next task of  this tg in next attempt
              }
            } else {
              // no task left, so choose next item
              nextPodToScheduleOption = None
            }
            allocations.append(allocation)
            forceConsumingFromBackoff = false
          }

        }
      }
    }

    assert(nextPodToScheduleOption.isEmpty)

    if (allocations.isEmpty && isScheduling) {
      // nothing to do, we can simply do nothing and wait till resource become available
      isScheduling = false
    }

    if (!isScheduling) {
      assert(allocations.isEmpty)
      // nothing to do
    } else {
      // check used think time again

      assert(allocations.size == 1, "K8 is currently not supporting batch scheduling")
      val thinkTime = allocations.head.startTime - now
      applyAllocationAfterThinkTime(allocations = allocations, thinkTime = thinkTime, andRunScheduleAfterwards = true)
    }

    timeIt.stop
  }

  override def jobWasUpdated(job: Job): Unit = {
    if (!job.isCanceled) {
      // re-insert all task groups that might be already in the system
      logVerbose(s"job $job was updated by flavor selector, so put all task groups back into the queues")
      for (tg <- job.taskGroups if tg.submitted <= simulator.currentTime()) {
        val timeAdded = if (takeServerFallbackBeforeUnsched || takeServerFallbackBeforeBackoff) tg.submitted else simulator.currentTime()
        pushToActiveQueue(new KubeSchedulingItem(tg, timeAdded = timeAdded))
      }
    }
    if (!isScheduling) {
      schedule()
    }
  }
}



