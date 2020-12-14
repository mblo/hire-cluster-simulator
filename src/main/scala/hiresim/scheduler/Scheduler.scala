package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.scheduler.flow.FlowBasedScheduler
import hiresim.simulation.SimTypes.simTime
import hiresim.simulation.Simulator
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.mutable

/** Scheduler interface.
 *
 * @param name this scheduler's name.
 * @param cell the physical cell on which this scheduler operates.
 */
abstract class Scheduler(val name: String,
                         val cell: Cell,
                         val simulator: Simulator,
                         val thinkTimeScaling: Double,
                         val pauseSchedulingWhenUtilizationAbove: Double = 0.98,
                         val schedulingComplexityPer100ThinkTime: Int = 5000) {

  var isScheduling: Boolean = false

  var statisticsTotalThinkTime: BigInt = BigInt(0)
  var statisticsTotalThinkTimeWasted: BigInt = BigInt(0)
  var statisticsTotalJobsFullyScheduled: Int = 0
  var statisticsTotalJobsFullyScheduledWithInp: Int = 0
  var statisticsTotalJobsDone: Int = 0
  var statisticsTotalJobsDoneWithInp: Int = 0
  var statisticsTotalJobQueueTime: BigInt = BigInt(0)
  var statisticsSchedulingAttempts: Int = 0
  var statisticsPerformedAllocations: Long = 0
  var statisticsPerformedTaskAllocations: Long = 0
  var statisticsScheduledTasks: Long = 0
  var statisticsTotalTaskPreemptions: Long = 0
  var statisticsServerFallbackResubmit: Int = 0
  var statisticsFlavorTakeServer: Int = 0
  var statisticsFlavorTakeInp: Int = 0

  val mostRecentPlacementLatencies: mutable.ArrayDeque[Long] = mutable.ArrayDeque()

  private val jobDoneHooks: mutable.ListBuffer[Job => Unit] = mutable.ListBuffer()

  protected val allNotFinishedJobs: mutable.Set[Job] = mutable.Set()
  protected val allNotScheduledJobs: mutable.Set[Job] = mutable.Set()
  private val notifyJobList: mutable.Set[Job] = mutable.Set()

  def getSchedulingAttempts: Int = {
    statisticsSchedulingAttempts
  }

  def getNotFinishedJobs: Iterator[Job] = {
    allNotFinishedJobs.iterator
  }

  def getNotFinishedJobSize: Int = {
    allNotFinishedJobs.size
  }

  def getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions: Int = {
    allNotScheduledJobs.size
  }


  private var cachedMaxWaitingTime: simTime = -1
  private var cachedMaxWaitingTimeTick: Int = -1

  def getMaxWaitingTime: Int = {
    val now = simulator.currentTime()
    if (now != cachedMaxWaitingTimeTick) {
      var oldestSubmitTime = now
      for (job <- allNotScheduledJobs) {
        for (tg <- job.taskGroups) if (tg.notStartedTasks > 0) {
          if (tg.submitted < oldestSubmitTime)
            oldestSubmitTime = tg.submitted
        }
      }
      cachedMaxWaitingTime = now - oldestSubmitTime
      cachedMaxWaitingTimeTick = now
    }
    cachedMaxWaitingTime
  }

  /**
   * need to check all pending jobs, not cached - use only for statistics
   */
  def getNumberOfNotFullyAndFullyScheduledJobsTillNow: (Int, Int) = {
    val now = simulator.currentTime()
    var countFully = 0
    var countNotFully = 0
    for (job <- allNotScheduledJobs) {
      var readyTaskGroupsNotFullyScheduled = false
      job.flavorHasBeenChosen
      // we check if all submitted TGs that match to the job or that could match, are scheduled.
      for (tg <- job.taskGroups) {
        if (!readyTaskGroupsNotFullyScheduled && tg.submitted <= now && tg.notStartedTasks > 0) {
          if (job.checkIfTaskGroupMightBeInFlavorSelection(tg)) {
            readyTaskGroupsNotFullyScheduled = true
          }
        }
      }
      if (readyTaskGroupsNotFullyScheduled) {
        countNotFully += 1
      } else {
        countFully += 1
      }
    }
    (countNotFully, countFully)
  }

  def getNumberOfPendingJobs: Int

  def getNumberOfPendingSchedulingObjects: Int = getNumberOfPendingJobs

  def registerJobDoneHook(hook: Job => Unit): Unit = jobDoneHooks.append(hook)

  def addJobAndSchedule(newJob: Job): Unit

  protected def preemptTaskGroupAndAffectedFlavorPartOfJob(tg: TaskGroup): Unit = {
    val now = simulator.currentTime()
    val job = tg.job.get
    val oldPreemptCounter = tg.preemptionCounter

    val jobFlavorWasChosenBefore = job.flavorHasBeenChosen
    val jobWasIncBefore = job.statisticsInpChosen

    // preempt all allocations, which are still active, and which overlap with the given task group's flavor
    for (alloc <- job.allocations) {
      // all active allocations
      if (!alloc.wasPreempted && alloc.isConsumingResources) {
        // simply everything that belongs to the switch flavor
        if (alloc.taskGroup.inOption.intersect(tg.inOption).nonEmpty) {
          // looks like there is something allocated belonging to the inp flavor
          alloc.preempt(now)
          statisticsTotalTaskPreemptions += alloc.numberOfTasksToStart
        }
      }
    }

    // revert the flavor option
    job.removeChosenOption(tg)

    if (tg.preemptionCounter == oldPreemptCounter) {
      tg.preemptionCounter += 1
      tg.job.get.preemptionCounter += 1
    }

    if (jobFlavorWasChosenBefore) {
      if (jobWasIncBefore)
        statisticsFlavorTakeInp -= 1
      else
        statisticsFlavorTakeServer -= 1
    }
  }

  def withdrawJob(oldJob: Job, issuedByFlavorSelector: Boolean = false): Unit = {
    assert(allNotFinishedJobs.remove(oldJob), s"somebody tries to withdraw a job, " +
      s"which is not in the list of not finished jobs! ${oldJob.detailedToString()}")

    // check if job withdraw is allowed
    if (!issuedByFlavorSelector)
      this match {
        case selector: SchedulerWithFlavorSelector =>
          assert(selector.flavorSelector.allowSchedulerToWithdrawJobAndResubmit,
            s"a scheduler (${this.name}, with ${selector.flavorSelector}) " +
              s"wants to withdraw a job, but this scheduler is not allowed to withdraw jobs")
        case _ =>
      }

    if (allNotScheduledJobs.contains(oldJob)) {
      allNotScheduledJobs.remove(oldJob)
    }
    val preemptedTasks = oldJob.withdrawJob(simulator.currentTime())
    statisticsTotalTaskPreemptions += preemptedTasks

    jobDoneHooks.foreach(_ (oldJob))
  }

  def isPendingJobQueueEmpty: Boolean

  /**
   * will be triggerend only! if notifyMeForeachTaskGroupWhenReady is called for a job
   */
  def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit

  def notifyMeForeachTaskGroupWhenReady(job: Job): Unit = {
    if (!notifyJobList.contains(job)) {

      var latestTime = -1
      val currentTime = simulator.currentTime()

      job.taskGroups.foreach(tg => {
        if (tg.submitted > currentTime) {
          simulator.scheduleActionWithDelay(s => {
            // Maybe the job got canceled while we waited for this TGs submission time
            if (!job.isCanceled) {

              if (tg.submitted > latestTime) {
                latestTime = tg.submitted
              }

              taskGroupNowReady(job, tg)

            }
          }, tg.submitted - currentTime)
        }
      })

      if (latestTime > 0) {
        notifyJobList.add(job)
        simulator.scheduleActionWithDelay(s => {
          notifyJobList.remove(job)
        }, latestTime - currentTime)
      }

    }
  }

  @inline final protected[scheduler] def logDetailedVerbose(str: => String): Unit =
    if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED) {
      val s: String = str
      logVerbose(s)
    }

  @inline final def logVerbose(str: => String): Unit =
    if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER)
      simulator.logDebug(s"${this.getClass.getSimpleName}: $str", forcePrint = true)


  protected def someTasksAreDoneAndFreedResources(alloc: Allocation): Unit = {
    // is scheduler is not running currently, inform about scheduling
    if (!isScheduling && !isPendingJobQueueEmpty) {
      schedule()
    }
  }

  cell.setHookInformOnResourceRelease(someTasksAreDoneAndFreedResources)

  /**
   * could be also called w/o allocations
   */
  def applyAllocationAfterThinkTime(allocations: Iterable[Allocation],
                                    thinkTime: simTime,
                                    andRunScheduleAfterwards: Boolean,
                                    runAfterThinkTime: () => Unit = () => {},
                                    ignoreIsSchedulingFlag: Boolean = false,
                                    doNotUpdateThinkTimeStatistics: Boolean = false
                                   ): Unit = {
    assert(thinkTime >= 0)

    statisticsSchedulingAttempts += 1

    if (!ignoreIsSchedulingFlag) {
      assert(isScheduling)
    }

    val desiredStartTime = thinkTime + simulator.currentTime()
    simulator.scheduleActionWithDelay(simulator => {
      val fullyScheduledJobs: mutable.Set[Job] = mutable.Set()
      statisticsPerformedAllocations += allocations.size

      for (allocation: Allocation <- allocations) {
        statisticsPerformedTaskAllocations += allocation.numberOfTasksToStart

        assert(allocation.startTime == desiredStartTime, s"scheduler tries to start " +
          s"an allocation @${allocation.startTime} before the actual think time is over @${desiredStartTime}")

        assert(allocation.taskGroup.submitted <= simulator.currentTime(), "a scheduler is trying to schedule a task group before it is intentionally submitted")

        statisticsScheduledTasks += allocation.numberOfTasksToStart

        if (allocation.wasPreempted) {
          // this might happen if the flavor selector changes the chosen flavor of a job in the time after
          // after the scheduler created this allocation, but before the allocation was "activated" .. due to thinkTime
          // In such cases, we can simply ignore the allocation
          logVerbose(s"allocation $allocation was preempted in the time frame between scheduler creates allocation " +
            s"and think time applies allocation")
        } else {

          if (allocation.job.checkIfFullyAllocated()) {
            fullyScheduledJobs += allocation.job
          }

          if (allocation.taskGroup.notStartedTasks == 0) {
            val taskGroupPlacementLatency = allocation.startTime - allocation.taskGroup.submitted

            logDetailedVerbose(s"tg ${allocation.taskGroup.shortToString()} " +
              s"with placement latency:$taskGroupPlacementLatency")
            mostRecentPlacementLatencies.append(taskGroupPlacementLatency)
            if (mostRecentPlacementLatencies.size > 1000)
              mostRecentPlacementLatencies.dropInPlace(0)
          }

          if (allocation.taskGroup.isDaemonOfJob) {
            // special case, we need to ge triggered from the job when all non daemon tasks of a job are over
          } else
          // Queue a event that will release the the acquired resources again.
            simulator.scheduleActionWithDelay(_ => {
              if (allocation.wasPreempted)
                logVerbose(s"Task done event invalid, since the task was preempted previously")
              else {

                // Inform about this event if desired
                logDetailedVerbose(s"Allocation of TaskGroup ${allocation.taskGroup} is completed. Freeing resources.")
                // Release the resources of the allocation in Scheduler
                allocation.release()

                // Check if this was the last active allocation of the job
                if (allocation.job.isDone && !allocation.job.isCanceled) {

                  // Call hooks if so...
                  jobDoneHooks.foreach(_ (allocation.job))

                  // Keep track of statistics
                  assert(allNotFinishedJobs.remove(allocation.job), s"seems like somebody removed my job ${allocation.job.detailedToString()}")
                  statisticsTotalJobsDone += 1

                  if (allocation.job.statisticsInpChosen)
                    statisticsTotalJobsDoneWithInp += 1

                }
              }
            }, allocation.taskGroup.duration)

        }

      }

      // update think time statistics
      for (job <- fullyScheduledJobs) {
        // we need to check if this job triggered already such a case... e.g. Sparrow might send multiple
        // applySchedule at the same time (from different rpc calls from serer), which ends up checking the some job multiple times...
        if (allNotScheduledJobs.remove(job)) {
          statisticsTotalJobsFullyScheduled += 1
          if (job.statisticsInpChosen) {
            statisticsTotalJobsFullyScheduledWithInp += 1
          }
          statisticsTotalJobQueueTime += (simulator.currentTime() - job.submitted)
        }
      }

      if (!doNotUpdateThinkTimeStatistics) {
        if (allocations.isEmpty) {
          statisticsTotalThinkTimeWasted += thinkTime
        } else {
          statisticsTotalThinkTime += thinkTime
        }
      }

      if (!ignoreIsSchedulingFlag) {
        isScheduling = false
      }

      // run the extra closure of the scheduler
      runAfterThinkTime()

      // run scheduler for next attempt if requested

      if (andRunScheduleAfterwards && !isPendingJobQueueEmpty) {
        schedule()
      }
    }, thinkTime)
  }

  protected def schedule(): Unit

}

object Scheduler {

  def getExpectedTotalThinkTime(scheduler: Scheduler): Long = {
    scheduler match {

      // flow based schedulers
      case sched: FlowBasedScheduler =>
        // get appx of scheduler load
        val ratioSchedLoad = (sched.statisticsScheduledTasks / sched.statisticsSchedulingAttempts).toDouble / sched.cell.numMachines.toDouble
        assert(ratioSchedLoad >= 0 && ratioSchedLoad <= 1)
        // for the high load phase, consider load% of the total tasks
        (ratioSchedLoad * sched.statisticsSchedulingAttempts * sched.thinkTimeForExpectedTasksToStart((ratioSchedLoad * sched.statisticsScheduledTasks).toLong)
          // for low load phase, consider "empty" scheduling problem
          + (1.0 - ratioSchedLoad) * sched.statisticsSchedulingAttempts * sched.thinkTimeForExpectedTasksToStart(0L)).toInt

      // queue based schedulers
      case sched: QueueBasedScheduler =>
        sched.statisticsPerformedAllocations * sched.thinkTimeForCheckedMachines((sched.cell.numServers * .10).toInt, false)
    }
  }
}

trait SchedulerWithFlavorSelector extends Scheduler {
  def jobWasUpdated(job: Job): Unit

  def flavorSelector: FlavorSelector
}

trait QueueBasedScheduler extends Scheduler {

  // normalize with a value of 5000 (we use 5000 as a base value in our codebase).. all experiments run with 5000, so no scaling applied
  private val scaledSchedulingComplexity: Double = schedulingComplexityPer100ThinkTime.toDouble / 5000.0

  def thinkTimeForCheckedMachines(checkedMachines: Int, isSwitch: Boolean): simTime = {
    // cluster size systems typically have number like:
    // Omega paper "production system’s behavior: tjob = 0.1 s and ttask = 5 ms"
    // Hydra paper / Borg 2020 Eurosys paper: $0.4-7.2$~ms per task allocation

    // our cluster has ~5000 nodes
    // scaledSchedulingComplexity is a scaling factor, for all experiments set to 1.0
    // schedulingDecisionsPer100ThinkTime gives the number of decisions per 100 ms
    // 5 % of the machines is the minimum for most schedulers, so scale the think time so that 5% accounts to 0.8ms

    val machinesProbed: Double = checkedMachines.toDouble / (if (isSwitch) cell.numSwitches else cell.numServers)

    val thinkTime = ((machinesProbed / 0.05) * scaledSchedulingComplexity * 0.8).toInt max 1

    val scaledThinkTime = (thinkTimeScaling * thinkTime).toInt max 1
    scaledThinkTime
  }

}