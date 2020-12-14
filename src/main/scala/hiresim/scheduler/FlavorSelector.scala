package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.simulation.{SimTypes, Simulator}
import hiresim.tenant.{Job, TaskGroup, TaskGroupConnection}
import hiresim.workload.WorkloadProvider

import scala.collection.mutable

trait FlavorSelector {

  def allowSchedulerToWithdrawJobAndResubmit: Boolean

  val maxServerFallbackTime: SimTypes.simTime = 600000

  def jobSubmitted(job: Job)(implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Unit

  def applyServerFallback(job: Job, replaceJobAndResubmitIfNecessary: Boolean)
                         (implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Boolean = {
    applyServerFallback(job, replaceJobAndResubmitIfNecessary, issuedByFlavorSelector = false)
  }

  protected def enforceAllServerOption(job: Job): Unit = {
    val jobsInpOptions = WorkloadProvider.getInpFlavors(job.allPossibleOptions)
    val jobsServerOptions = WorkloadProvider.getServerFlavors(job.allPossibleOptions)

    job.removeChosenOption(
      toRemoveIncludedOptions = jobsInpOptions,
      toRemoveExcludedOptions = jobsServerOptions)

    job.updateChosenOption(
      newIncludedOption = jobsServerOptions,
      newExcluddedOption = jobsInpOptions)
  }


  protected def enforceAllInpOption(job: Job): Unit = {
    val jobsInpOptions = WorkloadProvider.getInpFlavors(job.allPossibleOptions)
    val jobsServerOptions = WorkloadProvider.getServerFlavors(job.allPossibleOptions)

    job.removeChosenOption(
      toRemoveIncludedOptions = jobsServerOptions,
      toRemoveExcludedOptions = jobsInpOptions)
    job.updateChosenOption(
      newIncludedOption = jobsInpOptions,
      newExcluddedOption = jobsServerOptions)
  }

  protected def applyServerFallback(job: Job, replaceJobAndResubmitIfNecessary: Boolean, issuedByFlavorSelector: Boolean)
                                   (implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Boolean = {
    if (!job.isCanceled)
      if (job.allocations.isEmpty) {
        // no allocation done so far, so simply force now to do server stuff
        // reset old INC flavor first
        if (job.flavorHasBeenChosen && job.statisticsInpChosen) {
          sched.statisticsFlavorTakeInp -= 1
          sched.statisticsFlavorTakeServer += 1
        }
        enforceAllServerOption(job)

        sched.jobWasUpdated(job)
        true
      } else if (job.excludedJobOption.isEmpty) {
        // there are some allocations, but none of them strictly sets the flavor ..
        // so then simply force server for now on
        // reset old INC flavor first

        if (job.flavorHasBeenChosen && job.statisticsInpChosen) {
          sched.statisticsFlavorTakeInp -= 1
          sched.statisticsFlavorTakeServer += 1
        }

        enforceAllServerOption(job)

        sched.jobWasUpdated(job)
        true
      } else {
        // so there are allocations which set the flavor to INC... which is bad..
        if (replaceJobAndResubmitIfNecessary) {
          // simply kill this job

          // INP is not satisfied, so withdraw and resubmit without INP
          sched.logVerbose(s"FlavorSelector triggers resubmission of non-INP flavor for job $job, " +
            s"since INP allocation was not successful")

          if (job.statisticsInpChosen) {
            sched.statisticsServerFallbackResubmit += 1
            sched.statisticsFlavorTakeInp -= 1
          }

          sched.withdrawJob(job, issuedByFlavorSelector = issuedByFlavorSelector)
          sched.jobWasUpdated(job)
          val newJob: Job = {
            // resubmit the job but only with task groups and connections part of the server flavor
            val tgMapOld2New = mutable.HashMap[TaskGroup, TaskGroup]()
            val now = sim.currentTime()
            val newTgConnection = job.arcs.filter(conn =>
              (!conn.src.belongsToInpFlavor) &&
                (!conn.dst.belongsToInpFlavor)).map(oldConn => {
              val src = tgMapOld2New.getOrElseUpdate(oldConn.src,
                oldConn.src.cloneWithNewSubmissionTime(oldConn.src.submitted, replaceWithEmptyFlavorOptions = true))
              val dst = tgMapOld2New.getOrElseUpdate(oldConn.dst,
                oldConn.dst.cloneWithNewSubmissionTime(oldConn.dst.submitted, replaceWithEmptyFlavorOptions = true))
              new TaskGroupConnection(src, dst)
            })

            // check for task groups without any connections but part of the server only flavor
            job.taskGroups.foreach(oldTg => {
              if (!oldTg.belongsToInpFlavor) {
                // did we consider this tg already?
                if (!tgMapOld2New.contains(oldTg)) {
                  // tg not yet considered, so add it
                  tgMapOld2New.put(oldTg, oldTg.cloneWithNewSubmissionTime(oldTg.submitted, replaceWithEmptyFlavorOptions = true))
                }
              }
            })

            assert(tgMapOld2New.values.nonEmpty)

            new Job(submitted = job.submitted, isHighPriority = job.isHighPriority,
              taskGroups = tgMapOld2New.values.toArray, arcs = newTgConnection)
          }
          // we should have all non-inp related task groups!
          assert(job.taskGroups.count(!_.belongsToInpFlavor) ==
            newJob.taskGroups.count(!_.belongsToInpFlavor))
          assert(!WorkloadProvider.flavorHasInp(newJob.allPossibleOptions))
          enforceAllServerOption(newJob)
          sim.addJob(newJob)

          true
        } else {
          sched.logVerbose(s"FlavorSelector would like to trigger server fallback - but job resubmit is not allowed")
          false
        }
      } else false
  }
}

class SchedulerDecidesFlavorSelector extends FlavorSelector {

  override def allowSchedulerToWithdrawJobAndResubmit: Boolean = true

  override def jobSubmitted(job: Job)(implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Unit = {}
}

class ForceInpFlavor extends FlavorSelector {

  override def allowSchedulerToWithdrawJobAndResubmit: Boolean = false

  override def jobSubmitted(job: Job)(implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Unit = {
    if (!job.flavorHasBeenChosen) {
      // if the job has an INP option
      if (WorkloadProvider.flavorHasInp(job.allPossibleOptions)) {
        enforceAllInpOption(job)
        sched.statisticsFlavorTakeInp += 1
      }
    }
  }
}

class ForceServerFlavor extends FlavorSelector {

  override def allowSchedulerToWithdrawJobAndResubmit: Boolean = false

  override def jobSubmitted(job: Job)(implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Unit = {
    if (!job.flavorHasBeenChosen) {
      // if the job has an INP option
      if (WorkloadProvider.flavorHasInp(job.allPossibleOptions)) {
        enforceAllServerOption(job)
        sched.statisticsFlavorTakeServer += 1
      }
    }
  }
}

class ForceInpButDelayedServerFallbackFlavorSelector(configuredServerFallbackDelay: Int, // if smaller 0, treat it as percentage of tg duration
                                                     val replaceJobAndResubmitIfNecessary: Boolean = true) extends FlavorSelector {

  override def allowSchedulerToWithdrawJobAndResubmit: Boolean = false

  def serverFallbackDelay(forDuration: SimTypes.simTime): SimTypes.simTime = {
    val computed = if (configuredServerFallbackDelay > 0) configuredServerFallbackDelay else ((-forDuration.toDouble / configuredServerFallbackDelay.toDouble) max 1).toInt
    computed min maxServerFallbackTime
  }

  override def jobSubmitted(job: Job)(implicit sim: Simulator, cell: Cell, sched: SchedulerWithFlavorSelector): Unit = {
    if (!job.flavorHasBeenChosen) {
      // if the job has an INP option
      if (WorkloadProvider.flavorHasInp(job.allPossibleOptions)) {
        // always force INP flavor
        enforceAllInpOption(job)
        sched.statisticsFlavorTakeInp += 1

        // get the time of the first task group which has actually INP

        val (timeOfFirstTaskGroupWithInp: Int, duration: Int) = {
          var earliest = Int.MaxValue
          var duration = 0L
          var affected = 0
          job.taskGroups.foreach(tg => {
            if (tg.belongsToInpFlavor) {
              if (tg.submitted < earliest) {
                earliest = tg.submitted
              }
              duration += tg.duration
              affected += 1
            }
          })
          assert(affected > 0, s"there is a job with no INP taskgroup, but the jobs claims there is INP!?")
          (earliest, (duration / affected).toInt)
        }
        val effectiveFallbackDelay = timeOfFirstTaskGroupWithInp - job.submitted + serverFallbackDelay(duration)

        assert(effectiveFallbackDelay > 0, s"there is something going wrong with ${job}")

        // after delay, check
        sim.scheduleActionWithDelay(delay = effectiveFallbackDelay, action = s => {
          var couldBeChanged = true
          var allocationSatisfiesPreferredFlavor = false
          if (job.checkIfFullyAllocated()) {
            couldBeChanged = false
            allocationSatisfiesPreferredFlavor = true
          } else {
            if (job.flavorHasBeenChosen && !job.statisticsInpChosen) {
              // job not fully allocated, but it is already server flavor - which is fine
            } else {
              applyServerFallback(job, replaceJobAndResubmitIfNecessary, true)
            }
          }
        })

      }
    }

  }
}
