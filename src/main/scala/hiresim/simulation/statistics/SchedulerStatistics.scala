package hiresim.simulation.statistics

import hiresim.scheduler.Scheduler
import hiresim.scheduler.flow.FlowBasedScheduler
import hiresim.simulation.statistics.SimStats.{activatePollingStatistics, bigDecimalFormatter}
import hiresim.simulation.{SimTypes, Simulator}

import scala.collection.mutable.ArrayBuffer

object SchedulerStatistics {

  implicit def bigDecimalToString(bd: BigDecimal): String = bigDecimalFormatter.format(bd)

  def activate(simulator: Simulator,
               scheduler: Scheduler,
               file_path: String,
               interval: SimTypes.simTime): Unit = {

    val scheduler_is_flow_based = scheduler.isInstanceOf[FlowBasedScheduler]

    val data_header =
      ArrayBuffer(
        "Time",
        "PastSystemMs",
        "Attempts",
        "Allocations",
        "ScheduledTasks",
        "ThinkTime",
        "ThinkTimeWasted",
        "JobsInQueue",
        "TotalJobsFullyScheduled",
        "TotalJobsFullyScheduledINP",
        "TotalJobQueueTime",
        "CountJobs",
        "CountJobsWithInp",
        "CountJobsFinished",
        "CountJobsFinishedINP",
        "CountPreemptions",
        "PendingSchedulingItems",
        "PendingJobsNotFullyScheduled",
        "PendingJobsFullyScheduled",
        "PendingJobsNotFullyScheduledPlusFutureTgSubmissions",
        "TimesServerFallbackResubmit",
        "TimesServerFlavorTaken",
        "TimesSwitchFlavorTaken"
      )

    // If this is a flow based scheduler we also want to track how many entries
    // there are in the backlog
    if (scheduler_is_flow_based)
      data_header += "BacklogSize"

    val stats = activatePollingStatistics(
      simulator,
      file_path,
      interval,
      // Providing the CSV header
      data_header.toArray
      ,
      // The dumping function
      (stat, sim) => {

        val (notFullyScheduledTillNow, fullyScheduledTillNow) = scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow

        val data_point: Seq[String] =
          Seq(
            sim.currentTime().toString,
            sim.getPassedMsSinceSimulationStart.toString,
            scheduler.statisticsSchedulingAttempts.toString,
            scheduler.statisticsPerformedAllocations.toString,
            scheduler.statisticsScheduledTasks.toString,
            scheduler.statisticsTotalThinkTime.toString,
            scheduler.statisticsTotalThinkTimeWasted.toString,
            scheduler.getNumberOfPendingJobs.toString,
            scheduler.statisticsTotalJobsFullyScheduled.toString,
            scheduler.statisticsTotalJobsFullyScheduledWithInp.toString,
            scheduler.statisticsTotalJobQueueTime.toString,
            sim.statisticsTotalJobs.toString,
            sim.statisticsTotalJobsWithInp.toString,
            scheduler.statisticsTotalJobsDone.toString,
            scheduler.statisticsTotalJobsDoneWithInp.toString,
            scheduler.statisticsTotalTaskPreemptions.toString,
            scheduler.getNumberOfPendingSchedulingObjects.toString,
            notFullyScheduledTillNow.toString,
            fullyScheduledTillNow.toString,
            scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.toString,
            scheduler.statisticsServerFallbackResubmit.toString,
            scheduler.statisticsFlavorTakeServer.toString,
            scheduler.statisticsFlavorTakeInp.toString
          )

        if (scheduler_is_flow_based)
          data_point +: scheduler.asInstanceOf[FlowBasedScheduler].getJobBacklogSize.toString

        // Write the current state to the log
        stat.addEntry(data_point: _*)
      })

    // simple check that there is no seperator in the schedulers' name
    assert(!scheduler.name.contains(stats.separator))

  }

}
