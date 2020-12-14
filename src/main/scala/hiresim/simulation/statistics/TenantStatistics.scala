package hiresim.simulation.statistics

import hiresim.cell.Cell
import hiresim.scheduler.Scheduler
import hiresim.shared.Logging
import hiresim.simulation.statistics.SimStats.bigDecimalFormatter
import hiresim.simulation.{ClosureEvent, SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}
import hiresim.workload.WorkloadProvider

import java.math.MathContext
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.max

object TenantStatistics {

  implicit def bigDecimalToString(bd: BigDecimal): String = bigDecimalFormatter.format(bd)

  def activate(simulator: Simulator,
               scheduler: Scheduler,
               path_jobs: String,
               path_taskgroups: String,
               write_taskgroup_details: Boolean = true,
               write_job_details: Boolean = true,
               termination_timestamp: Option[SimTypes.simTime] = None): Unit = {

    // Make sure the activation actually made sense
    assert(write_job_details || write_taskgroup_details, "TenantStatistics activation useless if nothing should be written")

    // Whether no more statistics should be taken
    var statistics_terminated = false

    // The statistics writer for task groups
    val taskgroup_statistics = new SimStatsWriter(path_taskgroups, flushBuffer = 10000)

    // Dumps all not finished task groups to the statistics log
    def onDumpNotFinishedTaskGroups(job_statistics: SimStats, simulator: Simulator): Unit = {
      if (!statistics_terminated) {

        val it = scheduler.getNotFinishedJobs
        var i = 0

        while (it.hasNext) {
          i += 1

          if (i % 30000 == 0)
            Simulator.gc()

          onJobCompleted(it.next, scheduler, simulator, job_statistics,
            taskgroup_statistics, write_job_details, write_taskgroup_details)

        }

      }
    }

    // The statistics writer for jobs
    val job_statistics = new SimStats(new SimStatsWriter(path_jobs), simulator, simDoneHook = (statistics, simulator) => {
      // Dump all not finished jobs on simulation end
      onDumpNotFinishedTaskGroups(statistics, simulator)
      // Also close the taskgroup writer here as it has no autoclose
      taskgroup_statistics.close()
    })

    // We support a termination timestamp that needs extra care...
    if (termination_timestamp.isDefined) {
      simulator.scheduleEvent(new ClosureEvent(sim => {

        // Give info
        sim.logInfo(s"Tenant statistics writer reached termination snapshot!")

        // Dump all not finished jobs on simulation end
        onDumpNotFinishedTaskGroups(job_statistics, simulator)
        // Flag as terminated
        statistics_terminated = true

        // Manually close both writers
        taskgroup_statistics.close()
        job_statistics.writer.close()

      }, time = termination_timestamp.get))
    }

    // Register a job done hook so we can dump every job when it is completed
    scheduler.registerJobDoneHook(job => {
      if (!statistics_terminated)
        onJobCompleted(job, scheduler, simulator, job_statistics,
          taskgroup_statistics, write_job_details, write_taskgroup_details)
    })

    // Write the CSV header of the taskgroup statistics
    taskgroup_statistics.addEntry(
      "JobID",
      "JobStatus",
      "FlavorInp",
      "ValidForJob",
      "TaskGroupID",
      "SubmissionTime",
      "TotalTasks",
      "TaskGroupType",
      "TasksStarted",
      "AvgQueueing",
      "PlacementLatency",
      "Duration",
      "OriginalDuration",
      "DetourInc",
      "InvolvedRacks",
      "MaxDiameter")

    // Write the CSV header of the job statistics
    job_statistics.addEntry(
      "JobID",
      "JobStatus",
      "InvolvedTGCnt",
      "TasksTotal",
      "TasksStarted",
      "AvgTaskQueueing",
      "AvgTaskDuration",
      "AvgTaskDurationOriginal",
      "InpStatus"
    )

    // Signal success to console
    Logging.verbose("Statistics initialized for tenant.")

  }

  private def onJobCompleted(job: Job, scheduler: Scheduler, simulator: Simulator,
                             job_writer: SimStats, taskgroup_writer: SimStatsWriter,
                             write_job_details: Boolean, write_taskgroup_details: Boolean): Unit = {

    val num_servers = scheduler.cell.numServers
    val num_switches = scheduler.cell.numSwitches

    //  W withdrawn
    //  D done
    //  A fully allocated
    //  O ongoing, not fully allocated
    val job_status = if (job.isCanceled) "W" else if (job.isDone) "D" else if (job.checkIfFullyAllocated()) "A" else "O"

    // Determine the inp status of this Job
    // Y <- INP chosen
    // N <- Server chosen
    // S <- Static, nothing to choose from
    val inp_status =
    if (job.statisticsInpChosen)
      "Y"
    else if (WorkloadProvider.flavorHasInp(job.allPossibleOptions))
      "N"
    else
    // This is a static job with no flavor.
      "S"

    val now = simulator.currentTime()

    val (taskgroup_to_allocations: mutable.HashMap[TaskGroup, ListBuffer[Allocation]],
    taskgroup_to_machines: mutable.HashMap[TaskGroup, Array[Int]]) = {

      val tgToAlloc: mutable.HashMap[TaskGroup, ListBuffer[Allocation]] = mutable.HashMap()
      val tg2MachineCounter: mutable.HashMap[TaskGroup, Array[Int]] = mutable.HashMap()

      for (tg <- job.taskGroups) {
        tgToAlloc.put(tg, mutable.ListBuffer.empty[Allocation])

        if (tg.notStartedTasks < tg.numTasks) {
          if (tg.isSwitch) {
            // size + 1 so that last position holds total task counter
            tg2MachineCounter.put(tg, Array.fill(num_switches + 1)(0))
          } else {
            tg2MachineCounter.put(tg, Array.fill(num_servers + 1)(0))
          }
        } else {
          tg2MachineCounter.put(tg, Array.empty)
        }
      }

      (tgToAlloc, tg2MachineCounter)
    }

    // Now parse every allocation we made
    for (alloc <- job.allocations) {
      // We consider only allocations that are not preempted
      if (!alloc.wasPreempted) {
        // The set that collects all machines where tasks of the TG  have been placed
        val machines = taskgroup_to_machines(alloc.taskGroup)
        // Remember the machines where the alloc took place
        machines(alloc.machine) += alloc.numberOfTasksToStart
        machines(machines.length - 1) += alloc.numberOfTasksToStart
        // Also make a mapping for TG -> Allocs
        taskgroup_to_allocations(alloc.taskGroup).addOne(alloc)
      }
    }

    // Accumulating task counts
    var total_task_count_started: Long = 0L
    var total_task_count: Long = 0L
    // Accumulating task queueing times
    var total_task_queueing_time: BigInt = 0
    // Accumulating durations
    var total_task_duration: BigInt = 0
    var total_task_duration_original: BigInt = 0
    // Counting the involved taskgroups
    var total_taskgroups_involved: Long = 0

    class TaskGroupStatistics(val tasks_not_started: Int,
                              val time_queueing: BigInt,
                              val lastAllocationTime: Int,
                              val affectedMachinesOfTaskGroup: mutable.BitSet,
                              val validOutgoingConnectedTaskGroupIds: mutable.HashSet[Long])
    val taskGroupStatistics: Array[TaskGroupStatistics] = Array.ofDim(job.taskGroups.length)


    // do 1st pass of all task groups and calculate connectivity metrics
    val taskGroupIdToIdx: mutable.HashMap[Long, Int] = mutable.HashMap()
    var current_taskgroup_idx = 0
    while (current_taskgroup_idx < job.taskGroups.length) {
      val taskGroup = job.taskGroups(current_taskgroup_idx)
      // We consider only those TaskGroups which are submitted and that need to be considered for scheduling
      if ((taskGroup.submitted <= now) && job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)) {
        taskGroupIdToIdx(taskGroup.id) = current_taskgroup_idx
        // Check the allocations to see how many tasks we started
        var tasks_not_started: Int = taskGroup.numTasks
        var time_queueing: BigInt = BigInt(0)
        var lastAllocationTime: Int = -1

        var affectedMachinesOfTaskGroup: mutable.BitSet = mutable.BitSet()

        for (alloc <- taskgroup_to_allocations(taskGroup)) {
          tasks_not_started -= alloc.numberOfTasksToStart
          time_queueing += BigInt(alloc.startTime - taskGroup.submitted) * alloc.numberOfTasksToStart
          if (alloc.startTime > lastAllocationTime) {
            lastAllocationTime = alloc.startTime
          }

          affectedMachinesOfTaskGroup.add(alloc.machine)
        }

        // Also take all those TaskGroups into account that are still awaiting placement
        time_queueing += (BigInt(now - taskGroup.submitted) * taskGroup.notStartedTasks)

        val validOtherTgs: mutable.HashSet[Long] = mutable.HashSet()

        for (otherTg: TaskGroup <- taskGroup.outgoingConnectedTaskGroups) {
          if ((otherTg.submitted <= now) && job.checkIfTaskGroupIsDefinitelyInFlavorSelection(otherTg)) {
            validOtherTgs.add(otherTg.id)
          }
        }

        taskGroupStatistics(current_taskgroup_idx) = new TaskGroupStatistics(tasks_not_started = tasks_not_started,
          time_queueing = time_queueing,
          lastAllocationTime = lastAllocationTime,
          affectedMachinesOfTaskGroup = affectedMachinesOfTaskGroup,
          validOutgoingConnectedTaskGroupIds = validOtherTgs)
      }
      current_taskgroup_idx += 1
    }

    // do 2nd pass of all task groups and perform aggregate statistics
    current_taskgroup_idx = 0
    while (current_taskgroup_idx < job.taskGroups.length) {
      val taskgroup = job.taskGroups(current_taskgroup_idx)
      // We consider only those TaskGroups which are submitted and that need to be considered for scheduling
      if ((taskgroup.submitted <= now) && job.checkIfTaskGroupMightBeInFlavorSelection(taskgroup)) {
        val stats: TaskGroupStatistics = taskGroupStatistics(current_taskgroup_idx)

        val tasks_not_started: Int = stats.tasks_not_started
        var time_queueing: BigInt = stats.time_queueing
        val lastAllocationTime: Int = stats.lastAllocationTime

        val involvedRacks: Int = if (taskgroup.isSwitch) stats.affectedMachinesOfTaskGroup.size else
          stats.affectedMachinesOfTaskGroup.map(x => scheduler.cell.lookup_ServerNoOffset2ToR(x)).toSet.size

        val allOtherConnectedServers: mutable.BitSet = mutable.BitSet()
        val allOtherConnectedSwitches: mutable.BitSet = mutable.BitSet()
        stats.validOutgoingConnectedTaskGroupIds.foreach(otherTgId => {
          val otherTg = job.taskGroups(taskGroupIdToIdx(otherTgId))
          val otherStats = taskGroupStatistics(taskGroupIdToIdx(otherTgId))
          if (otherTg.isSwitch)
            allOtherConnectedSwitches.addAll(otherStats.affectedMachinesOfTaskGroup)
          else
            allOtherConnectedServers.addAll(otherStats.affectedMachinesOfTaskGroup)
        })

        val outgoingConnectionMaxDiameter: Int = if (stats.validOutgoingConnectedTaskGroupIds.isEmpty) -1 else {
          var maxDiameter = 0
          val sourceMachines: mutable.BitSet = if (taskgroup.isSwitch) stats.affectedMachinesOfTaskGroup else {
            // we consider involved ToR switches, so increase diameter by 1
            maxDiameter += 1
            stats.affectedMachinesOfTaskGroup.map(scheduler.cell.lookup_ServerNoOffset2ToR)
          }

          maxDiameter += scheduler.cell.getMaxDistanceBetween(leftGroupSwitches = sourceMachines,
            rightGroupServers = allOtherConnectedServers,
            rightGroupSwitches = allOtherConnectedSwitches)

          maxDiameter
        }

        val detourInc: Int = {
          if (taskgroup.isSwitch) {
            if (allOtherConnectedServers.isEmpty || stats.affectedMachinesOfTaskGroup.isEmpty)
              -1
            else
              scheduler.cell.getAdditionalSwitchLevelToReachServers(switches = stats.affectedMachinesOfTaskGroup,
                servers = allOtherConnectedServers)
          } else {
            if (stats.affectedMachinesOfTaskGroup.isEmpty || allOtherConnectedSwitches.isEmpty)
              -1
            else
              scheduler.cell.getAdditionalSwitchLevelToReachServers(switches = allOtherConnectedSwitches,
                servers = stats.affectedMachinesOfTaskGroup)
          }
        }

        // The number of placed tasks
        val tasks_started: Int = taskgroup.numTasks - tasks_not_started
        // Calculate the average time the tasks had to wait to be placed
        val time_avg_queueing: BigDecimal = BigDecimal(time_queueing, MathContext.DECIMAL32) / taskgroup.numTasks

        // Accumulate total job statistics
        total_task_queueing_time += time_queueing
        total_task_count += taskgroup.numTasks
        total_task_count_started += tasks_started
        total_taskgroups_involved += 1

        // Take durations into account
        total_task_duration += taskgroup.duration * taskgroup.numTasks
        total_task_duration_original += taskgroup.statisticsOriginalDuration * taskgroup.numTasks

        val type_indicator =
          if (taskgroup.isSwitch)
            "I"
          else
            "S"

        val taskgrpup_inp_status = if (taskgroup.belongsToInpFlavor) "Y" else if (taskgroup.inAllOptions) "A" else "N"

        val taskgroup_belongs_to_job = if (job.checkIfTaskGroupMightBeInFlavorSelection(taskgroup)) "Y" else "N"

        // Dump taskgroup details if desired
        if (write_taskgroup_details) {
          // Push a entry for this TG to the disk
          taskgroup_writer.addEntry(
            // Push id tags
            job.id.toString,
            job_status,
            taskgrpup_inp_status,
            taskgroup_belongs_to_job,
            taskgroup.id.toString,
            taskgroup.submitted.toString,
            // Push the task count
            taskgroup.numTasks.toString,
            type_indicator,
            tasks_started.toString,
            // Push timed values
            time_avg_queueing.toString, // Queueing
            lastAllocationTime.toString, // PlacementLatency
            taskgroup.duration.toString,
            taskgroup.statisticsOriginalDuration.toString,
            detourInc.toString,
            involvedRacks.toString,
            outgoingConnectionMaxDiameter.toString
          )
        }

      }

      current_taskgroup_idx += 1
    }

    // Dump job statistics if desired
    if (write_job_details) {
      job_writer.addEntry(
        // Pushing the id of this job
        job.id.toString,
        job_status,
        // Pushing the involved tg count
        total_taskgroups_involved.toString, // task group id is useless
        // Pushing task counts
        total_task_count.toString, // overall tasks
        total_task_count_started.toString, // overall started
        // Average values
        BigDecimal(total_task_queueing_time) / BigDecimal(max(1, total_task_count)),
        BigDecimal(total_task_duration) / BigDecimal(max(1, total_task_count)),
        BigDecimal(total_task_duration_original) / BigDecimal(max(1, total_task_count)),
        // Dump the inp status
        inp_status,
      )
    }

  }

}
