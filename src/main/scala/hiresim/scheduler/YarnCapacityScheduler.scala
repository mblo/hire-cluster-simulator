package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.{EmptyEvent, SimTypes, Simulator}
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.mutable

object YarnCapacityScheduler {

  val PlacementModeNode: Int = 0
  val PlacementModeRack: Int = 1
  val PlacementModeAny: Int = 2

  val NoPlacementAvailable: Int = -1

}

/*
    Implemented as stated in https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html
 */
class YarnCapacityScheduler(override val name: String,
                            implicit override val cell: Cell,
                            implicit override val simulator: Simulator,
                            override val thinkTimeScaling: Double = 1.0,
                            val flavorSelector: FlavorSelector,
                            val node_locality_delay: Int = 26 * 1000, // after 26s we check for new machines (in racks)
                            var recheck_delay: Int = 1000, // Should model the HeartBeat
                            var rack_locality_additional_delay: Int = -1, // If positive used for the extra delay on any mode
                            val maximum_container_assignments: Int = 4000,
                            val maximum_offswitch_assignments: Int = 1000,
                            override val schedulingComplexityPer100ThinkTime: Int = 5000,
                            val takeServerFallbackBeforeBackoff: Boolean = false,
                            val serverFallbackTimeout: SimTypes.simTime = 10000
                           ) extends Scheduler(name = name, cell = cell, simulator = simulator,
  schedulingComplexityPer100ThinkTime = schedulingComplexityPer100ThinkTime,
  thinkTimeScaling = thinkTimeScaling) with SchedulerWithFlavorSelector with QueueBasedScheduler {

  private implicit val _this: SchedulerWithFlavorSelector = this

  class YarnJobEntry(var drf_score: Int,
                     var entries: mutable.ArrayDeque[YarnSchedulingEntry],
                     var server_claimed_resources: Array[BigInt],
                     var switch_claimed_resources: Array[BigInt]) {}

  val backoffQueue: mutable.PriorityQueue[YarnSchedulingEntry] = mutable.PriorityQueue()(Ordering.by[YarnSchedulingEntry, SimTypes.simTime](_.next_check_time).reverse)
  val jobToDataMapping: mutable.Map[Job, YarnJobEntry] = mutable.HashMap()
  val jobScores: Array[mutable.ArrayDeque[Job]] = Array.fill(SimulationConfiguration.PRECISION.toInt)(mutable.ArrayDeque[Job]())
  val taskGroupLocalAllocations: mutable.Map[TaskGroup, mutable.BitSet] = mutable.Map[TaskGroup, mutable.BitSet]()

  def getDominantResourceScore(job: Job): Int = {

    val claimed = jobToDataMapping(job)
    var max_share: Double = Double.MinValue

    for (res_switch <- 0 until cell.resourceDimSwitches)
      max_share = max_share max ((BigDecimal(claimed.switch_claimed_resources(res_switch)) / BigDecimal(cell.maxCapacitySwitches(res_switch))).toDouble)

    for (res_serv <- 0 until cell.resourceDimServer)
      max_share = max_share max ((BigDecimal(claimed.server_claimed_resources(res_serv)) / BigDecimal(cell.maxCapacityServers(res_serv))).toDouble)

    (max_share * SimulationConfiguration.PRECISION).toInt

  }

  def getJobData(job: Job): YarnJobEntry = {
    jobToDataMapping.getOrElseUpdate(job, {

      val entry = new YarnJobEntry(
        drf_score = 0,
        entries = mutable.ArrayDeque(),
        server_claimed_resources = Array.fill(cell.resourceDimServer)(BigInt(0)),
        switch_claimed_resources = Array.fill(cell.resourceDimSwitches)(BigInt(0))
      )

      jobScores(entry.drf_score).append(job)

      entry
    })
  }

  override def getNumberOfPendingJobs: Int = allNotFinishedJobs.size

  override def isPendingJobQueueEmpty: Boolean = backoffQueue.isEmpty && !jobToDataMapping.exists(_._2.entries.nonEmpty)

  override def addJobAndSchedule(job: Job): Unit = {

    flavorSelector.jobSubmitted(job)
    notifyMeForeachTaskGroupWhenReady(job)

    allNotFinishedJobs.add(job)
    allNotScheduledJobs.add(job)

    val data = getJobData(job)

    job.taskGroups.foreach(tg => {

      if (tg.submitted <= simulator.currentTime())
        data.entries.append(new YarnSchedulingEntry(tg, timeAdded = simulator.currentTime()))

    })

  }

  private def addTaskGroupAfterSubmission(taskgroup: TaskGroup,
                                          data: YarnJobEntry): Unit = {

    val job = taskgroup.job.get

    if (taskgroup.submitted <= simulator.currentTime() && // TaskGroup must be ready
      taskgroup.notStartedTasks > 0 && // Must have not started tasks
      !job.isCanceled && // Job must still be valid
      job.checkIfTaskGroupMightBeInFlavorSelection(taskgroup) // Must be in flavor selection
    ) {
      data.entries.append(new YarnSchedulingEntry(
        taskGroup = taskgroup,
        timeAdded = simulator.currentTime())
      )
    }

  }

  override def jobWasUpdated(job: Job): Unit = {
    if (!job.isCanceled) {

      val data = getJobData(job)

      for (tg <- job.taskGroups)
        addTaskGroupAfterSubmission(tg, data)

    } else {
      jobToDataMapping.remove(job)
      job.taskGroups.foreach(tg => {
        onDropAllocationTargetBuffer(tg)
      })
    }

  }

  override def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit = {
    assert(taskGroup.submitted == simulator.currentTime())
    addTaskGroupAfterSubmission(taskGroup, getJobData(job))
  }


  override protected def someTasksAreDoneAndFreedResources(alloc: Allocation): Unit = {
    onAllocationLifeCycleEvent(alloc, destroying = true)
  }

  private def onAllocationLifeCycleEvent(allocation: Allocation, destroying: Boolean): Unit = {

    val tg = allocation.taskGroup
    val job = tg.job.get

    // Flag cached load entry as dirty
    if (tg.isSwitch)
      switch_load(allocation.machine) = -1
    else
      server_load(allocation.machine) = -1

    // Only update the resources if we still need it
    if (jobToDataMapping.contains(job)) {

      // Trigger dominant resource share upgrade for that user/job
      val job_data = jobToDataMapping(job)
      jobScores(job_data.drf_score) -= job

      val multiplier =
        if (destroying)
          -1
        else
          1

      if (tg.isSwitch) {

        val resources = tg.resources.asInstanceOf[SwitchResource]
        val switch_claimed = job_data.switch_claimed_resources

        for (dim <- 0 until cell.resourceDimSwitches)
          switch_claimed(dim) += (resources.numericalResources(dim) * multiplier)

      } else {

        val resources = tg.resources.asInstanceOf[ServerResource].numericalResources
        val server_claimed = job_data.server_claimed_resources

        for (dim <- 0 until cell.resourceDimServer)
          server_claimed(dim) += (resources(dim) * multiplier)

      }

      // Maybe the job was already removed, because fully scheduled
      job_data.drf_score = getDominantResourceScore(job)
      jobScores(job_data.drf_score) += job

    }

  }

  private def onDropAllocationTargetBuffer(tg: TaskGroup): Unit = {
    taskGroupLocalAllocations.remove(tg).foreach(claimed_set => {
      if (tg.isSwitch)
        claimed_set.foreach(id => switch_load(id) = -1)
      else
        claimed_set.foreach(id => server_load(id) = -1)
    })
  }

  private val server_load: Array[Double] = Array.fill(cell.numServers)(0.0)
  private val switch_load: Array[Double] = Array.fill(cell.numSwitches)(0.0)

  /**
   * The only implemented policy is
   * https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/scheduler/placement/ResourceUsageMultiNodeLookupPolicy.java
   * which ranks the machines by their resource usage. I.e. low utilized machines will be taken into consideration first.
   */
  private def getTargetMachine(taskGroup: TaskGroup,
                               available: Iterable[NodeID]): (NodeID, Int) = {

    var checkedMachines = 0

    def getPlacementScore(node: NodeID): Double = {

      val target_array =
        if (taskGroup.isSwitch)
          switch_load
        else
          server_load

      var score = target_array(node)

      if (score == -1) {

        // The load of that machine
        val load = cell.getMachineLoad(node, taskGroup.isSwitch, SimulationConfiguration.PRECISION)
        // Average result
        score = load.sum.toDouble / load.length.toDouble
        // Save that load for later
        target_array(node) = score

      }

      assert(score > -1, s"Score was ${score} but should be >= 0!")
      score
    }

    var current_best_target: NodeID = -1
    var current_best_score: Double = SimulationConfiguration.PRECISION.toDouble
    var found_lowest_score = false

    val iterator = available.iterator

    while (iterator.hasNext && !found_lowest_score) {

      val option: NodeID = iterator.next()
      checkedMachines += 1

      if (cell.checkMaxTasksToAllocate(taskGroup, option) > 0) {

        val score = getPlacementScore(option)

        if (current_best_target == -1 || score < current_best_score) {
          current_best_target = option
          current_best_score = getPlacementScore(option)

          // If we found a machine with the lowest score we can just skip the rest
          if (score == 0)
            found_lowest_score = true

        }

      }
    }

    (current_best_target, checkedMachines)
  }

  // Register scheduling as a post timestep hook at this point.
  simulator.registerPostSimulationStepHook(_ => onSchedulingRoundAboutToFinish())

  private def onSchedulingRoundAboutToFinish(): Unit = {
    if (!isScheduling && !isPendingJobQueueEmpty && cell.currentServerLoadMax().toDouble <= SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE)
      runSchedulingLogic()
  }

  override protected def schedule(): Unit = {}

  /*
      The CapacityScheduler uses Delay Scheduling as proposed by
      https://cs.stanford.edu/~matei/papers/2010/eurosys_delay_scheduling.pdf

      The Intuition:
      Each job begins at a “locality level” of 0, where it can only launch
      node-local tasks. If it waits at least W1 seconds,it goes to locality
      level 1 and may launch rack-local tasks. If it waits a further W2 seconds,
      it goes to level 2 and may launch off-rack tasks.
   */
  private def runSchedulingLogic(): Unit = {
    assert(!isScheduling)
    isScheduling = true

    // Push in all the TG's that become active again
    while (backoffQueue.nonEmpty && backoffQueue.head.next_check_time <= simulator.currentTime()) {
      val poppedFromBackOff = backoffQueue.dequeue()
      val job = poppedFromBackOff.taskGroup.job.get

      if (!job.isCanceled)
        getJobData(job).entries.append(poppedFromBackOff)

    }

    val allocations: mutable.ListBuffer[Allocation] = mutable.ListBuffer()

    var remaining_local_assignments = maximum_container_assignments
    var remaining_other_assignments = maximum_offswitch_assignments

    var maxDecisionsToBeDone: Int = schedulingComplexityPer100ThinkTime max 1

    var actualAllocatedTasks = 0

    var consumedThinkTime = 0

    def doResourceAllocation(taskGroup: TaskGroup, numTasks: Int, machine: NodeID, checkedMachines: Int): Unit = {

      val thinkTime = thinkTimeForCheckedMachines(checkedMachines = checkedMachines, taskGroup.isSwitch)
      consumedThinkTime += thinkTime
      val taskStartTime: SimTypes.simTime = simulator.currentTime() + thinkTime

      val job = taskGroup.job.get
      val jobWasChosenBeforeNewAllocation = job.flavorHasBeenChosen

      val allocation = job.doResourceAllocation(
        taskGroup = taskGroup,
        numberOfTasksToStart = numTasks,
        machine = machine,
        cell = cell,
        startTime = taskStartTime
      )

      allocations += allocation
      maxDecisionsToBeDone -= numTasks
      actualAllocatedTasks += numTasks

      onAllocationLifeCycleEvent(allocation, destroying = false)

      if (job.flavorHasBeenChosen && !jobWasChosenBeforeNewAllocation) {
        if (job.statisticsInpChosen)
          statisticsFlavorTakeInp += 1
        else
          statisticsFlavorTakeServer += 1
      }

      maxDecisionsToBeDone = 0
    }

    def getAdditionalAnyDelay(tg: TaskGroup): Int = {
      if (rack_locality_additional_delay > 0)
        rack_locality_additional_delay
      else
      //  the number of missed opportunities for assigning off-switch containers is
      //  calculated based on the formula L * C / N, where L is number of locations
      //  (nodes or racks) specified in the resource request, C is the number of requested
      //  containers, and N is the size of the cluster.
      // => We only have number of requested containers!
        (1000 * tg.numTasks) / cell.numServers
    }

    def onDoSchedulingForTaskGroup(placing: YarnSchedulingEntry, taskGroup: TaskGroup): Unit = {

      // Flag no changes happened. This will change later
      placing.allocated = false
      // Delta between last check and now
      val delta_last_checked: Int = (simulator.currentTime() - placing.last_checked).toInt
      // Set last checked to now
      placing.last_checked = simulator.currentTime()


      // Depending on where we are currently trying to place the next Task
      placing.level match {

        // Same node only
        case YarnCapacityScheduler.PlacementModeNode if remaining_local_assignments > 0 =>

          // First time we see this TaskGroup. Find a good starting node and kick of first tasks
          if (!taskGroupLocalAllocations.contains(taskGroup)) {

            val (machine: NodeID, checkedMachines: Int) = getTargetMachine(taskGroup, if (taskGroup.isSwitch)
            // Only select ToR switches if
              cell.torSwitches
            else
              cell.servers.indices
            )

            if (machine != -1) {

              // Remember this decision as we want to schedule here again maybe later
              val affected_machines = mutable.BitSet()
              affected_machines += machine

              taskGroupLocalAllocations(taskGroup) = affected_machines

              // How many machines can we kick off?
              var run = cell.checkMaxTasksToAllocate(taskGroup, machine)
              run = math.min(run, remaining_local_assignments)

              // There is free space. Do the allocation.
              doResourceAllocation(taskGroup, run, machine, checkedMachines)

              remaining_local_assignments -= run
              placing.allocated = true

            }
            // There are already nodes that are running our Tasks of our TaskGroups
          } else {

            // The nodes that run our tasks
            val affected = taskGroupLocalAllocations(taskGroup)
            val (machine: NodeID, checkedMachines: Int) = getTargetMachine(taskGroup, affected)

            if (machine != -1) {

              var run = cell.checkMaxTasksToAllocate(taskGroup, machine)
              run = math.min(run, remaining_local_assignments)

              // There is free space. Do the allocation.
              doResourceAllocation(taskGroup, run, machine, checkedMachines)

              remaining_local_assignments -= run
              placing.allocated = true
            }

          }

          // We allocated reset waiting time
          if (placing.allocated) {
            placing.waiting = 0

            // We did not manage to allocate :( increase waiting time
          } else {

            placing.waiting += delta_last_checked

            if (placing.waiting >= node_locality_delay) {
              // Only change placement mode if we have decided on some placement!
              if (taskGroupLocalAllocations.contains(taskGroup)) {

                // Switches can not be placed into Rack, so we skip this phase
                if (taskGroup.isSwitch)
                  placing.level = YarnCapacityScheduler.PlacementModeAny
                else
                // For servers try to find a spot in the Rack next.
                  placing.level = YarnCapacityScheduler.PlacementModeRack

                logVerbose(s"TaskGroup ${taskGroup} was moved to placement stage ${placing.level} as it failed placing for ${placing.waiting}ms")

              }
            }

          }

        // Same rack only. We only need to consider servers here. Switches cannot be grouped into racks
        case YarnCapacityScheduler.PlacementModeRack if remaining_local_assignments > 0 =>

          assert(!taskGroup.isSwitch, "Can not place switches into racks...")
          assert(taskGroupLocalAllocations.contains(taskGroup), "Rack has not yet been decided")

          // Where is that TaskGroup already running?
          val affected_machines = taskGroupLocalAllocations(taskGroup)
          // This collection will hold oll the nodes in every affected rack that is not yet affected
          val options: mutable.BitSet = mutable.BitSet()

          // First add all nodes of every affected rack
          for (machine <- affected_machines)
            options.addAll(cell.lookup_ToR2MembersWithoutOffset(cell.lookup_ServerNoOffset2ToR(machine)))

          // Then remove the ones we already have
          for (machine <- affected_machines)
            options -= machine

          // Get an iterable over the nodes that can host our TG in ordered fashion (low resource usage to high usage)
          val (machine: NodeID, checkedMachines: Int) = getTargetMachine(taskGroup, options)

          // There is a node, we can allocate to
          if (machine != -1) {
            var run = cell.checkMaxTasksToAllocate(taskGroup, machine)
            run = math.min(run, remaining_local_assignments)

            // Start one Task at that machine in the Rack
            doResourceAllocation(taskGroup, run, machine, checkedMachines)

            remaining_local_assignments -= run
            affected_machines += machine
            placing.waiting = 0
            // Reset level. Maybe we can push more locality on this node now
            placing.level = 0

          } else {

            placing.waiting += delta_last_checked

            if (placing.waiting >= node_locality_delay + getAdditionalAnyDelay(placing.taskGroup)) {
              logVerbose(s"TaskGroup ${taskGroup} was moved to ANY placement stage as it failed placing for ${placing.waiting}ms")
              placing.level = YarnCapacityScheduler.PlacementModeAny
            }

          }

        // Anywhere
        case YarnCapacityScheduler.PlacementModeAny if remaining_other_assignments > 0 =>

          // There are no placement constraints here. Put it anywhere
          val (machine: NodeID, checkedMachines: Int) = getTargetMachine(taskGroup, if (taskGroup.isSwitch)
            cell.switches.indices
          else
            cell.servers.indices
          )

          // If there is a machine able to host it
          if (machine != -1) {

            var run = cell.checkMaxTasksToAllocate(taskGroup, machine)
            run = math.min(run, remaining_other_assignments)

            // Start one Task at that machine
            doResourceAllocation(taskGroup, run, machine, checkedMachines)

            remaining_other_assignments -= run
            taskGroupLocalAllocations(taskGroup) += machine
            // Go back to level 0! Maybe we can push more locality on the already affected nodes!
            placing.level = 0
            placing.waiting = 0
            placing.times_failed_in_any = 0

          } else {
            placing.times_failed_in_any += 1
            logVerbose(s"Skipped TaskGroup ${taskGroup} in placement stage ANY as no machine is currently free to host it")
          }

        case _ =>
        // Fallthrough case

      }

    }

    val putback: mutable.ArrayDeque[YarnSchedulingEntry] = mutable.ArrayDeque()
    var current_score_idx = 0

    // Are there more entries AND are we allowed to make more placements?
    while ((current_score_idx < SimulationConfiguration.PRECISION) && (maxDecisionsToBeDone > 0) && (remaining_local_assignments > 0 || remaining_other_assignments > 0)) {

      // The current Entry to schedule
      val placing: YarnSchedulingEntry = {

        var selected_entry: YarnSchedulingEntry = null

        while (selected_entry == null && (current_score_idx < SimulationConfiguration.PRECISION)) {
          val jobs_with_current_score = jobScores(current_score_idx)

          if (jobs_with_current_score.nonEmpty) {
            val next_job = jobs_with_current_score.removeHead()
            // Is this entry still valid?
            if (jobToDataMapping.contains(next_job)) {
              val job_data = jobToDataMapping(next_job)
              // R
              if (job_data.entries.nonEmpty)
                selected_entry = job_data.entries.removeHead()
              // If that job has still data, we might want to add it back
              jobScores(current_score_idx) += next_job
            }
          }

          current_score_idx += 1
        }

        selected_entry

      }

      if (placing != null) {

        val taskGroup = placing.taskGroup
        val job = taskGroup.job.get

        val waiting_ok =
          if (placing.last_checked == -1)
          // First time we encounter this boy
            true
          else
            (simulator.currentTime() - placing.last_checked) >= recheck_delay

        val in_flavor =
          if (job.flavorHasBeenChosen)
            job.checkIfTaskGroupIsDefinitelyInFlavorSelection(taskGroup)
          else
            job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)

        val can_schedule = waiting_ok && in_flavor && taskGroup.notStartedTasks > 0 && !job.isDone

        if (can_schedule)
          onDoSchedulingForTaskGroup(placing, taskGroup)

        // If there are more tasks and we are in flavor, we'll put this TG back into the pending queue
        if (taskGroup.notStartedTasks > 0 && in_flavor && !job.isCanceled) {
          putback.append(placing)
        } else {
          // This TG won't show up again... Drop entry for it
          onDropAllocationTargetBuffer(taskGroup)
        }

        if (job.checkIfFullyAllocated()) {
          jobToDataMapping.remove(job)
        }

      }

    }

    // Putback every entry that is not completed
    putback.foreach(entry => {
      if (takeServerFallbackBeforeBackoff && allocations.isEmpty &&
        flavorSelector.allowSchedulerToWithdrawJobAndResubmit &&
        entry.taskGroup.job.get.statisticsInpChosen &&
        entry.taskGroup.isSwitch &&
        (entry.next_check_time - entry.timeAdded) > serverFallbackTimeout) {
        // trigger server fallback
        flavorSelector.applyServerFallback(entry.taskGroup.job.get, replaceJobAndResubmitIfNecessary = true)
      } else {
        val next_check_delay =
        // We are in ANY mode, there is no increase in level we can wait for
          if (entry.level == YarnCapacityScheduler.PlacementModeAny)
            recheck_delay * (entry.times_failed_in_any + 1 max 10)
          else {

            val next_stage_delay =
              if (entry.level == YarnCapacityScheduler.PlacementModeNode)
                node_locality_delay
              else
                node_locality_delay + getAdditionalAnyDelay(entry.taskGroup)

            // Next check will either be at the next check delay or the next level increase
            recheck_delay min ((next_stage_delay - entry.waiting) max 1)
          }

        entry.next_check_time = simulator.currentTime() + next_check_delay
        backoffQueue.enqueue(entry)
      }
    })

    //println(f"ActualDecisions: ${actualAllocatedTasks}")
    if (allocations.nonEmpty) {
      assert(consumedThinkTime > 0, "We have some allocations, but no think time!")
      applyAllocationAfterThinkTime(allocations = allocations, thinkTime = consumedThinkTime, andRunScheduleAfterwards = true)
    } else if (consumedThinkTime > 0) {
      applyAllocationAfterThinkTime(allocations = allocations, thinkTime = consumedThinkTime, andRunScheduleAfterwards = true)
    } else {
      // we did not check any machine, so there is no think time, so
      logVerbose("there was nothing to be checked for scheduling")
      isScheduling = false

      // shall we set a recheck timer?
      if (!isPendingJobQueueEmpty) {
        val delay_by_recheck = simulator.currentTime() + recheck_delay
        val delay =
          if (backoffQueue.isEmpty)
            delay_by_recheck
          else
            delay_by_recheck min backoffQueue.head.next_check_time
        simulator.scheduleEvent(new EmptyEvent(delay))
      }
    }
  }

}

class YarnSchedulingEntry(val taskGroup: TaskGroup,
                          val timeAdded: SimTypes.simTime,
                          var waiting: Int = -1,
                          var last_checked: Long = 0,
                          var next_check_time: SimTypes.simTime = 0,
                          var level: Int = 0,
                          var times_failed_in_any: Int = 0,
                          var allocated: Boolean = false) {}

