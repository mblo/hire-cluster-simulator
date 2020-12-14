package hiresim.tenant

import hiresim.cell.Cell
import hiresim.cell.machine.{Identifiable, ServerResource, SwitchProps, SwitchResource}
import hiresim.scheduler.flow.solver.graph.FlowArc
import hiresim.simulation.SimTypes
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.Graph.NodeID
import hiresim.workload.WorkloadProvider

import scala.collection.{immutable, mutable}

object Graph {

  /** Node ID */
  type NodeID = Int

  /** Arcs type */
  type Arcs = mutable.Seq[mutable.Map[NodeID, FlowArc]]
}


/**
 *
 * @param arcs the set of arcs connecting nodes.
 *             The `Vector` index represents the source node ID.
 *             Every node then has a Map[NodeID, Arc] indicating the
 *             destination node ID and the corresponding Arc object,
 *             respectively.
 */
class Job(val submitted: SimTypes.simTime,
          val isHighPriority: Boolean,
          val taskGroups: Array[TaskGroup],
          val arcs: Array[TaskGroupConnection]) extends Ordered[Job] with Identifiable {

  var mostRecentTgSubmissionTime: SimTypes.simTime = -1

  var lastPreemptionTime: SimTypes.simTime = -1
  var preemptionCounter: Int = 0

  /** Feasibility check */
  if (SimulationConfiguration.SANITY_CHECK_JOB_CHECK_RESOURCE_REQ) {
    assert(submitted >= 0, s"$this submitted at a negative time instant: $submitted.")
    assert(taskGroups.nonEmpty, s"$this contains no task groups.")

    // valid connections?
    for (arc: TaskGroupConnection <- arcs) {
      assert(taskGroups.contains(arc.dst))
      assert(taskGroups.contains(arc.src))
    }


    for (taskGroup: TaskGroup <- taskGroups) {
      assert(taskGroup.duration > 0, s"$taskGroup has a negative duration: $taskGroup.")

      taskGroup.resources match {
        case r: SwitchResource =>
        case r: ServerResource =>

          /** Check whether there's at least one positive resource requirement */
          assert(r.numericalResources.exists(_ > 0),
            s"$taskGroup has no positive numerical resource requirements.")
      }


    }
  }

  val allocations: mutable.ListBuffer[Allocation] = mutable.ListBuffer()

  @inline def chosenJobOption: immutable.BitSet = _chosenJobOption

  @inline def excludedJobOption: immutable.BitSet = _excludedJobOption

  private var _chosenJobOption: immutable.BitSet = immutable.BitSet()
  private var _excludedJobOption: immutable.BitSet = immutable.BitSet()

  val (allPossibleOptions: immutable.BitSet,
  taskGroupsCountWithFlavorOptions: Int,
  flavorDecisionPossibleSince: SimTypes.simTime) = {
    // here we built also the outgoingConnectedTaskGroups data structure
    val tmpTaskGroupToOutgoing: mutable.Map[TaskGroup, mutable.ListBuffer[TaskGroup]] = mutable.Map()
    arcs.foreach(e => {
      if (!tmpTaskGroupToOutgoing.contains(e.src)) {
        tmpTaskGroupToOutgoing(e.src) = mutable.ListBuffer()
      }
      tmpTaskGroupToOutgoing(e.src).addOne(e.dst)
    })

    var firstFlavorDecision: SimTypes.simTime = Int.MaxValue
    var tgInFlavor: Int = 0
    var allOpt = immutable.BitSet()
    var allNotOpt = immutable.BitSet()
    for (tg <- taskGroups) {
      allOpt |= tg.inOption
      allNotOpt |= tg.notInOption

      if (!tg.inAllOptions) {
        tgInFlavor += 1
        firstFlavorDecision = firstFlavorDecision min tg.submitted
      }

      if (tmpTaskGroupToOutgoing.contains(tg))
        tg.outgoingConnectedTaskGroups = tmpTaskGroupToOutgoing(tg).toArray
    }

    assert(allOpt.intersect(allNotOpt).size == allOpt.size, "job is corrupted - " +
      s"all flavor options (${allOpt}) of all tgs must be present also as not-flavor options (${allNotOpt})")

    (allOpt, tgInFlavor, firstFlavorDecision)
  }

  def allSwitchProperties: SwitchProps = {
    var out = SwitchProps.none
    for (tg <- taskGroups) {
      if (tg.isSwitch) {
        out = out.or(tg.resources.asInstanceOf[SwitchResource].properties)
      }
    }
    out
  }

  @inline def statisticsInpChosen: Boolean = statisticsInpChosenFlag

  private var statisticsInpChosenFlag: Boolean = false

  val selfref = Some(this)
  taskGroups.foreach(tg => {
    assert(tg.job.isEmpty)
    tg.job = selfref
  })

  protected var isDoneFlag = false
  protected var wasCanceledFlag = false
  protected var runningNonDaemonTasks = 0
  protected var fullyAllocatedFlag = false

  protected var notScheduledTasks: Long = 0
  protected var totalTasksCount: Long = 0

  private var flavorHasBeenChosenFlag: Boolean = false

  def detailedToString(): String = {
    var totalTasks = 0
    var tgString = ""
    for (tg <- taskGroups) {
      totalTasks += tg.numTasks

      tgString += s"\n\t " + tg.detailedToString
    }

    s"Job($id;@$submitted;${taskGroups.size}TGs;${taskGroups.foldLeft(0)((c, tg) => c + tg.numTasks)}Ts;" +
      s"possible:${allPossibleOptions},chosen:${_chosenJobOption},canceled:$wasCanceledFlag,done:$isDoneFlag,allocated:$fullyAllocatedFlag;" +
      s"preempted:${preemptionCounter}, last:${lastPreemptionTime}, recentTgSub:${mostRecentTgSubmissionTime} tgs:\t${tgString})"
  }

  protected def refreshCacheOfFlagsAndTaskCount(): Unit = {
    notScheduledTasks = 0
    totalTasksCount = 0

    // do we have a decision for all possible flavor bits?
    flavorHasBeenChosenFlag = _chosenJobOption.union(_excludedJobOption).size == allPossibleOptions.size
    // is there any INP in the chosen flavor?
    statisticsInpChosenFlag = WorkloadProvider.flavorHasInp(_chosenJobOption)

    for (tg <- taskGroups) {
      if (checkIfTaskGroupMightBeInFlavorSelection(tg)) {
        notScheduledTasks += tg.notStartedTasks
        totalTasksCount += tg.numTasks
      }
    }

    assert(totalTasksCount >= notScheduledTasks)
  }

  def getNotScheduledTasksCount: Long = notScheduledTasks

  def getScheduledTasksCount: Long = totalTasksCount - notScheduledTasks

  def getTasksCount: Long = totalTasksCount


  @inline final def flavorHasBeenChosen: Boolean = flavorHasBeenChosenFlag

  def isDone: Boolean = isDoneFlag

  def isCanceled: Boolean = wasCanceledFlag


  /**
   * return true if the given task group is definitely part of the job, no matter how future allocations look like
   *
   * @param taskgroup the taskgroup to check
   * @return true if the provided taskgroup is definitely in the flavor selection
   */
  def checkIfTaskGroupIsDefinitelyInFlavorSelection(taskgroup: TaskGroup): Boolean = {
    if (taskgroup.inAllOptions) {
      true
    } else {
      // there must be any other decision already
      _chosenJobOption.nonEmpty &&
        // tg exclude does not conflict with chosen options
        taskgroup.notInOption.intersect(_chosenJobOption).isEmpty &&
        // tg does not conflict with chosen excluded option
        taskgroup.inOption.intersect(_excludedJobOption).isEmpty &&
        // tg overlaps with already chosen flavor so that it is definitely part of job
        taskgroup.inOption.intersect(_chosenJobOption).nonEmpty &&
        // all excluded options of task group are also flagged as excluded in the
        taskgroup.notInOption.intersect(_excludedJobOption).size == taskgroup.notInOption.size
    }
  }

  /**
   *
   * @return true if the given task group belongs to the common set of a job (i.e. to the set of TGs that need to be scheduled anyway)
   *         true if the given task group belongs to the selected flavor
   *         false if the chosen flavor does not match with tg, or if the flavor selection is still not finished, and this TG may not belong to the job
   */
  def checkIfTaskGroupMightBeInFlavorSelection(tg: TaskGroup): Boolean = {
    if (tg.inAllOptions) {
      true
    } else {
      (// tg exclude does not conflict with chosen options
        tg.notInOption.intersect(_chosenJobOption).isEmpty &&
          // tg does not conflict with chosen excluded option
          tg.inOption.intersect(_excludedJobOption).isEmpty)
    }
  }

  @inline def updateChosenOption(taskGroup: TaskGroup): Unit =
    updateChosenOption(
      newIncludedOption = taskGroup.inOption,
      newExcluddedOption = taskGroup.notInOption)

  /**
   * does consider previous chosen option; if previously only 1 option is allowed, it cannot be changed. Otherwise, it takes the intersection of new and old selection
   */
  def updateChosenOption(newIncludedOption: immutable.BitSet,
                         newExcluddedOption: immutable.BitSet): Unit = {
    if (newIncludedOption.nonEmpty || newExcluddedOption.nonEmpty) {
      if (SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK) {
        assert(newExcluddedOption.intersect(_chosenJobOption).isEmpty)
        assert(newIncludedOption.intersect(_excludedJobOption).isEmpty)
        assert(newIncludedOption.intersect(newExcluddedOption).isEmpty)
      }

      _chosenJobOption = _chosenJobOption.union(newIncludedOption)
      _excludedJobOption = _excludedJobOption.union(newExcluddedOption)

      assert(_excludedJobOption.intersect(_chosenJobOption).isEmpty)

      refreshCacheOfFlagsAndTaskCount()
    }
  }

  @inline def removeChosenOption(taskGroup: TaskGroup): Unit =
    removeChosenOption(
      toRemoveIncludedOptions = taskGroup.inOption,
      toRemoveExcludedOptions = taskGroup.notInOption)

  def removeChosenOption(toRemoveIncludedOptions: immutable.BitSet,
                         toRemoveExcludedOptions: immutable.BitSet): Unit = {
    if (toRemoveExcludedOptions.nonEmpty || toRemoveIncludedOptions.nonEmpty) {
      _chosenJobOption = _chosenJobOption.--(toRemoveIncludedOptions)
      _excludedJobOption = _excludedJobOption.--(toRemoveExcludedOptions)

      allocations
        .foreach(alloc => if (alloc.isConsumingResources)
          if (!alloc.taskGroup.inAllOptions) {

            if (alloc.taskGroup.inOption.intersect(_chosenJobOption).isEmpty)
              throw new AssertionError("not allowed to replace the chosen options " +
                "if a previously made allocation would be then not valid anymore")
          })

      refreshCacheOfFlagsAndTaskCount()
    }
  }

  /**
   * this updates also the job option selection accordingly
   */
  def doResourceAllocation(taskGroup: TaskGroup,
                           numberOfTasksToStart: Int,
                           machine: Int,
                           machineNetworkID: Int = -1,
                           cell: Cell,
                           startTime: SimTypes.simTime): Allocation = {

    if (SimulationConfiguration.SANITY_CHECKS_CELL || SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK) {
      val checkedMaxTasksToStart: NodeID = cell.checkMaxTasksToAllocate(taskGroup = taskGroup, machineId = machine)
      assert(checkedMaxTasksToStart >= numberOfTasksToStart, s"allocation is invalid - somebody tries to start $numberOfTasksToStart" +
        s" tasks on machine:$machine " +
        s"${
          if (taskGroup.isSwitch)
            cell.switches(machine).numericalResources.mkString("[", "|", "]") else
            cell.servers(machine).mkString("[", "|", "]")
        }, " +
        s"but only $checkedMaxTasksToStart are feasible for ${taskGroup.detailedToString()}")
    }

    assert(!isDoneFlag)
    assert(!wasCanceledFlag)
    assert(numberOfTasksToStart > 0)
    assert(taskGroup.notStartedTasks >= numberOfTasksToStart)
    assert(startTime >= taskGroup.submitted)

    cell.logVerbose(s"do alloc on machine $machine before res ${
      if (taskGroup.isSwitch)
        cell.switches(machine)
      else
        cell.servers(machine).mkString("|")
    }")

    val allocation = new Allocation(
      taskGroup = taskGroup,
      numberOfTasksToStart = numberOfTasksToStart,
      machine = machine,
      machineNetworkID = machineNetworkID,
      job = this,
      cell = cell,
      startTime = startTime
    )

    // very important, that we do this !!!after we created the allocation
    if (!taskGroup.inAllOptions) {
      updateChosenOption(taskGroup)

      if (SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK) {
        val oldCntTotalTask = totalTasksCount
        val oldCntToStart = notScheduledTasks
        refreshCacheOfFlagsAndTaskCount()
        assert(totalTasksCount == oldCntTotalTask)
        assert(notScheduledTasks == oldCntToStart)
      }

    } else {
      refreshCacheOfFlagsAndTaskCount()
    }

    if (!taskGroup.isDaemonOfJob)
      runningNonDaemonTasks += numberOfTasksToStart

    cell.logVerbose(s"do alloc on machine $machine after res ${if (taskGroup.isSwitch) cell.switches(machine) else cell.servers(machine).mkString("|")}")

    allocations.append(allocation)

    allocation
  }

  def allocationPreempted(allocation: Allocation, timeNow: SimTypes.simTime): Unit = {
    assert(!isDoneFlag)
    assert(!wasCanceledFlag)
    if (!allocation.taskGroup.isDaemonOfJob) {
      runningNonDaemonTasks -= allocation.numberOfTasksToStart
    }
    fullyAllocatedFlag = false
    if (timeNow != lastPreemptionTime) {
      preemptionCounter += 1
      allocation.taskGroup.preemptionCounter += 1
    }
    lastPreemptionTime = timeNow
    refreshCacheOfFlagsAndTaskCount()
  }

  def allocationReleased(allocation: Allocation): Unit = {
    assert(!isDoneFlag)
    assert(!wasCanceledFlag)
    if (!allocation.taskGroup.isDaemonOfJob) {
      runningNonDaemonTasks -= allocation.numberOfTasksToStart
      // is this the last task?
      if (checkIfFullyAllocated() && runningNonDaemonTasks == 0) {
        // this job is done
        shutdownJob()
      }
    }
  }

  def withdrawJob(timeNow: SimTypes.simTime): Long = {
    assert(!isDoneFlag)
    assert(!wasCanceledFlag)
    // free resources by preempting allocation
    var preemptedTasks = 0L
    for (alloc: Allocation <- allocations) {
      if (alloc.isConsumingResources) {
        alloc.preempt(timeNow)
      }
      preemptedTasks += alloc.numberOfTasksToStart
    }
    assert(runningNonDaemonTasks == 0)
    isDoneFlag = true
    lastPreemptionTime = timeNow
    wasCanceledFlag = true
    allocations.clear()
    preemptedTasks
  }

  def shutdownJob(): Unit = {
    assert(!wasCanceledFlag)
    assert(!isDoneFlag)
    assert(runningNonDaemonTasks == 0)
    for (daemonAlloc <- allocations if daemonAlloc.taskGroup.isDaemonOfJob) {
      daemonAlloc.release()
    }
    // sanity
    for (alloc: Allocation <- allocations) {
      assert(!alloc.isConsumingResources, "there is an allocation which consumes resources, even though, the job is done")
    }

    //    allocations.clear()
    isDoneFlag = true
  }

  def checkIfFullyAllocated(): Boolean = {
    if (fullyAllocatedFlag || wasCanceledFlag) {
      true
    } else {
      if (SimulationConfiguration.SANITY_CHECKS_CELL) {

        var allAllocated = true
        // return true if all matching task groups are allocated

        // if there is no allocation, it was either already running or there is nothing allocated
        if (allocations.isEmpty) {
          allAllocated = false
        }
        // if there are some allocations, check that all TGs are scheduled
        val tgIt = taskGroups.iterator
        while (allAllocated && tgIt.hasNext) {
          val tg = tgIt.next()
          if ((tg.inAllOptions || _chosenJobOption.intersect(tg.inOption).nonEmpty) && (tg.notStartedTasks != 0)) {
            allAllocated = false
          }
        }

        if (fullyAllocatedFlag)
          assert(notScheduledTasks == 0, "there is some caching going wrong!")
      }

      fullyAllocatedFlag = notScheduledTasks == 0
      fullyAllocatedFlag
    }
  }

  override def compare(that: Job): NodeID = {
    /** Ordering `Job`s by their submission time */
    this.submitted compare that.submitted
  }

  override def toString: String = s"Job($id;@$submitted;${taskGroups.size}TGs;${taskGroups.foldLeft(0)((c, tg) => c + tg.numTasks)}Ts)"

  // as last constructor call, update caches
  refreshCacheOfFlagsAndTaskCount()
}

class Allocation(val taskGroup: TaskGroup,
                 val numberOfTasksToStart: Int,
                 val machine: NodeID,
                 var machineNetworkID: NodeID = -1, // The id of the node representing the machine within the graph
                 val job: Job,
                 val cell: Cell,
                 val startTime: SimTypes.simTime) extends Ordered[Allocation] {

  val myId: Long = Allocation.nextId

  def wasPreempted: Boolean = preempted

  protected var preempted: Boolean = false
  protected var isValid: Boolean = true

  @inline def detailedToString: String = s"alloc($myId) j:${job.id} tg:${taskGroup.id} #:$numberOfTasksToStart " +
    s"m:$machine n:$machineNetworkID t:$startTime -- v:$isValid p:$preempted"

  assert(numberOfTasksToStart > 0, s"invalid number of tasks to start, $numberOfTasksToStart, " +
    s"${taskGroup.detailedToString()}")

  if (!taskGroup.coLocateOnSameMachine)
    assert(numberOfTasksToStart == 1, s"invalid number of tasks to start, $numberOfTasksToStart, " +
      s"${taskGroup.detailedToString()}")

  taskGroup.notStartedTasks -= numberOfTasksToStart
  cell.claimResources(this)
  assert(taskGroup.notStartedTasks >= 0)

  def isConsumingResources: Boolean = {
    isValid
  }

  def preempt(timeNow: SimTypes.simTime): Unit = {
    assert(isValid)
    assert(!preempted)
    preempted = true
    isValid = false
    taskGroup.notStartedTasks += numberOfTasksToStart
    job.allocationPreempted(allocation = this, timeNow = timeNow)
    cell.releaseResources(this)
  }

  def release(): Unit = {
    assert(isValid)
    isValid = false
    taskGroup.finishedTasks += numberOfTasksToStart
    job.allocationReleased(this)
    cell.releaseResources(this)
  }

  override def compare(that: Allocation): Int = myId.compareTo(that.myId)
}

object Allocation {
  def nextId: Long = {
    lastId += 1
    lastId
  }

  var lastId: Long = -1
}

class TaskGroupConnection(val src: TaskGroup,
                          val dst: TaskGroup)
