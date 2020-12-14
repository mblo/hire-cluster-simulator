package hiresim.scheduler.flow.coco

import hiresim.cell.Cell
import hiresim.graph.NodeType
import hiresim.scheduler.flow.FlowBasedScheduler
import hiresim.scheduler.flow.solver.Solver
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.scheduler.flow.solver.mcmf.util.MCMFSolverStatistics
import hiresim.scheduler.{FlavorSelector, SchedulerDecidesFlavorSelector, SchedulerWithFlavorSelector}
import hiresim.shared.TimeIt
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.SimTypes.MEGA
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.TimingStatistics
import hiresim.simulation.{SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.mutable

class CoCoScheduler(override val name: String,
                    implicit override val cell: Cell,
                    override val solver: Solver,
                    implicit override val simulator: Simulator,
                    override val thinkTimeScaling: Double = 1.0,
                    val flavorSelector: FlavorSelector)
  extends FlowBasedScheduler(name = name, solver = solver, cell = cell, simulator = simulator, thinkTimeScaling = thinkTimeScaling) with SchedulerWithFlavorSelector {

  assert(!flavorSelector.isInstanceOf[SchedulerDecidesFlavorSelector], "CoCo cannot handle alternatives by its own!")

  // The GraphManager taking care of managing the graph
  private[flow] val graphManager: CoCoGraphManager = new CoCoGraphManager()(scheduler = this)

  // Redeclare this as implicit, so the FlavorSelector can find it without any trouble
  private implicit val _this: SchedulerWithFlavorSelector = this

  override def getNumberOfPendingJobs: Int = {
    graphManager.jobNodes.size
  }

  override def addJobAndSchedule(newJob: Job): Unit = {
    flavorSelector.jobSubmitted(newJob)
    super.addJobAndSchedule(newJob)
  }

  override def jobWasUpdated(job: Job): Unit = {
    // Only re-add this job if more work on it is required
    if (!job.checkIfFullyAllocated()) {
      // Remove that job from the graph
      graphManager.removeJob(job)
      // And re-add it again
      jobBacklog.append(job)
    } else if (job.isDone) {
      // probably because of flavor fallback
      graphManager.removeJob(job)
    }
  }


  override protected def runSchedulingLogic(): Unit = {
    val now = simulator.currentTime()
    assert(!isScheduling)
    isScheduling = true

    val t = TimeIt("schedule")

    // Cleanup the Graph before running the solver
    val tc = TimeIt("CleanupTime")
    val maxTaskCountToStart = graphManager.cleanup()
    tc.stop
    MCMFSolverStatistics.cleanup(tc.diff)
    TimingStatistics.onPostMeasurement("CleanupTime", tc)
    logVerbose(s"Cleanup completed: ${maxTaskCountToStart} tasks to start.")

    val affectedAllocations: mutable.ListBuffer[Allocation] = mutable.ListBuffer[Allocation]()
    // since we don't know ahead of allocations how many allocations we will do, align the think time later.

    if (maxTaskCountToStart == 0) {
      postponeSameSchedulingProblemTillProblemChanges()
      logVerbose(s"There is a situation with no tasks to start, so nothing to do here")
      isScheduling = false
    } else {
      if (SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS) {
        simulator.logInfo(s"Sent graph to solver," +
          s" skipped tgs:${graphManager.skippedTaskGroupsBecauseOverloaded}" +
          s" producers:${graphManager.graph.nodes.producerIds.size}" +
          s" max tasks to start:$maxTaskCountToStart " +
          s" sink:${graphManager.graph.nodes(graphManager.graph.sinkNodeId).supply}"
        )
      }

      // Now execute the solver to get some scheduling proposals
      val tSolve = TimeIt("Solver")

      val (resultGraph: FlowGraph, graphIsDuplicated: Boolean, actualSolver: Solver) = runSolver(maxTaskCountToStart)

      tSolve.stop
      TimingStatistics.onPostMeasurement("SolverTime", tSolve)
      logVerbose(s"Solver ${actualSolver} finished. Extracting results...")

      // Retrieve the Task -> Machine mapping
      val tGetResult = TimeIt("interpretResult")
      val roundResult: Iterable[SolverEntry] = interpretResult(resultGraph)
      tGetResult.stop
      TimingStatistics.onPostMeasurement("InterpretTime", tGetResult)

      logVerbose(s"Got ${roundResult.size} allocations... Applying...")
      // Reset potentials now as we might be adding new TG's in the following (when a Flavor
      // has been choosen, we re-add the TaskGroups that are in the flavor)
      if (!graphIsDuplicated) {
        graphManager.graph.nodes.foreach(node => {
          node.potential = 0L
        })
      }

      val thinkTime: SimTypes.simTime =
        thinkTimeForExpectedTasksToStart(maxTaskCountToStart)

      val desiredTaskStartTime = thinkTime + now


      for (solverEntry <- roundResult) {

        val producerId = solverEntry.producerId
        val allocationTargetId = solverEntry.allocationTargetId

        val producerNode: FlowNode = graphManager.graph.nodes(producerId)
        val allocationTargetNode: FlowNode = graphManager.graph.nodes(allocationTargetId)

        val producerType: NodeType = producerNode.nodeType
        val allocationTargetType: NodeType = allocationTargetNode.nodeType

        // If the producer has not been scheduled
        if (allocationTargetType == NodeType.TASK_GROUP_POSTPONE) {
          // increase supply by 1
          if (!graphIsDuplicated) {
            logVerbose(s"add 1 to supply of $producerId since 1 flow passes unscheduled node; new supply: ${graphManager.graph.nodes(producerId).supply}")
            producerNode.supply += 1L
          }

        } else if (allocationTargetType == NodeType.MACHINE_NETWORK ||
          allocationTargetType == NodeType.MACHINE_SERVER) {

          // As the allocationTargetId describes the node of the machine in the Graph, we have to resolve for its actual id
          val cellMachineID: NodeID = allocationTargetType match {

            case NodeType.MACHINE_NETWORK =>
              graphManager.mapping.getSwitchMachineIDFromNetworkID(allocationTargetId)

            case NodeType.MACHINE_SERVER =>
              graphManager.mapping.getServerMachineIDFromNodeID(allocationTargetId)

            case _ =>
              throw new AssertionError(s"The allocation victim does not represent a " +
                s"physical machine (Type: $allocationTargetType).")

          }

          val involvedTaskGroup: TaskGroup = graphManager.graph.nodes(producerId).taskGroup.get
          val involvedJob: Job = involvedTaskGroup.job.get

          // Debug output about the allocation we are about to make
          logVerbose(s"$producerType of ${involvedTaskGroup} allocated on $allocationTargetType $allocationTargetId (actual machine id: $cellMachineID).")

          val jobWasChosenBeforeNewAllocation = involvedJob.flavorHasBeenChosen
          // Do the resource allocation. This will also update potential flavor choices!
          val allocation: Allocation = involvedJob.doResourceAllocation(
            taskGroup = involvedTaskGroup,
            numberOfTasksToStart = 1,
            machine = cellMachineID,
            machineNetworkID = allocationTargetId,
            cell = cell,
            startTime = desiredTaskStartTime
          )

          // this makes the graphs (resultGraph and original graphManager) semantically identical wrt producer
          if (graphIsDuplicated) {
            producerNode.supply -= 1L
          }

          if (involvedJob.flavorHasBeenChosen && !jobWasChosenBeforeNewAllocation) {
            if (involvedJob.statisticsInpChosen)
              statisticsFlavorTakeInp += 1
            else
              statisticsFlavorTakeServer += 1
          }

          // Flag the machine as dirty. This will trigger some updates before the next scheduling attempt
          allocationTargetNode.dirty = true
          // Also Flag the TaskGroup as dirty
          involvedTaskGroup.dirty = true
          involvedTaskGroup.dirtyMachines.add(allocationTargetId)

          // We also have to make an entry in the nodes running task cache
          val statistics = graphManager.graph.nodes(allocationTargetId).allocatedTaskGroupsInSubtree
          statistics.update(involvedTaskGroup, statistics.getOrElse(involvedTaskGroup, 0) + 1)

          affectedAllocations += allocation

          if (involvedTaskGroup.notStartedTasks == 0) {
            // this was a normal allocation ("outside" of the flavor selection), and actually it was the last task to be scheduled
            logVerbose(s"This was last task allocation of task group $involvedTaskGroup (${producerId}), so remove node")
            graphManager.removeNode(producerId)

            // so was this the last allocation?
            if (involvedJob.checkIfFullyAllocated()) {
              // so remove the linker and the unschedule node
              logVerbose(s"job $involvedJob is fully allocated (after normal task group), remove linker")
              graphManager.removeJob(involvedJob)
            }
          }

        } else {
          throw new AssertionError(s"Wrong consumer node found, $allocationTargetType")
        }
      }

      if (affectedAllocations.isEmpty && maxTaskCountToStart > 0) {
        logVerbose(s"Cost model postpones scheduling, so simply postpone scheduling")
        postponeSameSchedulingProblemForCostUpdates()
      } else if (maxTaskCountToStart == 0) {
        logVerbose(s"Looks like cell is overloaded")
        postponeSameSchedulingProblemTillProblemChanges()
      }

      applyAllocationAfterThinkTime(allocations = affectedAllocations, thinkTime = thinkTime, andRunScheduleAfterwards = true)

      if (SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS) {
        val msg = if (maxTaskCountToStart == 0) "No feasible shortcut link, so we do not run the solver, " else
          s"Think:$thinkTime - Received ${affectedAllocations.size} allocs from solver (out of $maxTaskCountToStart | "


        simulator.logInfo(msg +
          s"still active jobs:${graphManager.jobNodes.size} " +
          s" received ${affectedAllocations.size} allocs from " +
          s"solver ${actualSolver.name} with ${tSolve.diff / MEGA}ms " +
          s" task groups:${graphManager.taskGroupNodes.size} " +
          s"| backlog job: ${jobBacklog.size} " +
          s"| SchedulingAttempts: ${statisticsSchedulingAttempts} " +
          s"| Current time: ${simulator.msToHuman(simulator.currentTime())}")
        if (statisticsSchedulingAttempts % 50 == 0) {
          simulator.logInfo(s" -- status -- 50 rounds done -- ${simulator.msToHuman(now)} - ")
          simulator.printQuickStats()
        }
      }

    }

    t.stop
    TimingStatistics.onPostMeasurement("TotalTime", t)
    TimingStatistics.onFinishRound()

    setRetriggerPostponedSchedulingEvent()
  }

}
