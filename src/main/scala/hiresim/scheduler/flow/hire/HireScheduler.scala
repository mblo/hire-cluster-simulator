package hiresim.scheduler.flow.hire

import hiresim.cell.Cell
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchProps}
import hiresim.graph.NodeType
import hiresim.scheduler.flow.FlowBasedScheduler
import hiresim.scheduler.flow.solver.Solver
import hiresim.scheduler.flow.solver.graph.FlowNode.Supply
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.scheduler.flow.solver.mcmf.util.MCMFSolverStatistics
import hiresim.shared.TimeIt
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.{CellINPLoadStatistics, TimingStatistics}
import hiresim.simulation.{SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}
import hiresim.workload.WorkloadProvider

import scala.collection.mutable

/**
 *
 * @param name   this scheduler's name.
 * @param cell   the physical cell on which this scheduler operates.
 * @param solver the solver who is in charge of solving resource allocation problems.
 * @param simulator
 */
class HireScheduler(override val name: String,
                    override val cell: Cell,
                    override val solver: Solver,
                    override val simulator: Simulator,
                    override val thinkTimeScaling: Double = 1.0,
                    val sanityChecks: Boolean = true,
                    val minQueuingTimeBeforePreemption: Int = (1.1 * SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER).toInt,
                    val maxInpFlavorDecisionsPerRound: Int = 20)
  extends FlowBasedScheduler(name = name, solver = solver, cell = cell, simulator = simulator, thinkTimeScaling = thinkTimeScaling) {

  assert(maxInpFlavorDecisionsPerRound > 0)

  // The GraphManager handling modifications on the FlowGraph
  private[flow] val graphManager: HireGraphManager = new HireGraphManager(random = random.copy, maxInpFlavorDecisionsPerRound = maxInpFlavorDecisionsPerRound)(scheduler = this)

  private val preemptionCandidates: mutable.ArrayDeque[TaskGroup] = mutable.ArrayDeque()
  private[hire] val jobsForForcedServerModeCandidates: mutable.ArrayDeque[Job] = mutable.ArrayDeque()

  if (!CellINPLoadStatistics.activated) {
    simulator.logInfo(s"Hire activates INP cell statistics")
    CellINPLoadStatistics.activate(
      simulator = simulator,
      cell = cell,
      inp_types = SwitchProps.allTypes,
      file = None,
      interval = 0
    )
  }

  @inline override def getNumberOfPendingJobs: Int = {
    graphManager.jobNodes.size
  }

  def addPreemptionCandidate(taskGroup: TaskGroup): Unit = {
    if (jobBacklog.isEmpty) {
      if (taskGroup.isSwitch &&
        taskGroup.belongsToInpFlavor) {
        preemptionCandidates.addOne(taskGroup)
      }
    }
  }

  override def someTasksAreDoneAndFreedResources(allocation: Allocation): Unit = {
    // in case the flavor part has some nodes, we need to set the dirty flag of the flavor part
    val superFlavorNode = graphManager.graph.nodes(graphManager.super_flavor_node_id)
    if (superFlavorNode.outgoing.nonEmpty) {
      superFlavorNode.dirty = true
    }

    // The upper implementation is handling dirty flags and statistics
    super.someTasksAreDoneAndFreedResources(allocation)

    // If this is a switch we need to remember this allocation to propagate the resource changes correctly
    if (allocation.taskGroup.isSwitch) {
      val t = TimeIt("updateSwitchesLocalGains")
      graphManager.locality.updateSwitchesLocalGains(allocation, freeing = true)
      t.stop
    }
  }


  override def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit = {
    val now = simulator.currentTime()
    //    job.lastPreemptionTime = now
    job.mostRecentTgSubmissionTime = now
    super.taskGroupNowReady(job, taskGroup)
  }

  @inline override protected def graphHasSomeWorkLeft(): Boolean = {

    val now = simulator.currentTime()
    var workLeft = jobBacklog.nonEmpty

    if (doNotRunSameSchedulingProblemBefore > now ||
      graphManager.skippedTaskGroupsBecauseOverloaded > 0 ||
      graphManager.skippedFlavorSelectorBecauseOverloaded > 0) {
      logDetailedVerbose("  workleft=true (trigger set)")
      workLeft = true
    }

    val superFlavorNode = graphManager.graph.nodes(graphManager.super_flavor_node_id)

    if (!workLeft) {
      // pending flavor decision?
      workLeft = superFlavorNode.dirty || superFlavorNode.supply > 0
      logDetailedVerbose(s" workleft=$workLeft (SFN)")
    }

    if (!workLeft) {
      // check producers in the graph
      val producers = graphManager.graph.nodes.producerIds
      // We must not count the SFN node when checking for pending tg allocs
      workLeft =
        if (superFlavorNode.supply > 0)
          producers.size > 1
        else
          producers.nonEmpty

      logDetailedVerbose(s" workleft=$workLeft (producers)")
    }

    workLeft
  }

  private var previousSchedulingAtTime: SimTypes.simTime = Integer.MIN_VALUE
  private var previousSchedulingAllocations: Int = Integer.MAX_VALUE
  private var previousSpentSolverTime: Long = 0

  protected def runSchedulingLogic(): Unit = {
    assert(graphManager.graph.nonEmpty)
    val now = simulator.currentTime()
    assert(!isScheduling)
    isScheduling = true
    var newSolverInpDecisions = 0
    var newSolverServerDecisions = 0

    val t = TimeIt("schedule")

    previousSchedulingAtTime = now

    logVerbose(s"\n\n===== Round ${HireScheduler.round} =====")

    // Cleanup the Graph before running the solver
    val tc = TimeIt("Cleanup Graph")
    val maxTaskCountToStart: Supply = graphManager.cleanup()
    tc.stop
    MCMFSolverStatistics.cleanup(tc.diff)
    TimingStatistics.onPostMeasurement("CleanupTime", tc)

    if (maxTaskCountToStart == 0 && !graphHasSomeWorkLeft()) {
      logVerbose("nothing to be done")
      isScheduling = false
    } else {

      HireScheduler.round += 1
      val affectedAllocations: mutable.ListBuffer[Allocation] = mutable.ListBuffer[Allocation]()
      // since we don't know ahead of allocations how many allocations we will do, align the think time later.

      var solverInCharge: Option[Solver] = None
      var solverRunnedParallel = false

      val realisticMaxTaskCountToStart: Supply = cell.numMachines.toLong min maxTaskCountToStart

      var switchTaskGroupsBeforeSolving = 0
      graphManager.taskGroupNodesInCommonPart.foreach(tg => if (tg.taskGroup.get.isSwitch) switchTaskGroupsBeforeSolving += 1)
      val serverTaskGroupsBeforeSolving = graphManager.taskGroupNodesInCommonPart.size - switchTaskGroupsBeforeSolving

      val thinkTime: SimTypes.simTime = thinkTimeForExpectedTasksToStart(maxTaskCountToStart)
      if (maxTaskCountToStart == 0) {
        applyAllocationAfterThinkTime(allocations = Nil, thinkTime = thinkTime,
          andRunScheduleAfterwards = true)
      } else {

        logVerbose(s"Cluster util - Server: ${cell.currentServerLoads()} - Switch: ${cell.currentSwitchLoads()} " +
          s"skipped:[${graphManager.skippedTaskGroupsBecauseOverloaded}g|${graphManager.skippedFlavorSelectorBecauseOverloaded}f] tgs:${graphManager.taskGroupNodesInCommonPart.size}, " +
          s"tasksToStart:$realisticMaxTaskCountToStart producers:${graphManager.graph.nodes.producerIds.size}")

        if (SimulationConfiguration.SANITY_CHECKS_GRAPH_COSTS) {
          graphManager.runGraphSanityCheck()
        }

        if (SimulationConfiguration.SANITY_CHECKS_HIRE)
          sanityCheckAllocatableSubtreesInGraph()

        // Now execute the solver to get some scheduling proposals
        val tSolve = TimeIt("Solver")
        val (resultGraph: FlowGraph, graphIsDuplicated: Boolean, actualSolver: Solver) = runSolver(realisticMaxTaskCountToStart)

        solverRunnedParallel = graphIsDuplicated
        solverInCharge = Some(actualSolver)

        // resultGraph may hold the very same object as graphManager.graph.

        tSolve.stop
        previousSpentSolverTime = tSolve.diff
        TimingStatistics.onPostMeasurement("SolverTime", tSolve)

        val tGetResult = TimeIt("interpretResult")
        // Retrieve the Task -> Machine mapping
        val roundResult: Iterable[SolverEntry] = interpretResult(resultGraph)
        tGetResult.stop
        TimingStatistics.onPostMeasurement("InterpretTime", tGetResult)

        if (!graphIsDuplicated) {
          // Reset potentials now as we might be adding new TG's in the following (when a Flavor
          // has been choosen, we re-add the TaskGroups that are in the flavor)
          graphManager.graph.nodes.foreach(node => {
            node.potential = 0L
          })
        }

        logDetailedVerbose(s"Result: ${roundResult.mkString(", ")}")

        val desiredTaskStartTime = thinkTime + now

        val sanityConsideredServerMachines: mutable.BitSet = mutable.BitSet()
        val sanityConsideredSwitchMachines: mutable.BitSet = mutable.BitSet()
        val sanityConsideredGraphNodes: mutable.BitSet = mutable.BitSet()

        // for each allocated producer ID
        for (solverEntry <- roundResult) {
          val producerId = solverEntry.producerId
          val actualProducerId = solverEntry.actualProducerId
          val allocationTargetId = solverEntry.allocationTargetId
          // producer refers to the guy which has supply > 0, which might be a TG or a FlavorSelector
          // actual producer refers always to the task group of this flow
          // machine refers to the server/switch ID, or to the unscheduled node id

          if (SimulationConfiguration.SANITY_CHECKS_HIRE) {
            assert(graphManager.graph.nodes.contains(producerId))
            assert(graphManager.graph.nodes.contains(actualProducerId))
            assert(graphManager.graph.nodes.contains(allocationTargetId))
          }

          val producerNode: FlowNode = graphManager.graph.nodes(producerId)

          assert(producerNode.nodeType == NodeType.NETWORK_TASK_GROUP ||
            producerNode.nodeType == NodeType.SERVER_TASK_GROUP ||
            producerNode.nodeType == NodeType.JOB_FLAVOR_SELECTOR, s"invalid producer returned, ${producerNode}")

          val allocationTargetNode: FlowNode = graphManager.graph.nodes(allocationTargetId)
          val allocationTargetType: NodeType = allocationTargetNode.nodeType

          // If the producer has not been scheduled
          if (allocationTargetType == NodeType.TASK_GROUP_POSTPONE) {
            // this might be an unschedule flow for a task group, which we finally assigned this round, so check if this guy still exists
            if (!graphIsDuplicated && graphManager.graph.nodes.contains(producerId)) {

              if (producerNode.nodeType != NodeType.JOB_FLAVOR_SELECTOR) {
                // increase supply by 1
                logDetailedVerbose(s"add 1 to supply of $producerId since 1 flow passes unscheduled node; " +
                  s"new supply: ${graphManager.graph.nodes(producerId).supply}")
                producerNode.supply += 1L
              }
            }

          } else if (allocationTargetType == NodeType.MACHINE_NETWORK ||
            allocationTargetType == NodeType.MACHINE_SERVER) {

            val producerType: NodeType = producerNode.nodeType
            val actualProducerNode = graphManager.graph.nodes(actualProducerId)
            val actualProducerType: NodeType = actualProducerNode.nodeType

            // this makes the graphs (resultGraph and original graphManager) semantically identical wrt producer
            if (graphIsDuplicated && producerNode.nodeType != NodeType.JOB_FLAVOR_SELECTOR) {
              producerNode.supply -= 1L
            }

            assert(actualProducerType == NodeType.NETWORK_TASK_GROUP || actualProducerType == NodeType.SERVER_TASK_GROUP,
              s"found wrong actual producer of type: $actualProducerType")

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

            val involvedTaskGroup: TaskGroup = graphManager.graph.nodes(actualProducerId).taskGroup.get
            val involvedJob: Job = involvedTaskGroup.job.get

            var ignoreThisAllocation = false

            if (producerType == NodeType.JOB_FLAVOR_SELECTOR) {
              if (involvedTaskGroup.belongsToInpFlavor) {
                newSolverInpDecisions += 1
                if (newSolverInpDecisions > maxInpFlavorDecisionsPerRound) {
                  // this was an INP flavor decisions, but we already took too many decisions, so skip this
                  ignoreThisAllocation = true

                }
              } else if (!involvedTaskGroup.inAllOptions && !involvedTaskGroup.belongsToInpFlavor) {
                newSolverServerDecisions += 1
              }

            }

            if (!ignoreThisAllocation) {
              // Debug output about the allocation we are about to make
              logDetailedVerbose(s"$producerType $actualProducerId of ${involvedTaskGroup.shortToString()} " +
                s"(+${producerNode.supply}) allocated " +
                s"on $allocationTargetType $allocationTargetId (actual machine id: $cellMachineID).")

              if (SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK) {
                // check if this allocation really fits to the job
                assert(involvedJob.checkIfTaskGroupMightBeInFlavorSelection(involvedTaskGroup),
                  s"check failed for ${involvedTaskGroup.detailedToString()}")
                assert(involvedJob.excludedJobOption.intersect(involvedTaskGroup.inOption).isEmpty,
                  s"check failed for ${involvedTaskGroup.detailedToString()}")
                assert(involvedJob.chosenJobOption.intersect(involvedTaskGroup.notInOption).isEmpty,
                  s"check failed for ${involvedTaskGroup.detailedToString()}")

                // check if we do really only one allocation on a machine per round
                def failMsg(): String = s"${involvedTaskGroup.detailedToString()}, " +
                  s"in this round we did already ${sanityConsideredServerMachines.size} server allocations, " +
                  s"and ${sanityConsideredSwitchMachines.size} switch allocations"

                if (involvedTaskGroup.isSwitch) {
                  assert(sanityConsideredSwitchMachines.add(cellMachineID), failMsg())
                } else {
                  assert(sanityConsideredServerMachines.add(cellMachineID), failMsg())
                }

                assert(sanityConsideredGraphNodes.add(allocationTargetId))

                // check if the resources in the graph match the resources in the cell
                val tgRes: Array[NumericalResource] = if (involvedTaskGroup.isSwitch) {
                  cell.calculateEffectiveSwitchDemand(involvedTaskGroup, cell.getActiveInpPropsOfSwitch(cellMachineID)).firstTaskReserve
                } else
                  involvedTaskGroup.resources.asInstanceOf[ServerResource].numericalResources

                val cellRes: Array[NumericalResource] = if (involvedTaskGroup.isSwitch)
                  cell.switches(cellMachineID).numericalResources
                else
                  cell.servers(cellMachineID)

              }

              // Do the resource allocation. This will also update potential flavor choices!
              val allocation: Allocation = involvedJob.doResourceAllocation(
                taskGroup = involvedTaskGroup,
                numberOfTasksToStart = 1,
                machine = cellMachineID,
                machineNetworkID = allocationTargetId,
                cell = cell,
                startTime = desiredTaskStartTime
              )

              // Flag the machine as dirty. This will trigger some updates before the next scheduling attempt
              allocationTargetNode.dirty = true
              // Also Flag the TaskGroup as dirty
              involvedTaskGroup.dirty = true
              involvedTaskGroup.dirtyMachines.add(allocationTargetId)

              // We also have to make an entry in the nodes running task cache
              val statistics = graphManager.graph.nodes(allocationTargetId).allocatedTaskGroupsInSubtree
              val newVal = statistics.getOrElse(involvedTaskGroup, 0) + allocation.numberOfTasksToStart
              statistics.update(involvedTaskGroup, newVal)
              assert(newVal <= involvedTaskGroup.runningTasks, s"invalid: $newVal <= ${involvedTaskGroup.runningTasks} for ${involvedTaskGroup.detailedToString()}")

              if (allocationTargetType == NodeType.MACHINE_NETWORK) {
                val t = TimeIt("updateSwitchesLocalGains")
                graphManager.locality.updateSwitchesLocalGains(allocation, freeing = false)
                t.stop
              }

              affectedAllocations += allocation

              if (producerType == NodeType.JOB_FLAVOR_SELECTOR) {
                // this was a flavor selection, so check if we need to change something with all the involved task groups
                logDetailedVerbose(s"Allocation from flavor selector on task group $actualProducerNode, " +
                  s"so check if we need to update job $involvedJob")
                graphManager.checkFlavorSelectorAfterFlavorAllocationAndUpdateGraph(involvedJob)
              } else if (involvedTaskGroup.notStartedTasks == 0) {
                // this was a normal allocation ("outside" of the flavor selection), and actually it was the last task to be scheduled
                logDetailedVerbose(s"This was last task allocation of task group $involvedTaskGroup (${actualProducerId}), " +
                  s"so remove node, (supply:${actualProducerNode.supply})")
                graphManager.removeNode(actualProducerId)
              } else {
                // is this an allocation of the common part and the supply 0?
                if (producerType != NodeType.JOB_FLAVOR_SELECTOR
                  && producerNode.shouldHaveUnscheduledConnection
                  && producerNode.supply < 1) {
                  // temporarily fix this, so that this producer is still a valid producer - having a supply > 0
                  producerNode.supply = 1L
                }

              }

              // so was this the last allocation?
              if (involvedJob.checkIfFullyAllocated()) {
                // so remove the linker and the unschedule node
                logDetailedVerbose(s"job $involvedJob is fully allocated (after flavor selection), remove linker")
                graphManager.removeNode(graphManager.jobNodes(involvedJob).unschedule.id)
                graphManager.jobNodes.remove(involvedJob)
              }
            }

          } else {
            throw new AssertionError(s"Wrong consumer node found, $allocationTargetType")
          }
        }

        graphManager.graph.nodes.producerIds.clone().foreach(prod => {
          val node = graphManager.graph.nodes(prod)
          if (prod != graphManager.super_flavor_node_id &&
            !node.isProducer &&
            node.taskGroup.get.notStartedTasks > 0) {
            node.supply = 1L
          }
        })
        applyAllocationAfterThinkTime(allocations = affectedAllocations, thinkTime = thinkTime,
          andRunScheduleAfterwards = true)
        t.stop
        TimingStatistics.onPostMeasurement("TotalTime", t)
        TimingStatistics.onFinishRound()
      }

      // at this point, either scheduling==false  or  applyAllocationAfterThinkTime was called (maybe with empty allocations)

      previousSchedulingAllocations = affectedAllocations.size
      var forcedServerModeJobs = 0
      val minAgeBeforeForceServer = now - SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER
      while (jobsForForcedServerModeCandidates.nonEmpty) {
        val candidateJob = jobsForForcedServerModeCandidates.removeHead()
        if (!candidateJob.flavorHasBeenChosen &&
          candidateJob.mostRecentTgSubmissionTime < minAgeBeforeForceServer) {
          logVerbose(s"set job $candidateJob to server mode for all pending task group flavors, " +
            s"simply because hire asks for it :)")

          var newEnforcedServerFlavor = WorkloadProvider.emptyFlavor
          var newExcludedFlavor = WorkloadProvider.emptyFlavor

          candidateJob.taskGroups.foreach(tg => {
            if (tg.submitted <= now &&
              tg.notStartedTasks > 0 &&
              tg.runningTasks == 0 &&
              !tg.inAllOptions &&
              candidateJob.checkIfTaskGroupMightBeInFlavorSelection(tg)) {
              // this is a flavored task group which could be scheduled, but is not scheduled so far

              if (
              // if tg is server flavor
                !tg.belongsToInpFlavor &&
                  // if tg does not collide with new exclude option
                  tg.inOption.intersect(newExcludedFlavor).isEmpty &&
                  // if tg exclude does not collide with new enforced option
                  tg.notInOption.intersect(newEnforcedServerFlavor).isEmpty) {

                newEnforcedServerFlavor |= tg.inOption
                newExcludedFlavor |= tg.notInOption
              }
            }
          })

          if (newEnforcedServerFlavor.nonEmpty) {
            candidateJob.updateChosenOption(
              newIncludedOption = newEnforcedServerFlavor,
              newExcluddedOption = newExcludedFlavor)
            graphManager.checkFlavorSelectorAfterFlavorAllocationAndUpdateGraph(candidateJob)
            forcedServerModeJobs += 1
          } else {
            simulator.logWarning(s"we have a enforce server candidate job," +
              s" but there is not enforce server option availalbe! ${candidateJob.detailedToString()}")
          }

        }
      }

      var jobsPreemptedThisRound = 0

      if (preemptionCandidates.nonEmpty) {
        // preemption candidates include only TGs which belong to the switch flavor, and of certain "queuing age"
        // preempt at most x% per round of all preemption candidates

        val affectedTaskGroups: mutable.HashSet[TaskGroup] = mutable.HashSet()
        var timeTillNextPreemption = minQueuingTimeBeforePreemption

        val minAgeBeforePreemption = now - minQueuingTimeBeforePreemption

        for (nextCandidateTaskGroup <- preemptionCandidates) {
          val job = nextCandidateTaskGroup.job.get
          // this tg may be outdated already, i.e. not necessary for the job anymore, or the job may be finalized already!
          if (!job.checkIfFullyAllocated() && job.checkIfTaskGroupMightBeInFlavorSelection(nextCandidateTaskGroup)) {
            // proceed only if this tg is still relevant
            if (nextCandidateTaskGroup.consideredForSchedulingSince <= minAgeBeforePreemption &&
              job.lastPreemptionTime <= minAgeBeforePreemption) {

              // now we double check.. we might have the case, that a INC task group joins later the job,
              // and would like to trigger a server fallback.. but this would cause very bad placement latencies for the
              // older - not scheduled - server task groups. So check for such a problem

              if (now - nextCandidateTaskGroup.submitted > SimulationConfiguration.HIRE_MAX_QUEUING_TIME_TO_PREEMPT) {
                logDetailedVerbose(s"Forbid preemption for ${nextCandidateTaskGroup.shortToString()}")
              } else {
                timeTillNextPreemption = 0
                affectedTaskGroups.add(nextCandidateTaskGroup)
              }
            } else {
              // we need to schedule an event to trigger the preemption next time
              val timeTillPreempt = ((nextCandidateTaskGroup.consideredForSchedulingSince - minAgeBeforePreemption)
                max (job.lastPreemptionTime - minAgeBeforePreemption))

              assert(timeTillPreempt > 0)
              assert(timeTillPreempt <= minQueuingTimeBeforePreemption)
              timeTillNextPreemption = timeTillNextPreemption min timeTillPreempt
              logDetailedVerbose(s"$timeTillPreempt = $minQueuingTimeBeforePreemption - " +
                s"($now - (${nextCandidateTaskGroup.submitted} " +
                s"max ${job.mostRecentTgSubmissionTime} max ${job.lastPreemptionTime}))")
            }
          }
        }

        if (timeTillNextPreemption > 0 && !graphHasSomeWorkLeft()) {
          simulator.scheduleActionWithDelay(sim => {
            logVerbose(s"Trigger schedule round, so that preemption logic fires if still necessary, fired from $now")
            graphManager.graph.nodes(graphManager.super_flavor_node_id).dirty = true
          }, delay = timeTillNextPreemption)
        }

        if (affectedTaskGroups.nonEmpty) {
          val preemptTaskGroups = affectedTaskGroups.toList.sortWith(_.submitted < _.submitted)
          for (tg <- preemptTaskGroups) {
            val job = tg.job.get
            if (job.lastPreemptionTime == now) {
              logVerbose(s"we preempted this ${job.detailedToString()} already this round, so skip for now")
            } else {

              preemptTaskGroupAndAffectedFlavorPartOfJob(tg)
              jobsPreemptedThisRound += 1

              if (tg.preemptionCounter > 0) {

                logVerbose(s"finnally set ${job} to opposite flavor of ${tg.shortToString()}")

                // apply the opposite of this task group, for flavor selection
                job.updateChosenOption(
                  newIncludedOption = tg.notInOption,
                  newExcluddedOption = tg.inOption)

                if (tg.belongsToInpFlavor)
                  forcedServerModeJobs += 1
              }

              // at this point, there is no allocation which conflicts with the flavor, however, there might a TG which conflicts!!!
              // maybe there is a TG missing in the graph because we previously removed it

              graphManager.checkFlavorSelectorAfterFlavorAllocationAndUpdateGraph(
                job = job,
                afterPreemption = true)

              logVerbose(s"Preempted job $tg because at least one " +
                s"task group (${preemptionCandidates.find(tg => tg.job.get == tg).get.shortToString()}) " +
                s"is complaining, " +
                s"after job queueing time:${now - tg.submitted}")
            }
          }
        }
        preemptionCandidates.clear()
      }


      if (SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS) {
        var switchTaskGroups = 0
        graphManager.taskGroupNodesInCommonPart.foreach(tg => if (tg.taskGroup.get.isSwitch) switchTaskGroups += 1)

        val msg = if (maxTaskCountToStart == 0) "No feasible shortcut link, so we did not run the solver, " else
          s"Think:$thinkTime Spent:${previousSpentSolverTime / SimTypes.MEGA}ms - " +
            s"Received ${affectedAllocations.size} allocs from " +
            s"solver ${if (solverRunnedParallel) "(multi-T)" else "(single-T)"} ${if (solverInCharge.isDefined) solverInCharge.get.name else '/'} " +
            s"(out of $maxTaskCountToStart ($realisticMaxTaskCountToStart)| " +
            s"new flavor decisions: $newSolverInpDecisions inp, $newSolverServerDecisions server, " +
            s" limit=$maxInpFlavorDecisionsPerRound, " +
            s"F nodes after alloc: ${graphManager.graph.nodes(graphManager.super_flavor_node_id).outgoing.size})"

        val fullMsg = msg +
          s" still active jobs:${graphManager.jobNodes.size} " +
          s" graph producers:${graphManager.getProducerCount}" +
          s" non-producers:${graphManager.graph.nodes.nonProducerIds.size}" +
          s" total supply:${graphManager.getTotalSupply}" +
          s" task groups switch:${switchTaskGroups}|${switchTaskGroupsBeforeSolving} " +
          s"server:${graphManager.taskGroupNodesInCommonPart.size - switchTaskGroups}|${serverTaskGroupsBeforeSolving}" +
          s" skipped tgs:${graphManager.skippedTaskGroupsBecauseOverloaded} f:${graphManager.skippedFlavorSelectorBecauseOverloaded}" +
          s" (props: ${graphManager.overloadedSwitchProp.zipWithIndex.filter(_._1).map(_._2).mkString("<", ",", ">")})" +
          s" flavor nodes:${graphManager.graph.nodes(graphManager.super_flavor_node_id).outgoing.size}" +
          s" forced server jobs:${forcedServerModeJobs}" +
          s" jobs preemted this round:${jobsPreemptedThisRound}" +
          s" | backlog job: ${jobBacklog.size}" +
          s" | SchedulingAttempts: ${statisticsSchedulingAttempts} " +
          s"| Current time: ${simulator.msToHuman(simulator.currentTime())}"

        if (maxTaskCountToStart > 0)
          simulator.logInfo(fullMsg)
        else
          logVerbose(fullMsg)

        if (statisticsSchedulingAttempts % 100 == 0
          || now < 1
          || (now < 10000 && statisticsSchedulingAttempts % 10 == 0)
          || (previousSpentSolverTime / SimTypes.GIGA) > 10) {
          simulator.logInfo(s" -- status -- ${statisticsSchedulingAttempts} rounds done -- ${simulator.msToHuman(now)} ")
          simulator.printQuickStats()
          TimeIt.printStatistics
        }
      }
      Simulator.gc()

      if (affectedAllocations.isEmpty) {
        var fromBacklog = 0
        if (jobBacklog.nonEmpty) {
          logVerbose(s"Nothing allocated, shall we push more job to the graph? Backlog: ${jobBacklog.size}")
          fromBacklog = onConsiderJobBacklog(flagSchedulingAsksForMore = true)
          if (fromBacklog > 0) {
            logVerbose(s"Pushed $fromBacklog jobs from backlog")
          }
        }

        // did something changed after cleanup?
        if (fromBacklog > 0 || forcedServerModeJobs > 0 || jobsPreemptedThisRound > 0) {
          logVerbose(s"Some jobs preempted ($jobsPreemptedThisRound) " +
            s"or forced to server ($forcedServerModeJobs), or from backlog, so re-trigger scheduling")
          resetPostponeSchedulingTrigger()
        } else {
          if (maxTaskCountToStart == 0) {
            logVerbose("No feasible allocation, probably because all guys ask for not available resources")
            postponeSameSchedulingProblemTillProblemChanges()
          } else {
            logVerbose("No feasible allocation.. cost model postpones allocations")
            postponeSameSchedulingProblemForCostUpdates()
          }
        }
      }
    }

    if (doNotRunSameSchedulingProblemBefore <= now) {
      assert(simulator.hasFutureEvents)
    }

    setRetriggerPostponedSchedulingEvent()
  }

  private def sanityCheckAllocatableSubtreesInGraph(): Unit = {
    graphManager.jobNodes.foreachEntry((job, linker) => {
      linker.forEachFlavorFlowNode(tgNode => checkTgNode(tgNode))
    })

    // check all machines
    graphManager.taskGroupNodesInCommonPart.foreach(tgNode => checkTgNode(tgNode))

    def checkTgNode(tgNode: FlowNode): Unit = {

      val tg = tgNode.taskGroup.get
      val options: mutable.ArrayDeque[FlowNode] =
        graphManager.resources.selectAllocatableSubtreesUsingCaches(tg)

      val directMachineCandidates: mutable.Seq[NodeID] = options.filter(fn => fn.isMachine).map(_.id)

      def addAllChilds(parent: FlowNode, childSet: mutable.HashSet[FlowNode]): Unit = {
        parent.outgoing.foreach(arc => {
          val child = graphManager.graph.nodes(arc.dst)
          if (child.isMachine)
            childSet.add(child)
          else addAllChilds(parent = child, childSet = childSet)
        })
      }

      val indirectMachineCandidates: mutable.Set[NodeID] = {
        val out: mutable.HashSet[FlowNode] = mutable.HashSet()
        options.filter(fn => !fn.isMachine).foreach(p => addAllChilds(parent = p, out))
        out.map(_.id)
      }

      if (tg.isSwitch) {
        // check if we checked all options
        cell.switches.indices.foreach(switch => {
          val switchNetId: NodeID = graphManager.mapping.getSwitchNetworkIDFromMachineID(switch)

          tgNode.outgoing.foreach(arc => {
            val dst = graphManager.graph.nodes(arc.dst)

            if (dst.isMachine)
              if (!directMachineCandidates.contains(dst.id) && !indirectMachineCandidates.contains(dst.id)) {
                simulator.logWarning(s"there is a arc: ${arc}, but the machine should not be accessible, ${tg.detailedToString()}, tg is definitely part of job:${tg.job.get.checkIfTaskGroupIsDefinitelyInFlavorSelection(tg)}")
                assert(false)
              }
          })

          assert((directMachineCandidates.contains(switchNetId) ||
            indirectMachineCandidates.contains(switchNetId)) == (cell.checkMaxTasksToAllocate(tg, switch) > 0),
            s"switch $switch ($switchNetId) is not found by select target, for ${tg.detailedToString()}," +
              s" but \n direct (${directMachineCandidates.size}):${directMachineCandidates} " +
              s"\n indirect (${indirectMachineCandidates.size}):$indirectMachineCandidates" +
              s"\n options holds ${options.size} entries")

        })
      } else {
        cell.servers.indices.foreach(server => {
          val serverNetId: NodeID = graphManager.mapping.getServerNodeIDFromMachineID(server)
          assert((directMachineCandidates.contains(serverNetId) ||
            indirectMachineCandidates.contains(serverNetId)) == (cell.checkMaxTasksToAllocate(tg, server) > 0),
            s"server $server ($serverNetId) is not found by select target, for ${tg.detailedToString()}," +
              s" but \n direct (${directMachineCandidates.size}):${directMachineCandidates} " +
              s"\n indirect (${indirectMachineCandidates.size}):$indirectMachineCandidates" +
              s"\n options holds ${options.size} entries")

        })
      }
    }
  }
}

object HireScheduler {
  var round: Long = 0
}