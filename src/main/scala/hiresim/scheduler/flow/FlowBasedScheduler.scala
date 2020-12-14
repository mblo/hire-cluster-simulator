package hiresim.scheduler.flow

import hiresim.cell.Cell
import hiresim.graph.NodeType
import hiresim.scheduler.Scheduler
import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.FlowNode.Supply
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}
import hiresim.scheduler.flow.solver.mcmf.util.MCMFSolverStatistics
import hiresim.scheduler.flow.solver.{ParallelSolver, Solver}
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.SimTypes.simTime
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.{ClosureEvent, RandomManager, SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.mutable

abstract class FlowBasedScheduler(override val name: String,
                                  override val cell: Cell,
                                  val solver: Solver,
                                  override val simulator: Simulator,
                                  override val schedulingComplexityPer100ThinkTime: Int = 5000,
                                  override val thinkTimeScaling: Double = 1.0)
  extends Scheduler(name = name, cell = cell, simulator = simulator, thinkTimeScaling = thinkTimeScaling,
    schedulingComplexityPer100ThinkTime = schedulingComplexityPer100ThinkTime) {

  private val thinkTimeBase = 100 / (Math.log(cell.numMachines) / Math.log(2))

  /**
   * for MCMF based schedulers, we set think time based on Fig 7 of Firmament,
   * i.e. with a cluster of 5000 machines, and 5000 tasks to start, think time is 100ms; with 50 pending tasks, it is 50ms.
   * When running HIRE with µ=100% (all jobs ask for INC), median solver time is at ~60ms, 99%q is <300ms.
   */
  def thinkTimeForExpectedTasksToStart(maxTaskCountToStart: Supply): SimTypes.simTime = {
    if (SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME) {
      val normalThinkTime = (thinkTimeBase * Math.log((maxTaskCountToStart max cell.numMachines).toDouble) / Math.log(2)).toInt
      val scaledThinkTime = (thinkTimeScaling * normalThinkTime).toInt max 1
      scaledThinkTime
    }
    else 1
  }

  private[flow] final val random: RandomManager = simulator.randomManager.copy
  private[flow] final val jobBacklog: mutable.Queue[Job] = mutable.Queue()

  /**
   * a flow scheduler may set this time threshold to a time value in future, which prevents re-running scheduling
   * of the very same scheduling problem before this time. I.e., scheduling will trigger only after this time,
   * or in the event of a new task group / job that is added / resource that becomes available
   */
  private[flow] var doNotRunSameSchedulingProblemBefore: SimTypes.simTime = 0

  @inline protected def postponeSameSchedulingProblemForCostUpdates(): Unit =
    doNotRunSameSchedulingProblemBefore = simulator.currentTime() + SimulationConfiguration.HIRE_POSTPONE_SCHEDULING_IF_STUCK

  @inline protected def postponeSameSchedulingProblemTillProblemChanges(): Unit = {
    logDetailedVerbose("long wait")
    doNotRunSameSchedulingProblemBefore = simulator.currentTime() + 100 * SimulationConfiguration.HIRE_POSTPONE_SCHEDULING_IF_STUCK
  }

  @inline protected def resetPostponeSchedulingTrigger(): Unit =
    doNotRunSameSchedulingProblemBefore = 0

  private[flow] val graphManager: FlowGraphManager

  // Before first scheduling attempt setup static resources in the graph
  simulator.scheduleEvent(new ClosureEvent(_ => {
    graphManager.resources.initializeStaticResources()
  }, 0))

  protected def runSolver(maxTaskCountToStart: Long): (FlowGraph, Boolean, Solver) = {
    solver match {
      case solver2: ParallelSolver =>
        if (simulator.currentTime() < SimulationConfiguration.MULTICORE_MCMF_SOLVER_ACTIVE_AFTER_TIME) {
          solver2.allowParallelProcessing(false)
        } else {
          solver2.allowParallelProcessing(true)
        }
        val (g: FlowGraph, s: Solver) = if (SimulationConfiguration.SANITY_HIRE_PARALLEL_SOLVER_SANITY_RUN_ALL_COMPARE_EQUALITY) {
          logVerbose("WARNING - run solver in compute extensive - all solver finish mode!")

          //          if (solver.getRunCount == 26) {
          //            println(graphManager.graph.exportDIMACS())
          //            System.out.flush()
          //          }

          val allSolverResults: Array[(FlowGraph, Solver)] = solver2.solveWithAll(graph = graphManager.graph, maxFlowsOnMachines = cell.numMachines.toLong.min(maxTaskCountToStart))

          val clonedResult = allSolverResults.head._1.clone()

          val resultWithAllocations: Array[(FlowGraph, Solver, Iterable[SolverEntry])] = allSolverResults.map(orig => {
            val result = interpretResult(orig._1, omitUnscheduled = false)
            (orig._1, orig._2, result)
          })

          resultWithAllocations.slice(1, resultWithAllocations.length).foreach(other => {
            assert(compareTwoSolverEntryLists(resultWithAllocations.head, other), s"found inconsistency of solver results, in run ${solver2.getRunCount}")
          })

          // we must return the cloned graph!
          (clonedResult, allSolverResults.head._2)
        } else {
          solver2.solveInlineOrCloned(graph = graphManager.graph, maxFlowsOnMachines = cell.numMachines.toLong.min(maxTaskCountToStart))
        }

        (g, g != graphManager.graph, s)
      case _ =>
        solver.solveInline(graph = graphManager.graph, maxFlowsOnMachines = cell.numMachines.toLong.min(maxTaskCountToStart))
        (graphManager.graph, false, solver)
    }
  }

  @inline override final def isPendingJobQueueEmpty: Boolean = {
    graphManager.graph.nodes.producerIds.isEmpty && jobBacklog.isEmpty
  }

  /**
   * Adds a new job to the scheduler. The scheduler will commence scheduling work on the individual
   * TaskGroups when space is available according to the selected backlog properties.
   *
   * @param job new job to be scheduled.
   */
  override def addJobAndSchedule(job: Job): Unit = {
    // Keep track of some statistics on that job
    allNotFinishedJobs.add(job)
    allNotScheduledJobs.add(job)

    // we add something to the graph, so we need to reset or guessed flag on a stuck scheduling situation
    resetPostponeSchedulingTrigger()

    // Add the job to the backlog. It will be added when there is space available or the feature is disabled.
    job.mostRecentTgSubmissionTime = job.submitted
    jobBacklog.enqueue(job)
  }

  override def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit = {
    // Push TaskGroup to graph. The GraphManager will check if this addition is legit or not (i.e. check flavor selections)
    graphManager.addTaskGroup(taskGroup)
    // we add something to the graph, so we need to reset or guessed flag on a stuck scheduling situation
    resetPostponeSchedulingTrigger()
  }

  def schedule(): Unit = {
    // Just enqueue a empty event. This will enforce that the post timestep hooks are called after this round
    // this should not be necessary... since this call is anyway in some time step.
    //    simulator.scheduleEvent(new EmptyEvent(simulator.currentTime()))

  }

  // Register scheduling as a post timestep hook at this point.
  simulator.registerPostSimulationStepHook(_ => if (!isScheduling) onTriggerScheduling())

  @inline protected def graphHasSomeWorkLeft(): Boolean = {
    graphManager.graph.nodes.producerIds.nonEmpty || jobBacklog.nonEmpty || getNumberOfPendingJobs > 0
  }

  protected def setRetriggerPostponedSchedulingEvent(): Unit = {
    val now = simulator.currentTime()
    if (doNotRunSameSchedulingProblemBefore > now) {
      logVerbose(s"add reminder to run scheduling later, in ${doNotRunSameSchedulingProblemBefore - now} " +
        s" @$doNotRunSameSchedulingProblemBefore")
      simulator.scheduleActionWithDelay(s => {}, doNotRunSameSchedulingProblemBefore - now)
    }
  }

  final protected def onTriggerScheduling(): Unit = {
    val now: simTime = simulator.currentTime()
    if (!isScheduling && cell.currentServerLoadMax().toDouble <= SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE) {
      logVerbose("check onTriggerScheduling fired")
      // Consider some unfinished business from the backlog before running the scheduling logic
      onConsiderJobBacklog()

      if (graphHasSomeWorkLeft()) {
        if (now >= doNotRunSameSchedulingProblemBefore) {

          // Post the cluster load to the MCMF stats
          MCMFSolverStatistics.load(cell.currentServerLoadMax().toDouble, cell.currentSwitchLoad().toDouble)

          runSchedulingLogic()
        } else {
          logVerbose(s"do not run scheduler, work left:${graphHasSomeWorkLeft()} but " +
            s"prevent scheduling before ${doNotRunSameSchedulingProblemBefore}")
        }
      }

    } else {
      logVerbose(s"check onTriggerSched failed.. isScheduling:${isScheduling} " +
        s"severLoad:${cell.currentServerLoadMax().toDouble <= SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE}")
    }
  }

  def getJobBacklogSize: Int = {
    jobBacklog.size
  }

  protected def onConsiderJobBacklog(flagSchedulingAsksForMore: Boolean = false): Int = {

    val supply_soft_limit_disabled = SimulationConfiguration.HIRE_BACKLOG_SUPPLY_SOFT_LIMIT == -1
    var jobs_added: Int = 0

    def onPushJobToHire(): Unit = {
      // we changed the scheduling problem, so simply reset the threshold
      resetPostponeSchedulingTrigger()

      val job = jobBacklog.dequeue()

      // Add all the TaskGroups whose submission is due.
      job.taskGroups.foreach((taskGroup: TaskGroup) => {
        if (taskGroup.submitted <= simulator.currentTime() && taskGroup.notStartedTasks > 0)
          graphManager.addTaskGroup(taskGroup)
      })

      // Register for a trigger when TaskGroups of that Job get ready.
      notifyMeForeachTaskGroupWhenReady(job)

      // Keep track of how many jobs we already added
      jobs_added += 1
      // And provide some info if desired
      simulator.logDebug(s"$this -> Pushed job $job for scheduling.")
    }

    var addJobs = 0
    if (flagSchedulingAsksForMore) {
      logVerbose(s"scheduler asks for more jobs (probably no allocation was possible)")
      addJobs += 1 max (SimulationConfiguration.HIRE_BACKLOG_MAX_NEW_JOBS / 10)
    }

    if (graphManager.getEstimatedSchedulingOptions >= SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT) {
      addJobs = 1
    }

    // If wanted we can always push at least one job. This is a fallback, so no matter how
    // many jobs are in the graph, we take always another job from the backlog
    while (addJobs > 0 && jobBacklog.nonEmpty) {
      onPushJobToHire()
      addJobs -= 1
    }

    // Add more producers.
    while (jobBacklog.nonEmpty &&
      // Either if backlog is not enabled
      (!SimulationConfiguration.HIRE_BACKLOG_ENABLE ||
        // Or we have more space available to add jobs
        ((jobs_added <= SimulationConfiguration.HIRE_BACKLOG_MAX_NEW_JOBS)
          && (graphManager.getEstimatedSchedulingOptions <= SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT)
          && (supply_soft_limit_disabled || graphManager.getTotalSupply <= SimulationConfiguration.HIRE_BACKLOG_SUPPLY_SOFT_LIMIT))))
      onPushJobToHire()

    logVerbose(s"Checked Backlog. Added ${jobs_added} jobs to the graph.")

    jobs_added
  }

  protected def runSchedulingLogic(): Unit

  class SolverEntry(val producerId: NodeID,
                    val actualProducerId: NodeID,
                    val allocationTargetId: NodeID,
                    val totalCost: Cost) {

    override def toString: String = s"Allocated producer $producerId (Actual: $actualProducerId)" +
      s" on $allocationTargetId with $totalCost$$"

  }

  protected def compareTwoSolverEntryLists(left: (FlowGraph, Solver, Iterable[SolverEntry]),
                                           right: (FlowGraph, Solver, Iterable[SolverEntry]),
                                           acceptDifferentResultWithEqualCost: Boolean = true): Boolean = {
    val entriesLeft = left._3
    val entriesRight = right._3
    val leftTotalCost = entriesLeft.foldLeft(0L)((c, e) => c + e.totalCost)
    val rightTotalCost = entriesRight.foldLeft(0L)((c, e) => c + e.totalCost)

    if (acceptDifferentResultWithEqualCost && leftTotalCost == rightTotalCost) {
      true
    } else {
      val leftSorted = entriesLeft.toList.sortBy(entry => entry.allocationTargetId)
      val rightSorted = entriesRight.toList.sortBy(entry => entry.allocationTargetId)

      val leftRepr = leftSorted.mkString("[\n\t", "\n\t", "\n]")
      val rightRepr = rightSorted.mkString("[\n\t", "\n\t", "\n]")

      if (leftRepr != rightRepr) {
        System.err.println(s"results differ, $leftTotalCost$$ vs $rightTotalCost$$: " +
          s"\nleft (${left._2.name}):${leftRepr}\n--" +
          s"\nright (${right._2.name}):${rightRepr}")
        false
      } else {
        true
      }
    }
  }

  /** Returns a map of producer IDs (server and switch tasks) with the physical machine ID on which
   * they have been allocated, including the corresponding allocation cost.
   * It also restores flows.
   *
   */
  protected def interpretResult(graph: FlowGraph,
                                omitUnscheduled: Boolean = true): Iterable[SolverEntry] = {

    // Result interpretation mapping virtual tasks to physical machines (servers or switches)
    val taskToMachineWithCost: mutable.ListBuffer[SolverEntry] = mutable.ListBuffer()

    // For each producer node (virtual task or 'unscheduled' nodes)
    for (super_producer_id: NodeID <- graph.nodes.producerIds) {

      val super_producer_node = graph.nodes(super_producer_id)
      var expectedOutgoingFlows = 0L
      var foundPaths = 0
      val producerOutArcs: mutable.Queue[FlowArc] = mutable.Queue()

      for (outgoing <- super_producer_node.outgoing) {
        if (outgoing.flow > 0) {
          // If the producer is a super flavor node, the producers are the nodes connected to that SFS
          if (super_producer_node.nodeType == NodeType.SUPER_JOB_FLAVOR_SELECTOR) {

            val flavor_node = graph.nodes(outgoing.dst)
            outgoing.residualCapacity += 1
            outgoing.reverseArc.residualCapacity -= 1


            for (flavor_option_arc <- flavor_node.outgoing) {
              if (flavor_option_arc.flow > 0) {
                producerOutArcs.enqueue(flavor_option_arc)
                expectedOutgoingFlows += flavor_option_arc.flow
              }
            }
          } else {
            producerOutArcs.enqueue(outgoing)
            expectedOutgoingFlows += outgoing.flow
          }
        }
      }


      while (producerOutArcs.nonEmpty) {

        val outgoingArcFromProducer: FlowArc = producerOutArcs.dequeue()
        val producer_id = outgoingArcFromProducer.src
        var actualProducerId = producer_id
        var totalCost: Cost = outgoingArcFromProducer.cost
        // Server or switch on which this producer has been allocated
        var allocationTargetNodeId: NodeID = -1
        // Visiting neighbors if there are any
        var currentNodeId: NodeID = producer_id
        var outgoingArc: FlowArc = outgoingArcFromProducer
        val nodeAfterProducer = graph.nodes(outgoingArc.dst)

        if (graph.nodes(currentNodeId).nodeType == NodeType.JOB_FLAVOR_SELECTOR) {
          if (nodeAfterProducer.nodeType == NodeType.SERVER_TASK_GROUP || nodeAfterProducer.nodeType == NodeType.NETWORK_TASK_GROUP) {
            actualProducerId = nodeAfterProducer.id
            val actualProducer = graph.nodes(actualProducerId)
            assert(actualProducer.nodeType == NodeType.NETWORK_TASK_GROUP || actualProducer.nodeType == NodeType.SERVER_TASK_GROUP, s"wrong actual producer type: ${actualProducer.nodeType}")
          }
        } else {
          val producer = graph.nodes(producer_id)
          assert(producer.nodeType == NodeType.NETWORK_TASK_GROUP || producer.nodeType == NodeType.SERVER_TASK_GROUP, s"wrong producer type: ${producer.nodeType}")
        }

        // Now find a path to the sink, so we can extract the machine to allocate on
        while (outgoingArc.dst != graph.sinkNodeId) {

          // Keeping track of already-visited arcs
          outgoingArc.residualCapacity += 1
          outgoingArc.reverseArc.residualCapacity -= 1

          // If this arc goes to a physical machine (server/switch) or a unschedule node
          if (graph.nodes(outgoingArc.dst).canHost) {
            // Keep track of this machine (server/switch) and put it in the result data structure
            allocationTargetNodeId = outgoingArc.dst
          }

          // Proceeding, till we do not find any other outgoing flow
          currentNodeId = outgoingArc.dst
          val it = graph.nodes(currentNodeId).outgoing.iterator
          var doSearch = true
          while (doSearch && it.hasNext) {
            val candidate = it.next()
            if (candidate.flow > 0) {
              doSearch = false
              outgoingArc = candidate
            }
          }

          totalCost += outgoingArc.cost

          assert(!doSearch, "We failed to find an outgoing arc with remaining flow, however, we did not reach the sink." +
            s" Looks like broken flow network. node:${currentNodeId}")
        }

        // The flow towards the sink should be updated too
        outgoingArc.residualCapacity += 1
        outgoingArc.reverseArc.residualCapacity -= 1

        val allocationTargetNode = graph.nodes(allocationTargetNodeId)

        if (allocationTargetNode.isUnschedule && omitUnscheduled) {
          super_producer_node.supply += 1
        } else {
          // Adding the mapping to the result
          taskToMachineWithCost += new SolverEntry(producerId = producer_id, actualProducerId = actualProducerId,
            allocationTargetId = allocationTargetNodeId, totalCost = totalCost)
        }

        foundPaths += 1

        // was the original arc flow more than 1, so is there something left?
        if (outgoingArcFromProducer.flow > 0) {
          // if so, consider it
          producerOutArcs.enqueue(outgoingArcFromProducer)
        }

      }

      assert(foundPaths == expectedOutgoingFlows, s"Solver result seems to be wrong, I found more outgoing flows " +
        s"from a producer than actual flow paths to the sink. $foundPaths < $expectedOutgoingFlows. " +
        s"Producer: $super_producer_id, remaining out arcs: ${producerOutArcs.mkString("|")}")

    }

    // Returning the interpreted result
    taskToMachineWithCost
  }

  override protected def someTasksAreDoneAndFreedResources(allocation: Allocation): Unit = {
    resetPostponeSchedulingTrigger()

    // The involved TaskGroup
    val taskGroup: TaskGroup = allocation.taskGroup
    // The node representing the affected machine
    val node = graphManager.graph.nodes(allocation.machineNetworkID)

    // Mark this node as dirty. This will trigger an update for all connected cached values of this machine
    node.dirty = true
    // Also mark the involved TaskGroup as dirty
    taskGroup.dirty = true
    taskGroup.dirtyMachines.add(allocation.machineNetworkID)

    // If there are no tasks left to be scheduled the GraphManager has dropped any reference to that TG. We have
    // to manually inform him that this TaskGroup is of interest now.
    if (taskGroup.notStartedTasks == 0)
      graphManager.onTaskGroupCompleted(taskGroup)

    // We also have to reduce the statistics on the number of running tasks on that node
    val statistics = node.allocatedTaskGroupsInSubtree
    val running = statistics(taskGroup) - allocation.numberOfTasksToStart
    assert(running >= 0)
    assert(running <= taskGroup.runningTasks)

    if (running == 0)
      statistics.remove(taskGroup)
    else
      statistics.update(taskGroup, running)

    // There may be options to schedule tasks now...
    schedule()

  }


}
