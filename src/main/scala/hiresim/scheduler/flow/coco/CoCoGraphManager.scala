package hiresim.scheduler.flow.coco

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.graph.NodeType
import hiresim.scheduler.flow.FlowGraphManager
import hiresim.scheduler.flow.hire.FlowNodeContainer
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraphUtils, FlowNode}
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.{Job, TaskGroup}

import scala.collection.mutable

class CoCoGraphManager(implicit scheduler: CoCoScheduler) extends FlowGraphManager {

  private[coco] var skippedTaskGroupsBecauseOverloaded = 0

  // The CapacityModel used to annotate the arcs in the MCMF Graph
  val capacities: CoCoCapacityModel = new CoCoCapacityModel
  // The CostModel used to annotate the arcs in the MCMF Graph
  val costs: CoCoCostModel = new CoCoCostModel

  private[flow] val mapping: PlaneGraphMapping = new PlaneGraphMapping()

  // Mapping for TaskGroup to their unique nodes (unschedule node / task aggregator)
  private[coco] val jobNodes: mutable.Map[Job, NodeLinker] = mutable.Map()
  private[coco] var taskGroupNodes: FlowNodeContainer = new FlowNodeContainer()

  private[coco] var minSwitchResources: Array[NumericalResource] = Array.fill(scheduler.cell.resourceDimSwitches)(Int.MaxValue)
  private[coco] var maxSwitchResources: Array[NumericalResource] = Array.fill(scheduler.cell.resourceDimSwitches)(0)
  private[coco] var minServerResources: Array[NumericalResource] = Array.fill(scheduler.cell.resourceDimServer)(Int.MaxValue)
  private[coco] var maxServerResources: Array[NumericalResource] = Array.fill(scheduler.cell.resourceDimServer)(0)

  // Initialize min/max available resources. Note that we can not change the topology on the fly as firmament can, so we
  // just initialize this statically. This models https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L797
  {

    for (server <- 0 until scheduler.cell.numServers) {
      val resources = scheduler.cell.totalMaxServerCapacity(server)

      for (dimension <- resources.indices) {

        if (resources(dimension) < minServerResources(dimension))
          minServerResources(dimension) = resources(dimension)

        if (maxServerResources(dimension) < resources(dimension))
          maxServerResources(dimension) = resources(dimension)

      }
    }

    for (switch <- 0 until scheduler.cell.numSwitches) {
      val resources = scheduler.cell.totalMaxSwitchCapacity(switch)

      for (dimension <- resources.indices) {

        if (resources(dimension) < minSwitchResources(dimension))
          minSwitchResources(dimension) = resources(dimension)

        if (maxSwitchResources(dimension) < resources(dimension))
          maxSwitchResources(dimension) = resources(dimension)
      }
    }

  }

  private[coco] def isGraphNonEmpty: Boolean = graph.nonEmpty

  private[coco] def isTaskGroupInGraph(taskgroup: TaskGroup): Boolean = {
    if (jobNodes.contains(taskgroup.job.get)) {
      jobNodes(taskgroup.job.get).taskGroupNodes.exists(node => {
        if (node.isTaskGroup)
          node.taskGroup.get eq taskgroup
        else
          false
      })
    } else
      false
  }

  private[coco] def cleanup(): Long = {

    var notScheduledTasksOnMachines = 0L
    var notScheduledTasks = 0L
    skippedTaskGroupsBecauseOverloaded = 0

    // Disconnect the TaskGroups from the topology graph. We will reconnect them
    // after we finished updating the resource availability information
    taskGroupNodes.foreach(entry => {
      disconnectAggregatorFromGraph(entry.id,
        exclude = jobNodes(entry.taskGroup.get.job.get).unscheduleNode.id)
    })

    resources.prepareSubtreeLookupCacheForCurrentRound()

    // Retrieve all the dirty servers
    val dirty_servers: Array[NodeID] = mapping.serverNodes.filter(id => graph.nodes(id).dirty).toArray
    // And propagate their new available resources through the tree
    resources.updateServerResourceStatistics(dirty_servers)

    // Retrieve all the dirty switches
    val dirty_switches: Array[NodeID] = mapping.switchNodes.filter(id => graph.nodes(id).dirty).toArray
    // And propagate their new available resources through the graph
    resources.updateSwitchResourceStatistics(dirty_switches)

    // All the machines that experienced some change
    val dirty_machines: Array[NodeID] = dirty_servers ++ dirty_switches

    // Update Machine -> Sink arcs on dirty machines
    dirty_machines.foreach((machineNetworkID: NodeID) => {
      // The concrete flow node representing the current machine
      val machine: FlowNode = graph.nodes(machineNetworkID)
      graph.updateArc(
        src = machineNetworkID,
        dst = graph.sinkNodeId,
        // The updated cost depending on whether we are dealing with a server or a switch
        newCost =
          if (machine.isServerMachine)
            costs.getServerToSink(machine)
          else
            costs.getSwitchToSink(machine))
    })


    val dirtyTaskGroups: mutable.ArrayDeque[TaskGroup] = {
      val out: mutable.ArrayDeque[TaskGroup] = mutable.ArrayDeque()
      taskGroupNodes.foreach(taskGroupNode => {
        val taskgroup = taskGroupNode.taskGroup.get

        // Only execute if that TaskGroup is dirty
        if (taskgroup.dirty)
          out.addOne(taskgroup)
      })

      // Then consider the TaskGroups that have been removed since the last run
      removed_taskgroup_backlog.foreach((taskgroup: TaskGroup) => {
        out.addOne(taskgroup)
      })
      out
    }

    // Recalculate the amount of running tasks in every affected node for every changed TaskGroup
    val fnUpdateMachinesTaskCount: FlowNode => Unit = resources.updateRunningTasksInSubtreeCountFn(
      dirtyTaskGroups,
      propagateMaxOfChildInsteadOfExactTaskCount = true)

    FlowGraphUtils.visitPhysicalParentsBottomUp(start = dirty_machines,
      graph = graph, function = (node: FlowNode) => {
        fnUpdateMachinesTaskCount(node)
      }, includeStart = true)


    // And clear the backlog of removed TaskGroups
    removed_taskgroup_backlog.clear()

    // check server load
    var limitServerTgs = false
    if (scheduler.cell.currentServerLoadMax().toDouble > SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD) {
      limitServerTgs = true
      scheduler.logVerbose(s"Overload - flag servers")
    }

    var allowedTaskGroupsToAdd = if (limitServerTgs) {
      (SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION * scheduler.cell.numServers).toInt max 10
    } else SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT
    assert(allowedTaskGroupsToAdd > 0)

    if (scheduler.jobBacklog.nonEmpty)
      allowedTaskGroupsToAdd = Int.MaxValue

    taskGroupNodes.foreach(taskGroupNode => {
      val taskGroup = taskGroupNode.taskGroup.get
      if (allowedTaskGroupsToAdd > 0) {
        allowedTaskGroupsToAdd -= 1

        // do we need to reactivate this guy as a producer node?
        if (!taskGroupNode.isProducer) {
          graph.nodes.migrateNonProducerToProducerNode(taskGroupNode.id)
        }
        taskGroupNode.supply = taskGroup.notStartedTasks

        connectTaskGroup(taskGroup = taskGroup, origin = taskGroupNode.id)

        notScheduledTasks += taskGroup.notStartedTasks
        notScheduledTasksOnMachines += taskGroup.maxAllocationsInOngoingSolverRound
      } else {
        skippedTaskGroupsBecauseOverloaded += 1
        // do we need to make this task group temporary a non producer?
        if (taskGroupNode.isProducer) {
          taskGroupNode.supply = 0L
          graph.nodes.migrateProducerToNonProducerNode(taskGroupNode.id)
        }
      }
      taskGroup.dirty = false
    })

    if (allowedTaskGroupsToAdd == 0) {
      scheduler.logDetailedVerbose(".. stop adding task groups to graph, reached soft limit already")
    }

    // Finally reset dirty flag and potentials for next solver run
    graph.nodes.foreach(node => {
      node.dirty = false
      node.potential = 0L
    })

    // Recalculate the supply of the sink node (most solvers need a net flow of 0)
    val sink = graph.nodes(graph.sinkNodeId)
    // The new supply is the negative amount of tasks to schedule (from FlavorSelectors and TaskGroups)
    sink.supply = -notScheduledTasks

    notScheduledTasksOnMachines
  }


  def addTaskGroup(taskGroup: TaskGroup): Option[FlowNode] = {

    def getNodeLinker(job: Job): NodeLinker = {
      jobNodes.getOrElseUpdate(job, {

        // Create the unschedule node of that job
        val unschedule_node = new FlowNode(
          supply = 0L,
          nodeType = NodeType.TASK_GROUP_POSTPONE,
          taskGroup = Some(taskGroup),
          level = scheduler.cell.highestLevel + 1
        )
        val unschedule_id: NodeID = graph.addNode(unschedule_node)

        // Connect the unschedule node to the sink
        graph.addArc(new FlowArc(
          src = unschedule_id,
          dst = graph.sinkNodeId,
          capacity = capacities.getJobUnscheduleToSink(job),
          cost = costs.getJobUnscheduleToSink(job)
        ))

        // Create the NodeLinker
        new NodeLinker(
          unscheduleNode = unschedule_node,
          taskGroupNodes = mutable.HashSet()
        )

      })
    }

    val job = taskGroup.job.get

    if (job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)
      && !job.isDone
      && taskGroup.notStartedTasks > 0
      // The following check ensures that if the FlavorSelector and TgReadyNow fire both at the same time,
      // the tg is only added once.
      && !isTaskGroupInGraph(taskGroup)) {

      // Get the linker storing all participating nodes in that job. If not present, create it.
      val linker = getNodeLinker(job)

      // Create the NTG/STG node representing the TaskGroup
      val taskgroup_node = new FlowNode(
        supply = taskGroup.notStartedTasks,
        nodeType =
          if (taskGroup.isSwitch)
            NodeType.NETWORK_TASK_GROUP
          else
            NodeType.SERVER_TASK_GROUP,
        taskGroup = Some(taskGroup),
        level = 4
      )
      // And add the new node instance to the graph
      val taskGroupNodeId = graph.addNode(taskgroup_node)

      // Remember the unschedule node used by this TaskGroup
      taskgroup_node.usedUnscheduleId = linker.unscheduleNode.id
      // Enqueue this node as a common option
      linker.taskGroupNodes.add(taskgroup_node)
      taskGroupNodes.add(taskgroup_node)

      // Connect the TaskGroup to the unschedule node
      graph.addArc(new FlowArc(
        src = taskGroupNodeId,
        dst = linker.unscheduleNode.id,
        capacity =
          if (taskGroup.isSwitch)
            capacities.getSwitchTaskGroupToUnschedule(taskGroup)
          else
            capacities.getServerTaskGroupToUnschedule(taskGroup),
        cost = 0L // will be updated later before scheduling
      ))

      Some(taskgroup_node)
    } else None

  }

  private[coco] def removeJob(job: Job, flag_dirty: Boolean = true): Unit = {
    // Only remove that job only if it was added before
    if (jobNodes.contains(job)) {

      val linker = jobNodes(job)

      // Make sure all the TG's are disconnected
      linker.taskGroupNodes.foreach(node => {
        // If needed, we can flag all of machines this TG's has allocs on as dirty
        if (flag_dirty) {
          val tg = node.taskGroup.get

          val machines =
            if (tg.isSwitch)
              mapping.switchNodes
            else
              mapping.serverNodes

          for (machine <- machines) {
            // The node representing that machine
            val nodes = graph.nodes(machine)
            // Is there a task hosted on that machine?
            if (node.allocatedTaskGroupsInSubtree.contains(tg))
              node.dirty = true
          }

        }

        removeNode(node.id)
      })

      // Also get rid of the unschedule node
      removeNode(linker.unscheduleNode.id)
      // Finally drop references to that linker
      jobNodes.remove(job)

    }
  }


  /**
   * Connects a TaskGroup node (In Firmament
   *
   * @param taskGroup the taskgroup
   * @param origin    the node representing the taskgroup
   */
  private[coco] def connectTaskGroup(taskGroup: TaskGroup,
                                     origin: NodeID): Unit = {


    val unschedCost = if (taskGroup.isSwitch)
      costs.getSwitchTaskToUnschedule(taskGroup)
    else
      costs.getServerTaskToUnschedule(taskGroup)
    // First update the arc connecting the TaskGroup to it's unschedule node
    graph.updateArc(
      src = origin,
      dst = jobNodes(taskGroup.job.get).unscheduleNode.id,
      newCost = unschedCost
    )

    // Find all the subtrees that can host this TaskGroup
    val allocatable: mutable.ArrayDeque[FlowNode] = resources.selectAllocatableSubtreesUsingCaches(taskGroup)
    // Reset to 0, so we can use this variable temporarily to accumulate outbound capacity
    taskGroup.maxAllocationsInOngoingSolverRound = 0L

    val targets: mutable.TreeMap[Cost, mutable.Stack[FlowNode]] = mutable.TreeMap()

    // Now map all of the options we got to their corresponding cost.
    allocatable.foreach((destination: FlowNode) => {

      val cost: Cost =
        if (taskGroup.isSwitch)
          costs.getSwitchTaskGroupToNode(taskGroup, destination)
        else
          costs.getServerTaskGroupToNode(taskGroup, destination)

      // Get the corresponding stack to that cost
      val target_stack = targets.getOrElseUpdate(cost.toLong, mutable.Stack())
      // And push it onto it
      target_stack.append(destination)

    })

    var validAllocationCheaperThanUnsched = false

    // Is the amount of shortcuts to be added unlimited?
    val shortcuts_unlimited = SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP == -1
    // Keep track of how many arcs we already added
    var shortcuts_added: NodeID = 0

    while (targets.nonEmpty
      && (shortcuts_unlimited || shortcuts_added < SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP)) {

      val (current_cost, current_targets) = targets.head
      targets.remove(current_cost)

      while (current_targets.nonEmpty
        && (shortcuts_unlimited || shortcuts_added < SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP)) {

        val destination = current_targets.removeHead()
        val capacity: Capacity = capacities.getTaskGroupToNode(taskGroup, destination)

        if (current_cost < unschedCost) {
          validAllocationCheaperThanUnsched = true
        }

        graph.addArc(new FlowArc(
          src = origin,
          dst = destination.id,
          capacity = capacity,
          cost = current_cost
        ))

        // Keep track of the current outgoing total capacity, which might constrain how many tasks we can start from
        // this TG in the next round.
        taskGroup.maxAllocationsInOngoingSolverRound += capacity
        // Also remember how many shortcuts we already added
        shortcuts_added += 1
      }
    }

    val nowCost = if (taskGroup.isSwitch)
      costs.getSwitchTaskToUnschedule(taskGroup)
    else
      costs.getServerTaskToUnschedule(taskGroup)

    assert(shortcuts_added == 0 || validAllocationCheaperThanUnsched, s"We have a taskgroup, which has shortcuts, " +
      s"but all are more expensive than the unschedule cost ($unschedCost)! ${taskGroup.detailedToString()}, $nowCost")

    // The maximum number of allocations is bound by the accumulated outbound capacity and the number of not started tasks
    taskGroup.maxAllocationsInOngoingSolverRound = math.min(taskGroup.maxAllocationsInOngoingSolverRound, taskGroup.notStartedTasks)
  }

  def removeNode(nodeId: NodeID): Unit = {

    if (SimulationConfiguration.SANITY_CHECKS_HIRE)
      assert(nodeId != -1, "Can not remove node with unset id tag!")

    val node: FlowNode = graph.nodes(nodeId)

    // If this node represents a TaskGroup, we need to drop references to this
    if (node.isTaskGroup) {

      val taskgroup = node.taskGroup.get
      // The linker holding all information about the nodes participating in the TaskGroup's job
      val linker: NodeLinker = jobNodes(taskgroup.job.get)

      // And drop references in corresponding collections
      linker.taskGroupNodes.remove(node)
      taskGroupNodes.remove(node)

      // Remember that we have deleted this TaskGroup. We still need to clear references to this TaskGroup
      // in the next round of 'clear'!!!
      removed_taskgroup_backlog += taskgroup
    }

    // remove all outgoing arcs and also clear parent/child relations
    graph.removeArcsWithSource(nodeId)
    graph.nodes.remove(nodeId)

  }

}

class NodeLinker(val unscheduleNode: FlowNode,
                 val taskGroupNodes: mutable.HashSet[FlowNode]) {

  assert(unscheduleNode.id >= 0)

  def foreach(fnc: FlowNode => Unit): Unit = {
    fnc(unscheduleNode)
    taskGroupNodes.foreach(taskNode => fnc(taskNode))
  }

  override def toString: String = s"U ${unscheduleNode.id}, ${taskGroupNodes.size}t"

}
