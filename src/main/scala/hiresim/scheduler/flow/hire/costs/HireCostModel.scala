package hiresim.scheduler.flow.hire.costs

import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.graph.NodeType
import hiresim.scheduler.flow.ArcDescriptor.CostModel
import hiresim.scheduler.flow.hire.HireLocalityHandlingMode.{DEFAULT, REMOVE, WEIGHT_MAX, WEIGHT_MIN}
import hiresim.scheduler.flow.hire.{HireGraphManager, HireScheduler, NodeLinker}
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.configuration.SimulationConfiguration.PRECISION
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.{Job, SharedResources, TaskGroup}

class HireCostModel(graph_manager: HireGraphManager)(implicit scheduler: HireScheduler) extends CostModel {

  private[hire] final val interference: HireInterferenceCostCalculator = new HireInterferenceCostCalculator(graph_manager)
  private[hire] final val priority: HirePriorityCostCalculator = new HirePriorityCostCalculator()
  private[hire] final val locality: HireLocalityCostCalculator = graph_manager.locality


  def onClearCaches(): Unit = {
    interference.onClearCaches()
    priority.onClearCaches()
    locality.onClearCaches()
  }


  override def getNetworkTopologyToServer(server: NodeID): Cost = 0L

  override def getNetworkTopologyToSwitch(server: NodeID): Cost = 0L


  override def getSwitchNetworkTopologyToNetworkTopology(target: NodeID): Cost = 0L

  override def getServerNetworkTopologyToNetworkTopology(target: NodeID): Cost = 0L


  override def getServerToSink(serverNetworkNode: FlowNode): Cost = {

    // Retrieve the resources of that server instance
    val utilization: Array[Cost] = scheduler.cell.getMachineLoad(
      machine = scheduler.graphManager.mapping.getServerMachineIDFromNodeID(serverNetworkNode.id),
      isSwitch = false,
      scaling = PRECISION)

    flatten(Array(flatten(utilization), PRECISION - stddv(utilization)))
  }

  override def getSwitchToSink(switchNetworkNode: FlowNode): Cost = {
    getSwitchToSinkCost(switchNetworkNode, scheduler.graphManager.graph)
  }


  def getSwitchToSinkCost(switchNetworkNode: FlowNode, graph: FlowGraph): Cost = {

    // Retrieve the id of the switch at the provided node
    val switch: NodeID = scheduler.graphManager.mapping.getSwitchMachineIDFromNetworkID(switchNetworkNode.id)
    // Retrieve the resources of the switch instance. Note that we are only interested in the numerical resource at this point
    val utilization: Array[Cost] = scheduler.cell.getMachineLoad(
      machine = switch,
      isSwitch = true,
      scaling = PRECISION)


    val costVec = Array(
      flatten(utilization),
      PRECISION - stddv(utilization),
      interference.getSwitchToSinkCost(switchNetworkNode, graph))

    SimulationConfiguration.HIRE_LOCALITY_MODE_M2S match {

      case DEFAULT =>
        flatten(costVec :+ locality.getSwitchToSinkCost(switchNetworkNode, graph))
      case WEIGHT_MIN =>
        flatten(costVec :+ 0L)
      case WEIGHT_MAX =>
        flatten(costVec :+ PRECISION)
      case REMOVE =>
        flatten(costVec)

    }

  }


  override def getServerTaskGroupToNode(taskGroup: TaskGroup,
                                        targetNode: FlowNode): Cost =
    getServerTaskGroupToNode(taskGroup, targetNode, isPartOfFlavorSelectorPart = false)

  def getServerTaskGroupToNode(taskGroup: TaskGroup,
                               targetNode: FlowNode,
                               isPartOfFlavorSelectorPart: Boolean): Cost = {

    // The required resources
    val required = taskGroup.resources.asInstanceOf[ServerResource]

    val demand_hadamard_division: Array[Long] = new Array(scheduler.cell.resourceDimServer)
    val max_available =
      if (targetNode.nodeType == NodeType.MACHINE_SERVER)
        scheduler.cell.servers(scheduler.graphManager.mapping.getServerMachineIDFromNodeID(targetNode.id))
      else
        targetNode.maxServerResourcesInSubtree.numericalResources

    // The combined resource
    var index = 0
    while (index < demand_hadamard_division.length) {
      assert(required.numericalResources(index) <= max_available(index), "looks like calculating edge cost for a not feasible allocation")
      demand_hadamard_division(index) = (PRECISION * required.numericalResources(index)) / max_available(index)
      index += 1
    }

    val cost_vector = Array(
      flatten(demand_hadamard_division),
      stddv(demand_hadamard_division),
      1L,
      priority.getServerTaskGroupToNodeCost(taskGroup, targetNode, isPartOfFlavorSelectorPart)
    )

    val flattened = SimulationConfiguration.HIRE_LOCALITY_MODE_STG2N match {
      case DEFAULT =>
        flatten(cost_vector :+ locality.getServerTaskGroupToNodeCost(taskGroup, targetNode, isPartOfFlavorSelectorPart))
      case WEIGHT_MIN =>
        flatten(cost_vector :+ 0L)
      case WEIGHT_MAX =>
        flatten(cost_vector :+ PRECISION)
      case REMOVE =>
        flatten(cost_vector)
    }

    flattened
  }


  override def getSwitchTaskGroupToNode(taskGroup: TaskGroup,
                                        targetNode: FlowNode): Cost =
    getSwitchTaskGroupToNode(taskGroup, targetNode, isPartOfFlavorSelectorPart = false)

  def getSwitchTaskGroupToNode(taskGroup: TaskGroup,
                               targetNode: FlowNode,
                               isPartOfFlavorSelectorPart: Boolean): Cost = {

    // The required resources
    val required: SwitchResource = taskGroup.resources.asInstanceOf[SwitchResource]
    val demand_hadamard_division: Array[Long] = new Array(scheduler.cell.resourceDimSwitches)
    val max_resources: SwitchResource =
      if (targetNode.nodeType == NodeType.MACHINE_NETWORK)
        scheduler.cell.switches(scheduler.graphManager.mapping.getSwitchMachineIDFromNetworkID(targetNode.id))
      else
        targetNode.maxSwitchResourcesInSubtree

    // The properties we still need to start
    val new_props = required.properties.remove(max_resources.properties)
    // The resources associated with the shared resources
    val (shared_resources, _) = SharedResources.getCellResourceDemandAndRealUsage(required.properties, new_props)
    // The max resources available at that node
    val max_available = max_resources.numericalResources

    // The combined resource
    var index = 0
    while (index < demand_hadamard_division.length) {
      val thisDimensionRes = required.numericalResources(index) + shared_resources(index)
      assert(thisDimensionRes <= max_available(index), "looks like calculating edge cost for a not feasible allocation")
      // special case, demand == available == 0
      if (max_available(index) == 0) {
        demand_hadamard_division(index) = 0
      } else {
        demand_hadamard_division(index) = (PRECISION * thisDimensionRes) / max_available(index)
      }
      index += 1
    }

    val cost_vector = Array(
      flatten(demand_hadamard_division),
      stddv(demand_hadamard_division),
      // Setup priority entry
      priority.getSwitchTaskGroupToNodeCost(taskGroup, targetNode, isPartOfFlavorSelectorPart),
      // Setup interference entry
      interference.getSwitchTaskGroupToNodeCost(taskGroup, targetNode, isPartOfFlavorSelectorPart)
    )

    val flattened =
      SimulationConfiguration.HIRE_LOCALITY_MODE_NTG2N match {

        case DEFAULT =>
          flatten(cost_vector :+ locality.getSwitchTaskGroupToNodeCost(taskGroup, targetNode, isPartOfFlavorSelectorPart))
        case WEIGHT_MIN =>
          flatten(cost_vector :+ 0L)
        case WEIGHT_MAX =>
          flatten(cost_vector :+ PRECISION)
        case REMOVE =>
          flatten(cost_vector)

      }

    val out = flattened
    assert(out <= PRECISION)
    out
  }


  override def getServerTaskGroupToUnschedule(taskGroup: TaskGroup): Cost = getTaskGroupToUnschedule(taskGroup)

  override def getSwitchTaskGroupToUnschedule(taskGroup: TaskGroup): Cost = getTaskGroupToUnschedule(taskGroup)

  def getTaskGroupToUnschedule(taskGroup: TaskGroup): Cost = {
    priority.getTaskGroupToUnscheduleCost(taskGroup) +
      ((5.0 + SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR) * PRECISION).toLong
  }


  override def getServerUnscheduleToSink(taskGroup: TaskGroup): Cost = 0L

  override def getSwitchUnscheduleToSink(taskGroup: TaskGroup): Cost = 0L


  def getFlavorSelectorToTaskGroupCapacity(flavorNodeLinker: NodeLinker,
                                           taskGroup: TaskGroup): Capacity = {
    interference.capacityFlavorSelector2TaskGroup(flavorNodeLinker, taskGroup)
  }


  def getFlavorSelectorToTaskGroup(flavorNodeLinker: NodeLinker,
                                   taskGroup: TaskGroup,
                                   taskGroupNode: FlowNode): Cost = {

    val value = interference.getFlavorSelectorToTaskGroupCost(flavorNodeLinker, taskGroup, taskGroupNode)
    assert(value >= 0)
    assert(value <= PRECISION)

    val penalty: Long = if (taskGroup.belongsToInpFlavor) {
      0L
    } else {
      // The time the job is already waiting for scheduling
      val waiting_time: Int = scheduler.simulator.currentTime() - taskGroup.consideredForSchedulingSince

      if (waiting_time <= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER) {
        (SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR * PRECISION).toLong
      } else {
        var ratio = (waiting_time.toDouble - SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER) /
          (SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER - SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER)
        assert(ratio >= 0.0)

        if (waiting_time >= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER) {
          ratio = 1.0
        }
        assert(ratio <= 1.0)

        // - tanh ( ratio * 3 - 3)
        val fnc = -math.tanh(ratio * 3.0 - 3.0)
        assert(fnc >= 0.0)
        assert(fnc <= 1.0)

        (fnc * PRECISION * SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR).toLong
      }
    }

    assert(penalty >= 0)
    assert(penalty <= SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR * PRECISION,
      s"$penalty >= ${SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR} * $PRECISION")

    value + penalty
  }

  def getSuperFlavorSelectorToFlavorSelector: Cost = 1L * PRECISION

  def getFlavorSelectorToUnschedule(job: Job, nodeLinker: NodeLinker): Cost = {
    val delayCost = PRECISION
    3L * PRECISION + delayCost
  }

  private def flatten(values: Array[Long]): Long = {
    // As for now every resource dimension has unit weight
    assert(values.count(v => (v < 0) || (v > PRECISION)) == 0, s"invalid cost vector ${values.mkString(",")} has ${values.count(v => (v < 0) || (v > PRECISION))} corrupted entries (${values.filter(v => (v < 0) || (v > PRECISION)).mkString})")
    mean(values)
  }

  private def variance(values: Array[Long]): Long = {
    mean(values.map(math.pow(_, 2).longValue)) - math.pow(mean(values), 2).longValue
  }

  private def stddv(values: Array[Long]): Long = {
    math.sqrt(variance(values)).toLong
  }

  private def mean(values: Array[Int]): Int = {
    values.sum / values.length
  }

  private def mean(values: Array[Long]): Long = {
    values.sum / values.length
  }

}
