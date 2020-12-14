package hiresim.scheduler.flow.coco

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.scheduler.flow.ArcDescriptor.CostModel
import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.{Job, TaskGroup}

object CoCoCostModel {
  // Default to 1000 as given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.h#L101
  final val Omega = 1000

  final val CompareEps = 0.001
}

class CoCoCostModel(implicit scheduler: CoCoScheduler) extends CostModel {

  private lazy val graphManager: CoCoGraphManager = scheduler.graphManager
  private lazy val mapping = graphManager.mapping

  private var infinity: Cost = 0L

  /**
   * Without loss of generality we swap Topology -> Machine with Machine -> Sink costs, as one of them is 0.
   * Resource Topology -> Leaf given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L594
   */
  override def getServerToSink(server: FlowNode): Cost = {

    // The id of this server within the cell
    val server_id = mapping.getServerMachineIDFromNodeID(server.id)
    // Available and free resources
    val free_resources = scheduler.cell.servers(server_id)
    val max_resources = scheduler.cell.totalMaxServerCapacity(server_id)

    // The cost vector
    val costs: Array[Long] = new Array(free_resources.length)

    for (dim <- free_resources.indices)
      costs(dim) = normalize(max_resources(dim) - free_resources(dim), max_resources(dim))

    // We don't use that as we don't set interference scores
    // ColocationHelper.computeInterferenceScore(server.id, scheduler.graphManager.graph)

    flatten(costs, 0)
  }

  override def getSwitchToSink(switch: FlowNode): Cost = 0L

  /**
   * Without loss of generality we swap Topology -> Machine with Machine -> Sink costs, as one of them is 0.
   * Resource Leaf -> Sink given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L652
   */
  override def getNetworkTopologyToServer(server: NodeID): Cost = 0L

  override def getNetworkTopologyToSwitch(server: NodeID): Cost = 0L

  /**
   * Topology -> Topology given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L629
   */
  override def getServerNetworkTopologyToNetworkTopology(target: NodeID): Cost = 0L

  override def getSwitchNetworkTopologyToNetworkTopology(target: NodeID): Cost = 0L


  /**
   * TaskGroup -> ResourceNode given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L700
   */
  override def getServerTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Cost = getTaskGroupToNode(taskGroup, target, getServerTaskToServerTaskGroup(taskGroup))

  override def getSwitchTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Cost = getTaskGroupToNode(taskGroup, target, getSwitchTaskToSwitchTaskGroup(taskGroup))

  def getTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode, t2tgCost: Cost): Cost = {

    // This calculation normally would consider interference scores... As we don't set
    // any interference category, we can simply consider it as 0

    // Task and Aggregators are condensed into one single onde! Note that
    // scheduling through the cluster aggregator (at level 1) will cost infinity (worst case)
    if (target.level == 1)
      infinity + t2tgCost
    else
      0L + t2tgCost
  }

  /**
   * Task -> U given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L529
   */
  override def getServerTaskGroupToUnschedule(taskGroup: TaskGroup): Cost = getServerTaskToUnschedule(taskGroup)

  def getServerTaskToUnschedule(taskGroup: TaskGroup): Cost = {

    val minServer = graphManager.minServerResources
    val requested = taskGroup.resources.asInstanceOf[ServerResource].numericalResources

    getTaskToUnschedule(taskGroup, minServer, requested)
  }

  override def getSwitchTaskGroupToUnschedule(taskGroup: TaskGroup): Cost = getSwitchTaskToUnschedule(taskGroup)

  def getSwitchTaskToUnschedule(taskGroup: TaskGroup): Cost = {

    val minSwitch = graphManager.minSwitchResources
    val requested = taskGroup.resources.asInstanceOf[SwitchResource].numericalResources

    getTaskToUnschedule(taskGroup, minSwitch, requested)
  }

  def getTaskToUnschedule(taskGroup: TaskGroup,
                          minResources: Array[NumericalResource],
                          requestedResources: Array[NumericalResource]): Cost = {

    val vector: Array[Cost] = new Array(minResources.length + 2)

    // Representing machine_type_score
    vector(1) = 1L
    // Representing interference_score
    vector(2) = 1L

    for (dim <- minResources.indices)
      vector(2 + dim) = CoCoCostModel.Omega + normalize(1L, 1L)

    // Priority must be passed individually
    val priority =
      if (taskGroup.job.get.isHighPriority)
        1L
      else
        0L

    val base_cost = flatten(vector, priority);

    // Firmament is actually measuring in ms. So we're trying to somehow model that
    // so behaves like https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L553
    val time_since_submit = scheduler.simulator.currentTime() - taskGroup.submitted
    val wait_time_cost = (time_since_submit / 10.0).toLong

    // Result is the sum of waiting time costs and the base cost
    wait_time_cost + base_cost
  }

  /**
   * Task -> TaskAggregator as given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L666
   */
  def getServerTaskToServerTaskGroup(taskGroup: TaskGroup): Cost = {

    val maxServer = graphManager.maxServerResources
    val requested = taskGroup.resources.asInstanceOf[ServerResource].numericalResources

    getTaskToTaskGroup(taskGroup, maxServer, requested)
  }

  def getSwitchTaskToSwitchTaskGroup(taskGroup: TaskGroup): Cost = {

    val maxSwitch = graphManager.maxSwitchResources
    val requested = taskGroup.resources.asInstanceOf[SwitchResource].numericalResources

    getTaskToTaskGroup(taskGroup, maxSwitch, requested)
  }

  def getTaskToTaskGroup(taskGroup: TaskGroup,
                         maxResources: Array[NumericalResource],
                         requestedResources: Array[NumericalResource]): Cost = {

    val vector: Array[Cost] = new Array(maxResources.length)
    for (dim <- maxResources.indices)
      vector(dim) = normalize(requestedResources(dim), maxResources(dim))

    // Priority must be passed individually
    val priority =
      if (taskGroup.job.get.isHighPriority)
        1L
      else
        0L
    flatten(vector, priority);
  }

  /**
   * Unschedule -> Sink given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L574
   */
  override def getServerUnscheduleToSink(taskGroup: TaskGroup): Cost = getJobUnscheduleToSink(taskGroup.job.get)

  override def getSwitchUnscheduleToSink(taskGroup: TaskGroup): Cost = getJobUnscheduleToSink(taskGroup.job.get)

  def getJobUnscheduleToSink(job: Job): Cost = 0L


  private def normalize(raw_cost: Cost,
                        max_cost: Cost): Cost = {

    if (CoCoCostModel.Omega == 0 || math.abs(max_cost) < CoCoCostModel.CompareEps)
      return 0L
    ((raw_cost.toDouble / max_cost.toDouble) * CoCoCostModel.Omega).toLong
  }

  /**
   * Flattens a cost vector. This method models
   * https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L334
   *
   * @param vector   the cost vector
   * @param priority the priority dimension
   * @return the flattened result
   */
  private def flatten(vector: Array[Cost],
                      priority: Double): Cost = {
    val sum: Cost = vector.sum
    if (sum > infinity)
      infinity = sum + 1
    sum + (priority * CoCoCostModel.Omega).toLong
  }

}
