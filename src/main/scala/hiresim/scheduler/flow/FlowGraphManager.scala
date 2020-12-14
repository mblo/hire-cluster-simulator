package hiresim.scheduler.flow

import hiresim.scheduler.flow.ArcDescriptor.{CapacityModel, CostModel}
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.shared.TimeIt
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.TaskGroup

import scala.collection.mutable

abstract class FlowGraphManager(implicit scheduler: FlowBasedScheduler) {

  // A Backlog that holds all the TaskGroups that have been removed since the last invocation of clean.
  private[flow] lazy val removed_taskgroup_backlog: mutable.Set[TaskGroup] = mutable.HashSet()
  // The resource helper managing the distribution of resource availability
  private[flow] val resources: PhysicalResourceHelper = new PhysicalResourceHelper()

  // The CapacityModel to determine a arc's capacity
  private[flow] val capacities: CapacityModel
  // The CostModel to determine a arc's cost
  private[flow] val costs: CostModel

  // The mapping from node ids to machine ids, etc
  private[flow] val mapping: FlowGraphStructure
  // The graph modelling the MCMF problem
  private[flow] lazy val graph: FlowGraph = mapping.getEmptyFlowGraph(costs, capacities)


  @inline def getProducerCount: Int = {
    graph.nodes.producerIds.size
  }

  def getEstimatedSchedulingOptions: Int = {
    getProducerCount
  }

  def getTotalSupply: Long = {

    val t = TimeIt("getTotalSupply")

    // The accumulator for the supply
    var supply: Long = 0

    // Now accumulate the total supply over all the producers in the graph
    graph.nodes.producerIds.foreach((producer_id: NodeID) => {
      supply += graph.nodes(producer_id).supply
    })

    t.stop

    // All the supply in the graph
    supply
  }

  def getHasProducers: Boolean = getProducerCount > 0


  def addTaskGroup(taskGroup: TaskGroup): Option[FlowNode]

  def onTaskGroupCompleted(taskGroup: TaskGroup): Unit = {
    removed_taskgroup_backlog += taskGroup
  }

  protected def disconnectAggregatorFromGraph(node_id: NodeID,
                                              exclude: NodeID = -1): Unit = {

    // If no specific node to exclude was provided, we must not compare it to every arc
    if (exclude == -1)
      graph.removeArcsWithSource(node_id)
    // Else, we compare for each node
    else {
      graph.removeArcsOfNodeExcept(node_id, exclude = exclude)

    }
  }

}
