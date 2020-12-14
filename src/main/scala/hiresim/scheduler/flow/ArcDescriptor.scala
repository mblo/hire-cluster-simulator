package hiresim.scheduler.flow

import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.TaskGroup

object ArcDescriptor {

  type CostModel = ArcDescriptor[Cost]

  type CapacityModel = ArcDescriptor[Capacity]

  /**
   * Constant used for edges that do not carry any specific cost value
   */
  final val NoCosts: Cost = 0L

}

trait ArcDescriptor[T] {

  /**
   * Values for the topology of the network
   */


  def getServerToSink(server: FlowNode): T

  def getSwitchToSink(switch: FlowNode): T


  def getNetworkTopologyToServer(server: NodeID): T

  def getNetworkTopologyToSwitch(server: NodeID): T


  def getServerNetworkTopologyToNetworkTopology(target: NodeID): T

  def getSwitchNetworkTopologyToNetworkTopology(target: NodeID): T


  /**
   * Values for TaskGroup related arcs
   */


  def getServerTaskGroupToNode(taskGroup: TaskGroup,
                               target: FlowNode): T

  def getSwitchTaskGroupToNode(taskGroup: TaskGroup,
                               target: FlowNode): T


  def getServerTaskGroupToUnschedule(taskGroup: TaskGroup): T

  def getSwitchTaskGroupToUnschedule(taskGroup: TaskGroup): T


  def getServerUnscheduleToSink(taskGroup: TaskGroup): T

  def getSwitchUnscheduleToSink(taskGroup: TaskGroup): T

}
