package hiresim.scheduler.flow

import hiresim.scheduler.flow.ArcDescriptor.{CapacityModel, CostModel}
import hiresim.scheduler.flow.solver.graph.FlowGraph
import hiresim.shared.graph.Graph.NodeID

abstract class FlowGraphStructure {

  /**
   * Provides the node id's of the switch network root nodes.
   *
   * @return the id's of the root nodes in the switch network
   */
  @inline private[flow] def getSwitchNetworkRootNodes: Seq[NodeID]

  @inline private[flow] def getSwitchNodes: Seq[NodeID]

  /**
   * Provides the node id's of the server network root nodes.
   *
   * @return the id's of the root nodes in the server network
   */
  @inline private[flow] def getServerNetworkRootNodes: Seq[NodeID]

  @inline private[flow] def getServerNodes: Seq[NodeID]

  /**
   * Maps the provided switch id to the corresponding network node id
   *
   * @param switch the id of the switch
   * @return the corresponding node id in the network
   */
  @inline private[flow] def getSwitchNetworkIDFromMachineID(switch: NodeID): NodeID

  /**
   * Maps the provided switch network node id to the corresponding switch id. Either the
   * NetworkNode id or the SwitchNode id can be provided
   *
   * @param switchNetworkNodeId the id of the node representing the switch in the graph
   * @return the id of the switch
   */
  @inline private[flow] def getSwitchMachineIDFromNetworkID(switchNetworkNodeId: NodeID): NodeID


  /**
   * Maps the provided server id to the corresponding network node id
   *
   * @param server the id of the server
   * @return the corresponding node id in the network
   */
  @inline private[flow] def getServerNodeIDFromMachineID(server: NodeID): NodeID

  /**
   * Maps the provided server network node id to the corresponding server id
   *
   * @param serverNetworkNodeId the id of the node representing the server the network
   * @return the id of the server
   */
  @inline private[flow] def getServerMachineIDFromNodeID(serverNetworkNodeId: NodeID): NodeID


  /**
   * Generates a new graph that has the structure desribed by this mapping
   *
   * @param costs      the cost model to be used
   * @param capacities the capacity model to be used
   * @return the graph
   */
  private[flow] def getEmptyFlowGraph(costs: CostModel,
                                      capacities: CapacityModel): FlowGraph

}
