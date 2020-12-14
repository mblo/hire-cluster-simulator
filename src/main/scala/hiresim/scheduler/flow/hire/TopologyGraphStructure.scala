package hiresim.scheduler.flow.hire

import hiresim.cell.{Cell, CellEdge}
import hiresim.graph.NodeType
import hiresim.scheduler.Scheduler
import hiresim.scheduler.flow.ArcDescriptor.{CapacityModel, CostModel}
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.scheduler.flow.{ArcDescriptor, FlowGraphStructure}
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

/**
 * Maps the topology of the underlying cell to the resource topology.
 *
 * @param scheduler the scheduler that operates on
 */
class TopologyGraphStructure(implicit scheduler: Scheduler) extends FlowGraphStructure {

  private val cell: Cell = scheduler.cell

  private[scheduler] val numNetworkNodes: Int = cell.switches.length
  private[scheduler] val numTorSwitches: Int = cell.numTorSwitches
  private[scheduler] val numNetworkNodeDummies: Int = numNetworkNodes
  private[scheduler] val numNetworkGroups: Int = numNetworkNodes
  private[scheduler] val numServerNodes: Int = cell.numServers

  if (SimulationConfiguration.SANITY_CHECKS_CELL) {
    assert(numNetworkNodes >= 0, s"Negative number of NNs: $numNetworkNodes.")
    assert(numTorSwitches >= 0, s"Negative number of ToR switches: $numTorSwitches.")
    assert(numNetworkNodeDummies >= 0, s"Negative number of NNDs: $numNetworkNodeDummies.")
    assert(numNetworkGroups >= 0, s"Negative number of NGs: $numNetworkGroups.")
    assert(numServerNodes >= 0, s"Negative number of SNs: $numServerNodes.")
  }

  /** Modelling the network topology of the switch network * */

  @inline private[scheduler] final def switchNetworkNodes: Range = 0 until numNetworkNodes

  // Top Level nodes
  @inline private[scheduler] final def switchNetworkRootNodes: Range = switchNetworkNodes.start until switchNetworkNodes.start + scheduler.cell.numRootSwitches

  // Bottom level nodes
  @inline private[scheduler] final def switchNetworkGroupLeaves: Range = switchNetworkNodes.end - numTorSwitches until switchNetworkNodes.end


  @inline override private[flow] def getSwitchNetworkRootNodes: Range = switchNetworkRootNodes

  @inline private[scheduler] final def networkNodeRelativeIndex(nn: NodeID): NodeID = {
    val nnRelativeIndex: NodeID = nn - switchNetworkNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(nnRelativeIndex), s"Invalid NN relative index: $nnRelativeIndex.")

    nnRelativeIndex
  }


  /** Modelling the switch type machines * */

  @inline private[scheduler] final def switchNodes: Range = switchNetworkNodes.end until switchNetworkNodes.end + numNetworkNodeDummies

  @inline private[scheduler] final def switchToRNodes: Range = switchNodes.end - numTorSwitches until switchNodes.end

  @inline override private[flow] def getSwitchNodes: Range = switchNodes

  /**
   * Maps the provided switch id to the corresponding network node id
   *
   * @param switch the id of the switch
   * @return the corresponding node id in the network
   */
  @inline private[flow] final def getSwitchNetworkIDFromMachineID(switch: NodeID): NodeID = {

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(switch), s"Provided switch id is invalid: $switch")

    switch + switchNodes.start
  }

  /**
   * Maps the provided switch network node id to the corresponding switch id. Either the
   * NetworkNode id or the SwitchNode id can be provided
   *
   * @param switchNetworkNodeId the id of the node representing the switch in the graph
   * @return the id of the switch
   */
  @inline private[scheduler] final def getSwitchMachineIDFromNetworkID(switchNetworkNodeId: NodeID): NodeID = {

    val switch: NodeID = if (switchNodes.contains(switchNetworkNodeId))
      switchNetworkNodeId - switchNodes.start
    else
      switchNetworkNodeId

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(switch), s"Invalid NN index: $switch.")

    switch
  }

  /**
   * Maps the provided network node id of a switch machine to the id of the directly connected topology
   * node in the switch topology network.
   *
   * @param switchNodeId the id of the network node representing the switch machine
   * @return the id of the switch topology network node directly connected to the switch network node
   */
  @inline private[scheduler] final def getSwitchNetworkNodeIDForSwitchNodeID(switchNodeId: NodeID): NodeID = {

    val topologyNode = switchNodeId - switchNodes.start + switchNetworkNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(topologyNode), s"Conversion from switch network node to corresponding topology node failed. Produced id: ${topologyNode}")

    topologyNode
  }

  /**
   * Maps the provided topology node id of a switch network topology node to the id of the directly connected
   * switch node.
   *
   * @param switchNetworkId the id of the switch topology node to find the connected switch node if for
   * @return the id of the switch node that is directly connected to the provided topology node id
   */
  @inline private[scheduler] final def getSwitchNodeIDForSwitchNetworkNodeID(switchNetworkId: NodeID) = {

    val switchNode = switchNetworkId - switchNetworkNodes.start + switchNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNodes.contains(switchNode), s"Conversion from switch network topology node to corresponding switch network node failed. Produced id: ${switchNode}")

    switchNode
  }

  /**
   * Maps the provided server network id to the corresponding switch node id.
   *
   * @param serverNodeId the id of the server network node
   * @return the id of the switch node that corresponds to the given server node id
   */
  @inline private[scheduler] final def getSwitchNodeIDFromServerNodeID(serverNodeId: NodeID): NodeID = {

    val serverNetworkToRNode = getServerTorNetworkNodeIDForServerNodeID(serverNodeId)
    val switchNetworkNode = getSwitchNetworkNodeIDFromServerNetworkNodeID(serverNetworkToRNode)
    val switchNode = getSwitchNodeIDForSwitchNetworkNodeID(switchNetworkNode)

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNodes.contains(switchNode), s"Conversion from ServerNetworkNodeID to corresponding ToRSwitchNodeID failed (Produced: ${switchNode} For: ${serverNodeId}).")

    switchNode

  }

  /** Modelling the topology for the server network * */

  @inline private[scheduler] final def serverNetworkNodes: Range = switchNodes.end until switchNodes.end + numNetworkGroups

  // Top Level nodes
  @inline private[scheduler] final def serverNetworkRootNodes: Range = serverNetworkNodes.start until serverNetworkNodes.start + scheduler.cell.numRootSwitches

  // Bottom level nodes
  @inline private[scheduler] final def serverNetworkGroupLeaves: Range = serverNetworkNodes.end - numTorSwitches until serverNetworkNodes.end


  @inline override private[flow] def getServerNetworkRootNodes: Range = serverNetworkRootNodes


  /**
   * Maps the provided switch network node to the corresponding server network node
   *
   * @param node a node in the switch network to find the counterpart to
   * @return the corresponding server network node
   */
  @inline private[scheduler] final def getSwitchNetworkNodeIDFromServerNetworkNodeID(node: NodeID): NodeID = {

    val nn: NodeID = node - serverNetworkNodes.start + switchNetworkNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(nn), s"Conversion from server network node to switch network node failed. Result id not in correct region: $nn.")

    nn
  }

  /**
   * Maps the provided server network node to the corresponding switch network node
   *
   * @param node the node in the server network to find the counterpart to
   * @return the corresponding switch network node
   */
  @inline private[scheduler] final def getServerNetworkNodeIDFromSwitchNetworkNodeID(node: NodeID): NodeID = {

    val serverNetworkNode: NodeID = node - switchNetworkNodes.start + serverNetworkNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(serverNetworkNodes.contains(serverNetworkNode), s"Conversion from switch network node to server network node failed. Result id not in correct region: $serverNetworkNode.")

    serverNetworkNode
  }

  /** Modelling the server type machines  * */


  @inline private[scheduler] final def serverNodes: Range = serverNetworkNodes.end until serverNetworkNodes.end + numServerNodes

  @inline override private[flow] def getServerNodes: Range = serverNodes


  /**
   * Maps the provided server id to the corresponding network node id
   *
   * @param server the id of the server
   * @return the corresponding node id in the network
   */
  @inline private[scheduler] final def getServerNodeIDFromMachineID(server: NodeID): NodeID = {
    val snWithOffset: NodeID = server + serverNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(snWithOffset >= serverNodes.start && snWithOffset <= serverNodes.end, s"Invalid SN (machine id) index: $snWithOffset.")

    snWithOffset
  }

  /**
   * Maps the provided server network node id to the corresponding server id
   *
   * @param serverNetworkNodeId the id of the node representing the server the network
   * @return the id of the server
   */
  @inline private[scheduler] final def getServerMachineIDFromNodeID(serverNetworkNodeId: NodeID): NodeID = {
    val snWithoutOffset: NodeID = serverNetworkNodeId - serverNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(snWithoutOffset >= 0 && snWithoutOffset < numServerNodes, s"Invalid SN (without offsets) index: $snWithoutOffset.")

    snWithoutOffset
  }

  /**
   * Maps the provided server network node id to the id of the directly connected tor server
   * network node.
   *
   * @param serverNodeId the id of the server network node
   * @return the id of the tor server network node that is directly connected to the provided server
   */
  @inline private[scheduler] final def getServerTorNetworkNodeIDForServerNodeID(serverNodeId: NodeID): NodeID = {

    val edgeTor2Machine = cell.links.find(edge => edge.srcSwitch && !edge.dstSwitch && (edge.dstWithOffset + serverNetworkNodes.start) == serverNodeId).get
    val torNode = edgeTor2Machine.srcWithOffset + serverNetworkNodes.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(serverNetworkGroupLeaves.contains(torNode), s"Conversion from server network node id to corresponding tor server network node id failed. (Input: $serverNodeId Result: ${torNode} Desired: ${serverNetworkGroupLeaves})")

    torNode
  }

  /** Other Helpers * */

  @inline private[scheduler] final def leaves: Seq[NodeID] = switchNodes ++ serverNodes

  /** Generating a empty topology graph * */

  override private[flow] def getEmptyFlowGraph(costs: CostModel,
                                               capacities: CapacityModel) = {

    // Network Nodes of the Switch Graph
    val switchNetworkNodes: Seq[FlowNode] = for (nn: NodeID <- this.switchNetworkNodes) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.NETWORK_NODE_FOR_INC,
        level = scheduler.cell.switchLevels(nn)
      )
    }

    // The Switch Nodes of the Switch Graph
    val switchNodes: Seq[FlowNode] = for (nnd: NodeID <- this.switchNodes) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.MACHINE_NETWORK,
        level = scheduler.cell.switchLevels(nnd - this.switchNodes.start)
      )
    }

    // Network Nodes of the Server Graph
    val serverNetworkNodes: Seq[FlowNode] = for (ng <- this.serverNetworkNodes) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.NETWORK_NODE_FOR_SERVERS,
        level = scheduler.cell.switchLevels(this.getSwitchNetworkNodeIDFromServerNetworkNodeID(ng))
      )
    }

    // The Server Nodes of the Server Graph
    val serverNodes: Seq[FlowNode] = for (sn: NodeID <- this.serverNodes) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.MACHINE_SERVER,
        level = -1
      )
    }

    // The Sink Node of the Graph
    val sinkNode: FlowNode = new FlowNode(
      supply = FlowNode.`Supply.MinValue`,
      nodeType = NodeType.SINK,
      level = -2
    )

    // All the nodes (in correct order) to be inserted into the graph
    val allPhysicalNodes: Seq[FlowNode] = (switchNetworkNodes ++ switchNodes ++ serverNetworkNodes ++ serverNodes) :+ (sinkNode)

    val graph: FlowGraph = new FlowGraph(sanityChecks = SimulationConfiguration.SANITY_CHECKS_HIRE)
    // Insert the nodes into the graph
    graph.addNodes(allPhysicalNodes)
    graph.sinkNodeId = sinkNode.id

    // Connect Switch Network Nodes to each other according to the physical topology
    for (link: CellEdge <- scheduler.cell.links.filter(l => l.srcSwitch && l.dstSwitch)) {
      val target_switch_node_id = this.switchNetworkNodes.start + link.dstWithOffset

      graph.addArc(new FlowArc(
        src = this.switchNetworkNodes.start + link.srcWithOffset,
        dst = target_switch_node_id,
        capacity = capacities.getSwitchNetworkTopologyToNetworkTopology(target_switch_node_id),
        cost = ArcDescriptor.NoCosts
      ))
    }

    // Connect each Switch Network Node to its corresponding Switch Node
    for (switchNetworkNode: NodeID <- this.switchNetworkNodes) {
      val target_switch_node_id = switchNetworkNode + switchNetworkNodes.length

      graph.addArc(new FlowArc(
        src = switchNetworkNode,
        dst = target_switch_node_id,
        capacity = capacities.getNetworkTopologyToSwitch(target_switch_node_id),
        cost = ArcDescriptor.NoCosts
      ))
    }

    // Connect Server Network Nodes to each other according to the physical topology
    for (link: CellEdge <- scheduler.cell.links.filter(l => l.srcSwitch && l.dstSwitch)) {
      val target_server_node_id = this.serverNetworkNodes.start + link.dstWithOffset
      graph.addArc(new FlowArc(
        src = this.serverNetworkNodes.start + link.srcWithOffset,
        dst = target_server_node_id,
        capacity = capacities.getServerNetworkTopologyToNetworkTopology(target_server_node_id),
        cost = ArcDescriptor.NoCosts
      )
      )
    }

    // Connect each Server Network Leave Node to theirs Server Nodes
    scheduler.cell.links.filter(l => l.srcSwitch && !l.dstSwitch).foreach((link: CellEdge) => {
      val target_server_node_id = this.serverNetworkNodes.start + link.dstWithOffset

      graph.addArc(new FlowArc(
        src = this.serverNetworkNodes.start + link.srcWithOffset,
        dst = target_server_node_id,
        capacity = capacities.getNetworkTopologyToServer(target_server_node_id),
        cost = ArcDescriptor.NoCosts
      ))
    })

    // Connect Switch Nodes and Server Nodes to the Sink Node
    this.leaves.foreach((machine_network_id: NodeID) => {
      val machine = graph.nodes(machine_network_id)

      graph.addArc(new FlowArc(
        machine_network_id,
        graph.sinkNodeId,
        cost = ArcDescriptor.NoCosts, // Will have initially 0 costs. This will be updated later on
        capacity =
          if (machine.isSwitchMachine)
            capacities.getSwitchToSink(machine)
          else
            capacities.getServerToSink(machine)
      ))
    })

    // The result will be assigned to this class' field
    graph

  }

}