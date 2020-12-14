package hiresim.scheduler.flow.coco

import hiresim.cell.Cell
import hiresim.cell.machine.SwitchProps
import hiresim.graph.NodeType
import hiresim.scheduler.Scheduler
import hiresim.scheduler.flow.ArcDescriptor.{CapacityModel, CostModel}
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.scheduler.flow.{ArcDescriptor, FlowGraphStructure}
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.BitSet

class PlaneGraphMapping(implicit scheduler: Scheduler) extends FlowGraphStructure {

  private val cell: Cell = scheduler.cell

  // We have one rack for each inp type
  private[flow] val numSwitchRacks: Int = SwitchProps.all.size
  private[flow] val numSwitches = cell.numSwitches
  private[flow] val numSwitchNetworkNodes = 1 + numSwitchRacks

  private[flow] val numServerRacks = cell.numTorSwitches
  private[flow] val numServers = cell.numServers
  private[flow] val numServerNetworkNodes = 1 + numServerRacks


  /** Modelling the network topology of the switch network * */

  @inline private[scheduler] final def switchNetworkNodes: Range = 0 until numSwitchNetworkNodes

  // Top Level nodes
  @inline private[scheduler] final def switchNetworkRootNodes: Range = switchNetworkNodes.start until switchNetworkNodes.start + 1

  // Bottom level nodes
  @inline private[scheduler] final def switchNetworkGroupLeaves: Range = switchNetworkNodes.end - numSwitchRacks until switchNetworkNodes.end


  @inline override private[flow] def getSwitchNetworkRootNodes: Range = switchNetworkRootNodes


  /** Modelling the switch type machines * */

  @inline private[scheduler] final def switchNodes: Range = switchNetworkNodes.end until switchNetworkNodes.end + numSwitches

  @inline override private[flow] def getSwitchNodes: Range = switchNodes


  @inline private[flow] final def getSwitchNetworkIDFromMachineID(switch: NodeID): NodeID = {

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switchNetworkNodes.contains(switch), s"Provided switch id is invalid: $switch")

    switch + switchNodes.start
  }

  @inline private[scheduler] final def getSwitchMachineIDFromNetworkID(switchNetworkNodeId: NodeID): NodeID = {

    val switch: NodeID =
      if (switchNodes.contains(switchNetworkNodeId))
        switchNetworkNodeId - switchNodes.start
      else
        switchNetworkNodeId

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(switch >= 0 && switch < numSwitches, s"Conversion from network id to machine id for switches failed! (Input: ${switchNetworkNodeId} Produced: ${switch})")

    switch
  }

  private[flow] final def getSwitchRackNodeIDsFromSwitchNodeID(switchNodeID: NodeID,
                                                               graph: FlowGraph): Seq[NodeID] = {

    val machine_id = getSwitchMachineIDFromNetworkID(switchNodeID)
    val properties: BitSet = scheduler.cell.switches(machine_id).properties.capabilities

    properties.toSeq.map(property => property + switchNetworkGroupLeaves.start - 1)

  }

  /** Modelling the topology for the server network * */

  @inline private[scheduler] final def serverNetworkNodes: Range = switchNodes.end until switchNodes.end + numServerNetworkNodes

  // Top Level nodes
  @inline private[scheduler] final def serverNetworkRootNodes: Range = serverNetworkNodes.start until serverNetworkNodes.start + 1

  // Bottom level nodes
  @inline private[scheduler] final def serverNetworkGroupLeaves: Range = serverNetworkNodes.end - numServerRacks until serverNetworkNodes.end


  @inline override private[flow] def getServerNetworkRootNodes: Range = serverNetworkRootNodes


  /** Modelling the server type machines  * */

  @inline private[scheduler] final def serverNodes: Range = serverNetworkNodes.end until serverNetworkNodes.end + numServers

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
      assert(snWithoutOffset >= 0 && snWithoutOffset < numServers, s"Invalid SN (without offsets) index: $snWithoutOffset.")

    snWithoutOffset
  }

  /**
   * Maps the provided server network node id to the id of the directly connected tor server
   * network node.
   *
   * @param serverNodeId the id of the server network node
   * @return the id of the tor server network node that is directly connected to the provided server
   */
  @inline private[scheduler] final def getServerRackNodeIDForServerNodeID(serverNodeId: NodeID): NodeID = {

    val edge_rack_to_machine = cell.links.find(edge => edge.srcSwitch && !edge.dstSwitch && edge.dstWithOffset == (serverNodeId - serverNodes.start + numSwitches)).get
    val rack_node_id = edge_rack_to_machine.srcWithOffset - (numSwitches - numServerRacks) + serverNetworkGroupLeaves.start

    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      assert(serverNetworkGroupLeaves.contains(rack_node_id), s"Conversion from server network node id to corresponding tor server network node id failed. (Input: $serverNodeId Result: ${rack_node_id} Desired: ${serverNetworkGroupLeaves})")

    rack_node_id
  }

  /** Other Helpers * */

  @inline private[scheduler] final def leaves: Seq[NodeID] = switchNodes ++ serverNodes


  /** Generating a empty topology graph * */

  override private[flow] def getEmptyFlowGraph(costs: CostModel, capacities: CapacityModel) = {

    val switchAggregatorNode: FlowNode = new FlowNode(
      supply = 0L,
      nodeType = NodeType.NETWORK_NODE_FOR_INC,
      level = 1
    )

    // Network Nodes of the Switch Graph
    val switchRackNodes: Seq[FlowNode] = for (nn: NodeID <- this.switchNetworkGroupLeaves) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.NETWORK_NODE_FOR_INC,
        level = 0
      )
    }

    // The Switch Nodes of the Switch Graph
    val switchMachineNodes: Seq[FlowNode] = for (nnd: NodeID <- this.switchNodes) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.MACHINE_NETWORK,
        level = -1
      )
    }

    val serverAggregatorNode: FlowNode = new FlowNode(
      supply = 0L,
      nodeType = NodeType.NETWORK_NODE_FOR_SERVERS,
      level = 1
    )

    // Network Nodes of the Server Graph
    val serverRackNodes: Seq[FlowNode] = for (ng <- this.serverNetworkGroupLeaves) yield {
      new FlowNode(
        supply = 0L,
        nodeType = NodeType.NETWORK_NODE_FOR_SERVERS,
        level = 0
      )
    }

    // The Server Nodes of the Server Graph
    val serverMachineNodes: Seq[FlowNode] = for (sn: NodeID <- this.serverNodes) yield {
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
    val allPhysicalNodes: Seq[FlowNode] = (Seq(switchAggregatorNode) ++ switchRackNodes ++ switchMachineNodes ++ Seq(serverAggregatorNode) ++ serverRackNodes ++ serverMachineNodes) :+ (sinkNode)

    val graph: FlowGraph = new FlowGraph(sanityChecks = SimulationConfiguration.SANITY_CHECKS_HIRE)
    // Insert the nodes into the graph
    graph.addNodes(allPhysicalNodes)
    graph.sinkNodeId = sinkNode.id

    // Connect Switch Aggregator to all the Rack nodes
    for (target_switch_rack: NodeID <- this.switchNetworkGroupLeaves) {
      graph.addArc(new FlowArc(
        src = switchAggregatorNode.id,
        dst = target_switch_rack,
        capacity = capacities.getSwitchNetworkTopologyToNetworkTopology(target_switch_rack),
        cost = ArcDescriptor.NoCosts
      ))
    }

    for (switch_network_id <- this.switchNodes) {
      for (switch_rack <- getSwitchRackNodeIDsFromSwitchNodeID(switch_network_id, graph)) {
        graph.addArc(new FlowArc(
          src = switch_rack,
          dst = switch_network_id,
          capacity = capacities.getNetworkTopologyToSwitch(switch_network_id),
          cost = costs.getNetworkTopologyToSwitch(switch_network_id)
        ))
      }
    }

    // Connect the server network aggregator to all of the racks
    for (server_rack <- this.serverNetworkGroupLeaves) {
      graph.addArc(new FlowArc(
        src = serverAggregatorNode.id,
        dst = server_rack,
        capacity = capacities.getServerNetworkTopologyToNetworkTopology(server_rack),
        cost = ArcDescriptor.NoCosts
      ))
    }

    // Connect Server Network Nodes to each other according to the physical topology
    for (server_network_id <- this.serverNodes) {
      val rack_node_id = getServerRackNodeIDForServerNodeID(server_network_id)
      graph.addArc(new FlowArc(
        src = rack_node_id,
        dst = server_network_id,
        capacity = capacities.getNetworkTopologyToServer(server_network_id),
        cost = costs.getNetworkTopologyToServer(server_network_id)
      ))
    }

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
