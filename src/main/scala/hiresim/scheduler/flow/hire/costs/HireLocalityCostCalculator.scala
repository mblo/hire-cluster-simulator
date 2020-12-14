package hiresim.scheduler.flow.hire.costs

import hiresim.graph.NodeType
import hiresim.scheduler.flow.hire.{HireGraphManager, HireScheduler}
import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.configuration.SimulationConfiguration.PRECISION
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.{Allocation, TaskGroup}

import scala.collection.mutable
import scala.math.min

object HireLocalityCostCalculator {

  /**
   * The decay factor for inc locality propagation
   */
  var DecayFactor: BigInt = 8

  /**
   * The initial locality gain parameter
   */
  var InitialLocalityGain: BigInt = 16

}


class HireLocalityCostCalculator(graph_manager: HireGraphManager)(implicit scheduler: HireScheduler) extends HireCostDimensionCalculator {

  /** Switches local gains for NTG -> NN/NND arcs */
  private[hire] val switchesLocalGains: mutable.Map[NodeID, mutable.Map[TaskGroup, BigInt]] = mutable.Map()
  private[hire] val maxTaskGroupLocalGains: mutable.Map[TaskGroup, BigInt] = mutable.Map()

  /** Storing local gains for STG -> NG arcs */
  private[hire] val serverLocalGains: mutable.Map[NodeID, mutable.Map[TaskGroup, Long]] = mutable.Map()

  private lazy val mapping = graph_manager.mapping
  private lazy val graph = graph_manager.graph

  @inline private[hire] def updateServerLocalGainsFn(taskGroups: Iterable[TaskGroup]): (FlowNode => Unit) = {
    (node: FlowNode) => {
      for (taskGroup <- taskGroups) {
        updateServerLocalGain(node, taskGroup)
      }
    }
  }

  private[hire] def updateSwitchesLocalGains(allocation: Allocation,
                                             freeing: Boolean): Unit = {
    updateSwitchesLocalGains(allocation.machineNetworkID, allocation.taskGroup, freeing)
  }

  private[hire] def updateSwitchesLocalGains(switchNetworkID: NodeID,
                                             taskGroup: TaskGroup,
                                             freeing: Boolean): Unit = {

    assert(graph_manager.graph.nodes.contains(switchNetworkID))

    if (graph.sanityChecks)
    // Check if the provided allocation really reefers to a switch
      assert(mapping.switchNetworkNodes.contains(switchNetworkID) || mapping.switchNodes.contains(switchNetworkID), s"Trying to update a local gain on a ${graph.nodes(switchNetworkID).nodeType}.")

    // Initialize gain to initial value
    var gain: BigInt = HireLocalityCostCalculator.InitialLocalityGain

    if (freeing)
      gain *= -1

    // Storing information about already visited nodes
    val visited: mutable.BitSet = mutable.BitSet()

    // Storing information about nodes still to be visited
    val visit: mutable.ArrayBuffer[NodeID] = mutable.ArrayBuffer()
    // And enqueue starting node
    visit += switchNetworkID

    // Storing information about which nodes to visit in next run
    val next: mutable.Set[NodeID] = mutable.Set()

    @inline def onCheckArc(arc: FlowArc): Unit = {
      if (graph.nodes(arc.dst).isPhysical)
        next += arc.dst
    }

    while (gain != 0 && visit.nonEmpty) {
      // For all nodes that we need to visit but haven't visited before
      for (node <- visit) {
        if (!visited.contains(node)) {

          if (!switchesLocalGains.contains(node))
            switchesLocalGains += node -> mutable.Map()

          // Retrieve current value or 0 if not jet set
          var current: BigInt = switchesLocalGains(node).getOrElse(taskGroup, 0)
          // Update by propagated gain
          current += gain

          // And push back if the value needs to be stored
          if (current != 0)
            switchesLocalGains(node)(taskGroup) = current
          // Otherwise remove the entry about this TaskGroup
          else
            switchesLocalGains(node) -= taskGroup

          // Mark the current node as visited
          visited += node

          val node_instance = graph.nodes(node)
          // And mark all neigbours for next round
          node_instance.outgoing.foreach(onCheckArc)
          node_instance.incoming.foreach(onCheckArc)
        }
      }

      // Clear of visit as we have processed them now
      visit.clear()

      // Swap next set into visit set in preparation for next iteration (do not add already visited nodes)
      for (candidate <- next) {
        if (!visited.contains(candidate))
          visit += candidate
      }
      // Clear of next set
      next.clear()

      // And update gain
      gain = gain / HireLocalityCostCalculator.DecayFactor
    }

    // Finally compute the new max local gain. Unfortunately we have to look at every node again
    val max: BigInt = switchesLocalGains.keys.map[BigInt]((id: NodeID) => switchesLocalGains(id).getOrElse(taskGroup, 0)).maxOption.getOrElse(0)
    // And update that entry if it needs to be stored
    if (max != 0)
      maxTaskGroupLocalGains(taskGroup) = max
    // Else forget about it
    else
      maxTaskGroupLocalGains -= taskGroup

  }

  /**
   * server locality gain is simply |tasks of this tg not running on a node| / |tg| for servers.
   * For network nodes we take the average of their children.
   * -> a subtree with no running task propagates 1
   */
  private def updateServerLocalGain(node: FlowNode,
                                    taskGroup: TaskGroup): Unit = {
    // if nothing was started so far, simply take shortcut
    if (taskGroup.runningTasks == 0 && taskGroup.finishedTasks == 0) {
      serverLocalGains.getOrElseUpdate(node.id, mutable.HashMap()).remove(taskGroup)
    } else {
      // Depending on the type we have 2 valid choices
      val score: Long = node.nodeType match {

        // The case where we have a basic server node
        case NodeType.MACHINE_SERVER =>

          // How many tasks of the TaskGroup are running on the current machine
          val local_tasks = node.allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0)
          // How many tasks are there in the TaskGroup?
          val total_tasks = taskGroup.numTasks

          assert(local_tasks <= total_tasks, s"not valid: $local_tasks <= $total_tasks, " +
            s"${taskGroup.detailedToString()}, ${taskGroup.job.get.detailedToString()}")

          // The locality score for this machine is the number of tasks of the TaskGroup not
          // running on this machine divided by the total number of tasks in that Group
          (PRECISION * (total_tasks - local_tasks)) / total_tasks

        // The case where we are at a topology node
        case NodeType.NETWORK_NODE_FOR_SERVERS =>

          // And accumulate their locality scores
          var accumulator: Long = 0
          var arcs: Int = 0

          // since each entry of the sum is at most of size PRECISION
          assert(Long.MaxValue / node.outgoing.size > PRECISION,
            s"PRECISION is too big, to fit into LONG, for ${node.outgoing.size} entries, for ${taskGroup.detailedToString()}")

          for (arc <- node.outgoing) {
            // The gains for the current child
            val gains: Option[mutable.Map[TaskGroup, Cost]] = serverLocalGains.get(arc.dst)

            // If we have a valid entry for the locality score in this subtree, we take it
            // Otherwise we assume that no task of this TG is running in that subtree, so the
            // locality score in that has is 1
            if (gains.isDefined)
              accumulator += gains.get.getOrElse(taskGroup, PRECISION)
            else
              accumulator += PRECISION

            assert(accumulator >= 0, "Long overflow")

            arcs += 1
          }

          accumulator / arcs

        case _ =>
          throw new AssertionError(s"Server locality scoring does not take nodes of type ${node.nodeType} into account.")

      }

      // If score becomes == PRECISION, there is no task running anymore in this tree, so simply get rid of the entry
      if (score == PRECISION)
        serverLocalGains.getOrElseUpdate(node.id, mutable.HashMap()).remove(taskGroup)
      else {
        assert(score < PRECISION)
        assert(score >= 0)
        serverLocalGains.getOrElseUpdate(node.id, mutable.HashMap()).update(taskGroup, score)
      }

    }
  }


  override def getServerToSinkCost(server: FlowNode): Cost = PRECISION

  override def getSwitchToSinkCost(switch: FlowNode): Cost = {
    getSwitchToSinkCost(switch, graph)
  }

  def getSwitchToSinkCost(switch: FlowNode, graph: FlowGraph): Cost = {
    // TOR switches has lowest level
    // CORE switches has highest level

    (PRECISION * (scheduler.cell.highestLevel - switch.level)) / scheduler.cell.highestLevel
  }


  override def getServerTaskGroupToNodeCost(taskGroup: TaskGroup, targetNode: FlowNode, isPartOfFlavorSelectorPart: Boolean): Cost = {

    // If the target node is a member of the server topology network, we can calculate the score
    // as per case (a)
    if (mapping.serverNetworkNodes.contains(targetNode.id))
      getCombinedCostsForNetworkNode(graph, targetNode.id, taskGroup)
    // Otherwise we have case (c) and the provided target node refers to a server node
    else {
      var costs: Cost = getServerCostContribution(graph, targetNode.id, taskGroup) * taskGroup.numTasks
      var tasks: Int = taskGroup.numTasks

      // Retrieve the id of the tor node in server and switch network connecting the provided
      // server to the network topology.
      val (serverNetworkNode, switchNetworkNode) = getNetworkNodesForNode(graph, mapping.getServerTorNetworkNodeIDForServerNodeID(targetNode.id));

      // Taking connected TaskGroup contributions into account
      for (participating <- taskGroup.outgoingConnectedTaskGroups.slice(0, SimulationConfiguration.HIRE_MAX_CONNECTED_TASK_GROUPS_TO_CHECK_LOCALITY)) {
        // Calculate costs for that TaskGroup
        costs += getSingleCostsForNetworkNode(graph, participating, serverNetworkNode, switchNetworkNode) * participating.numTasks
        // And remember how many Tasks there are now
        tasks += participating.numTasks
        assert(costs >= 0L, "long overflow")
      }

      costs /= tasks
      // And return the costs
      costs

    }
  }

  override def getSwitchTaskGroupToNodeCost(taskGroup: TaskGroup,
                                            targetNode: FlowNode,
                                            isPartOfFlavorSelectorPart: Boolean): Cost = {

    // If the target is a topology node in the switch network, calculate the costs as stated
    // by case (a) in the paper
    if (mapping.switchNetworkNodes.contains(targetNode.id))
      getCombinedCostsForNetworkNode(graph, targetNode.id, taskGroup)
    // Otherwise the targetNode must point to a switch machine node. In this case we take the
    // connected topology node to proceed like in case (a). This is case (b)
    else
      getCombinedCostsForNetworkNode(graph, mapping.getSwitchNetworkNodeIDForSwitchNodeID(targetNode.id), taskGroup)

  }

  private def getCombinedCostsForNetworkNode(graph: FlowGraph,
                                             target: NodeID,
                                             taskGroup: TaskGroup): Cost = {

    val (serverNetworkNode, switchNetworkNode) = getNetworkNodesForNode(graph, target);
    // Tracking the number of tasks involved
    var tasks: Long = 0
    // Accumulated costs
    var costs: BigInt = 0

    // First respect the origin taskgroup
    costs += getSingleCostsForNetworkNode(graph, taskGroup, serverNetworkNode, switchNetworkNode) * taskGroup.numTasks
    tasks += taskGroup.numTasks

    taskGroup.outgoingConnectedTaskGroups.foreach(connected => {
      // Get the cost term for the TaskGroup weighted by the number of tasks in that Group
      costs += getSingleCostsForNetworkNode(graph, connected, serverNetworkNode, switchNetworkNode) * connected.numTasks
      // Finally keep track of how many tasks there are such that we can calculate the weighted average
      tasks += connected.numTasks
    })

    costs /= tasks
    // And return the costs
    costs.toLong
  }

  private def getSingleCostsForNetworkNode(graph: FlowGraph,
                                           taskGroup: TaskGroup,
                                           serverNetworkNode: NodeID,
                                           switchNetworkNode: NodeID): Cost = {
    val server_contribution = getServerCostContribution(graph, serverNetworkNode, taskGroup)

    val switch_contribution =
      if (switchesLocalGains.contains(switchNetworkNode)) {

        // The gain on this switch. 0 if none
        val gain: BigInt = switchesLocalGains(switchNetworkNode).getOrElse(taskGroup, 0)
        // The maximum gain for the provided TaskGroup. Should have a valid entry if switchesLocalGains has a valid entry
        val max: BigInt = maxTaskGroupLocalGains.getOrElse(taskGroup, 1)

        // If the gain is 0 we can shortcut the calculation and simply return 1 (or PRECISION)
        if (gain == 0)
          PRECISION
        // Use normalization function to calculate gain from switch
        else
          (PRECISION * (math.exp(gain.toDouble / max.toDouble) / math.E)).toLong

        // If there is no gain on this switch it costs maximum possible value to schedule here
      } else
        PRECISION

    (server_contribution + switch_contribution) / 2
  }

  private def getServerCostContribution(graph: FlowGraph,
                                        serverNetworkNode: NodeID,
                                        taskGroup: TaskGroup): Long = {
    // The worst possible contribution is 1 (PRECISION). Take this as initial value for reduction.
    var contribution: Long = PRECISION
    if (serverLocalGains.contains(serverNetworkNode)) {
      val gain: Option[Long] = serverLocalGains(serverNetworkNode).get(taskGroup)
      // Have we found a parent with a better score?
      if (gain.isDefined)
        contribution = min(contribution, gain.get)

    }
    contribution
  }

  @inline
  private def getNetworkNodesForNode(graph: FlowGraph,
                                     node: NodeID): (NodeID, NodeID) = {

    // The provided node represents a server, so we have to figure out what it's counterpart in the switch network is.
    if (mapping.serverNetworkNodes.contains(node))
      (node, mapping.getSwitchNetworkNodeIDFromServerNetworkNodeID(node))
    // The provided node resides in the switch network.
    else
      (mapping.getServerNetworkNodeIDFromSwitchNetworkNodeID(node), node)
  }


}
