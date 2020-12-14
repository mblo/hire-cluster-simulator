package hiresim.scheduler.flow.solver.mcmf

import hiresim.scheduler.flow.solver.TrackedSolver
import hiresim.scheduler.flow.solver.graph.FlowGraph
import hiresim.scheduler.flow.solver.mcmf.algo.MaxFlow
import hiresim.scheduler.flow.solver.mcmf.util.GraphIntegrityValidator
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable
import scala.math.{ceil, log, min}

/**
 * Implements the CostScaling algorithm over a residual graph. The runtime complexity
 * of the algorithm is O(pow(n, 2) * m * log(n*C)) where n is the number of nodes in the graph,
 * m the number of arcs and C the maximum cost that can be found on one of the arcs.
 *
 */
class CostScalingSolver extends TrackedSolver {

  /**
   * Provides the name of this solver. This could be e.g.
   * the name of the used algorithm
   *
   * @return the name of this solver
   */
  override def name: String = "CostScaling"

  /** Solves a transportation problem instance.
   *
   * @param graph              the input graph, containing arcs and nodes.
   * @param maxFlowsOnMachines the max number of flows possible to pass nodes of type isMachine
   */
  override def solveInline0(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = {
    // The algorithm start with any feasible flow
    MaxFlow.forward(graph, graph.nodes.producerIds)

    // Set of active nodes. i.e. all nodes with positive excess/supply. We are using
    // collections here, because a lookup on a Queue is not constant, but is on a BitSet.
    val active: mutable.Queue[NodeID] = mutable.Queue()
    val registered_active: mutable.BitSet = mutable.BitSet()

    // The epsilon used for scaling.
    var epsilon = onSetupGraphAndCalculateInitialEpsilon(graph)

    while (epsilon > 1) {

      if (Thread.interrupted()) {
        // stop early
        throw new InterruptedException()
      }

      epsilon = epsilon >> 1

      // Adapt the edges. This might change the active set
      onAdaptEdges(graph)
      // Find all nodes that are initially active
      onCollectActiveNodes(graph, active, registered_active)

      // Discharge every active node!
      while (active.nonEmpty) {

        val current = active.dequeue()

        // Discharge the complete supply of that node into neighbours
        onDischargeNode(graph, epsilon, current, active, registered_active)
        // The node is no longer active... (This operation is O(1))
        registered_active -= current

        if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
          GraphIntegrityValidator.checkSupplyOnNode(expected = 0L, current, graph)

      }

    }

    if (SimulationConfiguration.SANITY_MCMF_CHECK_ZERO_SUPPLY_NETWORK)
      GraphIntegrityValidator.checkAllNodesZeroSupply(graph)
  }


  private def onSetupGraphAndCalculateInitialEpsilon(graph: FlowGraph): Int = {

    var max_cost: Long = 0
    // It can be shown that by multiplying arc costs with (n + 1) we can avoid a epsilon that might
    // not be integral and thus can terminate when epsilon <= 1 holds.
    val n = graph.nodes.size + 1

    graph.nodes.foreach(node => {
      node.foreachOutAndIncoming(arc => {
        // Initialize the reducedCost. This is simply the cost of that arc as potential is 0 initially
        arc.reducedCost = arc.cost * n

        // Find the initial epsilon
        if (max_cost < arc.cost)
          max_cost = arc.cost
      })
    })

    if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
      assert(max_cost > 0, s"Maximum cost in graph may not be smaller or equal to 0 (is $max_cost)!")

    1 << ceil(log(n * max_cost)).toInt

  }

  private def onAdaptEdges(graph: FlowGraph): Unit = {
    val nodes = graph.nodes
    nodes.foreach(source => {
      source.foreachOutAndIncoming(arc => {
        // Do not look at already 0-reduced cost arcs...
        if (arc.reducedCost != 0) {
          val destination = nodes(arc.dst)

          // If ReducedCapacity < 0 then we push flow = capacity through it
          if (arc.reducedCost < 0) {

            // Adapt the error term of connected nodes. Effectively we have to send "residual capacity" more flow
            source.supply -= arc.residualCapacity
            destination.supply += arc.residualCapacity

            arc.reverseArc.residualCapacity += arc.residualCapacity
            arc.residualCapacity = 0L

            // GraphIntegrityValidator.checkMaxFlowArc(arc)
          }

        }
      })
    })

  }

  private def onCollectActiveNodes(graph: FlowGraph,
                                   active: mutable.Queue[NodeID],
                                   added: mutable.BitSet): Unit = {

    graph.nodes.foreach(node => {

      if (node.supply > 0 && added.add(node.id))
        active.enqueue(node.id)

    })

  }

  private def onDischargeNode(graph: FlowGraph,
                              epsilon: Int,
                              node_id: NodeID,
                              active: mutable.Queue[NodeID],
                              registered_active: mutable.BitSet): Unit = {

    val nodes = graph.nodes

    val node = nodes(node_id)
    // Whether a admissible arc was found or not
    var relabel = true

    // Discharge as long as the node is active
    while (node.supply > 0) {
      val iterator = node.outgoing.iterator
      val iterator2 = node.incoming.iterator

      while ((iterator.hasNext || iterator2.hasNext) && node.supply > 0) {

        val arc = if (iterator.hasNext) iterator.next() else iterator2.next()

        // If the arc is admissible (-0.5*epsilon <= reducedCost < 0), we can push flow through it
        if (arc.residualCapacity > 0 && -epsilon <= arc.reducedCost && arc.reducedCost < 0) {

          // Relabel is no longer necessary
          relabel = false

          // Determine the amount of flow we can push onto this arc
          val delta = min(node.supply, arc.residualCapacity)
          val destination = nodes(arc.dst)

          // First adapt supply/excess in source / dest
          node.supply -= delta
          destination.supply += delta

          // And adapt flow through arcs
          arc.residualCapacity -= delta
          arc.reverseArc.residualCapacity += delta

          // If the destination is now active remember it as such.
          if (destination.supply > 0 && registered_active.add(destination.id))
            active.enqueue(destination.id)

        }
      }

      // If we weren't successful in pushing flow, we must change the nodes potential
      if (relabel) {

        var delta: Long = Long.MaxValue

        node.foreachOutAndIncoming(arc => {
          if (arc.residualCapacity > 0)
            delta = min(delta, arc.reducedCost)
        })

        // There was no matching arc... Fall back to 0
        if (delta == Long.MaxValue)
          delta = epsilon
        else
          delta += epsilon

        node.foreachOutAndIncoming(arc => {
          // ReducedCost = Cost(i, j) - Potential(i) + Potential(j)
          // As Potential(i) gets increased by delta, we must decrease the ReducedCost term
          arc.reducedCost -= delta
          arc.reverseArc.reducedCost += delta

        })

      } else
        relabel = true
    }
  }

}
