package hiresim.scheduler.flow.solver.mcmf.algo

import hiresim.scheduler.flow.solver.graph.FlowNode.`Distance.MaxValue`
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}
import hiresim.shared.graph.Graph.NodeID

import scala.collection.mutable

/**
 * Implements the BellmanFord algorithm over the residual graph of the MCMF
 * problem.
 */
object BellmanFord {

  def potentials(graph: FlowGraph,
                 source: NodeID,
                 delta: Int,
                 predecessors: mutable.LongMap[FlowArc]): Unit = {

    val nodes = graph.nodes

    // Storing all old active ids (prior to applying a round of bellman ford). This set
    // represents all nodes that need to be checked
    var current_active_ids: mutable.BitSet = new mutable.BitSet()
    current_active_ids += source

    // The source has 0 distance
    nodes(source).distance = 0L

    var limiter: Int = 1;
    while (limiter <= graph.nodes.size && current_active_ids.nonEmpty) {
      val new_active_ids: mutable.BitSet = mutable.BitSet()

      for (node_id <- current_active_ids) {
        val source = nodes(node_id)

        if (source.distance < `Distance.MaxValue`) {
          // Check all neighbours of the current node
          source.outgoing.foreach(arc => {
            // Check if we can relax the path to the neighbor node of current
            if (arc.residualCapacity >= delta && source.distance + arc.reducedCost < nodes(arc.dst).distance) {
              new_active_ids += arc.dst
              nodes(arc.dst).distance = source.distance + arc.reducedCost
              predecessors.addOne(arc.dst, arc)
            }
          })

        }

      }
      // Swap the sets as in the next round we want to check the modified nodes from this round
      current_active_ids = new_active_ids
      limiter += 1
    }


  }

  // Without Potentials
  def normal(graph: FlowGraph,
             active_nodes: mutable.BitSet,
             predecessors: mutable.LongMap[FlowArc]): Unit = {

    //solve(graph,active_nodes, 0, CostForArc, GR, predecessors)

    val nodes = graph.nodes

    // Storing all old active ids (prior to applying a round of bellman ford). This set
    // represents all nodes that need to be checked
    var current_active_ids: mutable.BitSet = new mutable.BitSet()

    // Apply initial setup. All active nodes have distance 0.
    for (active <- active_nodes) {
      nodes(active).distance = 0L
      current_active_ids += active
    }

    var limiter: Int = 1;
    while (limiter <= graph.nodes.size && current_active_ids.nonEmpty) {
      val new_active_ids: mutable.BitSet = mutable.BitSet()

      for (node_id <- current_active_ids) {
        val source = nodes(node_id)

        if (source.distance < `Distance.MaxValue`) {

          // Check all neighbours of the current node
          source.outgoing.foreach(arc => {
            // Check if we can relax the path to the neighbor node of current
            if (arc.residualCapacity > 0 && source.distance + arc.cost < nodes(arc.dst).distance) {
              new_active_ids += arc.dst
              nodes(arc.dst).distance = source.distance + arc.cost
              predecessors.addOne(arc.dst, arc)
            }
          })

          source.incoming.foreach(arc => {
            // Check if we can relax the path to the neighbor node of current
            if (arc.residualCapacity > 0 && source.distance + arc.cost < nodes(arc.dst).distance) {
              new_active_ids += arc.dst
              nodes(arc.dst).distance = source.distance + arc.cost
              predecessors.addOne(arc.dst, arc)
            }
          })

        }

      }
      // Swap the sets as in the next round we want to check the modified nodes from this round
      current_active_ids = new_active_ids
      limiter += 1
    }

  }

}
