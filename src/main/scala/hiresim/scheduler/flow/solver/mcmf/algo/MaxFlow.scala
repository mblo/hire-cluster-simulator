package hiresim.scheduler.flow.solver.mcmf.algo

import hiresim.scheduler.flow.solver.graph.FlowNode.Supply
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}
import hiresim.scheduler.flow.solver.mcmf.util.GraphIntegrityValidator
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

/**
 * Helper class for computing the maximum flow over a graph. This has been
 * taken from: https://github.com/ICGog/Flowlessly/blob/master/src/misc/utils.cc
 */
object MaxFlow {

  /**
   * Computes the maximum flow over the graph using the Ford-Fulkerson algorithm. The
   * Complexity of this algorithm is O(E*F), where F ist the maximum flow value and E
   * the amount of edges in the graph.
   *
   * @param graph     the graph to operate on
   * @param producers the supply creating nodes
   */
  def forward(graph: FlowGraph,
              producers: mutable.BitSet): Unit = {

    val sink_node: NodeID = graph.sinkNodeId
    val nodes = graph.nodes

    // Create collections, which are used later on
    val predecessor: mutable.LongMap[FlowArc] = mutable.LongMap()
    val visited: mutable.LongMap[Supply] = mutable.LongMap()
    val to_visit = new mutable.Queue[NodeID]

    var sink_reached: Boolean = false
    // As long as we found a path in the last run there is a possibility to distribute
    // more flow in the network. Thus we repeat the process until
    producers.foreach(producer_id => {
      val producer = nodes(producer_id)

      if (SimulationConfiguration.SANITY_MCMF_COST_SCALING_SANITY_CHECKS)
        assert(producer.supply > 0, s"Expected node ${producer_id} to be a producer, but has ${producer.supply} supply!")

      while (producer.supply > 0) {

        sink_reached = false
        to_visit.clear()
        predecessor.clear()
        visited.clear()

        to_visit.enqueue(producer_id)
        visited.addOne(producer_id, producer.supply)

        // Try to find a path from the source to the sink. This should be effectively a BFS search.
        while (to_visit.nonEmpty && !sink_reached) {

          val current = nodes(to_visit.dequeue())
          f(current.outgoing.iterator)
          f(current.incoming.iterator)

          def f(iterator: Iterator[FlowArc]) =
            while (iterator.hasNext && !sink_reached) {
              val arc = iterator.next()

              // We can only visit the node at the current arcs other end if that node hasn't been visited
              // already and there is still remaining capacity to host more flow.
              if (arc.residualCapacity > 0 && !visited.contains(arc.dst)) {

                visited.addOne(arc.dst, Math.min(arc.residualCapacity, visited.getOrElse(current.id, 0L)))
                predecessor.addOne(arc.dst, arc)
                // Prepend to make the search more DFS style rather than BFS
                to_visit.enqueue(arc.dst)

                // We reached a sink node. Apply the maximum possible flow to all nodes on the
                // path from the sink to the source
                if (arc.dst == sink_node) {

                  sink_reached = true
                  val min_aux_flow = visited(arc.dst)

                  producer.supply -= min_aux_flow
                  current.supply += min_aux_flow

                  var go_back = current.id
                  while (go_back != producer_id) {

                    val arc = predecessor(go_back)
                    go_back = arc.src

                    // arc.flow += flowchange
                    arc.residualCapacity -= min_aux_flow
                    arc.reverseArc.residualCapacity += min_aux_flow

                    if (SimulationConfiguration.SANITY_MCMF_COST_SCALING_SANITY_CHECKS)
                      GraphIntegrityValidator.validateArc(arc)

                  }

                }
              }

            }
        }
      }
    })
  }

}
