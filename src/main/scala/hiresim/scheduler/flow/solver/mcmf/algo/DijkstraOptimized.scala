package hiresim.scheduler.flow.solver.mcmf.algo

import hiresim.graph.NodeStatus
import hiresim.scheduler.flow.solver.graph.FlowNode._
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.scheduler.flow.solver.mcmf.util.GraphIntegrityValidator
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

/**
 * A implementation of the Dijkstra algorithm based on a TreeMap for selecting the
 * the next node to explore.
 */
object DijkstraOptimized {

  /** Solves a shortest-path problem by using an optimized version of Dijkstra.
   *
   * @param graph     the input graph, containing arcs and nodes.
   * @param producers the list of nodes with a positive supply (flow producers).
   * @return
   */
  def solve(graph: FlowGraph, producers: mutable.BitSet): (NodeID, mutable.Map[NodeID, FlowArc]) = {
    // If desired, we can make sure that the graph does NOT contain any cycles, which will lead
    // to this algorithm not terminating
    if (SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_CYCLE_FREE)
      GraphIntegrityValidator.checkCycleFreeGraph(graph, producers)

    // If wanted, we can make sure that there is only one sink.
    if (SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK)
      GraphIntegrityValidator.checkSingleSinkGraph(graph)

    // Initialize the problem to the initial setup
    graph.nodes.foreach(node => {
      node.distance = `Distance.MaxValue`
      node.status = NodeStatus.NOT_VISITED
    })

    // The predecessor array containing the predecessor tree. It's structure is as follows: current node -> parent / predecessor node
    val predecessors: mutable.Map[NodeID, FlowArc] = mutable.Map()
    // The distance map used by dijkstra to evaluate which node to look at next
    val distance_map: mutable.TreeMap[Distance, mutable.Stack[NodeID]] = mutable.TreeMap()


    val initial_nodes: mutable.Stack[NodeID] = distance_map.getOrElseUpdate(0L, mutable.Stack())
    // All 'active' nodes are source nodes. Initializing their distances to zero and setting their status
    producers.foreach(starter_id => {

      // Retrieve the actual node such that we can set the attributes of it
      val starter = graph.nodes(starter_id)

      // Setup initial node with 0 distance and visiting status
      starter.distance = 0L
      starter.status = NodeStatus.VISITING

      // Finally insert into 0-distance initial stack
      initial_nodes += starter_id

    })

    var sink = -1
    var done = false

    // Cycling across the binomial heap until there are no more nodes to explore or we found a sink node
    while (distance_map.nonEmpty && !done) {

      // Retrieving the node ID with the minimum distance and still in visiting state
      val current: FlowNode = {

        var closest_nodes_nearby: (Distance, mutable.Stack[NodeID]) = distance_map.head
        var found_node_id: NodeID = -1

        while (closest_nodes_nearby._2.isEmpty) {
          distance_map.remove(closest_nodes_nearby._1)

          // this happens only if we cannot find a path, abort with error
          if (distance_map.isEmpty)
            throw new AssertionError("There is not path from a producer to a sink node available!")

          closest_nodes_nearby = distance_map.head

        }

        found_node_id = closest_nodes_nearby._2.removeHead()

        val found_node = graph.nodes(found_node_id)
        // Enforce that the node was in the correct set
        assert(found_node.distance == closest_nodes_nearby._1,
          s"Node ${found_node_id} was labeled incorrectly. " +
            s"Expected distance ${closest_nodes_nearby._1}, " +
            s"but was actually ${graph.nodes(found_node_id).distance}!")

        if (closest_nodes_nearby._2.isEmpty)
          distance_map.remove(closest_nodes_nearby._1)

        found_node
      }

      // We found a sink node. We can now stop the exploration as we can only look at one path at a time.
      if (current.supply < 0) {

        // Remember the sink node id
        sink = current.id
        // And mark as complete
        done = true

        // If the node is not a sink node, explore further...
      } else {

        // Mark the current node as visited as we are now working on it.
        current.status = NodeStatus.VISITED

        current.foreachOutAndIncoming(arc => {
          if (arc.residualCapacity > 0L) {

            val neighbor = graph.nodes(arc.dst)

            // The cost of using that arc to reach the neighbor side
            val cost = arc.cost - current.potential + neighbor.potential
            // The costs of reaching the neighbor via the current node
            val updated_neighbor_distance = current.distance + cost

            neighbor.status match {

              // We have already encountered that node
              case NodeStatus.VISITING =>
                // Only update if the route via the current node is better
                if (updated_neighbor_distance < neighbor.distance) {

                  // Remove reference to that node old distance
                  distance_map(neighbor.distance) -= neighbor.id
                  neighbor.distance = updated_neighbor_distance

                  // Push the node to the correct queue in the distance map
                  val target_queue: mutable.Stack[NodeID] = distance_map.getOrElseUpdate(neighbor.distance, mutable.Stack())
                  target_queue.prepend(neighbor.id)

                  // Remember where we came from so we can augment flow later
                  predecessors.addOne(neighbor.id, arc)

                }

              // First time encountering this node...
              case NodeStatus.NOT_VISITED =>

                // Update neighbour parameters
                neighbor.distance = updated_neighbor_distance
                neighbor.status = NodeStatus.VISITING

                // Push the node to the correct queue in the distance map
                val target_queue: mutable.Stack[NodeID] = distance_map.getOrElseUpdate(neighbor.distance, mutable.Stack())
                target_queue.prepend(neighbor.id)

                predecessors.addOne(neighbor.id, arc)

              case _ =>
              // This case does not occur.

            }

          }
        })

      }

    }

    // Return the found sink and the path to it given by the predecessor tree
    (sink, predecessors)
  }

}
