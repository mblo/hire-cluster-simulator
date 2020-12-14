package hiresim.scheduler.flow.solver.graph

import hiresim.shared.graph.Graph.NodeID

import scala.collection.mutable

object FlowGraphUtils {

  /**
   * Visits all physical parents of the provided starting node in
   * BFS fashion
   *
   * @param start        the node to start from
   * @param graph        the graph to operate on
   * @param includeStart weather the provided starting node should be included.
   * @param function     the function to apply to every discovered node
   */
  def visitPhysicalParents(start: NodeID,
                           graph: FlowGraph,
                           includeStart: Boolean = true,
                           function: FlowNode => Unit): Unit = {
    visitPhysicalParentsBottomUp(start :: Nil, graph, includeStart, function)
  }

  def visitPhysicalParentsBottomUp(start: Iterable[NodeID],
                                   graph: FlowGraph,
                                   includeStart: Boolean = true,
                                   function: FlowNode => Unit): Unit = {

    val nodes = graph.nodes

    // we need an offset, so that we can use node.level+offset to have an array index starting from 0 for lowest level
    val lvlOffset: Int = -graph.nodes.lowerLevelBound
    val backlog: Array[mutable.Queue[FlowNode]] = Array.fill(graph.nodes.upperLevelBound + lvlOffset + 1)(mutable.Queue())

    //  Initialize collections for keeping tack of state (to visited and already visited)
    val visited: mutable.Set[NodeID] = mutable.BitSet()

    // Add the start nodes to the collection depending on where to start
    if (includeStart) {
      for (n <- start)
        if (visited.add(n)) {
          val node = nodes(n)
          backlog(node.level + lvlOffset).enqueue(node)
        }

    } else {
      // do not include the start nodes themself, but their parents
      // Check for the parents to be visited
      for (n <- start) {
        nodes(n).incoming.foreach(arc => {
          val parent = nodes(arc.dst)

          if (parent.isPhysical && visited.add(arc.dst))
            backlog(parent.level + lvlOffset).enqueue(parent)
        })
      }
    }

    // now do a bfs style bottom up traverse
    var arrayIndex = 0
    while (arrayIndex < backlog.length) {
      val todo = backlog(arrayIndex)
      while (todo.nonEmpty) {
        val visiting: FlowNode = todo.dequeue()

        // apply user function
        function(visiting)

        // check all parents
        visiting.incoming.foreach(arc => {
          val parent = nodes(arc.dst)

          if (parent.isPhysical && visited.add(arc.dst))
            backlog(parent.level + lvlOffset).enqueue(parent)
        })

      }

      arrayIndex += 1
    }

  }

}
