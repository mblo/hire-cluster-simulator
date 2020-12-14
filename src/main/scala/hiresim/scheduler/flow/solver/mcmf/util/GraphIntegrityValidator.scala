package hiresim.scheduler.flow.solver.mcmf.util

import hiresim.scheduler.flow.solver.graph.FlowNode.Supply
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}
import hiresim.shared.graph.Graph.NodeID

import scala.collection.mutable

object GraphIntegrityValidator {

  def validateArc(arc: FlowArc): Unit = {
    // Make sure reverse arc and forward arc are aligned
    checkReverseArcIntegrity(arc)
    // Make sure the residual capacity is within bounds
    checkResidualCapacityBounds(arc)
  }

  def checkNodeBalancedDeficit(id: NodeID,
                               graph: FlowGraph): Unit = {

    val node = graph.nodes(id)

    var deficit = 0L
    deficit += node.supply

    node.outgoing.foreach(arc => {
      deficit += arc.flow
    })

    assert(deficit == 0, s"Node ${id} is not balanced (deficit is ${deficit})!")

  }

  def checkZeroFlowArc(arc: FlowArc): Unit = {
    assert(arc.flow == 0, s"Flow through arc ${arc} is not 0 (is ${arc.flow})!")
    checkReverseArcIntegrity(arc)
  }

  def checkSpecificFlowArc(arc: FlowArc,
                           flow: Long): Unit = {
    assert(arc.flow == flow, s"Flow through arc ${arc} diverges from expected value (is ${arc.flow} vs. ${flow})!")
    checkReverseArcIntegrity(arc)
  }

  def checkMaxFlowArc(arc: FlowArc): Unit = {
    assert(arc.flow == arc.capacity, s"Flow through arc ${arc} is not maximal (is ${arc.flow} vs. ${arc.capacity})!")
    checkReverseArcIntegrity(arc)
  }

  def checkReverseArcIntegrity(arc: FlowArc): Unit = {
    assert(arc.flow == -arc.reverseArc.flow, s"Flow on edge ${arc} should be equal to negative flow on reverse edge (${arc.flow} vs. ${arc.reverseArc.flow})!")
  }

  def checkResidualCapacityBounds(arc: FlowArc): Unit = {

    if (arc.fwd)
      assert(arc.residualCapacity <= arc.capacity, s"Residual capacity on arc ${arc} may not exceed the capacity limit (${arc.residualCapacity} <= ${arc.capacity})!")
    else
      assert(arc.residualCapacity <= arc.reverseArc.capacity, s"Residual capacity on arc ${arc} may not exceed the capacity limit (${arc.residualCapacity} <= ${arc.reverseArc.capacity})!")

    assert(arc.residualCapacity >= 0, s"Residual capacity on arc $arc is negative (Is ${arc.residualCapacity})!")

  }

  def checkAllNodesZeroSupply(graph: FlowGraph): Unit = {
    graph.nodes.foreach(node => {
      assert(node.supply == 0, s"Supply not zero on node ${node.id}, but is ${node.supply}.")
    })
  }

  def checkSupplyOnNode(expected: Supply,
                        node: NodeID,
                        graph: FlowGraph): Unit = {
    assert(graph.nodes(node).supply == expected, s"Expected ${expected} supply on node ${node}, but was ${graph.nodes(node).supply}!")
  }

  def checkSingleSinkGraph(graph: FlowGraph): Unit = {

    val sinks: mutable.BitSet = mutable.BitSet()

    // Find all the sinks present in the graph
    graph.nodes.foreach(node => {

      if (node.supply < 0L)
        sinks += node.id

    })

    assert(sinks.nonEmpty, "FlowGraph has no sink!")
    assert(sinks.size == 1, s"FlowGraph has multiple sinks (Found: ${sinks.mkString(", ")})!")

  }

  def checkCycleFreeGraph(graph: FlowGraph): Unit =
    checkCycleFreeGraph(graph, graph.nodes.producerIds)

  def checkCycleFreeGraph(graph: FlowGraph,
                          producers: mutable.Iterable[NodeID]): Unit = {

    val completed: mutable.BitSet = mutable.BitSet()

    def validateChild(start: NodeID, path: mutable.BitSet): IterableOnce[Int] = {

      val visited: mutable.BitSet = mutable.BitSet()
      val node = graph.nodes(start)

      node.outgoing.foreach(arc => {
        val destination = arc.dst

        assert(!path.contains(destination), s"FlowGraph contains a cycle. " +
          s"Found a path from ${start} -> ${destination}, although node ${destination} was already visited! " +
          s"Participating nodes in path are: ${path.mkString(", ")}.")

        if (!completed.contains(destination))
          visited.addAll(
            // Ascend to not visited child and check it
            validateChild(
              start = destination,
              path = path.clone().addOne(destination)
            ))

      })

      // Manage collections.
      visited.addAll(path)
      completed.addAll(visited)

      visited

    }

    for (prod <- producers)
      validateChild(prod, new mutable.BitSet(prod))

  }

}
