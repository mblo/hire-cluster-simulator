package hiresim.scheduler.flow.solver.mcmf

import hiresim.graph.NodeStatus
import hiresim.scheduler.flow.solver.TrackedSolver
import hiresim.scheduler.flow.solver.graph.FlowArc._
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}
import hiresim.scheduler.flow.solver.mcmf.algo.DijkstraOptimized
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.SimTypes
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

class SuccessiveShortestSolver(val stopWhenAllAllocationsFound: Boolean = true,
                               val stopAtFirstUnscheduledFlow: Boolean = false,
                               val maxSecondsToSearch: Long = 30L) extends TrackedSolver {

  override def name: String = "SuccessiveShortestPath"

  assert(!stopAtFirstUnscheduledFlow, s"Hire cost model and CoCo cost model must continue when a U flow is found")

  /** Solves a transportation problem instance.
   *
   * @param graph the input graph, containing arcs and nodes.
   */
  override def solveInline0(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = {
    /** Nodes with a positive supply, i.e., producing flow */
    val producers: mutable.BitSet = graph.nodes.producerIds.clone()

    val startTime: Long = System.nanoTime()
    val maxTime: Long = if (maxSecondsToSearch > 0) System.nanoTime() + maxSecondsToSearch * SimTypes.GIGA else Long.MaxValue
    var rounds = 0
    var solvedTotalFlow = 0L
    var solvedFlowsOnMachines = 0L
    var foundUnscheduleFlow = false
    val initialProducerSize = producers.size

    /** Until every producer has been allocated */
    while (producers.nonEmpty) {
      rounds += 1

      // Run Dijkstra to find a path from a producer to a sink
      val (demandNodeId: NodeID, predecessors: mutable.Map[NodeID, FlowArc]) = DijkstraOptimized.solve(graph, producers)

      if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
        assert(demandNodeId != -1, "Could not determine a path from source to a sink.")

      graph.nodes.foreach(node =>
        if (node.status == NodeStatus.VISITED)
          node.potential -= node.distance
        else
          node.potential -= graph.nodes(demandNodeId).distance
      )


      val nodeIdBeforeSink = predecessors(demandNodeId).src
      var minFlow: Flow = `Flow.MaxValue`

      var curNodeId: NodeID = demandNodeId
      while (!producers.contains(curNodeId)) {

        val arc: FlowArc = predecessors(curNodeId)
        minFlow = math.min(minFlow, arc.residualCapacity)

        curNodeId = arc.src
      }

      minFlow = math.min(minFlow, graph.nodes(curNodeId).supply)

      assert(minFlow > 0, s"invalid flow of size:${minFlow}")

      /** Updating the supply at the demand node */
      if (SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK && graph.nodes(demandNodeId).supply >= 0L && graph.nodes(demandNodeId).supply + minFlow < 0L) {
        throw new AssertionError(s"Node $demandNodeId has become a consumer.")
      }
      graph.nodes(demandNodeId).supply += minFlow

      curNodeId = demandNodeId
      while (!producers.contains(curNodeId)) {

        val arc: FlowArc = predecessors(curNodeId)

        /** " We don't have to update the supplies along the path. Only
         * the supplies of the source and destination nodes change. "
         */
        arc.residualCapacity -= minFlow
        arc.reverseArc.residualCapacity += minFlow

        /** Next node */
        curNodeId = arc.src
      }

      val currentNode = graph.nodes(curNodeId)

      /** Update the supply at the source active node */
      if (SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK && currentNode.supply >= 0L && currentNode.supply - minFlow < 0L) {
        throw new AssertionError(s"Node $curNodeId has become a consumer.")
      }
      currentNode.supply -= minFlow
      solvedTotalFlow += minFlow
      if (!graph.nodes(nodeIdBeforeSink).isUnschedule) {
        solvedFlowsOnMachines += minFlow
      } else {
        foundUnscheduleFlow = true
      }

      if (graph.nodes(curNodeId).supply == 0L) {
        producers -= curNodeId
      }

      if (Thread.interrupted()) {
        // stop early
        throw new InterruptedException()
      }

      // consider stop early only for larger cells and larger workloads
      if (solvedFlowsOnMachines >= maxFlowsOnMachines && stopWhenAllAllocationsFound) {
        if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER)
          Console.out.println(s"Found all (requested) flows, " +
            s"total:$solvedTotalFlow onMachines:${solvedFlowsOnMachines} hintOnMachines:$maxFlowsOnMachines " +
            s"doAtMost:$maxFlowsOnMachines producers:$initialProducerSize, stop here")
        producers.clear()
      } else if (foundUnscheduleFlow && stopAtFirstUnscheduledFlow) {
        if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER)
          Console.out.println(s"SSP hit a U flow (${graph.nodes(nodeIdBeforeSink)}), " +
            s"total:$solvedTotalFlow onMachines:${solvedFlowsOnMachines} hintOnMachines:$maxFlowsOnMachines " +
            s"doAtMost:$maxFlowsOnMachines producers:$initialProducerSize, stop here")
        producers.clear()
      } else if (rounds % 100 == 0 && System.nanoTime() > maxTime) {
        if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER)
          Console.out.println(s"SSP timeout after ${System.nanoTime() / SimTypes.GIGA - startTime / SimTypes.GIGA}s, " +
            s"total:$solvedTotalFlow onMachines:${solvedFlowsOnMachines} hintOnMachines:$maxFlowsOnMachines " +
            s"doAtMost:$maxFlowsOnMachines producers:$initialProducerSize, stop here")
        producers.clear()
      }
    }
    if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER) {
      Console.out.println(s"Solved SSP ${(System.nanoTime() - startTime) / 1000000}ms/${rounds} rounds, " +
        s"total:$solvedTotalFlow onMachines:${solvedFlowsOnMachines} hintOnMachines:$maxFlowsOnMachines " +
        s"doAtMost:$maxFlowsOnMachines producers:$initialProducerSize, stop here")
    }
  }
}
