package hiresim.scheduler.flow.solver.mcmf

import hiresim.graph.NodeType
import hiresim.scheduler.SchedulerUtils
import hiresim.scheduler.flow.solver.MultiThreadSolver
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.scheduler.flow.solver.mcmf.util.{GraphIntegrityValidator, MCMFOptimalityConditions}
import hiresim.simulation.configuration.SimulationConfigurationHelper
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.io.Source

class DumpedGraphProblem(val resourceName: String, val expectedCost: Long) {
  val dumpedGraph: List[String] = Source.fromResource(resourceName).getLines().toList
}

class SimpleMCMFSolverTest extends AnyFunSuite with BeforeAndAfterEach {

  val dumpedGraphs = Array(
    new DumpedGraphProblem("graph1.in", 534L),
    new DumpedGraphProblem("graph2.in", 201L),
    new DumpedGraphProblem("graph3.in", -1L),
    new DumpedGraphProblem("graph4.in", 8891L),
  )

  dumpedGraphs.foreach(dumpedGraph => {
    SchedulerUtils.onExecuteForeachSolver(solver =>
      test(s"MCMF.Solver.FromDIMACS.${dumpedGraph.resourceName}.${solver.name}")({
        if (solver.isInstanceOf[MultiThreadSolver])
          println("skip test with MultiThreadSolver")
        else {

          if (dumpedGraph.expectedCost == -1L) {
            assertThrows[AssertionError](FlowGraph.fromDIMACS(dumpedGraph.dumpedGraph.iterator))
          } else {
            val graph: FlowGraph = FlowGraph.fromDIMACS(dumpedGraph.dumpedGraph.iterator)

            println(s"producers: ${graph.nodes.producerIds.size}")

            //        println(graph.exportDIMACS())

            // Execute solver
            solver.solveInline(graph, Long.MaxValue)

            // Make checks
            graph.nodes.foreach(node => {
              assert(node.supply == 0L, s"Supply of node ${node.id} is not zero (is ${node.supply})!")
            })

            val cost = MCMFOptimalityConditions.checkGraphCost(graph, dumpedGraph.expectedCost)
            println(s"${solver.name} found solution with: $cost$$ for ${dumpedGraph.resourceName}")
          }

        }
      })
    )


    SchedulerUtils.onExecuteForeachSolver(solver =>
      test(s"MCMF.Solver.FromDIMACS.Cloned.${dumpedGraph.resourceName}.${solver.name}")({
        if (solver.isInstanceOf[MultiThreadSolver])
          println("skip test with MultiThreadSolver")
        else {
          if (dumpedGraph.expectedCost == -1L) {
            assertThrows[AssertionError](FlowGraph.fromDIMACS(dumpedGraph.dumpedGraph.iterator))
          } else {
            val graph: FlowGraph = FlowGraph.fromDIMACS(dumpedGraph.dumpedGraph.iterator).clone()

            println(s"producers: ${graph.nodes.producerIds.size}")

            //        println(graph.exportDIMACS())

            // Execute solver
            solver.solveInline(graph, Long.MaxValue)

            // Make checks
            graph.nodes.foreach(node => {
              assert(node.supply == 0L, s"Supply of node ${node.id} is not zero (is ${node.supply})!")
            })

            val cost = MCMFOptimalityConditions.checkGraphCost(graph, dumpedGraph.expectedCost)
            println(s"${solver.name} found solution with: $cost$$ for ${dumpedGraph.resourceName}")
          }
        }
      })
    )

  })

  override protected def beforeEach(): Unit = {
    SimulationConfigurationHelper.setDefaultSimulationConfiguration()
  }

  def getConstrainedGraph(): (FlowGraph, Long) = {

    val graph: FlowGraph = new FlowGraph()


    //useless guy for increasing node id
    graph.addNode(new FlowNode(0L, NodeType.JOB_FLAVOR_SELECTOR, None, 3))
    val source = new FlowNode(4L, NodeType.JOB_FLAVOR_SELECTOR, None, 3)
    graph.addNode(source)

    // two producers
    val p1 = new FlowNode(0L, NodeType.SERVER_TASK_GROUP, None, 3)
    val p2 = new FlowNode(0L, NodeType.SERVER_TASK_GROUP, None, 3)
    graph.addNode(p1)
    graph.addNode(p2)

    // each producer has a postpone unsched node
    val nPostpone1 = new FlowNode(0L, NodeType.TASK_GROUP_POSTPONE, None, 2)
    val nPostpone2 = new FlowNode(0L, NodeType.TASK_GROUP_POSTPONE, None, 2)
    graph.addNode(nPostpone1)
    graph.addNode(nPostpone2)

    // the two producers share a capacity constrained link
    val nCommon = new FlowNode(0L, NodeType.NETWORK_NODE_FOR_SERVERS, None, 2)
    graph.addNode(nCommon)

    val sink = new FlowNode(-4L, NodeType.SINK, None, 0)
    graph.addNode(sink)
    graph.sinkNodeId = sink.id

    graph.addArc(new FlowArc(source.id, p1.id, 2L, 0L, 0L))
    graph.addArc(new FlowArc(source.id, p2.id, 2L, 0L, 0L))

    // P1-Common 10$
    val arcP1alloc = new FlowArc(p1.id, nCommon.id, 2L, 0L, 10L)
    graph.addArc(arcP1alloc)
    // P2-Common 20$
    val arcP2alloc = new FlowArc(p2.id, nCommon.id, 2L, 0L, 20L)
    graph.addArc(arcP2alloc)

    // Common Sink constrained for capacity 3
    graph.addArc(new FlowArc(nCommon.id, sink.id, 2L, 0L, 0L))

    // P1-Unsched 1000$
    val arcP1postpone = new FlowArc(p1.id, nPostpone1.id, 2L, 0L, 100L)
    graph.addArc(arcP1postpone)
    graph.addArc(new FlowArc(nPostpone1.id, sink.id, 2L, 0L, 0L))

    // p2-Unsched 5000$
    val arcP2postpone = new FlowArc(p2.id, nPostpone2.id, 2L, 0L, 200L)
    graph.addArc(arcP2postpone)
    graph.addArc(new FlowArc(nPostpone2.id, sink.id, 2L, 0L, 0L))

    (graph, arcP2alloc.cost * 2 + arcP1postpone.cost * 2)
  }

  SchedulerUtils.onExecuteForeachSolver(solver =>
    test(s"MCMF.Solver.GlobalOptimum.${solver.name}")({
      if (solver.isInstanceOf[MultiThreadSolver])
        println("skip test with MultiThreadSolver")
      else {
        val (graph: FlowGraph, expected_cost: Long) = getConstrainedGraph()
        // Execute solver
        solver.solveInline(graph, Int.MaxValue)

        val cost = MCMFOptimalityConditions.checkGraphCost(graph, expected_cost)
        println(s"${solver.name} found solution with: $cost$$")
      }
    }))

  SchedulerUtils.onExecuteForeachSolver(solver =>
    test(s"MCMF.Solver.SimpleTest.${solver.name}")({
      if (solver.isInstanceOf[MultiThreadSolver])
        println("skip test with MultiThreadSolver")
      else {

        val (graph: FlowGraph, expected_flows, expected_cost: Long) = getSimpleGraph()

        // Execute solver

        solver.solveInline(graph, 3L)

        // Make checks

        graph.nodes.foreach(node => {
          assert(node.supply == 0L, s"Supply of node ${node.id} is not zero (is ${node.supply})!")
        })

        MCMFOptimalityConditions.checkGraphCost(graph, expected_cost)

        expected_flows.foreach(entry => {
          GraphIntegrityValidator.checkSpecificFlowArc(entry._1, entry._2)
        })

      }
    })
  )

  private def getSimpleGraph(): (FlowGraph, mutable.Map[FlowArc, Long], Long) = {

    val expected: mutable.Map[FlowArc, Long] = mutable.Map()
    val graph: FlowGraph = new FlowGraph()

    val producer = new FlowNode(3L, NodeType.NETWORK_TASK_GROUP, None, 3)
    graph.addNode(producer)

    val sink = new FlowNode(-3L, NodeType.SINK, None, 0)
    graph.addNode(sink)
    graph.sinkNodeId = sink.id

    val n1 = new FlowNode(0L, NodeType.NETWORK_NODE_FOR_SERVERS, None, 2)
    graph.addNode(n1)

    val n2 = new FlowNode(0L, NodeType.NETWORK_NODE_FOR_SERVERS, None, 2)
    graph.addNode(n2)

    val a1 = new FlowArc(producer.id, n1.id, 2L, 0L, 10L)
    graph.addArc(a1)

    val a2 = new FlowArc(producer.id, n2.id, 2L, 0L, 5L)
    graph.addArc(a2)

    val a3 = new FlowArc(n1.id, sink.id, 2L, 0L, 2L)
    graph.addArc(a3)

    val a4 = new FlowArc(n2.id, sink.id, 2L, 0L, 8L)
    graph.addArc(a4)

    expected.put(a1, 2L)
    expected.put(a2, 1L)
    expected.put(a3, 2L)
    expected.put(a4, 1L)

    val expected_costs = 2 * a1.cost + 2 * a3.cost + 1 * a2.cost + 1 * a4.cost

    (graph, expected, expected_costs)

  }


}
