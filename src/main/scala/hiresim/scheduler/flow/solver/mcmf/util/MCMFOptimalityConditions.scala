package hiresim.scheduler.flow.solver.mcmf.util

import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph}

object MCMFOptimalityConditions {

  def checkGraphCost(graph: FlowGraph,
                     costs: Cost): Cost = {
    val calculated = getFlowPrice(graph)
    assert(costs == calculated, s"Cost of flow through graph diverges from expected value (is ${calculated} vs. ${costs})!")
    calculated
  }

  def checkEOptimalityCondition(graph: FlowGraph,
                                epsilon: Int): Unit = {
    for (node <- graph.nodes) {
      for (arc <- node.outgoing) {
        assert(arc.reducedCost >= -epsilon, s"Arc $arc violates e-optimality condition. Reduced cost smaller than -epsilon (is: ${arc.reducedCost}, -epsilon: ${-epsilon})")
      }
      for (arc <- node.incoming) {
        assert(arc.reducedCost >= -epsilon, s"Arc $arc violates e-optimality condition. Reduced cost smaller than -epsilon (is: ${arc.reducedCost}, -epsilon: ${-epsilon})")
      }
    }
  }

  def checkEOptimalityConditionInForwardGraph(graph: FlowGraph,
                                              epsilon: Int): Unit = {

    def f(arc: FlowArc) = {

      if (arc.reducedCost > epsilon)
        assert(arc.flow == 0, s"Arc $arc violates e-optimality condition. Reduced cost greater than epsilon but flow not zero (flow: ${arc.flow}, ReducedCost: ${arc.reducedCost})")

      if (arc.reducedCost >= -epsilon && arc.reducedCost <= epsilon)
        assert(arc.flow >= 0 && arc.flow <= arc.capacity, s"Arc $arc violates e-optimality condition. Reduced cost in interval [-epsilon, epsilon] but flow not in bounds (flow: ${arc.flow})")

      if (arc.reducedCost < -epsilon)
        assert(arc.flow == arc.capacity, s"Arc $arc violates e-optimality condition. Reduced cost smaller than -epsilon but flow not maximal (flow: ${arc.flow}, ReducedCost: ${arc.reducedCost})")
    }

    for (node <- graph.nodes) {
      for (arc <- node.outgoing) f(arc)
      for (arc <- node.incoming) f(arc)
    }
  }

  def getFlowPrice(graph: FlowGraph): Cost = {

    var costs: Cost = 0L

    for (node <- graph.nodes)
      for (arc <- node.outgoing)
        costs += arc.flow * arc.cost

    costs

  }

}
