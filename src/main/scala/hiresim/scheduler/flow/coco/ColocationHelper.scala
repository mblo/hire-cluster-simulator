package hiresim.scheduler.flow.coco

import hiresim.graph.NodeType
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.TaskGroup

object ColocationHelper {

  /**
   * Flattens the inference vector as in
   * https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L352
   */
  private[coco] def getFattenedInterferenceScore(interferenceVector: Array[Int]): Int = interferenceVector.sum


  /**
   * Models the computation of the interference score for Tasks as in
   * https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L368
   */
  private[coco] def getInterferenceScoreForTaskGroup(taskGroup: TaskGroup): Array[Int] = {
    // Interference vectors have length of 4. Each dimension represention either rabbit, devil, ...
    val vector: Array[Int] = Array(4)
    // The colocation type of the taskgroup
    val taskGroupType = taskGroup.resources.CoCoType

    for (coCoType <- CoCoTaskType.TYPES)
      vector(coCoType.getInterferenceVectorIndex) += taskGroupType.getPenalty(coCoType)

    vector
  }

  /**
   * Implements the computation of interference scores as described in
   * https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L186
   */
  private[coco] def computeInterferenceScore(node: NodeID,
                                             graph: FlowGraph): Int = {

    // The scale factor. Normally it's calculated by exp( (t-i)/t) as we do not support preemption, it is always 1
    // val scale_factor = 1;

    // The node to look at
    val target: FlowNode = graph.nodes(node)
    // The accumulator for summing up the interference costs
    var summed_interference_costs: Int = 0

    target.nodeType match {

      // Network Topology
      case NodeType.NETWORK_NODE_FOR_SERVERS | NodeType.NETWORK_NODE_FOR_INC =>

        // For each child accumulate the costs
        for (arc <- target.outgoing)
          summed_interference_costs += computeInterferenceScore(arc.dst, graph)

        summed_interference_costs /= target.outgoing.size

      // Machines
      case NodeType.MACHINE_SERVER | NodeType.MACHINE_NETWORK =>

        // Go through all the tasks that are running on that machine
        for (allocation <- target.allocatedTaskGroupsInSubtree) {

          // Calculate the interference score for this TaskGroup
          val inference_score = getInterferenceScoreForTaskGroup(allocation._1)
          // Flatten it, so we can sum it to the total costs
          val flattened_score: Int = getFattenedInterferenceScore(inference_score)

          // As we may running multiple instances of that task on the machine, we have to multiply the costs
          summed_interference_costs += (flattened_score * allocation._2)
        }


    }

    // Scale the cost to usable size. Note that as we do not support preemption
    // https://github.com/camsas/firmament/blob/353beb30fe7c9a9a69fb92f85b5cca006c146c8d/src/scheduling/flow/coco_cost_model.cc#L240
    // would always return 0 at this point.
    (CoCoConstants.Scaling_Factor * summed_interference_costs).toInt
  }

}
