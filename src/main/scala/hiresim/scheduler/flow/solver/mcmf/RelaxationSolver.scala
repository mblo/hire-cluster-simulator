package hiresim.scheduler.flow.solver.mcmf

import hiresim.scheduler.flow.solver.TrackedSolver
import hiresim.scheduler.flow.solver.graph.FlowNode.`Supply.MaxValue`
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraph, FlowNode}
import hiresim.scheduler.flow.solver.mcmf.util.GraphIntegrityValidator
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

/**
 * Implements the RELAX-II algorithm for solving MCMF problems over a graph
 * network.
 *
 * Note that this implementation does NOT support minimum capacities / minimum flow
 * properties on the arcs of the graph! As this feature has not been used anywhere
 * in Hire (defaults to 0), it has not been implemented.
 */
class RelaxationSolver extends TrackedSolver {

  override def name: String = "Relaxation"


  /** Solves a transportation problem instance.
   *
   * @param graph the input graph, containing arcs and nodes.
   */
  override def solveInline0(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = {
    // Initializes all arcs in the graph with the correct rdcost values
    initialize(graph)

    // Scannable contains all nodes that are in the labelset but not in the scanset ordered by their distance to the sink
    val scannable: mutable.Queue[NodeID] = mutable.Queue() //mutable.PriorityQueue[NodeID] = mutable.PriorityQueue()(Ordering.by(getDistanceToSink).reverse)
    // Predecessors will hold the zero reduced cost tree relations
    val predecessors: mutable.LongMap[FlowArc] = mutable.LongMap[FlowArc]()

    val labelset: mutable.BitSet = new mutable.BitSet()
    val scanset: mutable.BitSet = new mutable.BitSet()
    var ascent: Boolean = false

    var node = getNodeWithUndistributedSupply(graph)

    var augnode: NodeID = -1
    var direction: Long = 0

    // Start relaxation iterations
    while (node != -1) {

      if (Thread.interrupted()) {
        // stop early
        throw new InterruptedException()
      }

      // Reset values
      augnode = -1
      direction = 0
      ascent = false

      // Setup initial state
      labelset += node
      scannable.enqueue(node)

      while (augnode == -1 && !ascent) {

        // Select a scanning node for this step
        var scanning: NodeID = scannable.dequeue()
        scanset += scanning

        // Do one scanning step. Will return a sink node id or -1
        augnode = onDoScanning(graph, scanning, labelset, scannable, predecessors, -1)
        // Recompute the scanset direction
        direction = onAdaptDirection(graph, scanset, scanning, direction)

        // Check dual ascend direction
        if (direction > 0)
          ascent = true
      }

      if (ascent)
        onDoAscent(graph, scanset, direction)
      else
        onAugmentFlow(graph, augnode, node, predecessors)

      predecessors.clear()
      scannable.clear()
      labelset.clear()
      scanset.clear()

      node = getNodeWithUndistributedSupply(graph)
    }

    if (SimulationConfiguration.SANITY_MCMF_CHECK_ZERO_SUPPLY_NETWORK)
      GraphIntegrityValidator.checkAllNodesZeroSupply(graph)
  }

  private def initialize(graph: FlowGraph): Unit = {

    // And initialize the reducedCost. This is simply the cost of that arc
    graph.nodes.foreach(node => {

      for (arc <- node.outgoing)
        arc.reducedCost = arc.cost
      for (arc <- node.incoming)
        arc.reducedCost = arc.cost

    })

  }

  /**
   * Returns a node of the graph with positive supply. If none found then the provided
   * default is returned.
   *
   * @param graph the graph to check
   * @return true if a supplyNode with undistributed supply has been found, false otherwise
   */
  private def getNodeWithUndistributedSupply(graph: FlowGraph,
                                             default: NodeID = -1): NodeID = {

    val nodes = graph.nodes
    var result = default

    var found = false
    var round = 0

    // Search all nodes for positive supply, meaning a node where supply > 0.
    while (!found && round < 2) {

      // We have two rounds. First we consider the producer ids as they are the
      // most promising and then look at the non producer ids
      val collection =
      if (round == 0)
        nodes.producerIds
      else
        nodes.nonProducerIds

      collection.foreach((node: NodeID) => {

        val check = nodes(node)

        if (check.supply > 0) {
          result = check.id
          found = true
        }

      })

      round += 1
    }

    result
  }

  private def onAdaptDirection(graph: FlowGraph,
                               scanset: mutable.BitSet,
                               newly: NodeID,
                               old_direction: Long): Long = {

    var direction: Long = old_direction
    val affected = graph.nodes(newly)

    direction += affected.supply

    def f(arc: FlowArc) = {
      if (arc.reducedCost == 0) {
        // The Arc is a internal arc to the Scanset. We need to remove it's
        // weight 'direction' as only arc on the cut will be considered.
        if (scanset.contains(arc.dst))
          direction += arc.reverseArc.residualCapacity
        // The arc is a new arc on the cut. Add (negative) weight to direction.
        else
          direction -= arc.residualCapacity
      }
    }

    for (arc <- affected.outgoing) f(arc)
    for (arc <- affected.incoming) f(arc)

    direction
  }

  /**
   * Finds a node directly connected to the provided node 'current', which should exhibit
   * unbalanced supply. It hereby also organizes a zero reduced cost tree, which provides
   * insight in predecessor information. If a node with negative supply is found, this
   * node will be returned, otherwise the provided default
   *
   * @param graph    the graph to operate on
   * @param labelset the set of nodes that have a "label attached" and still need to be scanset
   * @param current  the node to scan
   * @return a node id that exhibits a negative supply and is connected to target
   */
  private def onDoScanning(graph: FlowGraph,
                           current: NodeID,
                           labelset: mutable.BitSet,
                           scannable: mutable.Queue[NodeID],
                           predecessors: mutable.LongMap[FlowArc],
                           default: NodeID = -1): NodeID = {

    // The augnode is a supplyNode of negative supply (i.e. the sink).
    // If we find it, we return it, so we can send flow through this path
    var augnode: NodeID = default

    def f(outbound: FlowArc) = {

      if (outbound.residualCapacity > 0 && outbound.reducedCost == 0 && labelset.add(outbound.dst)) {

        // Mark this node as scannable. This operation has O(log(n)) in worstase (avg is O(1))
        if (outbound.fwd)
          scannable.prepend(outbound.dst)
        else
          scannable.append(outbound.dst)

        // And store information about where we came from
        predecessors.addOne(outbound.dst, outbound)

        val child: FlowNode = graph.nodes(outbound.dst)

        // Now check if the connected node exhibits some negative supply i.e. is a sink node
        if (child.supply < 0)
          augnode = outbound.dst

      }
    }

    for (outbound <- graph.nodes(current).outgoing) f(outbound)
    for (outbound <- graph.nodes(current).incoming) f(outbound)

    // Return any demand node we found that has negative supply
    augnode
  }

  private def onDoAscent(graph: FlowGraph,
                         scanset: mutable.BitSet,
                         old_direction: Long): Unit = {

    val nodes = graph.nodes

    // The current direction
    var direction = old_direction

    while (direction > 0) {

      // The maximum possible price change for the nodes in and around the scanset
      var pricechange: Long = `Supply.MaxValue`


      def f(arc: FlowArc, source: FlowNode) = {

        if (!scanset.contains(arc.dst)) {
          val destination = nodes(arc.dst)

          if (arc.reducedCost == 0) {

            destination.supply += arc.residualCapacity
            source.supply -= arc.residualCapacity

            arc.residualCapacity = 0L
            arc.reverseArc.residualCapacity =
              if (arc.fwd)
                arc.capacity
              else
                arc.reverseArc.capacity

            if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
              GraphIntegrityValidator.validateArc(arc)

          }

          // Compute the maximum possible price change.
          if (0 < arc.reducedCost && arc.reducedCost < pricechange)
            pricechange = arc.reducedCost

        }
      }

      // For all those arcs, whose src we already scanset, but destination not
      for (src <- scanset) {
        val source = nodes(src)

        for (arc <- source.outgoing) f(arc, source)
        for (arc <- source.incoming) f(arc, source)
      }

      if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
        assert(pricechange != `Supply.MaxValue`, "Could not find a valid price change!")

      def f2(arc: FlowArc) = {
        if (!scanset.contains(arc.dst)) {

          // If the arc has 0 reduced cost now, it won't have it later... So remove the weight of this arc
          if (arc.reducedCost == 0)
            direction += arc.residualCapacity

          arc.reducedCost -= pricechange
          arc.reverseArc.reducedCost += pricechange

          // The arc is balanced now, so take its weight into account
          if (arc.reducedCost == 0)
            direction -= arc.residualCapacity

        }

      }

      // Apply to all nodes on the cut where the head of an arc is inside scanset an tail is outside of that set
      for (src <- scanset) {
        for (arc <- nodes(src).outgoing) f2(arc)
        for (arc <- nodes(src).incoming) f2(arc)
      }
    }

  }

  /**
   * Pushes the maximum possible flow from the provided demand node
   * to the provided supply node. A path from the two nodes is found
   * by traversing the predecessor tree.
   *
   * @param graph        the graph to operate on
   * @param demandNode   a node that exhibits positive supply
   * @param supplyNode   a node that exhibits negative supply
   * @param predecessors the predecessor tree
   */
  private def onAugmentFlow(graph: FlowGraph,
                            demandNode: NodeID,
                            supplyNode: NodeID,
                            predecessors: mutable.LongMap[FlowArc]): Unit = {

    // Repack predecessor map for better performance
    predecessors.repack()

    // Used for more readible access
    val nodes = graph.nodes

    // This will be the amount of flow to be changed. We can only push the amount of
    // flow the supply node may has or the demand node may can take
    var flowchange = Math.min(nodes(supplyNode).supply, -nodes(demandNode).supply)

    var current = demandNode
    // Find the maximum possible flow change
    while (current != supplyNode) {

      val arc = predecessors(current)
      current = arc.src

      // The amount of flow we can push is also bounded by the smallest
      // remaining capacity on the path from the source to the sink
      flowchange = Math.min(flowchange, arc.residualCapacity)

    }

    nodes(supplyNode).supply -= flowchange
    nodes(demandNode).supply += flowchange

    current = demandNode
    while (current != supplyNode) {

      val arc = predecessors(current)
      current = arc.src

      // arc.flow += flowchange
      arc.residualCapacity -= flowchange
      arc.reverseArc.residualCapacity += flowchange

      if (SimulationConfiguration.SANIYY_MCMF_CHECKS)
        GraphIntegrityValidator.validateArc(arc)

    }

  }

}
