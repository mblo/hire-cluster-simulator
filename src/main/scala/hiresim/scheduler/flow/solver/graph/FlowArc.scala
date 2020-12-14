package hiresim.scheduler.flow.solver.graph

import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost, Flow}
import hiresim.shared.graph.Arc
import hiresim.shared.graph.Graph.NodeID

/** Graph arcs type definitions */
object FlowArc {

  /** The type of the cost's associated with a Arc */
  type Cost = Long

  /** The type of the capacity associated with a Arc */
  type Capacity = Flow

  /** The type of the flow associated with a Arc */
  type Flow = Long


  /** Represents the maximum capacity a arc can hold */
  final val `Capacity.MaxValue`: Capacity = 100000000L

  /** The maximum cost that can be associated with an arc */
  final val `Cost.MaxValue`: Cost = 100000000L

  /** The maximum flow value that can be associated with an arc */
  final val `Flow.MaxValue`: Cost = 100000000L

}

/**
 * Represents a arc of a MCMF (Minimum Cost Maximum Flow) Graph, connecting
 * two nodes. Each arc respectively has a maximum capacity, a minimum capacity
 * and a cost of traversal.
 *
 * @param src                the NodeID of the source
 * @param dst                the NodeID of the destination
 * @param capacity           the maximum capacity of this arc
 * @param capacityLowerBound the minimum capacity of this arc (Default: 0)
 * @param cost               the cost assigned to one unit of flow through this arc (Default: 0)
 * @param reverseArc         the corresponding revere arc (Default: null)
 * @param fwd                Indicates weather this is a reverse arc. False if so, true if not.
 */

class FlowArc(src: NodeID,
              dst: NodeID,
              val capacity: Capacity,
              var capacityLowerBound: Capacity = 0L,
              var cost: Cost = 0L,
              val fwd: Boolean = true,
              var reverseArc: FlowArc = null) extends Arc(src, dst) {

  /**
   * The minimum flow that has to flow through this arc
   */
  var minFlow: Flow = capacityLowerBound

  /**
   * This arc's residual capacity (considering current flows on it)
   */
  var residualCapacity: Capacity = capacity

  /**
   * Weather this edge was detected by following outgoing edges
   * or incoming edges during scanning
   */
  var inbound: Boolean = false

  /**
   * The reduced cost.
   */
  var reducedCost: Cost = 0L


  /**
   * The flow on this arc. As this is a graph network containing
   * a residual network the flow is capacity - residualcapacity
   *
   * @return the flow through this arc
   */
  @inline final def flow: Flow = capacity - residualCapacity


  override def toString: String = s"$src${
    if (fwd) "->" else "~>"
  }$dst"

  /**
   * Clones this FlowArc instance without the
   * matching reverse arc
   *
   * @return the cloned instance
   */
  override def clone(): FlowArc = {

    // Create the copy instance
    val copy = new FlowArc(
      src = this.src,
      dst = this.dst,
      capacity = this.capacity,
      capacityLowerBound = this.capacityLowerBound,
      cost = this.cost,
      fwd = this.fwd,
    )

    // Copy over the needed parameters
    copy.minFlow = this.minFlow
    copy.residualCapacity = this.residualCapacity

    // And return it.
    // NOTE: Reverse arc is NOT cloned!
    copy

  }

}
