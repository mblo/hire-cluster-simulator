package hiresim.scheduler.flow.solver.graph

import hiresim.shared.graph.ElementStore
import hiresim.shared.graph.Graph.NodeID

import scala.collection.mutable

/**
 * Provides a simple container data structure of nodes.
 *
 * @tparam N the type of nodes this container will hold
 */
abstract class NodeContainer[N >: Null <: FlowNode](val producerIds: mutable.BitSet = mutable.BitSet(),
                                                    val nonProducerIds: mutable.BitSet = mutable.BitSet()) extends ElementStore[NodeID, N] {

  protected def copyPrivates(to: NodeContainer[N]): Unit = {
    super.copyPrivates(to)
    to.minLevel = minLevel
    to.maxLevel = maxLevel

    to.allNodes = getArray(allNodes.length)

    // we need to connect the reverseArc's relations after cloning the arcs
    for (i <- producerIds.|(nonProducerIds)) {
      val org: N = allNodes(i)
      to.allNodes(i) = org.clone().asInstanceOf[N]
      to.allNodes(i).outgoing.foreach(arc => f(arc = arc, i = i, reverse = false))
      to.allNodes(i).incoming.foreach(arc => f(arc = arc, i = i, reverse = true))
    }

    @inline def f(arc: FlowArc, i: NodeID, reverse: Boolean): Unit = {
      // simply do the cycle mapping starting from the latter one, so that we already cloned the former one
      if (arc.dst < i) {
        val otherArc: FlowArc = if (reverse) to.allNodes(arc.dst).outgoing.get(i) else to.allNodes(arc.dst).incoming.get(i)
        arc.reverseArc = otherArc
        otherArc.reverseArc = arc
      }
    }
  }

  private var minLevel: Int = 0
  private var maxLevel: Int = 0

  // note, we don't care about level updates when nodes are removed.. so this gives only appx a bound, which holds all possible levels, for sure
  def lowerLevelBound: Int = minLevel

  def upperLevelBound: Int = maxLevel

  /**
   * Adds a node to this container.
   *
   * @param node the node to be added to the container
   * @return the NodeID which has been assigned to the added node
   */
  override def add(node: N): NodeID = {
    // Try adding the provided node this container. Will fail by raising a exception of not possible
    val id: NodeID = super.add(node)

    /** Producer case */
    if (node.isProducer)
      producerIds += id
    else
      nonProducerIds += id

    if (node.level < minLevel)
      minLevel = node.level
    else if (node.level > maxLevel)
      maxLevel = node.level

    id
  }

  @inline def contains(nodeId: NodeID): Boolean = producerIds.contains(nodeId) || nonProducerIds.contains(nodeId)

  @inline def containsNonProducer(nodeId: NodeID): Boolean = nonProducerIds.contains(nodeId)

  @inline def containsProducer(nodeId: NodeID): Boolean = producerIds.contains(nodeId)

  override def remove(nodeId: NodeID): Unit = {
    // Remove the node from the underlying Array
    super.remove(nodeId)
    // And detach id from producer mappings
    producerIds.remove(nodeId)
    nonProducerIds.remove(nodeId)
  }

  /**
   * Applies the provided function to all of the registered nodes
   * of this container.
   *
   * @param fnc the function to be used
   */
  override def foreach(fnc: N => Unit): Unit = {
    nonProducerIds.foreach(id => fnc(allNodes(id)))
    producerIds.foreach(id => fnc(allNodes(id)))
  }

  /**
   * Applies the provided function to all of the registered node /
   * node id pairs of this container.
   *
   * @param fnc the function to be used
   */
  override def foreachPair(fnc: (NodeID, N) => Unit): Unit = {
    nonProducerIds.foreach(id => fnc(id, allNodes(id)))
    producerIds.foreach(id => fnc(id, allNodes(id)))
  }

  def migrateNonProducerToProducerNode(nodeId: NodeID): Unit = {
    assert(nonProducerIds.remove(nodeId), s"the given node $nodeId is not a nonProducer")
    assert(producerIds.add(nodeId), s"the given node $nodeId is already a producer node")
  }

  def migrateProducerToNonProducerNode(nodeId: NodeID): Unit = {
    assert(producerIds.remove(nodeId), s"the given node $nodeId is not a producer")
    assert(nonProducerIds.add(nodeId), s"the given node $nodeId is already a nonProducer node")
  }


}
