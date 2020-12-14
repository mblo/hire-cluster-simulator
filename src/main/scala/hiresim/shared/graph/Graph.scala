package hiresim.shared.graph

import hiresim.shared.graph.Graph.NodeID

object Graph {

  /**
   * Abstract type for a node's ID.
   */
  type NodeID = Int

  /**
   * Abstract type for a arc's ID.
   */
  type ArcID = Int

}

abstract class Graph[A <: Arc, N <: Node] {
  def isEmpty: Boolean

  def nonEmpty: Boolean

  def addNode(newNode: N): NodeID

  def addNodes(newNodes: Seq[N]): Unit = newNodes.foreach(addNode)

  def addNodes(newNodes: Iterable[N]): Iterable[NodeID] = newNodes.map(addNode)

  def containsNode(nodeId: NodeID): Boolean

  def getNode(index: NodeID): N

  def addArc(newArc: A): Unit

  def removeArc(arc: A): Unit

  def removeOutgoingArc(src: NodeID, dst: NodeID): Unit

  def removeArcsWithSource(source: NodeID): Unit

  def removeArcsOfNodeExcept(origin: NodeID, exclude: NodeID): Unit

}
