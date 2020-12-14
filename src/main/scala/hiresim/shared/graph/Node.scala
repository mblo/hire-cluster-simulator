package hiresim.shared.graph

import hiresim.shared.graph.Graph.NodeID

/**
 * Supertype of all Node types
 *
 * @param id the id of this node
 */
abstract class Node(var id: NodeID = -1) extends Referencable[NodeID] {

  override def toString: String = s"Node(ID: $id)"

}

