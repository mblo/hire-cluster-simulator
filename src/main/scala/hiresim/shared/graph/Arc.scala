package hiresim.shared.graph

import hiresim.shared.graph.Graph.NodeID

/**
 * Superclass for all Arc types. A Arc connects two nodes
 * identified by their unique NodeID's
 *
 * @param src the NodeID of the source
 * @param dst the NodeID of the destination
 *
 */
class Arc(val src: NodeID,
          val dst: NodeID) {

  override def toString: String = s"Arc($src -> $dst)"

}