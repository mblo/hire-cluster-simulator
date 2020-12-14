package hiresim.scheduler.flow.solver.graph

import hiresim.graph.NodeType
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost, Flow}
import hiresim.shared.graph.Graph
import hiresim.shared.graph.Graph.NodeID

import scala.collection.mutable

object FlowGraph {

  final val `NoNodeSet`: Int = -1

  def fromDIMACS(input: String): FlowGraph = fromDIMACS(input.linesIterator)

  def fromDIMACS(input: Iterator[String]): FlowGraph = {
    val graph: FlowGraph = new FlowGraph()
    var nodes: Array[FlowNode] = Array.empty
    var graphNodesAdded = false
    for (line <- input) {
      line.split(" +").toList match {
        case "c" :: tail => // comment, simply ignore
        case "p" :: "min" :: numNodes :: numArcs :: tail =>
          nodes = Array.ofDim(numNodes.toInt)
        case "a" :: src :: dst :: capacityLower :: capacity :: cost :: tail =>
          if (!graphNodesAdded) {
            // initialize all graph nodes
            for (i <- nodes.indices) {
              graph.addNode(nodes(i))
              assert(nodes(i).id == i)
              if (nodes(i).nodeType == NodeType.SINK)
                graph.sinkNodeId = i
            }
            graphNodesAdded = true
          }
          graph.addArc(new FlowArc(src.toInt, dst.toInt, capacity.toLong, capacityLower.toLong, cost.toLong))

        case "n" :: id :: supply :: nodeType :: tail =>
          assert(!graphNodesAdded)
          nodes(id.toInt) = new FlowNode(supply = supply.toLong, level = 0, nodeType = NodeType.getByName(nodeType))
      }
    }
    graph
  }

}


/**
 * The container storing all nodes registered within this graph. It allows
 * for efficient id to instance mapping.
 */
class MyNodeContainer(producerIds: mutable.BitSet = mutable.BitSet(),
                      nonProducerIds: mutable.BitSet = mutable.BitSet()) extends
  NodeContainer[FlowNode](producerIds = producerIds, nonProducerIds = nonProducerIds) {

  override def getArray(size: NodeID): Array[FlowNode] = new Array[FlowNode](size)

  override def clone(): MyNodeContainer = {
    val cloned = new MyNodeContainer(producerIds = this.producerIds.clone(),
      nonProducerIds = this.nonProducerIds.clone())
    this.copyPrivates(cloned)
    cloned
  }
}

class FlowGraph(var sanityChecks: Boolean = true,
                val nodes: MyNodeContainer = new MyNodeContainer()
               ) extends Graph[FlowArc, FlowNode] {

  def exportDIMACS(packIds: Boolean = true): String = {
    val out: mutable.StringBuilder = new mutable.StringBuilder()
    out.appendAll("c =========== \n")
    val forwardEdges: mutable.ArrayBuffer[FlowArc] = mutable.ArrayBuffer()
    val unusedNodeIds: mutable.BitSet = mutable.BitSet()
    (0 until nodes.size).foreach(i => unusedNodeIds.add(i))
    val mappedNodeIds: mutable.HashMap[Int, Int] = mutable.HashMap()
    nodes.foreach(node => {
      node.outgoing.foreach(arc => forwardEdges.addOne(arc))
      unusedNodeIds.remove(node.id)
    })
    out.appendAll(s"p min ${nodes.size} ${forwardEdges.size} \n")
    out.appendAll("c =========== \n")
    out.appendAll(s"c == all nodes using packed IDs:$packIds\n")
    nodes.foreach(node => {
      // check if we need to convert node id
      if (packIds && node.id >= nodes.size) {
        mappedNodeIds(node.id) = unusedNodeIds.head
        unusedNodeIds.remove(unusedNodeIds.head)
      } else {
        assert(!unusedNodeIds.contains(node.id))
        mappedNodeIds(node.id) = node.id
      }

      out.appendAll(s"c - node ${node.id} ${node.nodeType}\n")
      out.appendAll(s"n ${mappedNodeIds(node.id)} ${node.supply} ${node.nodeType}\n")
    })
    out.appendAll("c =========== \n")
    out.appendAll("c == all arcs \n")
    forwardEdges.foreach(arc => {
      assert(arc.fwd)
      out.appendAll(s"c - arc ${arc.src}->${arc.dst}\n")
      out.appendAll(s"a ${mappedNodeIds(arc.src)} ${mappedNodeIds(arc.dst)} ${arc.capacityLowerBound} ${arc.capacity} ${arc.cost}\n")
    })
    out.toString()
  }


  /**
   * The sink node of this graph
   */
  private[scheduler] var sinkNodeId: NodeID = FlowGraph.`NoNodeSet`


  @inline final def isEmpty: Boolean = nodes.isEmpty

  @inline final def nonEmpty: Boolean = nodes.nonEmpty

  @inline final def containsNode(nodeId: NodeID): Boolean = nodes.contains(nodeId)


  def addNode(newNode: FlowNode): NodeID = {
    /** New `Node` object ID */
    val newId = nodes.add(newNode)

    if (newNode.supply < 0L) {
      assert(sinkNodeId == FlowGraph.`NoNodeSet`, s"somebody adds another sink to the graph! $newNode")
    }

    newId
  }

  override def getNode(index: NodeID): FlowNode = nodes(index)

  def getArc(src: NodeID, dst: NodeID): FlowArc = nodes(src).outgoing.get(otherId = dst)

  def addArc(newArc: FlowArc): Unit = {
    /** Argument checks */
    if (sanityChecks) {
      assert(newArc.minFlow <= newArc.capacity)
      assert(nodes.contains(newArc.src),
        s"Arc $newArc has an illegal source node ID (${
          newArc.src
        }).")
      assert(nodes.contains(newArc.dst),
        s"Arc $newArc has an illegal destination node ID (${
          newArc.dst
        }).")
    }

    nodes(newArc.src).outgoing += newArc

    /** Creating the reverse `Arc` object */
    val reverseArc: FlowArc = new FlowArc(
      src = newArc.dst,
      dst = newArc.src,
      fwd = false,
      capacity = 0L, // initial flow is 0
      capacityLowerBound = 0L,
      cost = -newArc.cost,
      reverseArc = newArc
    )

    nodes(reverseArc.src).incoming += reverseArc

    /** Connecting this new `Arc` object with its reverse */
    newArc.reverseArc = reverseArc


    /** Push flow if reduced cost is < 0 */
    val reducedCost: Cost = newArc.cost - nodes(newArc.src).potential + nodes(newArc.dst).potential

    if (reducedCost < 0 && newArc.residualCapacity > 0) {

      if (sanityChecks && nodes(newArc.src).supply >= 0L && nodes(newArc.src).supply - newArc.residualCapacity < 0L)
        throw new AssertionError(s"Node ${newArc.src} (${nodes(newArc.src).nodeType}) has become a consumer.")

      nodes(newArc.src).supply -= newArc.residualCapacity

      if (sanityChecks && nodes(newArc.dst).supply >= 0L && nodes(newArc.dst).supply + newArc.residualCapacity < 0L)
        throw new AssertionError(s"Node ${newArc.dst} has become a consumer.")

      nodes(newArc.dst).supply += newArc.residualCapacity
      newArc.reverseArc.residualCapacity += newArc.residualCapacity
      newArc.residualCapacity = 0L

    }

    if (-reducedCost < 0L && newArc.reverseArc.residualCapacity > 0L) {

      if (sanityChecks && nodes(newArc.src).supply >= 0L && nodes(newArc.src).supply + newArc.reverseArc.residualCapacity < 0L)
        throw new AssertionError(s"Node ${newArc.src} has become a consumer.")

      nodes(newArc.src).supply += newArc.reverseArc.residualCapacity

      if (sanityChecks && nodes(newArc.dst).supply >= 0L && nodes(newArc.dst).supply - newArc.reverseArc.residualCapacity < 0L)
        throw new AssertionError(s"Node ${newArc.dst} has become a consumer.")

      nodes(newArc.dst).supply -= newArc.reverseArc.residualCapacity

      newArc.residualCapacity += newArc.reverseArc.residualCapacity
      newArc.reverseArc.residualCapacity = 0L

    }
  }


  /**
   * Updates the costs of an arc between two nodes
   *
   * @param src     the source of the arc to be updated
   * @param dst     the destination of the arc to be updated
   * @param newCost the new costs of the arc
   */
  @inline final def updateArc(src: NodeID,
                              dst: NodeID,
                              newCost: Cost): Unit = {

    updateArc(arc = getArc(src, dst), Some(newCost), newCapacity = None, newMinFlow = None)
  }

  /**
   * Updates the cost and capacity of the arc between two nodes
   *
   * @param src         the source of the arc to be updated
   * @param dst         the destination of the arc to be updated
   * @param newCost     the new cost of the arc
   * @param newCapacity the new cost of the arc
   */
  @inline final def updateArc(src: NodeID,
                              dst: NodeID,
                              newCost: Cost,
                              newCapacity: Capacity): Unit = {

    updateArc(arc = getArc(src, dst), newCost = Some(newCost), Some(newCapacity), newMinFlow = None)
  }

  /**
   * Updates the provided arc's attributes accordingly
   *
   * @param arc            the arc to modify
   * @param newCost        the new costs of the arc
   * @param newCapacity    the new capacity of the arc
   * @param newMinFlow     the new minimum flow / minimum capacity of the arc
   * @param alreadyReduced weather the costs have already been taken into account
   */
  def updateArc(arc: FlowArc,
                newCost: Option[Cost] = None,
                newCapacity: Option[Capacity] = None,
                newMinFlow: Option[Flow] = None,
                alreadyReduced: Boolean = true): Unit = {

    if (newCost.isDefined || newCapacity.isDefined) {

      val srcId: NodeID = arc.src
      val src: FlowNode = nodes(srcId)

      val dstId: NodeID = arc.dst
      val dst: FlowNode = nodes(dstId)

      val reverseArc: FlowArc = arc.reverseArc

      if (newCapacity.isDefined) {

        if (newCapacity.get == 0)
          removeArc(arc)

        val previousCapacity: Capacity = arc.residualCapacity + reverseArc.residualCapacity + arc.minFlow

        /** Update the minimum flow on the arc */
        if (newMinFlow.isDefined) {
          if (newMinFlow.get <= arc.minFlow) {

            val flowDifference: Flow = arc.minFlow - newMinFlow.get

            if (sanityChecks && src.supply >= 0L && src.supply + flowDifference < 0L)
              throw new AssertionError(s"Node $src has become a consumer.")

            src.supply += flowDifference

            if (sanityChecks && dst.supply >= 0L && dst.supply - flowDifference < 0L)
              throw new AssertionError(s"Node $dst has become a consumer.")

            dst.supply -= flowDifference
            arc.residualCapacity += flowDifference

          } else {

            val flowDifference: Flow = newMinFlow.get - arc.minFlow

            if (sanityChecks && src.supply >= 0L && src.supply - flowDifference < 0L)
              throw new AssertionError(s"Node $src has become a consumer.")

            src.supply -= flowDifference

            if (sanityChecks && dst.supply >= 0L && dst.supply + flowDifference < 0L)
              throw new AssertionError(s"Node $dst has become a consumer.")

            dst.supply += flowDifference
            arc.residualCapacity -= flowDifference

          }
          arc.minFlow = newMinFlow.get
        }

        /** Updating the arc capacity. Sending some flow back if required to keep it within the capacity limit. */
        if (previousCapacity < newCapacity.get)
          arc.residualCapacity += newCapacity.get - previousCapacity
        else {

          val capacityDifference: Capacity = previousCapacity - newCapacity.get
          if (arc.residualCapacity < capacityDifference) {

            /** Drain the eccess flow from the arc */
            val removedFlow: Flow = capacityDifference - arc.residualCapacity

            if (sanityChecks && src.supply >= 0L && src.supply + removedFlow < 0L)
              throw new AssertionError(s"Node $src has become a consumer.")

            src.supply += removedFlow

            if (sanityChecks && dst.supply >= 0L && dst.supply - removedFlow < 0L)
              throw new AssertionError(s"Node $dst has become a consumer.")

            dst.supply -= removedFlow
            reverseArc.residualCapacity -= removedFlow
            arc.residualCapacity = 0L

          } else
            arc.residualCapacity -= capacityDifference

        }

        /** Checks */
        assert(reverseArc.residualCapacity >= 0)
        assert(arc.residualCapacity + reverseArc.residualCapacity + arc.minFlow == newCapacity.get)
      }

      if (newCost.isDefined) {

        /** Updating the arc cost */
        arc.cost = newCost.get
        reverseArc.cost = -newCost.get

        /** Push flow if reduced cost is negative */
        if (!alreadyReduced) {

          val reducedCost: Cost = arc.cost - src.potential + dst.potential

          if (reducedCost < 0L && arc.residualCapacity > 0L) {

            if (sanityChecks && src.supply >= 0L && src.supply - arc.residualCapacity < 0L)
              throw new AssertionError(s"Node $src has become a consumer.")

            src.supply -= arc.residualCapacity

            if (sanityChecks && dst.supply >= 0L && dst.supply + arc.residualCapacity < 0L)
              throw new AssertionError(s"Node $dst has become a consumer.")

            dst.supply += arc.residualCapacity
            reverseArc.residualCapacity += arc.residualCapacity
            arc.residualCapacity = 0L

          }

          if (-reducedCost < 0L && reverseArc.residualCapacity > 0L) {

            if (sanityChecks && src.supply >= 0L && src.supply + arc.residualCapacity < 0L)
              throw new AssertionError(s"Node $src has become a consumer.")

            src.supply += reverseArc.residualCapacity

            if (sanityChecks && dst.supply >= 0L && dst.supply - arc.residualCapacity < 0L)
              throw new AssertionError(s"Node $dst has become a consumer.")

            dst.supply -= reverseArc.residualCapacity
            arc.residualCapacity += reverseArc.residualCapacity
            reverseArc.residualCapacity = 0L

          }
        }
      }
    }
  }

  override def removeArc(arc: FlowArc): Unit = {

    // Make sure this is a forward edge!
    if (sanityChecks)
      assert(arc.fwd, "Can not manually remove a reverse arc. Use the corresponding forward edge instead.")

    // Retrieve source and destination of the arc to be deleted
    val src: NodeID = arc.src
    val dst: NodeID = arc.dst

    nodes(src).outgoing -= arc
    nodes(dst).incoming -= arc.reverseArc

  }

  @inline final def removeOutgoingArc(src: NodeID, dst: NodeID): Unit = {
    val arc = nodes(src).outgoing.get(dst)
    removeArc(arc)
  }


  override def removeArcsWithSource(source: NodeID): Unit = {
    removeArcs(source, exclude = -1)
  }

  override def removeArcsOfNodeExcept(source: NodeID, exclude: NodeID): Unit = {
    removeArcs(source, exclude = exclude)
  }

  def removeArcs(origin: NodeID,
                 exclude: NodeID = -1): Unit = {

    val node = nodes(origin)

    // do we need to keep an arc?
    var keep: Option[FlowArc] = if (exclude != -1) node.outgoing.getOption(exclude) else None

    // remove all the reverse arcs of the destinations
    node.outgoing.foreach(arc => {
      if (arc.dst != exclude)
        nodes(arc.dst).incoming -= arc.reverseArc
    }, updateCache = false)

    // clear my own list of outgoing arcs
    node.outgoing.clear()
    // add excluded arc again
    if (keep.isDefined) node.outgoing.addOne(keep.get)

    keep = if (exclude != -1) node.incoming.getOption(exclude) else None

    node.incoming.foreach(arc => {
      if (arc.dst != exclude)
        nodes(arc.dst).outgoing -= arc.reverseArc
    }, updateCache = false)

    // clear my own list of incoming arcs
    node.incoming.clear()
    // add excluded arc again
    if (keep.isDefined) node.incoming.addOne(keep.get)


    // Do some sanity checks to ensure integrity
    if (sanityChecks) {
      if (exclude != -1) {
        assert(node.outgoing.size == 1 || node.incoming.size == 1)
      }
      node.outgoing.foreach(arc => assert(arc.dst == exclude))
      node.incoming.foreach(arc => assert(arc.dst == exclude))
    }

  }


  override def clone(): FlowGraph = {

    val copy = new FlowGraph(
      sanityChecks = this.sanityChecks,
      nodes = this.nodes.clone()
    )

    copy.sinkNodeId = this.sinkNodeId

    copy
  }

}




