package hiresim.scheduler.flow.solver.graph

import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.graph.{NodeStatus, NodeType}
import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.FlowNode._
import hiresim.shared.graph.Graph.NodeID
import hiresim.shared.graph.{ElementStore, Node}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.TaskGroup

import scala.collection.mutable

/** Graph nodes type definitions */
object FlowNode {

  /** The type of the potential associated with a Node */
  type Potential = Cost

  /** The type of the supply associated with a Node */
  type Supply = Long

  /** Node distance from the source dummy node */
  type Distance = Long

  /** The maximum supply a node can host */
  final val `Supply.MaxValue`: Supply = 100000000L

  /** The minimum supply a node has to host */
  final val `Supply.MinValue`: Supply = -100000000L


  @inline final def `Distance.MaxValue`: Distance = 100000000L

}

/**
 * Represents a Node within in the MCMF Graph.
 *
 * @param supply    the supply this node will have
 * @param nodeType  the type of the node
 * @param taskGroup the task group this node belongs to (Default: None)
 * @param level     the level of this node in the network topology
 */
class FlowNode(var supply: Supply,
               val nodeType: NodeType,
               val taskGroup: Option[TaskGroup] = None,
               val level: Int) extends Node() {

  if (supply < 0L)
    assert(nodeType == NodeType.SINK, s"there is a node with supply<0 which is no SINK! found:$nodeType")

  /**
   * The potential of this node
   */
  var potential: Potential = 0L

  var usedUnscheduleId: NodeID = -1

  @inline def shouldHaveUnscheduledConnection: Boolean = usedUnscheduleId != -1

  @inline def isPresentInGraph: Boolean = id != ElementStore.Unassigned

  /**
   * The distance this node has from a origin. Used by Dijkstra
   * while solving the MCMF problem with SSP
   */
  var distance: Distance = `Distance.MaxValue`

  /**
   * The current status of this node, while exploring the graph
   * with Dijkstra
   */
  var status: NodeStatus = NodeStatus.NOT_VISITED

  /**
   * The deficit of this node (Deficit = Supply + Inflow - Outflow). This
   * is used for solving the MCMF Problem by Relaxation
   */
  var deficit: Supply = 0L

  private[scheduler] val outgoing: FlowArcContainer = FlowArcContainer(holdsOutgoing = true, belongsTo = id)
  private[scheduler] val incoming: FlowArcContainer = FlowArcContainer(holdsOutgoing = false, belongsTo = id)

  @inline private[scheduler] def foreachOutAndIncoming(fnc: FlowArc => Unit, updateCache: Boolean = true): Unit = {
    outgoing.foreach(fnc, updateCache)
    incoming.foreach(fnc, updateCache)
  }

  /**
   *
   * @return true if this node is a producer, false otherwise
   */
  def isProducer: Boolean = supply > 0

  /**
   * Whether information concerning this node was changed and a
   * recalculation of corresponding data is required. Initially
   * the value is set to true such that all costs get re-computed
   * before the first run.
   */
  var dirty: Boolean = true

  /** Keeping track of the number of running tasks in this `Node`'s subtree.
   *
   * Key = `TaskGroup`
   * Value = number of running tasks in this `Node`'s subtree
   */
  var allocatedTaskGroupsInSubtree: mutable.Map[TaskGroup, Int] = mutable.Map()

  /**
   * for each res dim the resources (and switch props) EVERY switch holds
   */
  var minSwitchResourcesInSubtree: SwitchResource = _

  /**
   * for each res dim the resources (and switch props) AT LEAST ONE switch holds
   */
  var maxSwitchResourcesInSubtree: SwitchResource = _

  /**
   * holds the maximum for each resource dimension and all possible switch props at least one switch child could serve
   */
  var maxStaticSwitchResourcesInSubtree: SwitchResource = _

  /**
   * for each res dim the resources EVERY server holds
   */
  var minServerResourcesInSubtree: ServerResource = _

  /**
   * for each res dim the resources AT LEAST ONE server holds
   */
  var maxServerResourcesInSubtree: ServerResource = _

  @inline final def tasksOfThisGroupNotRunningInThisSubtree(taskGroup: TaskGroup): Int = {
    taskGroup.runningTasks - allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0)
  }

  final val isPhysical: Boolean = {
    nodeType == NodeType.NETWORK_NODE_FOR_SERVERS ||
      nodeType == NodeType.NETWORK_NODE_FOR_INC ||
      nodeType == NodeType.MACHINE_NETWORK ||
      nodeType == NodeType.MACHINE_SERVER
  }

  final val isMachine: Boolean = {
    nodeType == NodeType.MACHINE_NETWORK ||
      nodeType == NodeType.MACHINE_SERVER
  }

  // stores how many tasks can be scheduled most per round on this node, or any of its children
  var maxTaskFlows: Int = if (isMachine) 1 else 0

  final val isServerMachine: Boolean = {
    nodeType == NodeType.MACHINE_SERVER
  }

  final val isSwitchMachine: Boolean = {
    nodeType == NodeType.MACHINE_NETWORK
  }

  final val isTaskGroup: Boolean = {
    nodeType == NodeType.NETWORK_TASK_GROUP ||
      nodeType == NodeType.SERVER_TASK_GROUP
  }

  final val isUnschedule: Boolean = {
    nodeType == NodeType.TASK_GROUP_POSTPONE
  }

  final val canHost: Boolean = {
    nodeType == NodeType.TASK_GROUP_POSTPONE ||
      // For Network Tasks (NTs)
      nodeType == NodeType.MACHINE_NETWORK ||
      // For Server Tasks (STs)
      nodeType == NodeType.MACHINE_SERVER
  }

  override def toString: String = id.toString


  override def clone(): FlowNode = {

    // Create the instance of the copy
    val copy: FlowNode = new FlowNode(
      supply = this.supply,
      nodeType = this.nodeType,
      taskGroup = this.taskGroup,
      level = this.level
    )

    // Set the id tag to be equal!
    copy.id = id
    // Copy the dirty flag. Mostly likely the source is not dirty
    copy.dirty = dirty
    // Copy over unschedule node id
    copy.usedUnscheduleId = usedUnscheduleId

    // Copy over resources. Do not copy here, as not needed
    copy.minSwitchResourcesInSubtree = minSwitchResourcesInSubtree
    copy.maxSwitchResourcesInSubtree = maxSwitchResourcesInSubtree
    copy.minServerResourcesInSubtree = minServerResourcesInSubtree
    copy.maxServerResourcesInSubtree = maxServerResourcesInSubtree
    // Allocated tg in subtree is also same
    copy.allocatedTaskGroupsInSubtree = allocatedTaskGroupsInSubtree

    copy.outgoing.cloneEverythingFromOther(outgoing)
    copy.incoming.cloneEverythingFromOther(incoming)

    // And return the copy instance
    copy
  }
}

trait FlowArcContainer {

  def debugString(print: Boolean = true): String

  private[hiresim] def cloneEverythingFromOther(other: FlowArcContainer): Unit

  def clear(): Unit

  def cacheIsUpToDate(): Boolean

  def iterator: Iterator[FlowArc]

  def get(otherId: NodeID): FlowArc

  def getOption(otherId: NodeID): Option[FlowArc]

  def -=(arc: FlowArc): FlowArcContainer

  def foreach(fnc: FlowArc => Unit, updateCache: Boolean = true): Unit

  @inline def size: Int

  @inline def isEmpty: Boolean

  @inline def nonEmpty: Boolean

  @inline def existsArcWith(id: NodeID): Boolean

  def addOne(arc: FlowArc): FlowArcContainer

  def +=(arc: FlowArc): FlowArcContainer
}


object FlowArcContainer {
  var defaultUseOptimized: Boolean = true

  private[graph] val empty: Array[FlowArc] = Array.empty

  def apply(holdsOutgoing: Boolean, belongsTo: Int, optimize: Boolean = false): FlowArcContainer =
    if (optimize || defaultUseOptimized)
      new OptimizedFlowArcContainer(holdsOutgoing, belongsTo)
    else
      new ArrayDequeFlowArcContainer(holdsOutgoing, belongsTo)
}

class ArrayDequeFlowArcContainer(holdsOutgoing: Boolean, belongsTo: Int) extends FlowArcContainer {

  private val container: mutable.ArrayDeque[FlowArc] = mutable.ArrayDeque()

  override def debugString(print: Boolean = true): String = {
    val s = container.mkString("[", ",", "]")
    if (print)
      System.err.println(s)
    s
  }

  override private[hiresim] def cloneEverythingFromOther(other: FlowArcContainer): Unit = {
    container.clear()
    other.foreach(arc => addOne(arc.clone()))
  }

  override def clear(): Unit = container.clear()

  override def iterator: Iterator[FlowArc] = container.iterator

  override def get(otherId: NodeID): FlowArc = container.find(_.dst == otherId).get

  override def getOption(otherId: NodeID): Option[FlowArc] = container.find(_.dst == otherId)

  override def -=(arc: FlowArc): FlowArcContainer = {
    container.subtractOne(arc)
    this
  }

  override def foreach(fnc: FlowArc => Unit, updateCache: Boolean): Unit = container.foreach(fnc)

  override def size: NodeID = container.size

  override def isEmpty: Boolean = container.isEmpty

  override def nonEmpty: Boolean = container.nonEmpty

  override def existsArcWith(id: NodeID): Boolean = container.exists(_.dst == id)

  override def addOne(arc: FlowArc): FlowArcContainer = {
    container.addOne(arc)
    this
  }

  override def +=(arc: FlowArc): FlowArcContainer = addOne(arc)

  override def cacheIsUpToDate(): Boolean = true
}

class OptimizedFlowArcContainer(holdsOutgoing: Boolean, belongsTo: Int) extends FlowArcContainer {

  private var lazyArray: Array[FlowArc] = FlowArcContainer.empty
  private var lockNoEdits: Boolean = false
  private val container: mutable.TreeMap[Int, FlowArc] = mutable.TreeMap()

  def debugString(print: Boolean = true): String = {
    val sortedLazy: Array[FlowArc] = lazyArray.sortWith(_.dst < _.dst)
    val sortedContainer: Array[FlowArc] = container.values.toArray.sortWith(_.dst < _.dst)

    val t1 = sortedContainer.sameElements(container.values.toArray.asInstanceOf[Array[FlowArc]])
    val t11 = sortedContainer.sameElements(container.keys.map(k => container(k)).toArray.asInstanceOf[Array[FlowArc]])

    var s = s"$this lockNoEdits:$lockNoEdits \n bookkeeping: "
    s += s"\n currentState: ${container.mkString("[", ",", "]")} " +
      s"(${container.size}) correctly sorted:${t1} and with .keys, sorted:${t11}" +
      s"\n lazyArray: ${lazyArray.mkString("[", ",", "]")} " +
      s"(${lazyArray.length}) correctly sorted:${sortedLazy.sameElements(lazyArray)}"

    if (print)
      FlowArcContainer.synchronized {
        System.err.println(s)
      }
    s
  }

  private[hiresim] def cloneEverythingFromOther(other: FlowArcContainer): Unit = {
    container.clear()
    lockNoEdits = true
    var i = 0
    lazyArray = Array.ofDim(other.size)
    other.foreach(a => {
      lazyArray(i) = a.clone()
      i += 1
    })
    assert(lazyArray.length == i, s"the other guy reported a size of ${lazyArray.length}, " +
      s"but I received only ${i} entries!")

    if (SimulationConfiguration.SANITY_CHECKS_HIRE) {
      assert(!lazyArray.contains(null), "Array contained null element after cloning!")
      assert(other.size == i, s"Size mismatch. Origin size: ${other.size} Copied elements: ${i}")
    }
  }

  @inline private def updateCache(): Unit = {
    assert(!lockNoEdits)
    lazyArray = container.values.toArray
  }

  @inline private def invalidateCache(): Unit = {
    if (lazyArray.nonEmpty) {
      lazyArray = FlowArcContainer.empty
    }
  }

  def clear(): Unit = {
    container.clear()
    invalidateCache()
    lockNoEdits = false
  }

  def iterator: Iterator[FlowArc] = {
    // if lazyArray is empty, check if lock is set
    if (lazyArray.isEmpty && !lockNoEdits) {
      updateCache()
    }
    lazyArray.iterator
  }

  def getOption(otherId: NodeID): Option[FlowArc] = {
    if (lockNoEdits) {
      val found = searchArray(otherId)
      if (found == -1) None else Some(lazyArray(found))
    } else container.get(otherId)
  }

  private def searchArray(otherId: Int): Int = {
    var left = 0
    var right = lazyArray.length - 1
    var found = -1
    while (left <= right && found == -1) {
      val middle = left + (right - left) / 2
      val v = lazyArray(middle).dst
      if (v == otherId) {
        found = middle
      } else if (v < otherId) {
        left = middle + 1
      } else {
        right = middle - 1
      }
    }
    found
  }

  def get(otherId: NodeID): FlowArc = {
    if (lockNoEdits) {
      val found = searchArray(otherId)
      if (found == -1) {
        // shortly before bad memory access... so dump the state
        FlowArcContainer.synchronized {
          System.err.println(s"$this.FlowArcContainer.get(${otherId})")
          System.err.println(s" data: ${debugString(false)}")
          System.err.flush()
        }
      }
      lazyArray(found)
    } else container.apply(otherId)
  }

  def -=(arc: FlowArc): FlowArcContainer = {
    assert(!lockNoEdits)
    assert(arc.fwd == holdsOutgoing)

    val beforeSize = size
    container.subtractOne(arc.dst)
    invalidateCache()
    assert(size + 1 == beforeSize)

    this
  }

  def foreach(fnc: FlowArc => Unit, updateCache: Boolean = true): Unit = {
    // if lockNoEdits is set and lazyArray is empty, we do not need to refresh the array - the node simply is not connected!
    if (lazyArray.isEmpty && !lockNoEdits) {
      if (updateCache) {
        this.updateCache()
        lazyArray.foreach(x => fnc(x))
      } else container.values.foreach(x => fnc(x))
    } else {
      lazyArray.foreach(x => fnc(x))
    }
  }

  @inline def size: Int = if (lockNoEdits || lazyArray.nonEmpty) lazyArray.length else container.size

  @inline def isEmpty: Boolean = if (lockNoEdits) lazyArray.isEmpty else container.isEmpty

  @inline def nonEmpty: Boolean = if (lockNoEdits) lazyArray.nonEmpty else container.nonEmpty

  @inline def existsArcWith(id: NodeID): Boolean = {
    assert(!lockNoEdits, "search not supported")
    container.contains(id)
  }

  def addOne(arc: FlowArc): FlowArcContainer = {
    assert(!lockNoEdits)
    assert(arc.fwd == holdsOutgoing)

    val beforeSize = size
    container.addOne(arc.dst, arc)
    invalidateCache()
    assert(size - 1 == beforeSize)

    this
  }

  @inline def +=(arc: FlowArc): FlowArcContainer = addOne(arc)

  override def cacheIsUpToDate(): Boolean = lockNoEdits || lazyArray.nonEmpty

}
