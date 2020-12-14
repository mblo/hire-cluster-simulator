package hiresim.shared.graph

import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

/**
 * A basic trait for all types that can be stored within the ElementStore container.
 * It dictates a field for accessing the elements id
 *
 * @tparam I the type of index this element can be referenced by
 */
trait Referencable[I <: Int] {

  var id: I

}

/**
 * provides a simple container data structure of elements
 *
 * order is always based on NodeId -> so it is deterministic, but not based on insertion order
 *
 * complexity
 * - add is O(1) [if run out of space, internal data structure will be resized; but existing IDs remain as they are]
 * - remove is O(1)
 * - foreach is like BitSet.foreach -> which is faster than iterating over HashSets or HashMap keyviews, and is deterministically ordered
 *
 * @tparam I the type of the index to be used
 * @tparam N the type of elements this container will hold
 */
abstract class ElementStore[I <: Int, N >: Null <: Referencable[I]] {

  protected def copyPrivates(to: ElementStore[I, N]): Unit = {
    to.freeIndex = freeIndex.clone()
    to.size = size
  }

  private var freeIndex: mutable.Queue[Int] = mutable.Queue.from(0 until SimulationConfiguration.HIRE_INITIAL_NODE_CONTAINER_SIZE)
  protected var allNodes: Array[N] = getArray(freeIndex.size)

  var size: Int = 0

  /**
   * Provides an array holing the size amount elements of type N
   *
   * @param size the size of the array to be created
   * @return the array of given size
   */
  def getArray(size: Int): Array[N]

  /**
   * Returns weather this container is filled or not
   *
   * @return true if there are elements within this container, false otherwise
   */
  def nonEmpty: Boolean = size > 0

  /**
   * Returns weather this container is empty or not
   *
   * @return true if this container is empty, false otherwise
   */
  def isEmpty: Boolean = size == 0

  /**
   * Provides the capacity of this container
   *
   * @return the current capacity of this container
   */
  private def capacity: Int = allNodes.length

  /**
   * Adds a element to this container.
   *
   * @param element the element to be added to the container
   * @return the NodeID which has been assigned to the added node
   */
  def add(element: N): I = {
    // Make sure this node is not registered already
    assert(element.id == ElementStore.Unassigned)

    if (freeIndex.isEmpty)
      resizeInternal()

    val nodeId = freeIndex.dequeue()
    allNodes(nodeId) = element

    element.id = nodeId.asInstanceOf[I]

    size += 1
    nodeId.asInstanceOf[I]
  }

  /**
   * Returns the element at the desired index of this container
   *
   * @param index the index to be queried
   * @return the node at the provided index
   */
  @inline def apply(index: I): N = allNodes(index)

  /**
   * Applies the provided function to all of the registered elements
   * of this container.
   *
   * @param fnc the function to be used
   */
  def foreach(fnc: N => Unit): Unit

  /**
   * Applies the provided function to all of the registered node /
   * node id pairs of this container.
   *
   * @param fnc the function to be used
   */
  def foreachPair(fnc: (I, N) => Unit): Unit

  /**
   * Removes the nodes associated with the provided NodeID from this container.
   * The freed id is enqueued for later reassignment.
   *
   * @param nodeId the id to be removed
   * @throws AssertionError if the provided NodeID is not a member of this container
   */
  def remove(nodeId: I): Unit = {
    if (nodeId < capacity && !freeIndex.contains(nodeId)) {
      freeIndex.enqueue(nodeId)
      size -= 1
      allNodes(nodeId).id = ElementStore.Unassigned.asInstanceOf[I]
      allNodes(nodeId) = null
    } else
      throw new AssertionError(s"you want to remove a node ($nodeId) which is not in the data structure!")
  }

  private def resizeInternal(): Unit = {
    val old = allNodes
    allNodes = getArray(old.length * 2)
    System.arraycopy(old, 0, allNodes, 0, old.length)
    freeIndex = mutable.Queue.from(old.length until allNodes.length)
  }

}

object ElementStore {
  /**
   * This value is assigned to nodes that are removed from this container.
   * Nodes to added are also required to carry this identifier.
   */
  final val Unassigned: Int = -1

}