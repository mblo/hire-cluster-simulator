package hiresim.shared.graph

import scala.collection.mutable


abstract class FixedIndexElementStore[A >: Null <: AnyRef, U](indices: mutable.BitSet = mutable.BitSet(),
                                                              startNodeArray: Array[A] = Array.empty) {

  protected var allNodes: Array[A] = {
    if (startNodeArray.isEmpty)
      getArray(1000)
    else
      startNodeArray.asInstanceOf
  }

  def add(item: A, atIndex: Int): A = {
    if (atIndex >= allNodes.length) {
      resizeInternal(atIndex)
    }
    allNodes(atIndex) = item
    indices.add(atIndex)
    item
  }

  def size: Int = indices.size

  def getArray(size: Int): Array[A]

  def foreach(fnc: A => Unit): Unit = {
    indices.foreach(i => fnc(allNodes(i)))
  }

  def inlineFilter(fnc: A => Boolean): Unit = {
    indices.clone().foreach(i =>
      if (!fnc(allNodes(i))) remove(i)
    )
  }

  def remove(index: Int): Boolean = {
    allNodes(index) = null
    indices.remove(index)
  }

  private def resizeInternal(includingIndex: Int): Unit = {
    val old = allNodes
    val newSize = (old.length * 2) max (includingIndex + 1)
    allNodes = getArray(newSize)
    System.arraycopy(old, 0, allNodes, 0, old.length)
  }

  def shimCopy: U

}