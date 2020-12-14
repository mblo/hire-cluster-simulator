package hiresim.simulation

import scala.collection.BuildFrom
import scala.collection.mutable.ListBuffer

class RandomManager(val seed: Long = 0) {

  private val myNumberGenerator: util.Random = new scala.util.Random(seed)

  def copy: RandomManager = new RandomManager(seed)

  def getPlainRandomGenerator: util.Random = new scala.util.Random(seed)

  def shuffle[T, C](collection: IterableOnce[T])(implicit bf: BuildFrom[collection.type, T, C]): C = {
    myNumberGenerator.shuffle(collection)
  }

  def getInt(excludingMax: Int): Int = myNumberGenerator.nextInt(excludingMax)

  def getIntBetween(minIncluding: Int, excludingMax: Int): Int = {
    assert(minIncluding < excludingMax)
    minIncluding + myNumberGenerator.nextInt(excludingMax - minIncluding)
  }

  def getDouble: Double = myNumberGenerator.nextDouble()

  def getBoolean: Boolean = myNumberGenerator.nextBoolean()

  def getChoicesOfSeq[T >: Null <: AnyRef](choices: Int, seq: collection.IndexedSeq[T]): Array[T] = {
    val pick = getChoicesOfRange(choices, seq.size)
    val answer: Array[T] = new Array[Object](choices).asInstanceOf[Array[T]]

    var i = 0
    while (i < choices) {
      answer(i) = seq(pick(i))
      i += 1
    }
    answer
  }

  def getChoicesOfRange(choices: Int, rangeSize: Int): Array[Int] = {
    assert(choices <= rangeSize)
    val result: Array[Int] = new Array(choices)
    val available: ListBuffer[Int] = ListBuffer.from(0 until rangeSize)
    var i = 0
    while (i < choices) {
      val idx = myNumberGenerator.nextInt(available.size)
      result(i) = available(idx)
      available.remove(idx, count = 1)
      i += 1
    }
    result
  }

}
