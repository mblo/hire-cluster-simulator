package hiresim.shared

class NumberStats(val min: Long, val mean: Long, val median: Long, val max: Long)

object ArrayUtils {

  def getMedian(array: Array[Long], totalElements: Int): Long =
    getStats(array, totalElements).median

  def getStats(array: Array[Long], totalElements: Int): NumberStats = {
    assert(totalElements <= array.length)
    if (totalElements == 0) {
      throw new RuntimeException("empty array given")
    } else {

      var min = Long.MaxValue
      var max = Long.MinValue
      var sum = BigInt(0)
      var firstRound = true

      var pivIdx = totalElements / 2
      var tmp: Array[Long] = array.clone()
      var length = totalElements
      while (pivIdx > 0) {
        val pivot = tmp(pivIdx)
        var lengthSmaller = 0
        var lengthLarger = 0
        var lower = 0
        // sort values
        val smallerValues: Array[Long] = Array.ofDim(length)
        val largerValues: Array[Long] = Array.ofDim(length)
        while (lower < length) {
          if (firstRound) {
            sum += tmp(lower)
            if (tmp(lower) < min) min = tmp(lower)
            if (tmp(lower) > max) max = tmp(lower)
          }
          // does this set belong to
          if (tmp(lower) < pivot) {
            smallerValues(lengthSmaller) = tmp(lower)
            lengthSmaller += 1
          } else if (tmp(lower) > pivot) {
            largerValues(lengthLarger) = tmp(lower)
            lengthLarger += 1
          }
          lower += 1
        }

        if (pivIdx < lengthSmaller) {
          tmp = smallerValues
          length = lengthSmaller
        } else if (pivIdx < length - lengthLarger) {
          tmp(0) = pivot
          pivIdx = 0
        } else {
          pivIdx -= (length - lengthLarger)
          tmp = largerValues
          length = lengthLarger
        }

        firstRound = false
      }

      new NumberStats(min = min, max = max, mean = (sum / totalElements).toLong, median = tmp(0))
    }
  }

  def subtract(a: Array[Int], b: Array[Int]): Array[Int] = {
    val out = a.clone()
    for (i <- out.indices) {
      out(i) -= b(i)
    }
    out
  }

  @inline def subtractInplace(a: Array[Int], b: Array[Int]): Unit = {
    for (i <- a.indices) {
      a(i) -= b(i)
    }
  }

  @inline def array2string(a: IndexedSeq[Any]): String = a.map(_.toString).mkString("[", ",", "]")

}
