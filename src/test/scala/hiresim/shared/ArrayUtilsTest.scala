package hiresim.shared

import hiresim.simulation.RandomManager
import org.scalatest.concurrent.{ThreadSignaler, TimeLimitedTests}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

class ArrayUtilsTest extends AnyFunSuite {

  def getRandomArray(seed: Int, size: Int): Array[Long] = {
    val rand = new RandomManager(seed)
    (0 to size).map(_ => rand.getInt(size).toLong).toArray
  }

  test("medianOfRandomArray") {
    val givenArray: Array[Long] = getRandomArray(0, 100)
    val sorted: Array[Long] = givenArray.sorted
    val stats: NumberStats = ArrayUtils.getStats(givenArray, givenArray.length)
    stats.median.shouldBe(sorted(sorted.length / 2))
    stats.min.shouldBe(sorted.head)
    stats.max.shouldBe(sorted.last)
    stats.mean.shouldBe(sorted.sum / sorted.length)
  }

  test("medianOfAllEqualArray") {
    val givenArray: Array[Long] = Array.fill(10)(42)
    val sorted: Array[Long] = givenArray.sorted
    ArrayUtils.getMedian(givenArray, givenArray.length).shouldBe(sorted(sorted.length / 2))
  }

  test("medianOfAllEqualNegativeArray") {
    val givenArray: Array[Long] = Array.fill(10)(-42)
    val sorted: Array[Long] = givenArray.sorted
    ArrayUtils.getMedian(givenArray, givenArray.length).shouldBe(sorted(sorted.length / 2))
  }

  test("medianOfEmptyArray") {
    val givenArray: Array[Long] = Array.empty
    assertThrows[RuntimeException](ArrayUtils.getMedian(givenArray, givenArray.length))
  }


}
