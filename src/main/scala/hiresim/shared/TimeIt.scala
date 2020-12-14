package hiresim.shared

import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable

/**
 *
 * use it like
 * val t = TimeIt("your id")
 * t.stop
 *
 * // later
 *
 * TimeIt.printStatistics
 *
 */
class TimeIt(name: String, writeLater: Boolean) {
  final val start = System.nanoTime()
  private var written = false
  var diff = 0L

  if (!writeLater)
    writeTimer()

  def writeNow() = writeTimer()

  private def writeTimer(): Unit = {
    assert(!written)
    written = true
    if (SimulationConfiguration.TIME_IT_ACTIVATE_PRINT) {
      val entry = TimeIt.allTimer.getOrElseUpdate(name, new TimeItEntry())
      entry.queue.enqueue(this)
      entry.cnt += 1

      if (writeLater) {
        entry.sum += BigInt(diff)
      }
    }
  }

  @inline final def stop: Unit = {
    if (SimulationConfiguration.TIME_IT_ACTIVATE) {
      assert(diff == 0)
      diff = System.nanoTime() - start

      if (!writeLater) {
        val entry = TimeIt.allTimer.getOrElseUpdate(name, new TimeItEntry())
        entry.sum += BigInt(diff)
      }
    }
  }
}

class MeasurementStatistics(val statMin: Long,
                            val statMax: Long,
                            val statAvg: Double,
                            val statMedian: Long,
                            val entries: Int)

class Measurements(val maxEntries: Int = 10) extends FixedSizeFiFoArray[TimeIt](maxSize = maxEntries) {


  override protected def getArray(size: Int): Array[TimeIt] = Array.ofDim(size)

  def calcStatistics: MeasurementStatistics = {
    var count = 0
    var i = 0
    val stopBefore = getValidArrayBound
    val toSearchMedian: Array[Long] = Array.ofDim(stopBefore)
    while (i < stopBefore) {
      val t = all(i)
      if (t.diff > 0) {
        toSearchMedian(count) = t.diff
        count += 1
      }
      i += 1
    }

    if (count == 0) {
      new MeasurementStatistics(statMax = -1,
        statMin = -1, statAvg = -1, statMedian = -1, entries = 0)
    } else {
      val stats: NumberStats = ArrayUtils.getStats(toSearchMedian, count)
      new MeasurementStatistics(
        statMax = stats.max / 1000,
        statAvg = stats.mean / 1000,
        statMin = stats.min / 1000,
        statMedian = stats.median / 1000,
        entries = count)
    }
  }
}


class TimeItEntry(val queue: Measurements = new Measurements(TimeIt.maxEntries),
                  var sum: BigInt = BigInt(0),
                  var previous: BigInt = BigInt(0),
                  var cnt: Long = 0L)

object TimeIt {
  final val maxEntries = 1000

  final private val allTimer: mutable.TreeMap[String, TimeItEntry] = mutable.TreeMap()
  final private val placeholder = new TimeIt("", true)

  final def resetStatistics: Unit = {
    allTimer.clear()
  }

  @inline final def apply(name: => String, ignore: Boolean = false): TimeIt =
    if (SimulationConfiguration.TIME_IT_ACTIVATE) new TimeIt(name, writeLater = ignore) else placeholder

  final def printStatistics: Unit = {
    if (SimulationConfiguration.TIME_IT_ACTIVATE_PRINT) {
      val entries: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer()
      entries.append(List("Name", "#", "Min [µs]", "~ Avg [µs]", "Median [µs]", "Max [µs]", "Total [ms]", "+ total"))
      for ((name: String, entry: TimeItEntry) <- allTimer) {
        val statistics = entry.queue.calcStatistics

        entries.append(List(
          name,
          s"${statistics.entries}/${entry.queue.size} | ${entry.cnt}",
          statistics.statMin.toString,
          statistics.statAvg.toLong.toString,
          statistics.statMedian.toString,
          statistics.statMax.toString,
          s"${entry.sum / 1000000}",
          s"+${(entry.sum - entry.previous) / 1000000}"))
        // update previous field
        entry.previous = entry.sum
      }

      Console.out.println(s"(TimeIt considers only at most ${maxEntries} entries per key)")
      Console.out.println(Tabulator.format(entries))
    }
  }
}


