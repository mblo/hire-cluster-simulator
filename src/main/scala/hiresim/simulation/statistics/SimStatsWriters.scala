package hiresim.simulation.statistics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable

class SimStatsWriter(val filePath: String,
                     val separator: String = ",",
                     flushBuffer: Int = 1000) {

  private val entries: mutable.ListBuffer[String] = new mutable.ListBuffer()
  private val path = Paths.get(filePath)
  private val file = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  private var closed = false

  def addEntry(cols: String*): Unit = {
    assert(!closed)
    entries.append(cols.mkString(separator))
    if (entries.size > flushBuffer) {
      flush()
    }
  }

  def close(): Unit = {
    if (!closed) {
      flush()
      file.close()
      closed = true
    }
  }

  def flush(): Unit = {
    if (!closed) {
      for (s <- entries) {
        file.write(s)
        file.newLine()
      }
      entries.clear()
      file.flush()
    }
  }

}

trait StatsCdf {

  def addNormalizedDataPoint(value: Double,
                             key: String,
                             group: String = "_"): Unit = {
    throw new NotImplementedError()
  }

  def addRawDataPoint(value: Double,
                      key: String,
                      group: String = "_"): Unit = {
    throw new NotImplementedError()
  }

}

class SimStatsAutoscalerCdfWriter(filePath: String,
                                  separator: String = ",",
                                  flushBuffer: Int = 1000,
                                  val buckets: Int = 100,
                                  val logScale: Boolean = false)
  extends SimStatsWriter(filePath = filePath,
    separator = separator,
    flushBuffer = flushBuffer) with StatsCdf {

  private val rawValueHolder: mutable.HashMap[String, mutable.HashMap[String, mutable.ArrayBuffer[Double]]] = mutable.HashMap()
  private var finalized = false

  override def addRawDataPoint(value: Double,
                               key: String,
                               group: String = "_"): Unit = {
    assert(!finalized, "CDF Writer already finalized, cannot add more data")

    assert(!logScale || value >= 0.0, "value must be positive or cdf must be non-log scale")

    if (!rawValueHolder.contains(key)) {
      rawValueHolder(key) = mutable.HashMap()
    }
    if (!rawValueHolder(key).contains(group)) {
      rawValueHolder(key)(group) = mutable.ArrayBuffer()
    }

    rawValueHolder(key)(group) += value
  }

  override def addEntry(cols: String*): Unit = {
    assert(finalized, "this method can be used only for finalizing the CDF writer")
    super.addEntry(cols: _*)
  }

  private def writeCdfAndFinalize(): Unit = {
    assert(!finalized)
    finalized = true

    for ((key: String, groupedCDFs: mutable.HashMap[String, mutable.ArrayBuffer[Double]]) <- rawValueHolder) {
      for ((group: String, data: mutable.ArrayBuffer[Double]) <- groupedCDFs) {
        var (foundMin, foundMax) = (Double.MaxValue, Double.MinValue)
        // 1st iterate over data, find min, max
        for (v: Double <- data) {
          if (v < foundMin) {
            foundMin = v
          }
          if (v > foundMax) {
            foundMax = v
          }
        }
        val bucket_slots: Array[Double] = new Array[Double](buckets)
        val range = foundMax - foundMin
        // if there is only a single data point, omit all the data of this key, group
        if (range > 0) {
          // calculate bucket slots
          if (logScale) {
            val upper_log = Math.log10(foundMax)
            val lower_log = Math.log10(foundMin).floor
            val log_step = (upper_log - lower_log) / (buckets - 1)

            for (i <- bucket_slots.indices) {
              bucket_slots(i) = Math.pow(10, lower_log + log_step * i)
            }
          } else {
            val step: Double = range / (buckets - 1)
            for (i <- bucket_slots.indices) {
              bucket_slots(i) = foundMin + step * i
            }
          }

          val bucket_counter: Array[Long] = new Array(buckets)

          // 2nd iterate over data, calculate cdf
          for (v: Double <- data) {
            // find the larget bucket slot which is smaller than value
            var slot = 0
            while (slot < buckets && bucket_slots(slot) <= v) {
              slot += 1
            }
            // slot found the bucket next (larger) than the target slot
            if (slot > 0) {
              slot -= 1
            }
            bucket_counter(slot) += 1
          }

          // write cdf
          for (i <- bucket_counter.indices) {
            addEntry(key, group, bucket_slots(i).toString, bucket_counter(i).toString)
          }

          // clear data
          data.clear()
        }
      }
    }
  }

  override def close(): Unit = {
    // finalize this guy if not done already
    if (!finalized) {
      writeCdfAndFinalize()
      super.close()
    }
  }

  override def flush(): Unit = {
    // ignore as long as finalized is not set
    if (finalized) {
      super.flush()
    }
  }

}


class SimStatsNormalizedCdfWriter(filePath: String,
                                  separator: String = ",",
                                  flushBuffer: Int = 1000,
                                  val buckets: Int = 100)
  extends SimStatsWriter(filePath = filePath,
    separator = separator,
    flushBuffer = flushBuffer) with StatsCdf {

  private val bucketHolder: mutable.HashMap[String, mutable.HashMap[String, Array[Long]]] = mutable.HashMap()
  private var finalized = false

  override def addNormalizedDataPoint(value: Double,
                                      key: String,
                                      group: String = "_"): Unit = {
    assert(value >= 0.0 && value <= 1.0, s"Value needs to be normalized; value:$value")
    assert(!finalized, "CDF Writer already finalized, cannot add more data")

    if (!bucketHolder.contains(key)) {
      bucketHolder(key) = mutable.HashMap()
    }
    if (!bucketHolder(key).contains(group)) {
      bucketHolder(key)(group) = new Array[Long](buckets)
    }

    val bucket: Int = if (value == 1.0) buckets - 1 else (value * buckets).toInt
    bucketHolder(key)(group)(bucket) += 1
  }

  override def addEntry(cols: String*): Unit = {
    assert(finalized, "this method can be used only for finalizing the CDF writer")
    super.addEntry(cols: _*)
  }

  private def writeCdfAndFinalize(): Unit = {
    assert(!finalized)
    finalized = true

    for ((key: String, groupedCDFs: mutable.HashMap[String, Array[Long]]) <- bucketHolder) {
      for ((group: String, data: Array[Long]) <- groupedCDFs) {
        for (i: Int <- data.indices) {
          addEntry(key, group, i.toString, data(i).toString)
        }
      }
    }
  }

  override def close(): Unit = {
    // finalize this guy if not done already
    if (!finalized) {
      writeCdfAndFinalize()
      super.close()
    }
  }

  override def flush(): Unit = {
    // ignore as long as finalized is not set
    if (finalized) {
      super.flush()
    }
  }

}



