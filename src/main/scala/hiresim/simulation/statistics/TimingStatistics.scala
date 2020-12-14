package hiresim.simulation.statistics

import hiresim.shared.{Logging, TimeIt}
import hiresim.simulation.Simulator
import hiresim.simulation.statistics.SimStats.bigDecimalFormatter

import scala.collection.mutable

object TimingStatistics {

  implicit def bigDecimalToString(bd: BigDecimal): String = bigDecimalFormatter.format(bd)

  private var initialized = false
  private var writer: SimStatsWriter = _
  private var simulator: Simulator = _

  private val entry_names: Seq[String] = Seq(
    "SimulationTime",
    "CleanupTime",
    "SolverTime",
    "TotalTime",
    "InterpretTime"
  )
  private val no_value: Long = -1L

  private val current: mutable.Map[String, Long] = mutable.HashMap[String, Long]()

  def activate(simulator: Simulator,
               path: String,
               buffer: Int): Unit = {

    // The writer which will handle the I/O job
    writer = new SimStatsWriter(
      filePath = path,
      flushBuffer = buffer
    )

    // Add a sim done hook to close the file handle when simulation has finished
    simulator.registerSimDoneHook(simulator => {
      writer.close()
    })

    // Dump the header to the file
    writer.addEntry(entry_names: _*)

    // Remember needed organizing classes
    this.simulator = simulator
    // Finally mark this Statistics collector as intialized and thus usable
    this.initialized = true

    // Signal success to console
    Logging.verbose("Statistics initialized for timings.")
  }

  def onPostMeasurement(key: String, timeit: TimeIt): Unit = {
    onPostMeasurement(key, timeit.diff)
  }

  def onPostMeasurement(key: String, start: Long, end: Long): Unit = {
    onPostMeasurement(key, end - start)
  }

  def onPostMeasurement(key: String, duration: Long): Unit = {
    // Only if we have a column for that key reserved and are initialized
    if (initialized && entry_names.contains(key)) {
      current += (key -> duration)
    }
  }

  def onFinishRound(): Unit = {
    if (initialized) {

      // Save the current sim time
      current += ("SimulationTime" -> simulator.currentTime())
      // Construct the entry to dump
      var entry: Seq[String] = Seq()

      // Check every key
      for (key <- entry_names)
      // And append to output entry
        entry = (entry :+ current.getOrElse(key, no_value).toString)

      // Clear of timing results
      current.clear()
      // And dump to writer
      writer.addEntry(entry: _*)

    }
  }

}