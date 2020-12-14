package hiresim.simulation.statistics

import hiresim.simulation.{RepeatingEvent, SimTypes, Simulator}

import java.text.DecimalFormat

class SimStats(val writer: SimStatsWriter,
               simulator: Simulator,
               simDoneHook: (SimStats, Simulator) => Unit = (_, _) => ()) {
  val separator: String = writer.separator

  simulator.registerSimDoneHook((s: Simulator) => {
    simDoneHook(this, s)
    writer.close()
  })

  def addEntry(cols: String*): Unit = writer.addEntry(cols: _*)

}

object SimStats {

  final val bigDecimalFormatter = new DecimalFormat("#0.####")

  def newAutoscalingCdfWriter(sim: Simulator,
                              filePath: String,
                              buckets: Int,
                              logScale: Boolean): StatsCdf = {
    val cdfWriter = new SimStatsAutoscalerCdfWriter(filePath = filePath, buckets = buckets, logScale = logScale)
    // we must create a SimStats instance, so that we register the SimDone Hook
    new SimStats(cdfWriter, sim)
    cdfWriter
  }

  def newNormalizedCdfWriter(sim: Simulator,
                             filePath: String,
                             buckets: Int): StatsCdf = {
    val cdfWriter = new SimStatsNormalizedCdfWriter(filePath = filePath, buckets = buckets)
    // we must create a SimStats instance, so that we register the SimDone Hook
    new SimStats(cdfWriter, sim)
    cdfWriter
  }

  def activatePollingStatistics(sim: Simulator,
                                filePath: String,
                                interval: SimTypes.simTime,
                                colHeaders: Array[String],
                                action: (SimStats, Simulator) => Unit): SimStats = {
    val stats = new SimStats(new SimStatsWriter(filePath), sim)

    var lastFlush = 0
    val flushEverySimDuration = 3600000

    sim.scheduleEvent(
      new RepeatingEvent(s => {
        action(stats, s)
        val now = s.currentTime()
        if (lastFlush + flushEverySimDuration >= now) {
          stats.writer.flush()
          lastFlush = now
        }
      }, interval))

    stats.addEntry(colHeaders.toIndexedSeq: _*)
    stats
  }


}

