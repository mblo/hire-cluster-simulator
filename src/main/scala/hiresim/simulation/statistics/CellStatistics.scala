package hiresim.simulation.statistics

import hiresim.cell.Cell
import hiresim.scheduler.Scheduler
import hiresim.shared.Logging
import hiresim.simulation.statistics.SimStats.{activatePollingStatistics, bigDecimalFormatter}
import hiresim.simulation.{SimTypes, Simulator}

object CellStatistics {

  implicit def bigDecimalToString(bd: BigDecimal): String = bigDecimalFormatter.format(bd)

  def activate(simulator: Simulator,
               scheduler: Scheduler,
               file: String,
               interval: SimTypes.simTime): Unit = {

    val cell: Cell = scheduler.cell

    val capacities_max_server: Array[BigDecimal] = cell.sanityTotalFreeServerCapacity().map(BigDecimal(_))
    val capacities_max_switches: Array[BigDecimal] = cell.sanityTotalFreeSwitchCapacity().map(BigDecimal(_))

    val stats = activatePollingStatistics(
      simulator,
      file,
      // How often the data is to be dumped
      interval,
      // The CSV header describing the fields
      Array(
        "Time",
        "UtilizationSwitchINP"
      ) ++
        capacities_max_switches.indices.map(i => s"UtilizationSwitchesDim$i") ++
        capacities_max_switches.indices.map(i => s"ActualUtilizationSwitchesDim$i") ++
        capacities_max_server.indices.map(i => s"UtilizationServersDim$i"),
      // The dumper function
      (stat, sim) => {

        // Construct the entry for this timestamp
        val entries: Seq[String] =
          Seq[String](
            sim.currentTime().toString,
            cell.getActiveInpNormalizedLoadOfAllSwitches
          ) ++
            cell.statistics_switch_claimed_resources_scheduler.indices.map(idx => (BigDecimal(cell.statistics_switch_claimed_resources_scheduler(idx)) / BigDecimal(cell.maxCapacitySwitches(idx))).toString()) ++
            cell.statistics_switch_claimed_resources_actual.indices.map(idx => (BigDecimal(cell.statistics_switch_claimed_resources_actual(idx)) / BigDecimal(cell.maxCapacitySwitches(idx))).toString()) ++
            cell.statistics_server_claimed_resources_scheduler.indices.map(idx => (BigDecimal(cell.statistics_server_claimed_resources_scheduler(idx)) / BigDecimal(cell.maxCapacityServers(idx))).toString())

        // Now push those entries to the writer
        stat.addEntry(entries: _*)

      }
    )

    // simple check that there is no seperator in the name of the scheduler
    assert(!scheduler.name.contains(stats.separator))

    // Signal success to console
    Logging.verbose("Statistics initialized for cell.")

  }

}
