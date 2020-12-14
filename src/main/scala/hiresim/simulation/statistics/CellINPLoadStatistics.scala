package hiresim.simulation.statistics

import hiresim.cell.Cell
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.SwitchProps
import hiresim.shared.{ArrayUtils, Logging}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.SimStats.{activatePollingStatistics, bigDecimalFormatter}
import hiresim.simulation.{SimTypes, Simulator}

import scala.collection.{Iterable, mutable}

object CellINPLoadStatistics {

  implicit def bigDecimalToString(bd: BigDecimal): String = bigDecimalFormatter.format(bd)

  private var total_capacity_per_dimension: Array[BigInt] = Array.empty

  // holds the total resources available for each property
  private val resources_total_per_property: mutable.Map[Int, Array[BigInt]] = mutable.HashMap()
  // holds the total resources for each property, that are blocked by other properties on the very same switches
  private val resources_blocked_by_others_per_property: mutable.Map[Int, Array[BigInt]] = mutable.HashMap()

  // holds the used resources per property
  private val resources_used_per_property: mutable.Map[Int, Array[BigInt]] = mutable.HashMap()
  // holds the booked resources per property
  private val resources_reserved_per_property: mutable.Map[Int, Array[BigInt]] = mutable.HashMap()
  var activated = false

  def resetStaticField(): Unit = {
    activated = false
    total_capacity_per_dimension = Array.empty
    resources_total_per_property.clear()
    resources_blocked_by_others_per_property.clear()
    resources_used_per_property.clear()
    resources_reserved_per_property.clear()
  }

  def onResourcesChanged(resourcesReal: Array[NumericalResource], // Resources that are being acquired
                         resourcesReserve: Array[NumericalResource], // Resources that are being acquired
                         used_props: SwitchProps, // The props the allocation is targeting
                         col_located_props: SwitchProps, // The props the switch has active in total
                         freeing: Boolean
                        ): Unit = {
    if (activated) {
      val multiplier = if (freeing) -1 else 1

      // for all the other properties that are active on a switch, keep track of the resources that are used
      val other_blocked_props = col_located_props.remove(used_props)
      if (SimulationConfiguration.LOGGING_VERBOSE_CELL)
        if (other_blocked_props.size > 0) {
          println(s"onResourcesChanged -- used:$used_props, co-located:$col_located_props, blocked:${other_blocked_props}")
        }

      other_blocked_props.capabilities.foreach(property => {
        val otherRes = resources_blocked_by_others_per_property(property)
        resourcesReserve.indices.foreach(dimension => {
          otherRes(dimension) += BigInt(resourcesReserve(dimension) * multiplier)
          assert(otherRes(dimension).signum >= 0, s"somebody releases resources, which were never booked! ${otherRes(dimension)}, ${resourcesReserve}, ${freeing}")
          assert(otherRes(dimension) <= resources_total_per_property(property)(dimension))
        })
      })

      // Now accumulate the use on every used property
      used_props.capabilities.foreach(property => {
        // Again there must be a entry already for this prop
        val resUsed: Array[BigInt] = resources_used_per_property(property)
        val resReserved: Array[BigInt] = resources_reserved_per_property(property)
        // And accumulate demand along every dimension
        resUsed.indices.foreach(dimension => {
          resUsed(dimension) += BigInt(resourcesReal(dimension) * multiplier)
          resReserved(dimension) += BigInt(resourcesReserve(dimension) * multiplier)
          assert(resReserved(dimension).signum >= 0)
          assert(resUsed(dimension).signum >= 0)
          val somewhere_used = resReserved(dimension) + resources_blocked_by_others_per_property(property)(dimension)
          assert(somewhere_used <= resources_total_per_property(property)(dimension), s"something goes wrong.." +
            s" $somewhere_used >=  ${resources_total_per_property(property)(dimension)}; " +
            s"reserved:${resReserved(dimension)} " +
            s"blocked:${resources_blocked_by_others_per_property(property)(dimension)}")
        })
        if (SimulationConfiguration.LOGGING_VERBOSE_CELL)
          println(s" res:${ArrayUtils.array2string(resources_reserved_per_property(property))} " +
            s"total:${ArrayUtils.array2string(resources_total_per_property(property))} " +
            s"blocked:${ArrayUtils.array2string(resources_blocked_by_others_per_property(property))}")
      })

    }
  }

  def onTotalResourcesChanged(total_resources: Array[NumericalResource], // Total Resources of the target machine)
                              blocked_resources_by_others: Array[NumericalResource],
                              changed_props: SwitchProps, // The properties that are changed introduced
                              freeing: Boolean
                             ): Unit = {
    if (activated) {
      val multiplier = if (freeing) -1 else 1

      if (SimulationConfiguration.LOGGING_VERBOSE_CELL)
        println(s"onTotalResourcesChanged -- changed:$changed_props, freeing:$freeing, " +
          s"blocked:${ArrayUtils.array2string(blocked_resources_by_others)}")


      // If we changed new props, we must change the max for this prop
      changed_props.capabilities.foreach(property => {
        // If we are changed there must be a entry for this prop
        val total: Array[BigInt] = resources_total_per_property(property)
        val blocked: Array[BigInt] = resources_blocked_by_others_per_property(property)
        // And add every dimension to it
        total_resources.indices.foreach(dimension => {
          total(dimension) += BigInt(total_resources(dimension) * multiplier)
          blocked(dimension) += BigInt(blocked_resources_by_others(dimension) * multiplier)
          assert(total(dimension).signum >= 0)
          assert(blocked(dimension).signum >= 0)
          assert(blocked(dimension) <= total(dimension))
        })
      })
    }
  }

  def getSwitchLoadStatOfProperty(property: Int): Array[Double] = {
    val used = resources_used_per_property(property)
    val blocked = resources_blocked_by_others_per_property(property)
    val total = resources_total_per_property(property)

    val load: Array[Double] = Array.ofDim(used.length)
    for (dim <- load.indices) {
      if (total(dim).toInt == 0)
        load(dim) = 0.0
      else
        load(dim) = (BigDecimal(used(dim) + blocked(dim)) / BigDecimal(total(dim))).toDouble
    }
    load
  }


  def getSwitchReservedStatOfProperty(property: Int): Array[Double] = {
    val used = resources_used_per_property(property)
    val reserved = resources_reserved_per_property(property)

    val blocked = resources_blocked_by_others_per_property(property)
    val total = resources_total_per_property(property)

    val load: Array[Double] = Array.ofDim(used.length)
    for (dim <- load.indices) {
      assert(used(dim) <= reserved(dim))
      if (total(dim).toInt == 0)
        load(dim) = 0.0
      else
        load(dim) = (BigDecimal(reserved(dim) + blocked(dim)) / BigDecimal(total(dim))).toDouble
    }
    load
  }

  def getSwitchLoadStats(scaling: Int = 1, forProperties: Iterable[Int] = resources_used_per_property.keys): mutable.ArrayBuffer[String] = {
    if (activated) {
      val entries: mutable.ArrayBuffer[String] = mutable.ArrayBuffer()
      forProperties.foreach((property: Int) => {
        resources_used_per_property(property).indices.foreach(dimension => {
          val total: BigInt = resources_total_per_property(property)(dimension)

          if (total == 0) {
            entries += "0" // % used
            entries += "0" // % reserved
            entries += "0" // % of total capacity
            entries += "0" // % claimed by other properties
          } else {
            entries += (BigDecimal(scaling * resources_used_per_property(property)(dimension)) / BigDecimal(total))
            entries += (BigDecimal(scaling * resources_reserved_per_property(property)(dimension)) / BigDecimal(total))
            entries += (BigDecimal(scaling * BigDecimal(total)) / BigDecimal(total_capacity_per_dimension(dimension)))
            entries += (BigDecimal(scaling * resources_blocked_by_others_per_property(property)(dimension)) / BigDecimal(total))
          }

        })
      })

      entries
    } else {
      mutable.ArrayBuffer.empty
    }
  }

  def activate(simulator: Simulator,
               cell: Cell,
               inp_types: Array[SwitchProps],
               file: Option[String],
               interval: SimTypes.simTime): Unit = {

    assert(!activated, "somebody activated INP statistics already!")

    assert(interval > 0 == file.isDefined, s"If you deactivate polling, you should not expect to write into a file!")

    val switch_resource_dimensions = cell.resourceDimSwitches

    val header: mutable.ArrayBuffer[String] = mutable.ArrayBuffer("Time")
    // Append utilization for each inp type + dimension
    inp_types.foreach(properties => {

      // Properties must be atomic!
      assert(properties.size == 1, s"Property not atomic (Property: ${properties})!")

      properties.capabilities.foreach(property => {
        (0 until switch_resource_dimensions).foreach(dimension => {
          header += f"UtilizationSwitch_Prop${property}_Dim${dimension}"
          header += f"ReservedSwitch_Prop${property}_Dim${dimension}"
          header += f"TotalSwitch_Prop${property}_Dim${dimension}"
          header += f"BlockedByOthersSwitch_Prop${property}_Dim${dimension}"
        })
      })

    })

    total_capacity_per_dimension = cell.maxCapacitySwitches.clone()

    // Initialize storing maps
    inp_types.foreach(properties => {
      properties.capabilities.foreach((property: Int) => {
        resources_used_per_property(property) = Array.fill(switch_resource_dimensions)(BigInt(0))
        resources_reserved_per_property(property) = Array.fill(switch_resource_dimensions)(BigInt(0))
        resources_total_per_property(property) = Array.fill(switch_resource_dimensions)(BigInt(0))
        resources_blocked_by_others_per_property(property) = Array.fill(switch_resource_dimensions)(BigInt(0))
      })
    })

    if (interval > 0)
      activatePollingStatistics(
        simulator,
        file.get,
        // How often the data is to be dumped
        interval,
        // The CSV header describing the fields
        header.toArray,
        // The dumper function
        (stat, sim) => {

          // Construct the entry for this timestamp
          val entries: mutable.ArrayBuffer[String] = getSwitchLoadStats()
          val arrayEntries: Array[String] = Array.ofDim(entries.size + 1)
          entries.copyToArray(arrayEntries, 1)
          arrayEntries(0) = sim.currentTime().toString

          // Now push those entries to the writer
          stat.addEntry(arrayEntries.toIndexedSeq: _*)
        }
      )

    activated = true
    // Signal success to console
    Logging.verbose("Statistics initialized for cell inp.")
  }

}
