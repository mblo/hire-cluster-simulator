package hiresim.cell.factory

import hiresim.cell.factory.resources.MachineResourceProvider
import hiresim.cell.factory.util.CellFactoryIntegrityValidator
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{Resource, SwitchResource}
import hiresim.cell.{Cell, CellEdge}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.Graph.NodeID

import scala.collection.mutable.ArrayBuffer

object FatTree3NCellFactory extends CellFactory {

  override def names(): Array[String] = {
    Array("ft", "fat-tree")
  }

  override def create(k: Int,
                      resource_provider: MachineResourceProvider,
                      switchMaxActiveInpTypes: Int = 2): Cell = {

    // Make sure the size parameter is valid
    assert(k > 1 && k % 2 == 0, s"Invalid number of cell pods: $k. Must be a positive and even.")

    /* `NodeID`s
     *
     * +---------------+----------------------+--------------+---------+
     * | Core switches | Aggregation switches | ToR switches | Servers |
     * +---------------+----------------------+--------------+---------+
     */

    /** Number of switches */
    val numCoreSwitches: Int = (k * k) / 4
    val numAggregationSwitches: Int = (k / 2) * k
    val numTorSwitches: Int = (k / 2) * k

    val numSwitches: Int = numCoreSwitches + numAggregationSwitches + numTorSwitches
    val num_switches_calculated = 5 * (k * k / 4)

    assert(numSwitches == num_switches_calculated, s"Accumulated switch count and calculated switch count do not align! (accumulated: ${numSwitches} calculated: ${num_switches_calculated})")

    /** Indexes */
    val aggregationSwichesIndexes: ArrayBuffer[NodeID] = ArrayBuffer()

    /** Number of servers */
    val numSwitchPorts: Int = k / 2
    val numServers: Int = numTorSwitches * numSwitchPorts
    val torSwitchesPerPod: Int = numTorSwitches / k
    val serversPerPod: Int = (numTorSwitches / k) * numSwitchPorts

    println("servers: " + numServers)
    println("switches: " + numSwitches)

    // Resource arrays for servers and switches
    val servers: Array[Array[NumericalResource]] = Array.fill[Array[NumericalResource]](numServers)(Resource.EmptyServerResource.numericalResources)
    val switches: Array[SwitchResource] = Array.fill[SwitchResource](numSwitches)(Resource.EmptySwitchResource)
    // Storing information about individual levels of switches
    val switchLevels: Array[Int] = Array.fill(n = numSwitches)(elem = -3)

    // Setup switch level and resource needs
    {
      // Core switches are at the highest level. We'll treat them as spine switches.
      for (core_switch_id: NodeID <- 0 until numCoreSwitches) {
        switchLevels(core_switch_id) = 2
        switches(core_switch_id) = resource_provider.getSpineSwitchResources(core_switch_id)
      }

      // Aggregation levels are in the intermediate level. We'll treat them as fabric switches
      for (aggregation_switch_offset <- 0 until numAggregationSwitches) {
        val aggregation_switch_id = numCoreSwitches + aggregation_switch_offset
        switchLevels(aggregation_switch_id) = 1
        switches(aggregation_switch_id) = resource_provider.getSpineSwitchResources(aggregation_switch_offset)
      }

      // ToR levels are in the lowest level.
      for (tor_switch_offset <- 0 until numTorSwitches) {
        val tor_switch_id = numCoreSwitches + numAggregationSwitches + tor_switch_offset
        switchLevels(tor_switch_id) = 0
        switches(tor_switch_id) = resource_provider.getToRSwitchResources(tor_switch_offset)
      }

      // If desired we check if all switches have been initialize
      if (SimulationConfiguration.SANITY_CHECKS_CELL)
        CellFactoryIntegrityValidator.checkAllSwitchResourcesSet(switches)

    }

    /** Links in the physical topology */
    var links: ArrayBuffer[CellEdge] = ArrayBuffer[CellEdge]()

    /** For each pod */
    var absoluteTorCounter: Int = 0
    for (pod: Int <- 0 until k) {

      /** Core-to-aggregation links */
      for (aggr: NodeID <- 0 until numAggregationSwitches / k) {

        aggregationSwichesIndexes += aggr

        for (coreSwitch: NodeID <- (k / 2) * aggr until (k / 2) * (aggr + 1)) {

          // The id of the switch when only respecting aggregation switches
          val aggregation_switch_offset = aggr + (numAggregationSwitches / k * pod)
          // The id of the switch when also respecting the core switches
          val aggregation_switch_id = numCoreSwitches + aggregation_switch_offset

          links += new CellEdge(
            srcWithOffset = coreSwitch,
            srcSwitch = true,
            dstWithOffset = aggregation_switch_id,
            dstSwitch = true
          )

        }
      }

      var torCounterInPod: Int = 0
      for (torSwitchRelativeIndex: NodeID <- 0 until numTorSwitches / k) {

        val tor_switch_offset = torSwitchRelativeIndex + (numTorSwitches / k * pod)
        val tor_switch_id = tor_switch_offset + numCoreSwitches + numAggregationSwitches

        /** Aggregation-to-ToR links */
        for (x: NodeID <- (numTorSwitches / k) * pod until ((numTorSwitches / k) * (pod + 1))) {

          val aggregationSwitch: NodeID = aggregationSwichesIndexes(x) + numCoreSwitches + (numAggregationSwitches / k * pod)

          links += new CellEdge(
            srcWithOffset = aggregationSwitch,
            srcSwitch = true,
            dstWithOffset = tor_switch_id,
            dstSwitch = true
          )
        }

        /** ToR-to-server links */
        var serversInRack: Int = 0 // for each server in the rack
        var server: NodeID = tor_switch_id + numTorSwitches + absoluteTorCounter * (numSwitchPorts - 1)

        while (server < numCoreSwitches + numAggregationSwitches + numTorSwitches + numServers // until the destination represents a server
          && serversInRack < numSwitchPorts) { // until the server actually belongs to the rack

          links += new CellEdge(
            srcWithOffset = tor_switch_id,
            srcSwitch = true,
            dstWithOffset = server,
            dstSwitch = false
          )

          val server_offset: Int = server - numSwitches
          servers(server_offset) = resource_provider.getServerResources(tor_switch_offset, serversInRack)

          server += 1
          serversInRack += 1
        }

        torCounterInPod += 1
        absoluteTorCounter += 1
      }
    }

    // And also check if the server resources are initialized correctly
    if (SimulationConfiguration.SANITY_CHECKS_CELL)
      CellFactoryIntegrityValidator.checkAllServerResourcesSet(servers)

    new Cell(
      servers,
      switches,
      links.toArray,
      numTorSwitches,
      numRootSwitches = numCoreSwitches,
      serversPerRack = numSwitchPorts,
      switchLevels,
      switchMaxActiveInpTypes = switchMaxActiveInpTypes,
      name = "FatTree3N"
    )

  }

}
