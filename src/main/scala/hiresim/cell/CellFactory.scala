package hiresim.cell

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.SwitchResource
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.Graph.NodeID

import scala.collection.mutable.ArrayBuffer

object CellFactory {

  def newCell(k: Int,
              serverResources: Array[NumericalResource],
              switchResources: => SwitchResource): Cell = newCell(
    k = k,
    cellType = SimulationConfiguration.DEFAULT_CELL_TYPE,
    serverResources = serverResources,
    switchResources = switchResources)

  def newCell(k: Int,
              cellType: String,
              serverResources: Array[NumericalResource],
              switchResources: => SwitchResource,
              switchMaxActiveInpTypes: Int = 2
             ): Cell = {


    cellType match {
      case "binary-tree" | "binary" | "b" =>
        buildBinaryTree(
          height = k,
          serverResources,
          switchResources,
          switchMaxActiveInpTypes
        )
      case "fat-tree" | "ft" =>
        buildFatTree(
          k = k,
          numCoreSwitches = (k * k) / 4,
          numAggregationSwitches = (k * k) / 2,
          serverResources,
          switchResources,
          switchMaxActiveInpTypes
        )
    }
  }

  private def buildBinaryTree(height: Int,
                              serverResources: Array[NumericalResource],
                              switchResources: => SwitchResource,
                              switchMaxActiveInpTypes: Int): Cell = {

    /** Number of switches */
    val numSwitches: Int = math.pow(2, height + 1).toInt - 1
    val numTorSwitches: Int = (numSwitches + 1) / 2 // = math.pow(2, height)
    val numRootSwitches: Int = 1

    /** Switch levels */
    val switchLevels: Array[Int] = Array.fill(n = numSwitches)(elem = -3)

    /** Number of servers */
    val numTorSwitchPorts: Int = 2 // each ToR switch connects to numTorSwitchPorts servers
    val numServers: Int = numTorSwitches * numTorSwitchPorts

    /** Servers and switches have uniform resource requirements */
    val servers: Array[Array[NumericalResource]] =
      Array.fill[Array[NumericalResource]](numServers)(serverResources.clone())
    val switches: Array[SwitchResource] =
      Array.fill[SwitchResource](numSwitches)(switchResources.clone())

    /** Links in the physical topology */
    var links: ArrayBuffer[CellEdge] = ArrayBuffer[CellEdge]()

    /** Core switch is at the highest level */
    switchLevels(0) = height

    /** Creating the binary tree links */
    for (parent: NodeID <- 0 until numSwitches - numTorSwitches) {

      /** Creating the connection with the left child */
      val leftChildIndex: NodeID = 2 * parent + 1
      links += new CellEdge(
        srcWithOffset = parent,
        srcSwitch = true,
        dstWithOffset = leftChildIndex,
        dstSwitch = true
      )
      switchLevels(leftChildIndex) = height - math.floor(math.log10(leftChildIndex + 1) / math.log10(2)).toInt

      /** Creating the connection with the right child */
      val rightChildIndex: NodeID = 2 * parent + 2
      links += new CellEdge(
        srcWithOffset = parent,
        srcSwitch = true,
        dstWithOffset = rightChildIndex,
        dstSwitch = true
      )
      switchLevels(rightChildIndex) = height - math.floor(math.log10(rightChildIndex + 1) / math.log10(2)).toInt
    }

    /** Connecting ToR switches to servers */
    var torCounter: Int = 0
    for (torSwitch: NodeID <- numSwitches - numTorSwitches until numSwitches) { // ToR switches

      /** For each server in the rack */
      var serversInRack: Int = 0

      /** Compute its index */
      var server: NodeID = torSwitch + numTorSwitches + torCounter * (numTorSwitchPorts - 1)

      /** Until the destination represents a server and this ToR switch is connected to this server */
      while (server < numSwitches + numServers && serversInRack < numTorSwitchPorts) {

        links += new CellEdge(
          srcWithOffset = torSwitch,
          srcSwitch = true,
          dstWithOffset = server,
          dstSwitch = false
        )

        server += 1
        serversInRack += 1
      }

      torCounter += 1
    }

    new Cell(
      servers,
      switches,
      links.toArray,
      numTorSwitches,
      numRootSwitches,
      serversPerRack = numTorSwitchPorts,
      switchLevels,
      switchMaxActiveInpTypes = switchMaxActiveInpTypes,
      name = "BinaryTree"
    )
  }

  /** Fat-tree with one aggregation switch per pod (for a total of k aggregation switches)
   * and one overall core switch.
   *
   * @param k number of pods. Must be of the form k = pow(2, x).
   * @param numCoreSwitches
   * @param numAggregationSwitches
   * @param serverResources
   * @param switchResources
   * @return
   */
  private def buildFatTree(k: Int,
                           numCoreSwitches: Int,
                           numAggregationSwitches: Int,
                           serverResources: Array[NumericalResource],
                           switchResources: => SwitchResource,
                           switchMaxActiveInpTypes: Int): Cell = {

    /** Arguments checking */
    assert(k > 1 && k % 2 == 0,
      s"Invalid number of cell pods: $k. Must be a positive and even.")

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

    /** Indexes */
    val aggregationSwichesIndexes: ArrayBuffer[NodeID] = ArrayBuffer()
    val torSwitchesIndexes: ArrayBuffer[NodeID] = ArrayBuffer()

    /** Switch levels */
    val switchLevels: Array[Int] = Array.fill(n = numSwitches)(elem = -3)

    /** Core switches are at the highest level */
    for (coreSwitch: NodeID <- 0 until numCoreSwitches) {
      switchLevels(coreSwitch) = 2
    }

    /** Number of servers */
    val numSwitchPorts: Int = k / 2
    val numServers: Int = numTorSwitches * numSwitchPorts
    val torSwitchesPerPod: Int = numTorSwitches / k
    val serversPerPod: Int = (numTorSwitches / k) * numSwitchPorts

    println("servers: " + numServers)
    println("switches: " + numSwitches)

    /** Servers and switches have uniform resource requirements */
    val servers: Array[Array[NumericalResource]] =
      Array.fill[Array[NumericalResource]](numServers)(serverResources.clone())
    val switches: Array[SwitchResource] =
      Array.fill[SwitchResource](numSwitches)(switchResources.clone())

    /** Links in the physical topology */
    var links: ArrayBuffer[CellEdge] = ArrayBuffer[CellEdge]()

    /** For each pod */
    var absoluteTorCounter: Int = 0
    for (pod: Int <- 0 until k) {

      /** Core-to-aggregation links */
      for (aggr: NodeID <- 0 until numAggregationSwitches / k) {

        aggregationSwichesIndexes += aggr

        for (coreSwitch: NodeID <- (k / 2) * aggr until (k / 2) * (aggr + 1)) {

          val aggregationSwitch: NodeID = aggr + numCoreSwitches + (numAggregationSwitches / k * pod)

          links += new CellEdge(
            srcWithOffset = coreSwitch,
            srcSwitch = true,
            dstWithOffset = aggregationSwitch,
            dstSwitch = true
          )

          /** Aggregation switches are at the intermediate level */
          switchLevels(aggregationSwitch) = 1
        }
      }

      var torCounterInPod: Int = 0
      for (torSwitchRelativeIndex: NodeID <- 0 until numTorSwitches / k) {

        torSwitchesIndexes += torSwitchRelativeIndex

        val torSwitchIndex: NodeID = torSwitchRelativeIndex + numCoreSwitches + numAggregationSwitches + (numTorSwitches / k * pod)

        /** Aggregation-to-ToR links */
        for (x: NodeID <- (numTorSwitches / k) * pod until ((numTorSwitches / k) * (pod + 1))) {

          val aggregationSwitch: NodeID = aggregationSwichesIndexes(x) + numCoreSwitches + (numAggregationSwitches / k * pod)

          links += new CellEdge(
            srcWithOffset = aggregationSwitch,
            srcSwitch = true,
            dstWithOffset = torSwitchIndex,
            dstSwitch = true
          )

          /** ToR switches are at the lowest level */
          switchLevels(torSwitchIndex) = 0
        }

        /** ToR-to-server links */
        var serversInRack: Int = 0 // for each server in the rack
        var server: NodeID = torSwitchIndex + numTorSwitches + absoluteTorCounter * (numSwitchPorts - 1)
        while (server < numCoreSwitches + numAggregationSwitches + numTorSwitches + numServers // until the destination represents a server
          && serversInRack < numSwitchPorts) { // until the server actually belongs to the rack

          links += new CellEdge(
            srcWithOffset = torSwitchIndex,
            srcSwitch = true,
            dstWithOffset = server,
            dstSwitch = false
          )

          server += 1
          serversInRack += 1
        }

        torCounterInPod += 1
        absoluteTorCounter += 1
      }
    }

    new Cell(
      servers,
      switches,
      links.toArray,
      numTorSwitches,
      numRootSwitches = numCoreSwitches,
      serversPerRack = numSwitchPorts,
      switchLevels,
      switchMaxActiveInpTypes = switchMaxActiveInpTypes,
      name = "FatTree"
    )
  }
}
