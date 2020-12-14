package hiresim.cell.factory

import hiresim.cell.factory.resources.MachineResourceProvider
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{Resource, SwitchResource}
import hiresim.cell.{Cell, CellEdge}
import hiresim.tenant.Graph.NodeID

import scala.collection.mutable.ArrayBuffer

object BinaryTreeCellFactory extends CellFactory {

  override def names(): Array[String] = {
    Array("b", "binary", "binary-tree")
  }

  override def create(height: Int,
                      resource_provider: MachineResourceProvider,
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

    // Create server and switch resource arrays. Initialize with empty resources and fill correct values later.
    val servers: Array[Array[NumericalResource]] = Array.fill[Array[NumericalResource]](numServers)(Resource.EmptyServerResource.numericalResources)
    val switches: Array[SwitchResource] = Array.fill[SwitchResource](numSwitches)(Resource.EmptySwitchResource)

    /** Links in the physical topology */
    var links: ArrayBuffer[CellEdge] = ArrayBuffer[CellEdge]()

    // Core switch is at the highest level and we'll treat it as spine switch
    switches(0) = resource_provider.getSpineSwitchResources(0)
    switchLevels(0) = height

    /** Creating the binary tree links */
    for (parent: NodeID <- 0 until numSwitches - numTorSwitches) {

      val leftChildIndex: NodeID = 2 * parent + 1
      val rightChildIndex: NodeID = 2 * parent + 2

      // Creating the connections to the children
      links += new CellEdge(
        srcWithOffset = parent,
        srcSwitch = true,
        dstWithOffset = leftChildIndex,
        dstSwitch = true
      )

      links += new CellEdge(
        srcWithOffset = parent,
        srcSwitch = true,
        dstWithOffset = rightChildIndex,
        dstSwitch = true
      )

      // Setup resources (fabric switch indices start at 1, so align to 0)
      switches(leftChildIndex) = resource_provider.getFabricSwitchResources(leftChildIndex - 1)
      switches(rightChildIndex) = resource_provider.getFabricSwitchResources(rightChildIndex - 1)

      // Calculating the heights of the children
      switchLevels(rightChildIndex) = height - math.floor(math.log10(rightChildIndex + 1) / math.log10(2)).toInt
      switchLevels(leftChildIndex) = height - math.floor(math.log10(leftChildIndex + 1) / math.log10(2)).toInt

    }

    /** Connecting ToR switches to servers */
    var torCounter: Int = 0
    for (torSwitch: NodeID <- numSwitches - numTorSwitches until numSwitches) { // ToR switches

      // Set the resources of that ToR switch
      switches(torSwitch) = resource_provider.getToRSwitchResources(torCounter)

      /** For each server in the rack */
      var serversInRack: Int = 0
      /** Compute its index */
      var server: NodeID = torSwitch + numTorSwitches + torCounter * (numTorSwitchPorts - 1)

      /** Until the destination represents a server and this ToR switch is connected to this server */
      while (server < numSwitches + numServers && serversInRack < numTorSwitchPorts) {

        // Set the resources for that server
        val server_offset = numTorSwitchPorts * torCounter + serversInRack
        servers(server_offset) = resource_provider.getServerResources(torCounter, serversInRack)

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

}
