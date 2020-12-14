package hiresim.cell.factory.util

import hiresim.cell.CellEdge
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{Resource, SwitchResource}
import hiresim.shared.Logging

object CellFactoryIntegrityValidator {

  def checkAllServerResourcesSet(resources: Array[Array[NumericalResource]],
                                 empty: Array[NumericalResource] = Resource.EmptyServerResource.numericalResources): Unit = {

    // Now check every server individually
    for (server <- resources.indices) {
      assert(!(resources(server) sameElements empty), s"Resources for server ${server} have not been initialized!")
    }

    Logging.validation("Server resource vector correctly setup.")

  }

  def checkAllSwitchResourcesSet(resources: Array[SwitchResource],
                                 empty: SwitchResource = Resource.EmptySwitchResource): Unit = {

    // Now check every server individually
    for (switch <- resources.indices) {
      assert(!(resources(switch) == empty), s"Resources for switch ${switch} have not been initialized!")
    }

    Logging.validation("Switch resource vector correctly setup.")

  }

  def checkLinks(links: Array[CellEdge],
                 numSwitches: Int): Unit = {

    for (link <- links) {
      assert(
        if (link.dstSwitch)
          link.dstWithOffset < numSwitches
        else
          link.dstWithOffset >= numSwitches
      )
    }

    Logging.validation("Cell edge links correctly setup.")

  }

}
