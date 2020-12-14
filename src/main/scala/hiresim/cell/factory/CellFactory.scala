package hiresim.cell.factory

import hiresim.cell.Cell
import hiresim.cell.factory.resources.{MachineResourceProvider, StaticMachineResourceProvider}
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.SwitchResource
import hiresim.simulation.configuration.SimulationConfiguration

import scala.collection.mutable.ArrayBuffer

object CellFactory {

  val BinaryTree: CellFactory = BinaryTreeCellFactory
  val FatTree: CellFactory = FatTree3NCellFactory

  private lazy val registry: ArrayBuffer[CellFactory] = ArrayBuffer()

  def getCell(k: Int,
              cellType: String,
              serverResources: Array[NumericalResource],
              switchResources: SwitchResource,
              switchMaxActiveInpTypes: Int): Cell = {
    getCell(
      k = k,
      cellType = cellType,
      new StaticMachineResourceProvider(
        resource_server = serverResources,
        resource_tor_switch = switchResources,
        resource_fabric_switch = switchResources,
        resource_spine_switch = switchResources
      ),
      switchMaxActiveInpTypes = switchMaxActiveInpTypes
    )
  }

  def getCell(k: Int,
              cellType: String = SimulationConfiguration.DEFAULT_CELL_TYPE,
              resource_provider: MachineResourceProvider,
              switchMaxActiveInpTypes: Int = 2): Cell = {

    val factory_o = getCellFactory(name = cellType)
    assert(factory_o.isDefined, s"No factory for desired cell type ${cellType} found!")

    // And create the cell
    factory_o.get.create(
      k = k,
      resource_provider,
      switchMaxActiveInpTypes = switchMaxActiveInpTypes
    )

  }


  private def registerCellFactory(factory: CellFactory): Unit = {
    // We only allow one CellFactory to use a name
    assert(!registry.exists(registered => registered.names().exists(naming => factory.names().exists(new_naming => naming eq new_naming))), "Factory to be added uses naming that is already occupied!")
    // Register if ok
    registry += factory
  }

  private def getCellFactory(name: String): Option[CellFactory] = {
    registry.find(factory => factory.names().contains(name))
  }

}

trait CellFactory {

  // Register this instance within the CellFactory
  CellFactory.registerCellFactory(factory = this)


  def names(): Array[String]

  def create(k: Int,
             resource_provider: MachineResourceProvider,
             switchMaxActiveInpTypes: Int = 2): Cell


}
