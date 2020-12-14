package hiresim.cell.factory.resources

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{Resource, SwitchResource}

class StaticMachineResourceProvider(resource_server: Array[NumericalResource],
                                    resource_tor_switch: SwitchResource,
                                    resource_fabric_switch: SwitchResource,
                                    resource_spine_switch: SwitchResource = Resource.EmptySwitchResource) extends MachineResourceProvider {

  override def getSpineSwitchResources(spineSwitchId: NumericalResource): SwitchResource = {
    resource_spine_switch.clone()
  }

  override def getFabricSwitchResources(fabricSwitchId: NumericalResource): SwitchResource = {
    resource_fabric_switch.clone()
  }

  override def getToRSwitchResources(torSwitchId: Int): SwitchResource = {
    resource_tor_switch.clone()
  }

  override def getServerResources(torSwitchId: Int, serverId: Int): Array[NumericalResource] = {
    resource_server.clone()
  }

}
