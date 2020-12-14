package hiresim.cell.factory.resources

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.SwitchResource

/**
 * Provides a CellFactory with information on which machine has what
 * total number of available resources at hand
 */
trait MachineResourceProvider {

  /**
   * Returns the total available resources for the provided spine Switch
   *
   * @param spineSwitchId the id of the spine Switch
   * @return the maximum possible resources this switch can hosts
   */
  def getSpineSwitchResources(spineSwitchId: Int): SwitchResource

  /**
   * Returns the total available resources for the provided fabric Switch
   *
   * @param fabricSwitchId the id of the fabric Switch
   * @return the maximum possible resources this switch can hosts
   */
  def getFabricSwitchResources(fabricSwitchId: Int): SwitchResource

  /**
   * Returns the total available resources for the provided ToR Switch
   *
   * @param torSwitchId the id of the ToR Switch
   * @return the maximum possible resources this switch can host
   */
  def getToRSwitchResources(torSwitchId: Int): SwitchResource


  /**
   * Returns the total available resources for the provided server id
   * that is organized under the provided ToR Switch
   *
   * @param torSwitchId the id of the ToR Switch the server is organized in
   * @param serverId    the id of the server
   * @return the total availabe resources this instance has
   */
  def getServerResources(torSwitchId: Int,
                         serverId: Int): Array[NumericalResource]

}
