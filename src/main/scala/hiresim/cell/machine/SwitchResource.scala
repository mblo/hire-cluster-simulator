package hiresim.cell.machine

import hiresim.cell.machine.NumericalResource.NumericalResource

class SwitchResource(var numericalResources: Array[NumericalResource] = Array.fill[NumericalResource](n = 1)(elem = 0),
                     var properties: SwitchProps = SwitchProps.none)
  extends Resource {

  override def clone(): SwitchResource = new SwitchResource(numericalResources.clone(), properties)

  override def toString: String = s"[${numericalResources.mkString("[", ",", "]")}" +
    s"  cap${properties.toString}]"
}
