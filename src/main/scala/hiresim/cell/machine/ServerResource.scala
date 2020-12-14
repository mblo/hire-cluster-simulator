package hiresim.cell.machine

import hiresim.cell.machine.NumericalResource._

class ServerResource(var numericalResources: Array[NumericalResource] =
                     Array.fill[NumericalResource](n = 1)(elem = 0))
  extends Resource {

  override def clone(): AnyRef = numericalResources.clone()

  def update(i: Int, x: NumericalResource): Unit = numericalResources.update(i, x)

  def indices: Range = numericalResources.indices

  def apply(i: Int): NumericalResource = numericalResources.apply(i)

  def isEmpty: Boolean = numericalResources.isEmpty

  override def toString: String = numericalResources.mkString("[", ",", "]")

}
