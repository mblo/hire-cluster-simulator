package hiresim.cell.machine

object NumericalResource {

  type NumericalResource = Int

  @inline final def `NumericalResource.MaxValue`: NumericalResource = Int.MaxValue - 1
}