package hiresim.cell.machine

import hiresim.scheduler.flow.coco.CoCoTaskType

object Resource {

  final val EmptyServerResource: ServerResource = new ServerResource(Array())

  final val EmptySwitchResource: SwitchResource = new SwitchResource(Array())

}

trait Resource extends Identifiable {

  var CoCoType: CoCoTaskType = CoCoTaskType.UNDEFINED

  override def toString: String = this match {
    case resource: SwitchResource =>
      s"switch:${resource.properties.toString}:" + resource.numericalResources.mkString("[", "|", "]")
    case resource: ServerResource =>
      s"server:" + resource.numericalResources.mkString("[", "|", "]")
  }

}
