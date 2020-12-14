package hiresim.cell.machine

import hiresim.cell.machine.Identifiable.VertexIdentifier

trait Identifiable {

  val id: VertexIdentifier = Identifiable.nextId

  override def equals(other: Any): Boolean = {
    if (other.isInstanceOf[Identifiable]) {
      other.asInstanceOf[Identifiable].id == id
    } else {
      false
    }
  }

  override def hashCode: Int = id.##

  override def toString: String = id.toString
}

object Identifiable {

  type VertexIdentifier = Long
  var previousId = 0L

  private def nextId: VertexIdentifier = {
    previousId += 1
    previousId
  }
}

