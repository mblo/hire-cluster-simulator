package hiresim.cell.machine

import scala.collection.immutable

class SwitchProps(val capabilities: immutable.BitSet) {
  def remove(other: SwitchProps): SwitchProps = SwitchProps(capabilities.--(other.capabilities))

  def size: Int = capabilities.size

  // required for doing pattern matching on SwitchProps
  def unapply(arg: SwitchProps): immutable.BitSet = capabilities

  def or(other: SwitchProps) = SwitchProps(capabilities.union(other.capabilities))

  def and(other: SwitchProps) = SwitchProps(capabilities.intersect(other.capabilities))

  def containsFully(other: immutable.BitSet): Boolean = {
    var allFound = true
    other
      .foreach(otherBit => if (allFound && !capabilities.contains(otherBit)) {
        allFound = false
      })
    allFound
  }

  override def equals(o: Any): Boolean = if
  (o.isInstanceOf[SwitchProps]) o.asInstanceOf[SwitchProps].and(this).size == this.size
  else false

  def containsFully(other: SwitchProps): Boolean = containsFully(other.capabilities)


  def overlapsPartially(other: immutable.BitSet): Boolean = {
    capabilities.intersect(other).nonEmpty
  }

  def overlapsPartially(other: SwitchProps): Boolean = overlapsPartially(other.capabilities)

  override def toString: String = this.capabilities.toString
}

object SwitchProps {
  def valueOf(fl: String): SwitchProps = fl match {
    case "daiet" =>
      this.typeDaiet
    case "sharp" =>
      this.typeSharp
    case "incbricks" =>
      this.typeIncBricks
    case "netcache" =>
      this.typeNetCache
    case "distcache" =>
      this.typeDistCache
    case "netchain" =>
      this.typeNetChain
    case "harmonia" =>
      this.typeHarmonia
    case "hovercraft" =>
      this.typeHovercRaft
    case "r2p2" =>
      this.typeR2p2
  }

  private var i = 0

  private def nextI: Int = {
    i += 1
    i
  }

  final val typeDaiet = SwitchProps(immutable.BitSet(nextI))
  final val typeSharp = SwitchProps(immutable.BitSet(nextI))
  final val typeIncBricks = SwitchProps(immutable.BitSet(nextI))
  final val typeNetCache = SwitchProps(immutable.BitSet(nextI))
  final val typeDistCache = SwitchProps(immutable.BitSet(nextI))
  final val typeNetChain = SwitchProps(immutable.BitSet(nextI))
  final val typeHarmonia = SwitchProps(immutable.BitSet(nextI))
  final val typeHovercRaft = SwitchProps(immutable.BitSet(nextI))
  final val typeR2p2 = SwitchProps(immutable.BitSet(nextI))

  final val allTypes = Array(this.typeDaiet, this.typeSharp, this.typeIncBricks, this.typeNetCache,
    this.typeDistCache, this.typeNetChain, this.typeHovercRaft, this.typeR2p2, this.typeHarmonia)

  final val none: SwitchProps = SwitchProps(immutable.BitSet())

  final val all = allTypes.foldLeft(this.none)((a, b) => a.or(b))

  assert(i == all.size, "seems like there is a copy paste error")

  def apply(capabilities: immutable.BitSet): SwitchProps = new SwitchProps(capabilities)

  def apply(singleActiveProp: Int): SwitchProps = allTypes.find(p => p.capabilities.contains(singleActiveProp)).get
}



