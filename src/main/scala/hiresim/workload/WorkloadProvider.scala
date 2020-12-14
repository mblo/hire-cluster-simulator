package hiresim.workload

import hiresim.simulation.{Event, RandomManager, SimTypes, Simulator}

import scala.collection.immutable

abstract class WorkloadProvider(protected val random: RandomManager) {

  protected var jobCounter: Int = 0

  protected var timeOfLastEvent: SimTypes.simTime = -1

  def getNextBatchTillTime(requestTillTime: Int, simulator: Simulator): Iterator[Event]

}

object WorkloadProvider {

  def flavorHasInp(chosenJobOption: immutable.BitSet): Boolean = {
    chosenJobOption.exists(f => (f > 0 && f % 2 == 0))
  }

  def getInpFlavors(possibleFlavors: immutable.BitSet): immutable.BitSet = possibleFlavors.filter(f => (f > 0 && f % 2 == 0))

  def getServerFlavors(possibleFlavors: immutable.BitSet): immutable.BitSet = possibleFlavors.filter(f => (f > 0 && (f % 2 == 1)))

  def newInpFlavorBit(forExistingFlavors: immutable.BitSet): immutable.BitSet = {
    // find the next free bit with %2 == 0
    var toCheck = 2
    forExistingFlavors.foreach(x => {
      if (x == toCheck)
        toCheck += 2
    })
    immutable.BitSet(toCheck)
  }

  def newServerFlavorBit(forExistingFlavors: immutable.BitSet): immutable.BitSet = {
    // find the next free bit with %2 == 1
    var toCheck = 1
    forExistingFlavors.foreach(x => {
      if (x == toCheck)
        toCheck += 2
    })
    immutable.BitSet(toCheck)
  }


  final val emptyFlavor: immutable.BitSet = immutable.BitSet()

  final val simpleJobIncServerAllFlavorBits: (immutable.BitSet, immutable.BitSet, immutable.BitSet) = {
    val s = newServerFlavorBit(emptyFlavor)
    val i = newInpFlavorBit(emptyFlavor)
    (i, s, s.union(i))
  }

  final val minServerTasksRequiredForInp: Int = 1
}
