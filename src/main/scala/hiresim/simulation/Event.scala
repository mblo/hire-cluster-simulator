package hiresim.simulation

import hiresim.simulation.SimTypes.simTime

trait Event extends Comparable[Event] {

  val time: SimTypes.simTime
  val action: Simulator => Unit

  def process(simulator: Simulator): Unit =
    action(simulator)

  def compareTo(o: Event): SimTypes.simTime =
    o.time - time

}

class ClosureEvent(val action: Simulator => Unit,
                   val time: SimTypes.simTime) extends Event

/**
 * A special event that has no real action that is executed. It purpose is to
 * make sure that post/pre timestep hooks get executed
 */
class EmptyEvent(val time: simTime) extends Event {

  // Override action with a empty function. This event does nothing.
  override final val action: Simulator => Unit = _ => {}
}

class RepeatingEvent(repeatingAction: Simulator => Unit,
                     interval: SimTypes.simTime,
                     startTime: SimTypes.simTime = 0) extends Event {

  val time: simTime = startTime

  val action: Simulator => Unit = s => {
    repeatingAction(s)
    s.scheduleEvent(new RepeatingEvent(repeatingAction, interval, time + interval))
  }

}