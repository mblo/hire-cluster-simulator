package hiresim.tenant

import hiresim.cell.machine.{Identifiable, Resource, SwitchResource}
import hiresim.scheduler.flow.solver.graph.FlowArc.Capacity
import hiresim.simulation.SimTypes
import hiresim.simulation.SimTypes.simTime
import hiresim.workload.WorkloadProvider

import scala.collection.{immutable, mutable}

class TaskGroup(final val isSwitch: Boolean,
                final val inOption: immutable.BitSet,
                final val notInOption: immutable.BitSet,
                final val isDaemonOfJob: Boolean = false,
                final val numTasks: Int,
                final val submitted: SimTypes.simTime,
                final val duration: SimTypes.simTime,
                final val statisticsOriginalDuration: SimTypes.simTime,
                final val resources: Resource,
                final val allToAll: Boolean = false,
                final val coLocateOnSameMachine: Boolean = true)
  extends Identifiable {

  assert(inOption.intersect(notInOption).isEmpty)

  val belongsToInpFlavor: Boolean = WorkloadProvider.flavorHasInp(inOption)

  def shortToString() = s"tgId:${id}[(j:${job.get.id}) ${notStartedTasks}/${numTasks}t" +
    s" inOpt:${inOption}:¬${notInOption} " +
    s" ${resources.toString}" +
    s" ${submitted}|${_consideredForSchedulingSince}|${job.get.lastPreemptionTime}]"

  private var _consideredForSchedulingSince: simTime = submitted

  @inline def consideredForSchedulingSince: simTime = _consideredForSchedulingSince

  @inline def updateConsideredForSchedulingWith(now: simTime): Unit = if (now > _consideredForSchedulingSince)
    _consideredForSchedulingSince = now

  assert(duration <= statisticsOriginalDuration)

  def detailedToString(): String = {
    var tgString = ""
    tgString += s"tgId:${id} (j:${job.get.id}) " + resources.toString
    tgString += s" ${notStartedTasks}/${numTasks}t not started, colocate:${coLocateOnSameMachine} " +
      s"submitted:${submitted}|${_consideredForSchedulingSince}|${job.get.lastPreemptionTime}" +
      s" job:${job.get.id} job.opt:${job.get.chosenJobOption}:¬${job.get.excludedJobOption}" +
      s" all2all:${allToAll}" +
      s" inOpt:${inOption}:¬${notInOption}, " +
      s"outgoing conn:${outgoingConnectedTaskGroups.size} --" +
      s" ${outgoingConnectedTaskGroups.map(o => o.id).mkString("[", ",", "]")}"
    if (isSwitch) {
      tgString += s" ${resources.asInstanceOf[SwitchResource].properties}"
    }

    tgString
  }


  def cloneWithNewSubmissionTime(submitted: SimTypes.simTime,
                                 replaceWithEmptyFlavorOptions: Boolean = false): TaskGroup =
    new TaskGroup(isSwitch = isSwitch,
      inOption = if (replaceWithEmptyFlavorOptions) WorkloadProvider.emptyFlavor else inOption,
      notInOption = if (replaceWithEmptyFlavorOptions) WorkloadProvider.emptyFlavor else notInOption,
      isDaemonOfJob = isDaemonOfJob,
      numTasks = numTasks,
      submitted = submitted,
      duration = duration,
      resources = resources,
      allToAll = allToAll,
      coLocateOnSameMachine = coLocateOnSameMachine,
      statisticsOriginalDuration = statisticsOriginalDuration
    )

  override def clone(): TaskGroup = cloneWithNewSubmissionTime(submitted = this.submitted)

  final val inAllOptions: Boolean = notInOption.isEmpty

  assert(duration > 0, s"duration must be positive ($duration)")
  assert(numTasks > 0, s"task group must have at least one task ($numTasks)")

  assert((inOption.isEmpty == notInOption.isEmpty) || inOption.nonEmpty,
    "invalid flavor options of task group -" +
      " if flavor is set, the notInFlavor must not be empty")

  if (isSwitch) assert(resources.asInstanceOf[SwitchResource].numericalResources.max > 0, "there is a task group for a switch, which needs no resources")

  /** The number of unscheduled tasks */
  var notStartedTasks: Int = numTasks
  var finishedTasks: Int = 0

  var preemptionCounter: Int = 0

  // Whether resources for this TaskGroup have changed and thus need to be recomputed
  var dirty: Boolean = false
  var dirtyMachines: mutable.BitSet = mutable.BitSet()

  // used internally by Hire to keep track of the max allocations we could do during the current round
  var maxAllocationsInOngoingSolverRound: Capacity = 0L

  var job: Option[Job] = None

  var outgoingConnectedTaskGroups: Array[TaskGroup] = Array()

  @inline final def runningTasks: Int = numTasks - finishedTasks - notStartedTasks

  @inline final def scheduledTasks: Int = numTasks - notStartedTasks

  @inline final def willBeFullyAllocated: Boolean = notStartedTasks == 0

  @inline final def hasAlreadyBeenSubmitted(currentSimTime: simTime): Boolean = submitted <= currentSimTime

}
