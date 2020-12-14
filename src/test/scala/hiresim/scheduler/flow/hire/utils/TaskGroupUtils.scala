package hiresim.scheduler.flow.hire.utils

import hiresim.cell.machine.{Resource, ServerResource}
import hiresim.simulation.SimTypes
import hiresim.tenant.{Job, TaskGroup}
import hiresim.workload.WorkloadProvider

import scala.collection.immutable

object TaskGroupUtils {

  def createSimpleServerJob(duration: SimTypes.simTime = 10,
                            submissionTime: SimTypes.simTime = 0,
                            numTasksPerJob: Int = 7,
                            highPriority: Boolean = false,
                            resources: ServerResource = new ServerResource(Array(5, 5))): Job = {

    val taskgroup = createTaskGroup(duration, submissionTime, numTasksPerJob, switch = false, resources)

    new Job(
      isHighPriority = highPriority,
      submitted = submissionTime,
      taskGroups = Array[TaskGroup](taskgroup),
      arcs = Array()
    )

  }

  def createTaskGroup(duration: SimTypes.simTime,
                      submitted: SimTypes.simTime,
                      tasks: Int,
                      switch: Boolean = false,
                      resources: Resource,
                      inOption: immutable.BitSet = WorkloadProvider.emptyFlavor,
                      notInOption: immutable.BitSet = WorkloadProvider.emptyFlavor): TaskGroup = {
    new TaskGroup(
      isSwitch = switch,
      coLocateOnSameMachine = !switch,
      isDaemonOfJob = false,
      inOption = inOption,
      notInOption = notInOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submitted,
      numTasks = tasks,
      resources = resources,
      allToAll = true
    )
  }

}
