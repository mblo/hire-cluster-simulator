package hiresim.scheduler.flow.coco

import hiresim.scheduler.flow.ArcDescriptor.CapacityModel
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.{Job, TaskGroup}

import scala.collection.mutable

class CoCoCapacityModel(implicit scheduler: CoCoScheduler) extends CapacityModel {

  /*
   * Resource Leaf -> Sink given by https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L652
   * Where FLAGS_max_tasks_per_pu is normally set to 1. This is also mentioned on P.263 of Maltes Thesis.
   */

  override def getServerToSink(server: FlowNode): Capacity = getMachineToSink

  override def getSwitchToSink(switch: FlowNode): Capacity = getMachineToSink

  def getMachineToSink: Cost = 1L


  override def getNetworkTopologyToServer(server: NodeID): Capacity = getNetworkTopologyToMachine

  override def getNetworkTopologyToSwitch(server: NodeID): Capacity = getNetworkTopologyToMachine

  def getNetworkTopologyToMachine = 1L


  override def getServerNetworkTopologyToNetworkTopology(target: NodeID): Capacity = {
    val isSwitchLookup: Boolean = false
    getNetworkTopologyToNetworkTopology(target, isSwitchLookup)
  }

  override def getSwitchNetworkTopologyToNetworkTopology(target: NodeID): Capacity = {
    val isSwitchLookup: Boolean = true
    getNetworkTopologyToNetworkTopology(target, isSwitchLookup)
  }

  private def getNetworkTopologyToNetworkTopology(target: NodeID, isSwitchLookup: Boolean): Capacity = {
    val machines: NodeID = if (isSwitchLookup) scheduler.cell.numSwitches else scheduler.cell.numServers
    val to_visit: mutable.Queue[NodeID] = mutable.Queue()
    var machine_cnt: Int = 0

    scheduler.cell.links.filter(l => isSwitchLookup == l.srcSwitch && l.srcWithOffset == target).foreach(to_visit += _.dstWithOffset)
    while (to_visit.nonEmpty) {
      val currentNodeId: NodeID = to_visit.dequeue()

      if (currentNodeId < machines)
        scheduler.cell.links.filter(l => isSwitchLookup == l.srcSwitch && l.srcWithOffset == currentNodeId).foreach(to_visit += _.dstWithOffset)
      else
        machine_cnt += 1
    }

    machine_cnt
  }

  override def getServerTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = getTaskGroupToNode(taskGroup, target)

  override def getSwitchTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = getTaskGroupToNode(taskGroup, target)

  def getTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = taskGroup.notStartedTasks


  override def getServerTaskGroupToUnschedule(taskGroup: TaskGroup): Capacity = taskGroup.notStartedTasks

  override def getSwitchTaskGroupToUnschedule(taskGroup: TaskGroup): Capacity = taskGroup.notStartedTasks

  def getJobUnscheduleToSink(job: Job): Capacity = job.getNotScheduledTasksCount


  override def getServerUnscheduleToSink(taskGroup: TaskGroup): Capacity = taskGroup.job.get.getNotScheduledTasksCount

  override def getSwitchUnscheduleToSink(taskGroup: TaskGroup): Capacity = taskGroup.job.get.getNotScheduledTasksCount


}
