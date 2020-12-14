package hiresim.scheduler.flow.hire

import hiresim.scheduler.flow.ArcDescriptor.CapacityModel
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.{Job, TaskGroup}

import scala.collection.mutable

class HireCapacityModel()(implicit scheduler: HireScheduler) extends CapacityModel {


  override def getServerToSink(server: FlowNode): Capacity = getMachineToSink

  override def getSwitchToSink(switch: FlowNode): Capacity = getMachineToSink

  def getMachineToSink: Cost = 1L


  override def getNetworkTopologyToServer(server: NodeID): Capacity = getNetworkTopologyToMachine

  override def getNetworkTopologyToSwitch(server: NodeID): Capacity = getNetworkTopologyToMachine

  def getNetworkTopologyToMachine = 1L


  override def getServerNetworkTopologyToNetworkTopology(target: NodeID): Capacity = {
    val mapping: TopologyGraphStructure = scheduler.graphManager.mapping
    val switch_target = mapping.getSwitchNetworkNodeIDFromServerNetworkNodeID(target);
    // The node still to visit
    val to_visit: mutable.Queue[NodeID] = mutable.Queue()
    // Counting the number of servers we can allocate on underneath the target node id
    var server_cnt: Int = 0

    scheduler.cell.links.filter(l => l.srcSwitch && l.srcWithOffset == switch_target).foreach(to_visit += _.dstWithOffset)

    while (to_visit.nonEmpty) {

      val currentNodeId: NodeID = to_visit.dequeue()

      /** In case this node is a switch */
      if (currentNodeId < scheduler.cell.numSwitches)

      /** Visit its children */
        scheduler.cell.links.filter(l => l.srcSwitch && l.srcWithOffset == currentNodeId).foreach(to_visit += _.dstWithOffset)
      else
        server_cnt += 1
    }
    server_cnt
  }

  override def getSwitchNetworkTopologyToNetworkTopology(target: NodeID): Capacity = {
    val to_visit: mutable.Queue[NodeID] = mutable.Queue()
    // Counting the number of switche we can allocate on underneath the target node id
    var switch_cnt: Int = 0

    scheduler.cell.links.filter(l => l.srcSwitch && l.srcWithOffset == target).foreach(to_visit += _.dstWithOffset)

    while (to_visit.nonEmpty) {

      // Current node to visit
      val current = to_visit.dequeue()
      // The current node has a switch associated with it
      switch_cnt += 1

      // In case this node is a switch, visit its children
      if (current < scheduler.cell.numSwitches)
        scheduler.cell.links.filter(l => l.srcSwitch && l.srcWithOffset == current).foreach(to_visit += _.dstWithOffset)
    }

    switch_cnt
  }


  override def getServerTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = getTaskGroupToNode(taskGroup, target)

  override def getSwitchTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = getTaskGroupToNode(taskGroup, target)

  @inline def getTaskGroupToNode(taskGroup: TaskGroup, target: FlowNode): Capacity = taskGroup.notStartedTasks min target.maxTaskFlows


  override def getServerTaskGroupToUnschedule(taskGroup: TaskGroup): Capacity = getTaskGroupToUnschedule(taskGroup)

  override def getSwitchTaskGroupToUnschedule(taskGroup: TaskGroup): Capacity = getTaskGroupToUnschedule(taskGroup)

  def getTaskGroupToUnschedule(taskGroup: TaskGroup): Capacity = taskGroup.notStartedTasks


  override def getServerUnscheduleToSink(taskGroup: TaskGroup): Capacity =
    taskGroup.notStartedTasks + 1 // for all the tasks and the flavor selector

  override def getSwitchUnscheduleToSink(taskGroup: TaskGroup): Capacity =
    taskGroup.notStartedTasks + 1 // for all the tasks and the flavor selector


  def getJobUnscheduleToSink(job: Job): Capacity = job.getNotScheduledTasksCount

  def getFlavorSelectorUnscheduleToSink: Capacity = 1L


  def getFlavorSelectorToUnschedule: Capacity = 1L

  def getFlavorSelectorToTaskGroup(flavorNodeLinker: NodeLinker,
                                   taskGroup: TaskGroup): Capacity = scheduler.graphManager.costs.getFlavorSelectorToTaskGroupCapacity(flavorNodeLinker, taskGroup)

}
