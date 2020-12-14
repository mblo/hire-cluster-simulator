package hiresim.scheduler.flow.hire.costs

import hiresim.scheduler.flow.hire.NodeLinker
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.tenant.{Job, TaskGroup}

trait HireCostDimensionCalculator {


  def getServerToSinkCost(server: FlowNode): Cost =
    unsupported()

  def getSwitchToSinkCost(switch: FlowNode): Cost =
    unsupported()


  def getServerTaskGroupToNodeCost(taskGroup: TaskGroup,
                                   targetNode: FlowNode,
                                   isPartOfFlavorSelectorPart: Boolean): Cost =
    unsupported()

  def getSwitchTaskGroupToNodeCost(taskGroup: TaskGroup,
                                   targetNode: FlowNode,
                                   isPartOfFlavorSelectorPart: Boolean): Cost =
    unsupported()

  def getTaskGroupToUnscheduleCost(taskGroup: TaskGroup): Cost =
    unsupported()

  def getFlavorSelectorToTaskGroupCost(flavorNodeLinker: NodeLinker,
                                       taskGroup: TaskGroup,
                                       taskGroupNode: FlowNode): Cost =
    unsupported()

  def getFlavorselectorToUnscheduleCost(job: Job, nodeLinker: NodeLinker): Cost =
    unsupported()


  def unsupported(): Cost = {
    throw new AssertionError("Operation unsupported")
  }

  def onClearCaches(): Unit = {}

  def removeTaskGroupFromCaches(taskGroup: TaskGroup): Unit = {}

  def capacityFlavorSelector2TaskGroup(flavorNodeLinker: NodeLinker, taskGroup: TaskGroup): Capacity = {
    throw new AssertionError("Operation unsupported")
  }
}