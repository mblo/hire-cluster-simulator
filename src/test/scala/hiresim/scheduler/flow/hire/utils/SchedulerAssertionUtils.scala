package hiresim.scheduler.flow.hire.utils

import hiresim.cell.Cell
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.scheduler.flow.hire.{HireGraphManager, TopologyGraphStructure}
import hiresim.shared.graph.Graph.NodeID
import hiresim.tenant.{Job, TaskGroup}

object SchedulerAssertionUtils {

  def checkNoAllocationsInJob(job: Job): Unit =
    checkNumberOfAllocationsInJob(0, job)

  def checkNumberOfAllocationsInJob(expected: Int,
                                    job: Job): Unit = {
    assert(job.allocations.size == expected, s"Expected ${expected} allocation on job ${job} but found ${job.allocations.size}!")
  }

  def checkNumberOfTaskFromTaskGroupRunningOnMachine(expected: Int,
                                                     machineNetworkID: NodeID,
                                                     taskGroup: TaskGroup,
                                                     graphManager: HireGraphManager): Unit = {

    assert(graphManager.graph.nodes(machineNetworkID).allocatedTaskGroupsInSubtree.contains(taskGroup) || expected == 0, s"No tasks of TaskGroup ${taskGroup} running on node ${machineNetworkID} (Expected: ${expected})!")
    assert(graphManager.graph.nodes(machineNetworkID).allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0) == expected, s"Running tasks on machine $machineNetworkID diverges from expected number running tasks for" +
      s" TaskGroup $taskGroup (Running: ${graphManager.graph.nodes(machineNetworkID).allocatedTaskGroupsInSubtree(taskGroup)} Expected: ${expected})!")

  }

  def checkRunningTasksInTaskGroup(expected: Int,
                                   taskGroup: TaskGroup): Unit = {
    assert(taskGroup.scheduledTasks == expected, s"TaskGroup ${taskGroup} has ${taskGroup.scheduledTasks} scheduled tasks but expected $expected!")
  }

  def checkServerResourceEqual(server: NodeID,
                               required: Array[NumericalResource],
                               cell: Cell,
                               mapping: TopologyGraphStructure): Unit =
    checkServerResourcesEqual(Seq(server), required, cell, mapping)

  def checkServerResourcesEqual(servers: Seq[NodeID],
                                required: Array[NumericalResource],
                                cell: Cell,
                                mapping: TopologyGraphStructure): Unit = {

    servers.foreach(server => {
      assert(cell.servers(mapping.getServerMachineIDFromNodeID(server)).sameElements(required), s"Resources of server ${server} " +
        s"(Cell: ${mapping.getServerMachineIDFromNodeID(server)}) diverge from expected value (${cell.servers(mapping.getServerMachineIDFromNodeID(server)).mkString(",")} vs ${required.mkString(",")})!")
    })

  }

}
