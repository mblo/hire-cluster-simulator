package hiresim.scheduler.flow.hire.costs

import hiresim.scheduler.flow.hire.{HireScheduler, NodeLinker}
import hiresim.scheduler.flow.solver.graph.FlowArc.Cost
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.configuration.SimulationConfiguration.PRECISION
import hiresim.tenant.{Job, TaskGroup}

private[costs] class HirePriorityCostCalculator(implicit scheduler: HireScheduler) extends HireCostDimensionCalculator {


  override def getServerTaskGroupToNodeCost(taskGroup: TaskGroup,
                                            target: FlowNode,
                                            isPartOfFlavorSelectorPart: Boolean): Cost = getScoreForTaskGroup(taskGroup)

  override def getSwitchTaskGroupToNodeCost(taskGroup: TaskGroup,
                                            target: FlowNode,
                                            isPartOfFlavorSelectorPart: Boolean): Cost = getScoreForTaskGroup(taskGroup)

  private def getScoreForTaskGroup(taskGroup: TaskGroup): Cost = {
    // If this is a high priority job, return 0. So it's cheaper to route via high prio tasks
    if (taskGroup.job.get.isHighPriority)
      0L
    // Else 1
    else
      PRECISION
  }

  override def getTaskGroupToUnscheduleCost(taskGroup: TaskGroup): Cost = {

    // The time the TaskGroup is already waiting for scheduling
    val tg_waiting_time = scheduler.simulator.currentTime() - taskGroup.submitted
    // The maximum time a job is currently waiting
    val max_waiting_time = scheduler.getMaxWaitingTime

    assert(tg_waiting_time <= max_waiting_time && tg_waiting_time >= 0)

    // If the max waiting time is 0 the whole term is 0. Thus we can shortcut the calculation in this case
    if (max_waiting_time == 0)
      0L
    else {

      val numerator = tg_waiting_time * math.exp(taskGroup.scheduledTasks / taskGroup.numTasks.toDouble) * PRECISION
      val final_term = (numerator / (max_waiting_time * Math.E)).toLong

      // we might face some rounding issues, so apply some checks here
      if (final_term < 0)
        0L
      else if (final_term > PRECISION)
        PRECISION
      else
        final_term
    }
  }

  override def getFlavorselectorToUnscheduleCost(job: Job, nodeLinker: NodeLinker): Cost = {
    val now = scheduler.simulator.currentTime()

    if (now < nodeLinker.oldestSubmissionTimeOfFlavorNode) {
      0L
    } else {
      // The maximum time a job is currently waiting
      val max_waiting_time: Int = scheduler.getMaxWaitingTime

      // The time the job is already waiting for scheduling
      val job_waiting_time: Int = {
        val tmp = now - (nodeLinker.oldestSubmissionTimeOfFlavorNode)
        tmp min max_waiting_time
      }

      val scheduledTasks: Double = job.getScheduledTasksCount.toDouble
      val totalTasks: Double = job.getTasksCount.toDouble

      assert(totalTasks >= scheduledTasks, s"someone started more tasks (${scheduledTasks}) " +
        s"than the job has in total ${totalTasks}?")

      // if max waiting time is 0, simply use ratio like for max waiting time
      var ratio: Double = 1.0

      if (max_waiting_time > 0) {
        ratio = (job_waiting_time.toDouble / max_waiting_time.toDouble + scheduledTasks.toDouble / totalTasks.toDouble) / 2
      }

      if (job_waiting_time >= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER) {
        ratio = 1.0
      }

      assert(ratio <= 1.0)
      assert(ratio >= 0.0)

      // penalty = 0.5 * cos ( (ratio) * PI ) + 0.5
      val penaltyFactor: Double = 0.5 * math.cos((ratio - 1.0) * math.Pi) + 0.5
      assert(penaltyFactor >= 0.0)
      assert(penaltyFactor <= 1.0)

      (penaltyFactor * PRECISION).toLong
    }

  }

}
