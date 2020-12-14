package hiresim.workload

import hiresim.simulation.{ClosureEvent, Event, RandomManager, Simulator}
import hiresim.tenant.Job

import scala.collection.mutable

class OneTimeWorkloadProvider(random: RandomManager, jobList: Iterable[Job]) extends WorkloadProvider(random) {

  protected val remainingJobs: mutable.Stack[Job] = mutable.Stack.from(jobList.toList.sortWith(_.submitted < _.submitted))

  override def getNextBatchTillTime(requestTillTime: Int, simulator: Simulator): Iterator[Event] = {
    new Iterator[Event]() {
      override def hasNext: Boolean = timeOfLastEvent < requestTillTime && remainingJobs.nonEmpty

      override def next(): Event = {
        val job = remainingJobs.pop()
        timeOfLastEvent = job.submitted
        new ClosureEvent(_ => simulator.addJob(job), job.submitted)
      }
    }
  }

  def getAll: Iterator[Job] = remainingJobs.iterator

}
