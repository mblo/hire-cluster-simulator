package hiresim.simulation

import hiresim.cell.machine.SwitchProps
import hiresim.scheduler.Scheduler
import hiresim.shared.{ArrayUtils, FixedSizeFiFoArray, Tabulator, TimeIt}
import hiresim.simulation.SimTypes.simTime
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.tenant.Job
import hiresim.workload.WorkloadProvider

import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.collection.mutable

object SimTypes {

  type simTime = Int // we do not use long here, since int is sufficient also for ms granularity

  final val GIGA: Long = 1000000000
  final val MEGA: Long = 1000000
}

class Simulator(val workloadProvider: Seq[WorkloadProvider],
                val randomManager: RandomManager,
                val workloadBatchSize: Int = 5000 * 3600) {

  private val post_timestep_hooks: mutable.ListBuffer[Simulator => Unit] = mutable.ListBuffer()
  private val pre_timestep_hooks: mutable.ListBuffer[Simulator => Unit] = mutable.ListBuffer()
  private val sim_done_hooks: mutable.ListBuffer[Simulator => Unit] = mutable.ListBuffer()

  private val timestep_lookup: mutable.Map[Long, SimulationStep] = mutable.Map[Long, SimulationStep]()
  private val events: mutable.PriorityQueue[SimulationStep] = mutable.PriorityQueue[SimulationStep]()
  private val previousSimSpeedMeasurements =
    new FixedSizeFiFoArray[SpeedMeasurement](maxSize = 10) { // maxSize = 1000
      override protected def getArray(size: Int): Array[SpeedMeasurement] = Array.ofDim(size)

      override def pushPointer: Unit = {
        cachedMedianUsPerSimTimeMs = -1
        super.pushPointer
      }

      private var cachedMedianUsPerSimTimeMs: Long = -1

      def totalDelta: SpeedMeasurement = {
        val oldest = getOldestItem.getOrElse(new SpeedMeasurement(0, 0, 0, 0))
        val newest = getPreviousItem.getOrElse(new SpeedMeasurement(0, 0, 0, 0))
        newest - oldest
      }

      def getMedianUsPerSimTimeMs: Long = {
        if (cachedMedianUsPerSimTimeMs == -1) {

          val validSpeedMeasurements: Array[Long] = getUnsortedValidArraySlice.map(speed => {
            if (speed.deltaSimTime < 1 || speed.deltaMs < 1) -1L
            else speed.deltaMs * 1000 / speed.deltaSimTime
          }).filter(_ > 0L).toArray

          cachedMedianUsPerSimTimeMs = if (validSpeedMeasurements.isEmpty) 1L else
            ArrayUtils.getMedian(validSpeedMeasurements, validSpeedMeasurements.length)
          cachedMedianUsPerSimTimeMs
        } else cachedMedianUsPerSimTimeMs
      }
    }
  var scheduler: Scheduler = _
  var statisticsTotalJobs: Int = 0
  var statisticsTotalJobsWithInp: Int = 0
  private var debugHookInformJobArrival: Option[(Job) => Unit] = None
  private var time: SimTypes.simTime = 0

  private var systemTimeSimulationStart: Long = -1
  private var latestEndTime: SimTypes.simTime = Integer.MAX_VALUE
  private var expectedEndTime: SimTypes.simTime = -1

  def registerSimDoneHook(hook: Simulator => Unit): Unit =
    sim_done_hooks += hook

  def registerPostSimulationStepHook(hook: Simulator => Unit): Unit =
    post_timestep_hooks += hook

  def registerPreSimulationStepHook(hook: Simulator => Unit): Unit =
    pre_timestep_hooks += hook

  def setScheduler(scheduler: Scheduler): Unit = {
    this.scheduler = scheduler
  }

  def activateDebugHookInformAboutJobArrival(hook: (Job) => Unit): Unit = {
    assert(!debugHookInformJobArrival.isDefined)
    debugHookInformJobArrival = Some(hook)
  }

  def addJob(job: Job): Unit = {

    if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER)
      logDebug(s"add job $job to scheduler")

    if (debugHookInformJobArrival.isDefined)
      debugHookInformJobArrival.get(job)

    statisticsTotalJobs += 1

    if (WorkloadProvider.flavorHasInp(job.allPossibleOptions))
      statisticsTotalJobsWithInp += 1

    scheduler.addJobAndSchedule(job)

  }

  def scheduleActionWithDelay(action: Simulator => Unit, delay: SimTypes.simTime): Unit = {
    val newTime = time + delay
    assert(newTime >= time, s"negative delay ($delay) or maybe integer overflow ($newTime)!")
    scheduleEvent(new ClosureEvent(action, newTime))
  }

  def runSim(endTime: SimTypes.simTime): Unit = {
    var statusEachTicks = 10000
    var workloadRequestedTill = workloadBatchSize
    systemTimeSimulationStart = System.currentTimeMillis()
    askWorkloadForEvents(workloadRequestedTill)
    var continue = true
    var ticks = 0L
    var lastTickWhenPrintingInfo = 0L
    var lastTimeWhenPrintingInfo = System.currentTimeMillis()
    var simSpeedTicksPerSecond = 10L
    val terminate_simulation_on_completion = endTime == -1
    if (endTime > 0) {
      expectedEndTime = endTime
      latestEndTime = endTime

      // the simulator prevents to add events after endTime, so make sure we have a step right after the endTime, so
      //  that the simulation will stop correctly
      getTimeStep(endTime + 1)
    } else {
      logInfo(s"Simulator runs in makespan mode, running at least till ${events.last.time}, and stops when scheduler is done.")
      expectedEndTime = events.last.time
    }

    while (events.nonEmpty && continue) {

      val step: SimulationStep = events.dequeue()
      // First adapt simulation time
      time = step.time
      // Drop references to this timestep as it is now in the past
      timestep_lookup -= time

      // Give some info about the current timestep
      logDebug(s"Advancing to next timestep at ${step.time}. Currently there are ${step.events.size} " +
        s"events to be executed, |future events| >= ${events.size}")

      if (!terminate_simulation_on_completion && time > endTime) {
        logInfo(s"Stop simulation @$time before next timestep @${time}")
        continue = false
      } else {

        // do we need to ask for more workload events in the queue?
        while (workloadRequestedTill <= time) {
          workloadRequestedTill += workloadBatchSize
          logDebug(s"Simulator needs to refill events from workload @$time, up to $workloadRequestedTill")
          askWorkloadForEvents(workloadRequestedTill)
        }

        // Execute pre timestep hooks
        for (hook <- pre_timestep_hooks)
          hook(this)

        while (step.getHasMoreEvents) {
          ticks += 1

          // If wanted we can trigger garbage collection cleanup
          if (ticks % SimulationConfiguration.SIM_TRIGGER_GC_TICKS == 0) {
            Simulator.gc()
          }

          if (ticks % statusEachTicks == 0) {
            if (System.currentTimeMillis() > lastTimeWhenPrintingInfo + 120000) {
              val progress = time.toDouble / endTime.toDouble
              simSpeedTicksPerSecond = (simSpeedTicksPerSecond * 9 + (ticks - lastTickWhenPrintingInfo) / (1 + (System.currentTimeMillis() - lastTimeWhenPrintingInfo) / 1000)) / 10
              val simSpeedTicksPerMinute: Int = ((ticks + 1).toDouble / ((1 + System.currentTimeMillis() - systemTimeSimulationStart).toDouble / 60000.0)).toInt
              lastTickWhenPrintingInfo = ticks
              statusEachTicks = (statusEachTicks * 5 + simSpeedTicksPerMinute) / 6
              if (statusEachTicks < 60)
                statusEachTicks = 60
              else if (statusEachTicks > 6000000)
                statusEachTicks = 6000000

              lastTimeWhenPrintingInfo = System.currentTimeMillis()

              val (remainingMilliSeconds: Long, speedMeasurementDelta: SpeedMeasurement) = {
                val totalDelta: SpeedMeasurement = previousSimSpeedMeasurements.totalDelta
                val medianUsPerSimTimeMs: Long = previousSimSpeedMeasurements.getMedianUsPerSimTimeMs
                val remaining = ((expectedEndTime - time).toDouble * (medianUsPerSimTimeMs.toDouble / 1000.0)).toLong

                (remaining, totalDelta)
              }

              Console.out.println(s"Run:${SimulationConfiguration.ONGOING_RUN_ID} @${msToHuman(time)} (${(100.0 * progress).toInt}%) $ticks ticks; " +
                s"$simSpeedTicksPerSecond ticks/ms " +
                s"(avg $simSpeedTicksPerMinute ticks/min) ; " +
                s"${Calendar.getInstance().getTime()} ; ${msToHuman(remainingMilliSeconds)} remaining " +
                s"(${msToHuman(speedMeasurementDelta.deltaSimTime)} in ${msToHuman(speedMeasurementDelta.deltaMs)}, " +
                s"${msToHuman((1 + System.currentTimeMillis() - systemTimeSimulationStart))} spent)")

            }
          }

          val event = step.getNextEvent
          // And execute that event
          event.process(simulator = this)

        }

        // Execute post timestep hooks
        for (hook <- post_timestep_hooks)
          hook(this)

        // we consider the sim to be done, when
        // -> There are no more pending jobs
        // -> There are none not finished jobs
        continue = !terminate_simulation_on_completion ||
          (currentTime() < expectedEndTime ||
            scheduler.getNotFinishedJobSize > 0 ||
            scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions > 0)

        if (terminate_simulation_on_completion) {
          if (continue) {
            logDebug(s"Continue Simulation: ${continue} | Not finished jobs: ${scheduler.getNotFinishedJobSize} |" +
              s" NumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions: ${scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions}")

            if (!hasFutureEvents) {
              logWarning("This is a broken situation - the scheduler is not done with work, but does not continue scheduling")
              printQuickStats()
              throw new RuntimeException("Invalid makespan run")
            }
          } else {
            logInfo(s"Triggered termination of simulation," +
              s" 1:${currentTime() < expectedEndTime}" +
              s" 2:${scheduler.getNotFinishedJobSize > 0} " +
              s" 3:${scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions > 0}")
          }
        }
      }
    }

    events.clear()
    Simulator.gc()

    logInfo("Calling simulation termination hooks...")
    for (hook <- sim_done_hooks)
      hook(this)

    val systemEndTime = System.currentTimeMillis()
    logInfo(s"Run:${SimulationConfiguration.ONGOING_RUN_ID} - simulation of ${msToHuman(endTime)} (${endTime}ms) took ${msToHuman(systemEndTime - systemTimeSimulationStart)}")

    TimeIt.printStatistics
    printQuickStats()
  }

  @inline def hasFutureEvents: Boolean = events.nonEmpty

  def logInfo(str: => String): Unit =
    Console.out.println(s"@$time -> INFO $str")

  def logWarning(str: => String): Unit = {
    System.err.println(s"@$time -> WARN $str")
    System.err.flush()
  }

  def logDebug(str: => String, forcePrint: Boolean = false): Unit = {
    if (SimulationConfiguration.LOGGING_VERBOSE_SIM || forcePrint)
      Console.out.println(s"@$time -> DEBUG $str")
  }

  def askWorkloadForEvents(workloadRequestedTill: Int): Unit = {
    for (workload <- workloadProvider)
      workload.getNextBatchTillTime(workloadRequestedTill, this).foreach(event => scheduleEvent(event))
  }

  def scheduleEvent(event: Event): Unit = {

    assert(event.time >= time, s"Can not schedule event that is in the past (Desired: ${event.time} Currently: ${currentTime()}).")

    if (event.time > latestEndTime) {
      logDebug(s"skip event $event because event time will be after final end of simulation")
    } else {
      val timestep: SimulationStep = getTimeStep(event.time)
      // Only add the event if there is an action associated with it. If a EmptyEvent is
      // passed, we only want to ensure post/pre hooks are called at that time
      if (!event.isInstanceOf[EmptyEvent])
        timestep.addEvent(event)

    }
  }

  private def getTimeStep(time: simTime): SimulationStep = {
    timestep_lookup.getOrElseUpdate(time, {

      // Create the new SimulationStep element at that given timepoint
      val newly: SimulationStep = new SimulationStep(time)

      // Enqueue the element to the TimeStep Queue
      events.enqueue(newly)
      // And return the element such that is in the Map
      newly

    })
  }

  def printQuickStats(printOnConsole: Boolean = true, showRuntimeSpeed: Boolean = true): String = {
    val entries: mutable.ListBuffer[Seq[String]] = mutable.ListBuffer()
    val now = currentTime()
    val msSinceSimulationStart: Long = getPassedMsSinceSimulationStart

    Simulator.gc()

    if (previousSimSpeedMeasurements.isEmpty) {
      previousSimSpeedMeasurements.enqueue(new SpeedMeasurement(
        spentMs = msSinceSimulationStart,
        processedSimTime = now,
        deltaSimTime = now,
        deltaMs = msSinceSimulationStart))
    } else {
      val previous: SpeedMeasurement = previousSimSpeedMeasurements.getPreviousItem.getOrElse(new SpeedMeasurement(0, 0, 0, 0))
      val deltaMs = msSinceSimulationStart - previous.spentMs
      val deltaSimTime = now - previous.processedSimTime
      if (deltaMs > 0 && deltaSimTime > 0) {
        previousSimSpeedMeasurements.enqueue(new SpeedMeasurement(
          spentMs = msSinceSimulationStart,
          deltaMs = deltaMs,
          processedSimTime = now,
          deltaSimTime = deltaSimTime))
      }
    }

    val medianUsPerSimTimeMs: Long = previousSimSpeedMeasurements.getMedianUsPerSimTimeMs

    entries.append(List("Target", "Statistic", "Value"))
    entries.append(List(s"@$now", "TotalJobs", statisticsTotalJobs.toString))
    entries.append(List(scheduler.name, "Jobs with Inp", s"${statisticsTotalJobsWithInp.toString} " +
      s"(${if (statisticsTotalJobs > 0) f"${100.0 * statisticsTotalJobsWithInp / statisticsTotalJobs}%.1f" else "0"}%)"))

    val (notFullyScheduledTillNow, fullyScheduledTillNow) = scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow

    entries.append(List(s"isScheduling:${scheduler.isScheduling}", "TotalJobsDone", scheduler.statisticsTotalJobsDone.toString))
    entries.append(List("", "TotalJobsDoneInp", scheduler.statisticsTotalJobsDoneWithInp.toString))

    entries.append(List(if (showRuntimeSpeed) s"${msToHuman(msSinceSimulationStart)} spent" else "",
      "TotalJobsFullyScheduled", scheduler.statisticsTotalJobsFullyScheduled.toString)) // this
    entries.append(List(if (showRuntimeSpeed) f"${(currentTime()).toDouble / (msSinceSimulationStart + 1)}%.5fx total sim speedup" else "",
      "TotalJobsFullyScheduledInp", scheduler.statisticsTotalJobsFullyScheduledWithInp.toString))
    entries.append(List(if (showRuntimeSpeed) f"${(1000.0 / medianUsPerSimTimeMs.toDouble)}%.5fx recent sim speedup" else "",
      "Scheduling Success", if (statisticsTotalJobs - scheduler.statisticsServerFallbackResubmit > 0)
        f"${((scheduler.statisticsTotalJobsFullyScheduled + fullyScheduledTillNow).toDouble / (statisticsTotalJobs.toDouble - scheduler.statisticsServerFallbackResubmit.toDouble) * 100.0)}%.2f%%" else "/"))
    entries.append(List(if (showRuntimeSpeed) f"${if (expectedEndTime > 0) msToHuman(((expectedEndTime - now).toDouble * (medianUsPerSimTimeMs.toDouble / 1000.0)).toLong) else "NaN"} remaining" else "",
      "Preemptions", scheduler.statisticsTotalTaskPreemptions.toString))

    entries.append(List(if (showRuntimeSpeed) f"${100.0 * (scheduler.statisticsTotalThinkTime + scheduler.statisticsTotalThinkTimeWasted).toDouble / (now max 1)}%1.2f%% busyness" else "", "TotalThinkTime", scheduler.statisticsTotalThinkTime.toString))
    entries.append(List("", "SchedulingAttempts", scheduler.statisticsSchedulingAttempts.toString))
    entries.append(List("", "TotalScheduledTasks", scheduler.statisticsScheduledTasks.toString))
    entries.append(List("", "PerformedAllocations", scheduler.statisticsPerformedAllocations.toString))
    if (scheduler.mostRecentPlacementLatencies.nonEmpty) {
      val allocStats = ArrayUtils.getStats(scheduler.mostRecentPlacementLatencies.toArray,
        scheduler.mostRecentPlacementLatencies.size)
      entries.append(List("", "TG Placement Latency (last 1k)", s"${msToHuman(allocStats.median)} median |${msToHuman(allocStats.mean)} mean | ${msToHuman(allocStats.max)} max"))
    }
    entries.append(List("", "TasksScheduled", scheduler.statisticsPerformedTaskAllocations.toString))
    entries.append(List("", "TotalThinkTimeWasted", scheduler.statisticsTotalThinkTimeWasted.toString))
    entries.append(List("", "not fully scheduled, with future submissions", scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.toString))
    entries.append(List("", "not fully scheduled, till now", notFullyScheduledTillNow.toString))
    entries.append(List("", "pending 'scheduling items'", scheduler.getNumberOfPendingSchedulingObjects.toString))
    entries.append(List("", "fully scheduled, till now", fullyScheduledTillNow.toString))
    entries.append(List("", "take inp flavor", s"${scheduler.statisticsFlavorTakeInp} " +
      f"(${
        val tmp = (scheduler.statisticsServerFallbackResubmit +
          scheduler.statisticsFlavorTakeServer + scheduler.statisticsFlavorTakeInp)
        if (tmp > 0)
          100.0 * scheduler.statisticsFlavorTakeInp.toDouble / tmp
        else Double.NaN
      }%2.1f%%)"))
    entries.append(List("", "take server flavor", scheduler.statisticsFlavorTakeServer.toString))
    entries.append(List("", "server fallback resubmit", scheduler.statisticsServerFallbackResubmit.toString))

    entries.append(List("", "cell server load (now)", s"${scheduler.cell.currentServerLoads().map(l => f"${l * 100}%.2f").mkString(", ")}%"))
    entries.append(List("", "cell switch active INP (now)", f"${scheduler.cell.getActiveInpNormalizedLoadOfAllSwitches.toDouble * 100}%.2f%%"))
    entries.append(List("", "cell switch load (now)", s"${scheduler.cell.currentSwitchLoads().map(l => f"${l * 100}%.2f").mkString(", ")}%"))
    val detailedSwitchLoad: mutable.ArrayBuffer[String] = CellINPLoadStatistics.getSwitchLoadStats(100)
    val inpStatsEntriesPerProperty = 4
    if (detailedSwitchLoad.nonEmpty) {
      var offset = 0
      entries.append(List("", s"   detailed switch stats", (0 until scheduler.cell.resourceDimSwitches).map(
        i => s"  d$i use, % alloc, % total,% others".padTo(4 * 8 + 3, ' ')).mkString("[", ",", "]")))
      scheduler.cell.inpPropsOfCell.capabilities.foreach(capability => {
        entries.append(List("", s"   ${SwitchProps(capability)}",
          detailedSwitchLoad.
            slice(
              offset,
              offset + scheduler.cell.resourceDimSwitches * inpStatsEntriesPerProperty).
            map(s => s.reverse.padTo(8, ' ').reverse).
            mkString("[", ",", "]")))
        offset += scheduler.cell.resourceDimSwitches * inpStatsEntriesPerProperty
      })
    }

    val out = Tabulator.format(entries)
    if (printOnConsole)
      Console.println(out)

    if (scheduler.statisticsFlavorTakeInp + scheduler.statisticsFlavorTakeServer > statisticsTotalJobsWithInp) {
      logWarning(s"a scheduler reports wrong numbers! ${scheduler.statisticsFlavorTakeInp} +" +
        s" ${scheduler.statisticsFlavorTakeServer} > $statisticsTotalJobsWithInp")
    }

    out
  }

  @inline def currentTime(): simTime = time // ms

  def getPassedMsSinceSimulationStart: Long = (System.currentTimeMillis() - systemTimeSimulationStart)

  def msToHuman(ms: Long): String = {
    val timeH = TimeUnit.MILLISECONDS.toHours(ms)
    val timeM = TimeUnit.MILLISECONDS.toMinutes(ms) - timeH * 60
    val timeS = (ms / 1000).toInt - timeH * 3600 - timeM * 60
    val timeHuman = String.format("%dh:%02d:%02d.%03d", timeH, timeM, timeS, ms % 1000)
    timeHuman
  }

  class SpeedMeasurement(val spentMs: Long,
                         val deltaMs: Long,
                         val processedSimTime: SimTypes.simTime,
                         val deltaSimTime: SimTypes.simTime) {
    def -(that: SpeedMeasurement) =
      new SpeedMeasurement(
        spentMs = this.spentMs,
        deltaMs = this.spentMs - that.spentMs,
        processedSimTime = this.processedSimTime,
        deltaSimTime = this.processedSimTime - that.processedSimTime)
  }

  private class SimulationStep(val time: simTime) extends Comparable[SimulationStep] {
    lazy val events: mutable.Stack[Event] = mutable.Stack()

    def getNextEvent: Event = {
      events.removeHead()
    }

    def getHasMoreEvents: Boolean = {
      events.nonEmpty
    }

    def addEvent(event: Event): Unit = {

      if (SimulationConfiguration.SANITY_CHECKS_CELL)
        assert(time == event.time, s"Adding event at execution time ${event.time} to simulation step at execution time ${time}!")

      events += event
    }

    override def compareTo(other: SimulationStep): simTime = {

      if (SimulationConfiguration.SANITY_CHECKS_CELL)
        assert(time != other.time, "Found two simulation steps with the same execution time!")

      other.time.compareTo(time)

    }
  }

}

object Simulator {
  private var previousGcRun: Long = System.nanoTime()

  def gc(): Unit = {
    val now = System.nanoTime()
    if ((now - previousGcRun) / SimTypes.GIGA > SimulationConfiguration.SIM_GC_NOT_WITHIN_SECONDS) {
      previousGcRun = now
      System.gc()
    }
  }
}
