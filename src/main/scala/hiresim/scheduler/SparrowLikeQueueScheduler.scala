package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.simulation.SimTypes.simTime
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.{RandomManager, SimTypes, Simulator}
import hiresim.tenant.{Allocation, Job, TaskGroup}

import scala.collection.mutable
import scala.ref.WeakReference

class SparrowMachineLinker {
  val machines: mutable.BitSet = mutable.BitSet()
  var reChecks: Int = 0
  var isAskingForMore: Boolean = false

  def referenceCounter: Int = machines.size
}

class SparrowLikeQueueScheduler(override val name: String,
                                implicit override val cell: Cell,
                                implicit override val simulator: Simulator,
                                override val thinkTimeScaling: Double = 1.0,
                                override val schedulingComplexityPer100ThinkTime: Int = 5000,
                                val flavorSelector: FlavorSelector,
                                val samplingM: Int = 2,
                                val recheckTimeout: SimTypes.simTime = 1000,
                                val maxReChecks: Int = 10,
                                val maxSamplingWhenReCheck: Double = 1.0,
                                val applyServerFallback: Boolean = true) extends Scheduler(name = name, cell = cell,
  schedulingComplexityPer100ThinkTime = schedulingComplexityPer100ThinkTime,
  simulator = simulator, thinkTimeScaling = thinkTimeScaling)
  with SchedulerWithFlavorSelector with QueueBasedScheduler {

  protected val myRand: RandomManager = simulator.randomManager.copy

  // Sparrow runs fully distributed,
  // but our Sparrow uses centralized maps for re-inserting task groups in case more samples are required

  val rpcDelayWhenServersTriggerTheirReservation: SimTypes.simTime = 1 // based on the paper, 1ms
  val rpcProcessingThinkTime: SimTypes.simTime = (thinkTimeScaling * 20).toInt max 1

  private val tgToMachines: mutable.HashMap[TaskGroup, SparrowMachineLinker] = mutable.HashMap()

  private implicit val _this: SchedulerWithFlavorSelector = this // do I need to have this hack!?

  val reservationQueueServersLow: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] =
    Array.fill(cell.servers.length)(mutable.ArrayDeque())
  val reservationQueueServersHigh: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] =
    Array.fill(cell.servers.length)(mutable.ArrayDeque())
  val reservationQueueSwitchesLow: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] =
    Array.fill(cell.switches.length)(mutable.ArrayDeque())
  val reservationQueueSwitchesHigh: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] =
    Array.fill(cell.switches.length)(mutable.ArrayDeque())

  override def toString: String = s"SparrowSched($name)"

  override def getNumberOfPendingJobs: Int = allNotFinishedJobs.size

  protected val pendingTaskGroupsForScheduling: mutable.Queue[TaskGroup] = mutable.Queue()
  protected val rpcMessageQueue: mutable.Queue[SparrowLikeQueueScheduler => Unit] = mutable.Queue()


  protected var rpcMessagesLeftInThisThinkTime = 0

  override def getNumberOfPendingSchedulingObjects: simTime = rpcMessageQueue.size + pendingTaskGroupsForScheduling.size

  protected def refillRpcProcessingCount(): Unit = {
    rpcMessagesLeftInThisThinkTime += ((1.0 + cell.numMachines.toDouble / 1000.0)
      * schedulingComplexityPer100ThinkTime / 28.0).toInt max 1
  }

  private def enqueueRpcCall(call: SparrowLikeQueueScheduler => Unit): Unit = {
    rpcMessageQueue.enqueue(call)
    if (!isScheduling)
      schedule()
  }

  private def enqueueTaskGroup(taskGroup: TaskGroup): Unit = {
    val linker = tgToMachines(taskGroup)
    if (!linker.isAskingForMore) {
      linker.isAskingForMore = true
      pendingTaskGroupsForScheduling.enqueue(taskGroup)
    }
    if (!isScheduling)
      schedule()
  }

  override def addJobAndSchedule(newJob: Job): Unit = {
    allNotFinishedJobs.add(newJob)
    allNotScheduledJobs.add(newJob)
    flavorSelector.jobSubmitted(newJob)
    notifyMeForeachTaskGroupWhenReady(newJob)

    newJob.taskGroups.foreach(tg => {
      if (tg.submitted <= simulator.currentTime()) {
        logVerbose(s"add taskgroup ${tg.detailedToString()} to the pending list")
        initBackRef(tg)
        enqueueTaskGroup(tg)
      }
    })
    if (!isScheduling) {
      schedule()
    }
  }

  protected def checkReservationOnMachine(machine: Int, isSwitch: Boolean): Unit = {
    def removeOutdatedHeads(queue: mutable.ArrayDeque[WeakReference[TaskGroup]]): Unit = {
      while (queue.nonEmpty && queue.head.get.isEmpty) {
        queue.removeHead()
      }
    }

    // remove outdated entries
    if (isSwitch) {
      removeOutdatedHeads(reservationQueueSwitchesHigh(machine))
      removeOutdatedHeads(reservationQueueSwitchesLow(machine))
    } else {
      removeOutdatedHeads(reservationQueueServersHigh(machine))
      removeOutdatedHeads(reservationQueueServersLow(machine))
    }

    val affectedQueue: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] = if (isSwitch) {
      if (reservationQueueSwitchesHigh(machine).nonEmpty) {
        reservationQueueSwitchesHigh
      } else {
        reservationQueueSwitchesLow
      }
    } else {
      if (reservationQueueServersHigh(machine).nonEmpty) {
        reservationQueueServersHigh
      } else {
        reservationQueueServersLow
      }
    }

    if (affectedQueue(machine).nonEmpty) {
      val weakRefEntry: WeakReference[TaskGroup] = affectedQueue(machine).removeHead()
      assert(weakRefEntry.get.isDefined)

      val taskGroup: TaskGroup = weakRefEntry.apply()
      val job = taskGroup.job.get

      simulator.scheduleActionWithDelay(sim => {
        enqueueRpcCall(_ => {
          var reTriggerMachineCheck = false

          // check if this job is still valid (might be withdrawn)
          if (job.isDone || taskGroup.notStartedTasks == 0 || !job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)) {
            // job is done or task group is no remaining task to run or task group is not part of current selection, re-trigger check reservation
            logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) outdated")
            reTriggerMachineCheck = true
          } else {
            // job still valid and tasks pending

            // remove backref
            if (!updateBackRefForTaskGroup(taskGroup = taskGroup, machine = machine, add = false)) {
              simulator.logWarning(s"Releasing machine backref failed - for tg ${taskGroup.id}, of job ${job.detailedToString()}")
            }

            val startTasks = cell.checkMaxTasksToAllocate(taskGroup = taskGroup, machineId = machine)
            if (startTasks > 0) {
              // do allocation

              logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) valid allocation with $startTasks task(s)")

              val jobWasChosenBeforeNewAllocation = job.flavorHasBeenChosen
              val thinkTime = 1
              val alloc = job.doResourceAllocation(
                taskGroup = taskGroup,
                numberOfTasksToStart = startTasks,
                machine = machine,
                cell = cell,
                startTime = sim.currentTime() + thinkTime
              )

              val checkAllOtherActiveTaskGroupsOfJob =
                if (job.flavorHasBeenChosen && !jobWasChosenBeforeNewAllocation) {
                  if (job.statisticsInpChosen)
                    statisticsFlavorTakeInp += 1
                  else
                    statisticsFlavorTakeServer += 1

                  true
                } else false

              // was this the last allocation?
              val andInformAllMachines = taskGroup.notStartedTasks == 0

              applyAllocationAfterThinkTime(
                allocations = alloc :: Nil,
                thinkTime = thinkTime,
                andRunScheduleAfterwards = false,
                doNotUpdateThinkTimeStatistics = true,
                runAfterThinkTime = () => {
                  // check if something else can be started on this machine
                  checkReservationOnMachine(machine = machine, isSwitch = isSwitch)

                  // this was the last allocation of this task group, so inform all machines, and remove linker
                  if (andInformAllMachines) {
                    logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) this was last allocation, so remove all other triggers")
                    reCheckAndRemoveAllLinkedMachinesOfTaskGroup(taskGroup)
                  }

                  // if this allocation set the flavor, check all other task groups that are already submitted for outdated entries
                  if (checkAllOtherActiveTaskGroupsOfJob) {
                    val now = sim.currentTime()
                    for (tg <- job.taskGroups) {
                      if (tg.submitted <= now && tg.notStartedTasks > 0 && !job.checkIfTaskGroupMightBeInFlavorSelection(tg))
                        if (tgToMachines.contains(tg))
                          reCheckAndRemoveAllLinkedMachinesOfTaskGroup(tg)
                    }

                  }
                },
                ignoreIsSchedulingFlag = true)
            } else {
              // cannot start task because resources do not fit
              // is this a valid machine anyway?
              val isValidMachine: Boolean = 0 < cell.checkMaxTasksToAllocate(
                taskGroup = taskGroup,
                machineId = machine,
                ignoreRunningTasksOfTaskGroup = true,
                checkWithMaxMachineResources = true)

              reTriggerMachineCheck = true
              if (isValidMachine) {
                if (updateBackRefForTaskGroup(taskGroup = taskGroup, machine = machine, add = true)) {
                  logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) does not fit now, so add again")
                  affectedQueue(machine).append(weakRefEntry)
                }
              } else {
                logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) does not fit")
                // no, this is not a valid machine, so check the next option
              }
            }

            // shall we create more samplings?
            if (taskGroup.notStartedTasks > 0 && tgToMachines(taskGroup).referenceCounter == 0) {
              logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- job (${job.id}) asks for more samplings")
              enqueueTaskGroup(taskGroup)
            }
          }

          if (reTriggerMachineCheck) {
            logDetailedVerbose(s"check queue on machine:$machine switch:$isSwitch -- retrigger check")
            enqueueRpcCall(_ => {
              sim.scheduleActionWithDelay(_ => {
                this.checkReservationOnMachine(machine = machine, isSwitch = isSwitch)
              }, rpcDelayWhenServersTriggerTheirReservation)
            })
          }

        })
      }, rpcDelayWhenServersTriggerTheirReservation)


    }
  }

  override protected def someTasksAreDoneAndFreedResources(alloc: Allocation): Unit = {
    checkReservationOnMachine(alloc.machine, alloc.taskGroup.isSwitch)
    schedule()
  }

  override def taskGroupNowReady(job: Job, taskGroup: TaskGroup): Unit = {
    assert(taskGroup.submitted == simulator.currentTime())
    initBackRef(taskGroup)
    enqueueTaskGroup(taskGroup)
    schedule()
  }

  @inline protected def initBackRef(taskGroup: TaskGroup) = if (!tgToMachines.contains(taskGroup)) {
    def reCheckAction(s: Simulator): Unit = {
      val job = taskGroup.job.get
      if (taskGroup.notStartedTasks > 0 &&
        !job.isDone &&
        !job.isCanceled &&
        job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)) {

        logDetailedVerbose(s"reconsider for more checks, ${taskGroup.detailedToString()}")
        tgToMachines(taskGroup).reChecks += 1

        var tgIsUpdated = false
        if (tgToMachines(taskGroup).reChecks >= maxReChecks) {
          // apply server fallback if necessary
          if (applyServerFallback
            && flavorSelector.allowSchedulerToWithdrawJobAndResubmit
            && taskGroup.isSwitch
            && job.statisticsInpChosen
            && taskGroup.belongsToInpFlavor) {
            logVerbose(s"Apply server fallback ${job.detailedToString()}")
            tgIsUpdated = flavorSelector.applyServerFallback(job, replaceJobAndResubmitIfNecessary = true)
          }
        }

        if (!tgIsUpdated) {
          // do we have already too many sampling?
          if (maxSamplingWhenReCheck >
            (tgToMachines(taskGroup).referenceCounter.toDouble /
              (if (taskGroup.isSwitch) cell.numSwitches else cell.numServers))) {

            enqueueTaskGroup(taskGroup)
            schedule()
          }
        }
        simulator.scheduleActionWithDelay(reCheckAction, recheckTimeout)
      }
    }

    simulator.scheduleActionWithDelay(reCheckAction, recheckTimeout)
    tgToMachines.addOne((taskGroup, new SparrowMachineLinker()))
  }

  protected def updateBackRefForTaskGroup(taskGroup: TaskGroup, machine: Int, add: Boolean): Boolean = {
    val map = tgToMachines.get(taskGroup)
    if (map.isDefined) {
      if (add)
        map.get.machines.add(machine)
      else
        map.get.machines.remove(machine)
    }
    else false
  }

  override protected def schedule(): Unit = {
    if (!isScheduling && cell.currentServerLoadMax().toDouble <= SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE) {
      isScheduling = true

      // is there any reservation from a high prio queue?
      if (rpcMessageQueue.nonEmpty) {
        while (rpcMessagesLeftInThisThinkTime > 0 && rpcMessageQueue.nonEmpty) {
          val rpcMessage = rpcMessageQueue.dequeue()
          rpcMessage(this)

          rpcMessagesLeftInThisThinkTime -= 1
        }
      }

      val actions: mutable.ArrayDeque[Simulator => Unit] = mutable.ArrayDeque()
      if (pendingTaskGroupsForScheduling.nonEmpty) {
        /**
         * To schedule using batch sampling, a scheduler randomly selects dm worker machines (for d ≥ 1).
         * The scheduler sends a probe to each of the dm workers;
         * as with per-task sampling, each worker replies with the number of queued tasks.
         * The scheduler places one of the job’s m tasks on each of the m least loaded workers.
         * Unless otherwise specified, we use d = 2;
         *
         * With late binding, workers do not reply immediately to probes and instead place a
         * reservation for the task at the end of an internal work queue.
         * When this reservation reaches the front of the queue, the worker sends an RPC to
         * the scheduler that initiated the probe requesting a task for the corresponding job.
         * The scheduler assigns the job’s tasks to the first m workers to reply, and replies to the
         * remaining (d − 1)m workers with a no-op signaling that all of the job’s tasks have been launched.
         */
        while (rpcMessagesLeftInThisThinkTime > 0 && pendingTaskGroupsForScheduling.nonEmpty) {
          val nextTaskGroup = pendingTaskGroupsForScheduling.dequeue()
          val job = nextTaskGroup.job.get
          // is this task group still valid?
          if (!job.isDone && nextTaskGroup.notStartedTasks > 0 && job.checkIfTaskGroupMightBeInFlavorSelection(nextTaskGroup)) {
            val linker = tgToMachines(nextTaskGroup)
            assert(linker.isAskingForMore)
            linker.isAskingForMore = false

            logDetailedVerbose(s"process next tg (${nextTaskGroup.id}), rpcMessagesLeftInThisThinkTime:$rpcMessagesLeftInThisThinkTime")

            val possibleIndices: mutable.BitSet = {
              val all = mutable.BitSet.fromSpecific(if (nextTaskGroup.isSwitch) cell.switches.indices else cell.servers.indices)
              all.&~=(linker.machines)
            }
            // ask never for more machines than available
            val askForChoices = {
              val tasks = nextTaskGroup.numTasks * samplingM
              if (tasks > possibleIndices.size) {
                possibleIndices.size
              } else {
                tasks
              }
            }

            if (askForChoices > 0) {
              val choices: Array[Int] = myRand.getChoicesOfRange(askForChoices, possibleIndices.last + 1)

              rpcMessagesLeftInThisThinkTime -= choices.length

              val isHighPrio = nextTaskGroup.job.get.isHighPriority
              val affectedQueue: Array[mutable.ArrayDeque[WeakReference[TaskGroup]]] = if (nextTaskGroup.isSwitch) {
                if (isHighPrio)
                  reservationQueueSwitchesHigh
                else {
                  reservationQueueSwitchesLow
                }
              } else {
                if (isHighPrio)
                  reservationQueueServersHigh
                else
                  reservationQueueServersLow
              }

              logDetailedVerbose(s"do scheduling for ${nextTaskGroup.detailedToString()}, drawing a slice of $askForChoices machines, " +
                s"of prio high:$isHighPrio")

              actions.addOne((sim: Simulator) => {
                if (!job.isDone)
                  for (machine <- choices) {
                    if (updateBackRefForTaskGroup(taskGroup = nextTaskGroup, machine = machine, add = true)) {
                      affectedQueue(machine).append(new WeakReference[TaskGroup](nextTaskGroup))
                      if (affectedQueue(machine).size == 1) {
                        logDetailedVerbose("machine queue length was 1 when item was added, " +
                          "trigger check reservations")
                        checkReservationOnMachine(machine, nextTaskGroup.isSwitch)
                      }
                    }
                  }
              })
            }
          }
        }
      }

      def sendRpc(): Unit = {
        // put into queue by sending RPC, inform machine in case
        if (actions.nonEmpty) {
          logDetailedVerbose(s"prepare rpc send actions, delay:$rpcDelayWhenServersTriggerTheirReservation")
          simulator.scheduleActionWithDelay(sim => {
            for (action <- actions) {
              action(sim)
            }
          }, rpcDelayWhenServersTriggerTheirReservation)
        }
      }

      if (rpcMessagesLeftInThisThinkTime <= 0) {
        logDetailedVerbose(s"no rpc processing time left, so consume think time $rpcProcessingThinkTime")
        simulator.scheduleActionWithDelay(sim => {
          isScheduling = false
          refillRpcProcessingCount()

          sendRpc()

          statisticsTotalThinkTime += rpcProcessingThinkTime
          schedule()
        }, rpcProcessingThinkTime)
      } else {
        isScheduling = false
        sendRpc()
      }
    }
  }


  protected def reCheckAndRemoveAllLinkedMachinesOfTaskGroup(taskGroup: TaskGroup) = {
    val linkerOpt = tgToMachines.remove(taskGroup)
    linkerOpt match {
      case Some(linker) =>
        for (machine <- linker.machines) {
          enqueueRpcCall(_ => {
            checkReservationOnMachine(machine, taskGroup.isSwitch)
          })
        }
      case None =>
    }
  }

  protected def reCheckAndRemoveAllLinkedMachinesOfJob(job: Job) = {
    for (tg <- job.taskGroups) {
      reCheckAndRemoveAllLinkedMachinesOfTaskGroup(tg)
    }
  }

  registerJobDoneHook(reCheckAndRemoveAllLinkedMachinesOfJob)

  override def jobWasUpdated(job: Job): Unit = {
    // re-insert all task groups that might be already in the system
    if (job.isCanceled) {
      // trigger all affected machines to check again, maybe there was HOL blocking due to this job
      reCheckAndRemoveAllLinkedMachinesOfJob(job)
    } else {

      for (tg <- job.taskGroups) {
        if (tg.submitted <= simulator.currentTime() && tg.notStartedTasks > 0) {
          initBackRef(tg)
          enqueueTaskGroup(tg)
        }
      }

    }
    if (!isScheduling) {
      schedule()
    }
  }

  override def isPendingJobQueueEmpty: Boolean = pendingTaskGroupsForScheduling.isEmpty


}


