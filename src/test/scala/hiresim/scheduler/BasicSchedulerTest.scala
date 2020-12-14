package hiresim.scheduler

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.scheduler.flow.hire.utils.SchedulerManagementUtils.{createAlibabaLikeJob, createTestEnvironment}
import hiresim.simulation.RandomManager
import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.tenant.Job
import hiresim.workload.{OneTimeWorkloadProvider, WorkloadProvider}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable


class BasicSchedulerTest extends AnyFunSuite with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {

    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED = false
    SimulationConfiguration.LOGGING_VERBOSE_CELL = false

    // For this test we only want to connect to necessary nodes
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP = -1
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP = -1
    SimulationConfiguration.HIRE_BACKLOG_ENABLE = false
  }

  SchedulerUtils.onExecuteWithEachScheduler((name, schedulerGen) => {
    test(s"ThinkTimeTest.${name}") {

      val wlRandom = new RandomManager(42)

      SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = SharedResourceMode.DEACTIVATED
      SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME = true
      SimulationConfiguration.TIME_IT_ACTIVATE = true
      SimulationConfiguration.TIME_IT_ACTIVATE_PRINT = true
      SimulationConfiguration.SANITY_CHECKS_HIRE = false
      SimulationConfiguration.SANITY_CHECK_HIRE_CHECK_IF_UNSCHEDULED = false
      SimulationConfiguration.SANITY_CHECK_JOB_CHECK_RESOURCE_REQ = false
      SimulationConfiguration.SANITY_CHECKS_CELL = false
      SimulationConfiguration.SANITY_CHECKS_SCHEDULER = false
      SimulationConfiguration.SANITY_MCMF_COST_SCALING_SANITY_CHECKS = false
      SimulationConfiguration.SANIYY_MCMF_CHECKS = false

      // run sim for 2000
      val workload = new OneTimeWorkloadProvider(random = wlRandom,
        jobList = Array(
          createAlibabaLikeJob(submittedTime = Array(250, 500),
            duration = Array(1200, 1200),
            cpu = Array(.6, 1),
            mem = Array(.6, 1.0),
            instances = Array(2, 2),
            hasInpFlavor = false),
          createAlibabaLikeJob(submittedTime = Array(0),
            duration = Array(1000),
            cpu = Array(.6),
            mem = Array(.6),
            instances = Array(2),
            hasInpFlavor = false)
        ) ++ (0 until 20).map(i => {
          createAlibabaLikeJob(submittedTime = Array(13000, 13001),
            duration = Array(303, 1004),
            cpu = Array(0.01, 0.01),
            mem = Array(0.01, 0.01),
            setOfSwitchResourceTypes = SwitchProps.allTypes,
            instances = Array(5, 5),
            hasInpFlavor = true)
        }).toArray ++ Array(
          createAlibabaLikeJob(submittedTime = Array(120, 130),
            duration = Array(wlRandom.getIntBetween(1, 100), 10),
            cpu = Array(0.1, 0.1),
            mem = Array(0.1, 0.1),
            setOfSwitchResourceTypes = SwitchProps.allTypes,
            instances = Array(1000, 5000),
            hasInpFlavor = false),

          createAlibabaLikeJob(submittedTime = Array(250, 315),
            duration = Array(wlRandom.getIntBetween(1, 100), 10),
            cpu = Array(0.1, 0.1),
            mem = Array(0.1, 0.1),
            setOfSwitchResourceTypes = SwitchProps.allTypes,
            instances = Array(1000, 1000),
            hasInpFlavor = false),

          createAlibabaLikeJob(submittedTime = Array(600, 800),
            duration = Array(wlRandom.getIntBetween(1, 100), 10),
            cpu = Array(0.1, 0.1),
            mem = Array(0.1, 0.1),
            setOfSwitchResourceTypes = SwitchProps.allTypes,
            instances = Array(2000, 1000),
            hasInpFlavor = false)
        ))

      // Create a basic environment to test
      val (cell, simulator) = createTestEnvironment(
        k = 16,
        serverResource = Array[NumericalResource](100, 100),
        switchResource = new SwitchResource(Array(100, 100, 100), SwitchProps.all),
        workloadProvider = workload :: Nil
      )
      val sched = schedulerGen(cell, simulator)
      simulator.setScheduler(sched)

      simulator.runSim(15000)

      val expectedThinkTime: Long = Scheduler.getExpectedTotalThinkTime(simulator.scheduler)

      val actual = simulator.scheduler.statisticsTotalThinkTime + simulator.scheduler.statisticsTotalThinkTimeWasted

      val upper = 1.09
      val lower = .91
      println(f"actual think time of ${actual} should be in ${lower * 100}%1.0f%% / ${upper * 100}%1.0f%%" +
        f" of $expectedThinkTime, i.e. ${lower * expectedThinkTime}%1.0f < ? < ${upper * expectedThinkTime}%1.0f")

      actual.toLong should be < (expectedThinkTime.toDouble * upper).toLong
      actual.toLong should be > (expectedThinkTime.toDouble * lower).toLong
    }
  })


  SchedulerUtils.onExecuteWithEachScheduler((name, schedulerGen) => {
    test(s"SchedulerStatistics.${name}") {

      val wlRandom = new RandomManager(42)

      // 1 job with tgs at 250 and 500 time, all with duration of 1200
      // 1 job with tgs at 0, with duration of 1000

      // at 200, done 0, fully scheduled 1
      // at 251, done 0, fully scheduled 1, not scheduled 1
      // at 450, done 0, fully scheduled 1, 1 not scheduled with future tasks, 1 scheduled till now

      SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = SharedResourceMode.DEACTIVATED

      // run sim for 2000
      val workload = new OneTimeWorkloadProvider(random = wlRandom,
        jobList = Array(
          createAlibabaLikeJob(submittedTime = Array(250, 500),
            duration = Array(1200, 1200),
            cpu = Array(.6, 1),
            mem = Array(.6, 1.0),
            instances = Array(2, 2),
            hasInpFlavor = false),

          createAlibabaLikeJob(submittedTime = Array(1234, 1235, 1900),
            duration = Array(1200, 1200, 1900),
            cpu = Array(.1, .1, .1),
            mem = Array(.1, .1, .1),
            instances = Array(3, 3, 1),
            hasInpFlavor = true),
          createAlibabaLikeJob(submittedTime = Array(1234, 1235, 1900),
            duration = Array(800, 800, 1900),
            cpu = Array(.1, .1, .1),
            mem = Array(.1, .1, .1),
            instances = Array(3, 3, 10),
            hasInpFlavor = false),

          createAlibabaLikeJob(submittedTime = Array(0),
            duration = Array(1000),
            cpu = Array(.6),
            mem = Array(.6),
            instances = Array(2),
            hasInpFlavor = false)
        ) ++ (0 until 50).map(i => {
          createAlibabaLikeJob(submittedTime = Array(13000, 13001, 13002, 13003, 13004),
            duration = Array(13001, 16001, 13002, 10303, 13004),
            cpu = Array(0.3, 0.3, 0.3, 0.3, 0.3),
            mem = Array(0.3, 0.3, 0.3, 0.3, 0.3),
            setOfSwitchResourceTypes = SwitchProps.allTypes,
            instances = Array.fill(5)(1),
            hasInpFlavor = true)
        }).toArray)

      val allJobs: Array[Job] = workload.getAll.toArray


      // Create a basic environment to test
      val (cell, simulator) = createTestEnvironment(
        k = 4,
        serverResource = Array[NumericalResource](100, 100),
        switchResource = new SwitchResource(Array(100, 100, 100), SwitchProps.all),
        workloadProvider = workload :: Nil
      )

      CellINPLoadStatistics.activate(
        simulator = simulator,
        cell = cell,
        inp_types = SwitchProps.allTypes,
        file = None,
        interval = 0
      )

      val sched = schedulerGen(cell, simulator)
      simulator.setScheduler(sched)


      // at 200, done 0, fully scheduled 1
      // at 251, done 0, fully scheduled 1, not scheduled 1
      // at 450, done 0, fully scheduled 1, 1 not scheduled with future tasks, 1 scheduled till now

      simulator.scheduleActionWithDelay(s => {
        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(0)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(1)
        assert(s.scheduler.statisticsSchedulingAttempts > 0)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(0)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(0)
      }, 210)


      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(0)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(1)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(1)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(1)
      }, 300)

      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(0)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(1)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(1)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(1)
      }, 450)

      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(0)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(2)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(0)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(0)
      }, 605)


      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(1)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(2)
        s.scheduler.statisticsFlavorTakeInp.shouldBe(0)
        s.scheduler.statisticsFlavorTakeServer.shouldBe(0)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(0)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(0)
      }, 1150)

      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(1)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(2)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(2)
        notFullyScheduledTillNow.shouldBe(2)
        fullyScheduledTillNow.shouldBe(0)
      }, 1235)


      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(1)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(2)
        s.scheduler.statisticsTotalJobsFullyScheduledWithInp.shouldBe(0)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(2)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(2)
      }, 1434)

      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(2)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(2)
        s.scheduler.statisticsTotalJobsFullyScheduledWithInp.shouldBe(0)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(2)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(2)
      }, 1850)
      simulator.scheduleActionWithDelay(s => {
        //        s.printQuickStats()
        val (notFullyScheduledTillNow, fullyScheduledTillNow) = s.scheduler.getNumberOfNotFullyAndFullyScheduledJobsTillNow
        s.scheduler.statisticsTotalJobsDone.shouldBe(2)
        s.scheduler.statisticsTotalJobsFullyScheduled.shouldBe(4)
        (s.scheduler.statisticsFlavorTakeInp + s.scheduler.statisticsFlavorTakeServer).shouldBe(1)
        s.scheduler.statisticsTotalJobsFullyScheduledWithInp.shouldBe(s.scheduler.statisticsFlavorTakeInp)
        s.scheduler.getNumberOfNotFullyScheduledJobsIncludingFutureTaskGroupSubmissions.shouldBe(0)
        notFullyScheduledTillNow.shouldBe(0)
        fullyScheduledTillNow.shouldBe(0)
        assert(s.scheduler.statisticsSchedulingAttempts >= 3)
      }, 2100)


      val allJobsSeen: mutable.ArrayBuffer[Job] = mutable.ArrayBuffer()
      simulator.activateDebugHookInformAboutJobArrival(j => {
        allJobsSeen.append(j)
      })

      simulator.registerSimDoneHook(s => {
        s.logInfo(s"Validate now all statistics; Scheduler claims to have ${s.scheduler.getNumberOfPendingJobs} pending jobs.")

        var (claimedAllocated, claimedNotAllocated, claimedINC, claimedServer, claimedNoFlavor) = (0, 0, 0, 0, 0)
        sched.getNotFinishedJobs.foreach(j => {
          if (j.allPossibleOptions.isEmpty) {
            claimedNoFlavor += 1
          } else {
            if (j.flavorHasBeenChosen && j.statisticsInpChosen) {
              claimedINC += 1
            } else if (j.flavorHasBeenChosen && !j.statisticsInpChosen) {
              claimedServer += 1
            }
          }
          assert(!j.isDone, s"there is a finished job in the notFinishedList! ${j.detailedToString()}")
          if (j.checkIfFullyAllocated()) {
            claimedAllocated += 1
          } else {
            claimedNotAllocated += 1
          }
        })

        var (failSafeDone, failSafeCanceled, failSafeAllocated, failSafeAllocatedInc, failSafeAllocatedServer,
        failSafeINC, failSafeServer, failSafeNoFlavor) = (0, 0, 0, 0, 0, 0, 0, 0)
        assert(allJobsSeen.size >= allJobs.size)
        allJobsSeen.foreach(j => {
          if (j.isCanceled) {
            failSafeCanceled += 1
          } else {
            if (j.isDone) {
              failSafeDone += 1
            }

            if (j.allPossibleOptions.isEmpty || j.allPossibleOptions.size == 1) {
              failSafeNoFlavor += 1
            } else {
              if (j.flavorHasBeenChosen && j.statisticsInpChosen) {
                failSafeINC += 1
              } else if (j.flavorHasBeenChosen && !j.statisticsInpChosen) {
                failSafeServer += 1
              }
            }
            if (j.checkIfFullyAllocated()) {
              failSafeAllocated += 1
              if (WorkloadProvider.flavorHasInp(j.chosenJobOption)) {
                assert(j.statisticsInpChosen, "flag is not set")
                failSafeAllocatedInc += 1
              } else {
                assert(!j.statisticsInpChosen, "flag is wrongly set")
                failSafeAllocatedServer += 1
              }
            }
          }
        }
        )

        s.printQuickStats()

        s.logInfo(s"claimedDone:${sched.statisticsTotalJobsDone},claimedNotAllocated:${claimedNotAllocated} " +
          s"claimedDoneInp:${sched.statisticsTotalJobsDoneWithInp}, " +
          s"claimedAllocated:$claimedAllocated, claimedINC:$claimedINC, " +
          s"claimedServer:$claimedServer, claimedNoFlavor:$claimedNoFlavor")

        s.logInfo(s"failSafeDone:$failSafeDone, failSafeCanceled:$failSafeCanceled, " +
          s"failSafeAllocated:$failSafeAllocated" +
          s" ($failSafeAllocatedInc|$failSafeAllocatedServer), failSafeINC:$failSafeINC, " +
          s"failSafeServer:$failSafeServer, failSafeNoFlavor:$failSafeNoFlavor")

        Console.out.flush()

        sched.statisticsTotalJobsDone.shouldBe(failSafeDone)
        sched.statisticsFlavorTakeServer.shouldBe(failSafeServer)
        sched.statisticsServerFallbackResubmit.shouldBe(failSafeCanceled)
        sched.statisticsTotalJobsFullyScheduled.shouldBe(failSafeAllocated)
        sched.statisticsTotalJobsFullyScheduledWithInp.shouldBe(failSafeAllocatedInc)

        val diffNewJobs = allJobsSeen.size - allJobs.size
        sched.statisticsServerFallbackResubmit.shouldBe(diffNewJobs)

        s.statisticsTotalJobs.shouldBe(allJobsSeen.size)

        claimedNotAllocated.shouldBe(s.statisticsTotalJobs - failSafeAllocated - failSafeCanceled)

        //        s.printQuickStats()
      })


      simulator.runSim(80000)
    }

  }
  )

}
