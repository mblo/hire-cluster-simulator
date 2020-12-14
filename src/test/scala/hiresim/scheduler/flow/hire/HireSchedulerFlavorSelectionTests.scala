package hiresim.scheduler.flow.hire

import java.nio.file.Paths

import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchProps, SwitchResource}
import hiresim.scheduler.{ForceInpFlavor, KubernetesScheduler, SchedulerUtils, SparrowLikeQueueScheduler}
import hiresim.scheduler.flow.hire.HireLocalityHandlingMode.DEFAULT
import hiresim.scheduler.flow.hire.utils.SchedulerManagementUtils.{createAlibabaLikeJob, createSchedulerTestEnvironment, createTestEnvironment, setAvailableServerResources, setAvailableSwitchResources}
import hiresim.scheduler.flow.hire.utils.TaskGroupUtils
import hiresim.scheduler.flow.solver.Solver
import hiresim.scheduler.flow.solver.graph.FlowNode
import hiresim.scheduler.flow.solver.mcmf.SuccessiveShortestSolver
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.simulation.{ClosureEvent, RandomManager, Simulator}
import hiresim.tenant.{Job, SharedResources, TaskGroup, TaskGroupConnection}
import hiresim.workload.AlibabaClusterTraceWorkload.{RawJobSpec, RawTaskGroupSpec}
import hiresim.workload.{AlibabaClusterTraceWorkload, OneTimeWorkloadProvider, WorkloadProvider}
import javax.swing.plaf.DimensionUIResource
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers._

import scala.collection.{immutable, mutable}

class HireSchedulerFlavorSelectionTests extends AnyFunSuite with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {

    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.SANITY_CHECKS_CELL = true
    SimulationConfiguration.SANITY_CHECKS_GRAPH = true
    SimulationConfiguration.HIRE_BACKLOG_ENABLE = false
    SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME = true

    // For this test we only want to connect to necessary nodes
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP = -1
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP = -1
    // WILL be set later in each test case
    SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR = -1
  }

  def performFlavorSelectionBenchmark(solver: Solver,
                                      minQueuingTimeBeforePreemption: Int): Simulator = {
    val wlRandom = new RandomManager(42)

    val workload = new OneTimeWorkloadProvider(random = wlRandom,
      jobList = (0 until 6).map(_ => {
        createAlibabaLikeJob(submittedTime = Array(1),
          duration = Array(500),
          cpu = Array(1),
          mem = Array(1),
          setOfSwitchResourceTypes = SwitchProps.allTypes,
          instances = Array(1),
          hasInpFlavor = true,
          wlRandom = wlRandom,
          ratioOfIncTaskGroups = 0.8)
      }).toArray ++ Array(

        // this guy will not find INC resources.. so HIRE should assign server flavor directly
        createAlibabaLikeJob(submittedTime = Array(199),
          duration = Array(1000),
          cpu = Array(1.0),
          mem = Array(1.0),
          setOfSwitchResourceTypes = SwitchProps.allTypes,
          instances = Array(1),
          wlRandom = wlRandom,
          hasInpFlavor = true,
          ratioOfIncTaskGroups = 0.4)

      ) ++ (0 until 150).map(i => {
        createAlibabaLikeJob(submittedTime = Array(100, 500, 300, 600, 900),
          duration = Array(13001, 16001, 13002, 10303, 13004),
          cpu = Array(0.3, 0.3, 0.3, 0.3, 0.3),
          mem = Array(0.3, 0.3, 0.3, 0.3, 0.3),
          setOfSwitchResourceTypes = SwitchProps.allTypes,
          instances = Array.fill(5)(1),
          wlRandom = wlRandom,
          hasInpFlavor = true,
          ratioOfIncTaskGroups = 0.8)
      }).toArray)

    val allJobs: Array[Job] = workload.getAll.toArray

    val (cell, simulator) = createTestEnvironment(
      k = 4,
      switchMaxActiveInpTypes = 2,
      serverResource = Array[NumericalResource](1000, 1000),
      switchResource = new SwitchResource(Array(100, 100, 100), SwitchProps.all),
      workloadProvider = workload :: Nil
    )

    SharedResources.initializeWithCell(cell)
    AlibabaClusterTraceWorkload.setupSharedResourceDemand(100)

    val sched = new HireScheduler(s"Hire+${solver.name}", cell, solver, simulator,
      minQueuingTimeBeforePreemption = minQueuingTimeBeforePreemption,
      thinkTimeScaling = 100)

    //    val sched = new SparrowLikeQueueScheduler("sparrow", cell, simulator, flavorSelector = new ForceInpFlavor())

    simulator.setScheduler(sched)

    simulator
  }


  def performPackSwitchesBenchmark(solver: Solver): Unit = {
    val wlRandom = new RandomManager(42)
    val workload: OneTimeWorkloadProvider = new OneTimeWorkloadProvider(random = wlRandom,

      createAlibabaLikeJob(submittedTime = Array(0, 2000, 4000),
        duration = Array(10000, 10000, 10000),
        cpu = Array(0.1, 0.1, 0.1),
        mem = Array(0.1, 0.1, 0.1),
        setOfSwitchResourceTypes = Array(SwitchProps.typeR2p2),
        instances = Array(3, 3, 3),
        ratioOfIncTaskGroups = 1.0,
        wlRandom = wlRandom,
        hasInpFlavor = true) ::
        createAlibabaLikeJob(submittedTime = Array(1000, 3000, 5000),
          duration = Array(10000, 10000, 10000),
          cpu = Array(0.1, 0.1, 0.1),
          mem = Array(0.1, 0.1, 0.1),
          setOfSwitchResourceTypes = Array(SwitchProps.typeHarmonia),
          instances = Array(3, 3, 3),
          ratioOfIncTaskGroups = 1.0,
          wlRandom = wlRandom,
          hasInpFlavor = true) :: Nil)

    val (cell, simulator) = createTestEnvironment(
      k = 4,
      switchMaxActiveInpTypes = 2,
      serverResource = Array[NumericalResource](100, 100),
      switchResource = new SwitchResource(Array(100, 100, 100), SwitchProps.all),
      workloadProvider = workload :: Nil
    )

    SharedResources.initializeWithCell(cell)
    AlibabaClusterTraceWorkload.setupSharedResourceDemand(100)

    val sched = new HireScheduler("Hire", cell, solver, simulator,
      minQueuingTimeBeforePreemption = 1000, thinkTimeScaling = 100)
    simulator.setScheduler(sched)


    val maxSwitchTasksPerTaskGroupPerJob: Array[Int] = workload.getAll.toArray.flatMap(j => j.taskGroups).sortWith((t1, t2) => t1.submitted <= t2.submitted).map(tg => tg.numTasks)
    var activeSwitches: Int = 0

    (0 until 6).foreach(i => {
      simulator.scheduleActionWithDelay(s => {
        s.logInfo("check")
        s.printQuickStats()
        sched.getNumberOfNotFullyAndFullyScheduledJobsTillNow._1 should be(0)
        cell.getActiveSwitches should be <= (maxSwitchTasksPerTaskGroupPerJob(i) max activeSwitches)
        activeSwitches = cell.getActiveSwitches
      }, 500 + i * 1000)
    })

    simulator.runSim(1200000)
  }


}
