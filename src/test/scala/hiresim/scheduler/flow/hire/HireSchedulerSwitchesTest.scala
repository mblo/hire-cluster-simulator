package hiresim.scheduler.flow.hire

import hiresim.cell.{Cell, CellFactory}
import hiresim.simulation.configuration.{SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.scheduler.flow.solver.mcmf.SuccessiveShortestSolver
import hiresim.simulation.{RandomManager, SimTypes, Simulator}
import hiresim.tenant.{Job, TaskGroup, TaskGroupConnection}
import hiresim.workload.WorkloadProvider
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class HireSchedulerSwitchesTest extends AnyFunSuite with BeforeAndAfterEach {

  var cell: Cell = _
  var simulator: Simulator = _
  var jobs: Seq[Job] = _
  var scheduler: HireScheduler = _
  var workload: WorkloadProvider = _

  override def beforeEach(): Unit = {

    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    /** Data center physical cell initialization */
    cell = CellFactory.newCell(
      k = 4,
      serverResources = Array[NumericalResource](20, 20),
      switchResources = new SwitchResource(Array(20), SwitchProps.all)
    )

    /** Making some subtrees unfeasible */
    cell.servers(0)(0) = 0
    cell.servers(0)(1) = 0
    // There should be a direct STG -> SN arc to the second server
    // cell.servers(1)(0) = 0
    // cell.servers(1)(1) = 0

    /** Creating a simulator */
    val rand = new RandomManager(0)
    simulator = new Simulator(
      workloadProvider = Nil,
      randomManager = rand
    )

    /** Creating a scheduler */
    scheduler = new HireScheduler(
      name = "hire",
      cell,
      solver = new SuccessiveShortestSolver,
      simulator
    )
    simulator.setScheduler(scheduler)

    /** Enable sanity checks */
    SimulationConfiguration.SANITY_CHECKS_CELL = true
    SimulationConfiguration.SANITY_CHECKS_SCHEDULER = true
    scheduler.graphManager.graph.sanityChecks = true

  }

  override def afterEach(): Unit = {

  }

  test("stgOutgoingArcCosts") {

    /** Retrieving dummy server-only jobs */
    val duration: SimTypes.simTime = 10
    val submissionTime: SimTypes.simTime = 0
    val numTasksPerJob: Int = 4
    val resources: SwitchResource = new SwitchResource(Array(5), SwitchProps.typeHarmonia)
    val firstSwitchTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numTasksPerJob,
      resources = resources,
      allToAll = true
    )
    val secondSwitchTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numTasksPerJob,
      resources = resources,
      allToAll = true
    )
    val thirdSwitchTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime + 5,
      numTasks = numTasksPerJob,
      resources = resources,
      allToAll = true
    )
    val job: Job = new Job(
      isHighPriority = false,
      submitted = submissionTime,
      taskGroups = Array[TaskGroup](firstSwitchTaskGroup, secondSwitchTaskGroup, thirdSwitchTaskGroup),
      arcs = Array(
        new TaskGroupConnection(firstSwitchTaskGroup, secondSwitchTaskGroup),
        new TaskGroupConnection(firstSwitchTaskGroup, thirdSwitchTaskGroup),
        new TaskGroupConnection(secondSwitchTaskGroup, firstSwitchTaskGroup),
        new TaskGroupConnection(secondSwitchTaskGroup, thirdSwitchTaskGroup),
        new TaskGroupConnection(thirdSwitchTaskGroup, firstSwitchTaskGroup),
        new TaskGroupConnection(thirdSwitchTaskGroup, secondSwitchTaskGroup),
      )
    )

    /** Allocating `job1` on a server */
    simulator.addJob(job)
    simulator.runSim(endTime = 1000)
  }
}
