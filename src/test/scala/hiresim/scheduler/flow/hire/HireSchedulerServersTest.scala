package hiresim.scheduler.flow.hire

import hiresim.cell.{Cell, CellFactory}
import hiresim.simulation.configuration.{SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchProps, SwitchResource}
import hiresim.scheduler.flow.solver.mcmf.SuccessiveShortestSolver
import hiresim.simulation.{RandomManager, SimTypes, Simulator}
import hiresim.tenant.{Job, TaskGroup, TaskGroupConnection}
import hiresim.workload.WorkloadProvider
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

class HireSchedulerServersTest extends AnyFunSuite with BeforeAndAfterEach {


  var cell: Cell = _
  var simulator: Simulator = _
  var jobs: Seq[Job] = _
  var scheduler: HireScheduler = _
  var workload: WorkloadProvider = _

  override def beforeEach(): Unit = {

    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    // Data center physical cell initialization
    cell = CellFactory.newCell(
      // This will give us a cell with 16 servers
      k = 4,
      serverResources = Array[NumericalResource](20, 20),
      switchResources = new SwitchResource(Array(5), SwitchProps.all)
    )

    /** Making some subtrees unfeasible */
    cell.servers(0)(0) = 0 // ID 60
    cell.servers(0)(1) = 0 // ID 60
    cell.servers(2)(0) = 0 // ID 62
    cell.servers(2)(1) = 0 // ID 62

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
    val numTasksPerJob: Int = 7
    val resources: ServerResource = new ServerResource(Array(5, 5))

    /*  @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 61 (1) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 62 (2) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 63 (3) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 65 (5) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 72 (12) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 73 (13) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-0 on machine 74 (14) (resources: 15|15)  */

    val firstServerTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
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

    /*  @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 66 (6) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 67 (7) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 68 (8) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 69 (9) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 70 (10) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 71 (11) (resources: 15|15)
        @191 -> DEBUG HireScheduler: Registered 1 task of tg-1 on machine 75 (15) (resources: 15|15)  */

    val secondServerTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
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

    /*  @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 61 (1) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 62 (2) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 63 (3) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 64 (4) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 65 (5) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 67 (7) (resources: 15|15)
        @311 -> DEBUG HireScheduler: Registered 1 task of tg-2 on machine 75 (15) (resources: 15|15)  */

    val thirdServerTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
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
      taskGroups = Array[TaskGroup](firstServerTaskGroup, secondServerTaskGroup, thirdServerTaskGroup),
      arcs = Array(
        new TaskGroupConnection(firstServerTaskGroup, secondServerTaskGroup),
        new TaskGroupConnection(firstServerTaskGroup, thirdServerTaskGroup),
        new TaskGroupConnection(secondServerTaskGroup, firstServerTaskGroup),
        new TaskGroupConnection(secondServerTaskGroup, thirdServerTaskGroup),
        new TaskGroupConnection(thirdServerTaskGroup, firstServerTaskGroup),
        new TaskGroupConnection(thirdServerTaskGroup, secondServerTaskGroup)
      )
    )

    /** Allocating `job1` on a server */
    simulator.addJob(job)
    simulator.runSim(endTime = 1000)
  }
}
