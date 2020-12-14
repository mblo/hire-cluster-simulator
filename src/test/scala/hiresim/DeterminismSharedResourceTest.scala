package hiresim

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{Identifiable, ServerResource, SwitchProps, SwitchResource}
import hiresim.cell.{Cell, CellFactory}
import hiresim.scheduler.flow.coco.CoCoScheduler
import hiresim.scheduler.flow.hire.HireScheduler
import hiresim.scheduler.flow.solver.mcmf.{CostScalingSolver, RelaxationSolver, SuccessiveShortestSolver}
import hiresim.scheduler.{YarnCapacityScheduler, _}
import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.simulation.{RandomManager, SimTypes, Simulator}
import hiresim.tenant._
import hiresim.workload.{OneTimeWorkloadProvider, WorkloadProvider}
import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{ThreadSignaler, TimeLimitedTests}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import scala.collection.{immutable, mutable}

class DeterminismSharedResourceTest extends AnyFunSuite with BeforeAndAfterEach
  with TimeLimitedTests {

  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.0000000001)

  override def timeLimit: Span = 300.seconds

  override val defaultTestSignaler = ThreadSignaler

  val queueingTimeBeforePreemption: Int = 30000
  val constantSimTime: Int = 300000
  val constantSeed = 42
  val constantMaxTasks = 20
  val constantMaxTaskDuration = 70000
  val totalJobs = 60


  override protected def beforeEach(): Unit = {
    System.gc()

    Allocation.lastId = -1L
    Identifiable.previousId = 0L

    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    SimulationConfiguration.TIME_IT_ACTIVATE = false
    SimulationConfiguration.TIME_IT_ACTIVATE_PRINT = false

    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED = false
    SimulationConfiguration.LOGGING_VERBOSE_CELL = false

    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK = false
    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_CYCLE_FREE = false
    SimulationConfiguration.SANITY_CHECK_HIRE_CHECK_IF_UNSCHEDULED = true
    SimulationConfiguration.SANITY_CHECKS_HIRE = true
    SimulationConfiguration.SANITY_CHECKS_CELL = true
    SimulationConfiguration.SANITY_CHECK_JOB_CHECK_RESOURCE_REQ = true

    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP = 100
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP = 100
    SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME = true
    SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT = 100
    SimulationConfiguration.HIRE_BACKLOG_SUPPLY_SOFT_LIMIT = 100
    SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS = false
    SimulationConfiguration.PRECISION = 100
    SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE = 0.99

    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER = 10000
    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER = 40000
    SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR = 2.0

    CellINPLoadStatistics.activated = false

    SimulationConfiguration.TIME_IT_ACTIVATE_PRINT = true
    SimulationConfiguration.TIME_IT_ACTIVATE = true
  }

  def createArrayWithRangeValues(maxValues: Array[NumericalResource], randomManager: RandomManager): Array[NumericalResource] = {
    val cloned = maxValues.clone()
    cloned.indices.foreach(i => {
      cloned(i) = randomManager.getInt(maxValues(i)) + 1
    })
    cloned
  }

  def createServerJob(numServerTaskGroups: Int, submissionTime: SimTypes.simTime, randomManager: RandomManager, cell: Cell): Job = {
    val tgs = (0 until numServerTaskGroups).map(_ => {
      val duration = randomManager.getInt(constantMaxTaskDuration) + 1
      new TaskGroup(
        isSwitch = false,
        isDaemonOfJob = false,
        inOption = WorkloadProvider.emptyFlavor,
        notInOption = WorkloadProvider.emptyFlavor,
        duration = duration,
        statisticsOriginalDuration = duration,
        submitted = submissionTime,
        numTasks = randomManager.getInt(constantMaxTasks) + 1,
        resources = new ServerResource(createArrayWithRangeValues(cell.serverResources(0).map(x => x / 4), randomManager))
      )
    })

    val tgConnections = (randomManager.getInt(numServerTaskGroups) until numServerTaskGroups).map(i => {
      new TaskGroupConnection(
        src = tgs(i),
        dst = tgs(randomManager.getInt(numServerTaskGroups))
      )
    })

    new Job(
      submitted = submissionTime,
      isHighPriority = false,
      taskGroups = tgs.toArray,
      arcs = tgConnections.toArray
    )
  }

  def createPseudoTypeAIncLikeJob(numServerTasks: Int, numSwitchTasks: Int, submissionTime: SimTypes.simTime,
                                  randomManager: RandomManager, cell: Cell): Job = {
    val duration = randomManager.getInt(constantMaxTaskDuration) + 1
    val (inpOption: immutable.BitSet,
    serverOption: immutable.BitSet,
    jobOptions: immutable.BitSet) = WorkloadProvider.simpleJobIncServerAllFlavorBits

    val serverTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
      isDaemonOfJob = false,
      inOption = serverOption,
      notInOption = inpOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numServerTasks,
      resources = new ServerResource(createArrayWithRangeValues(cell.serverResources(0).map(x => x / 4), randomManager))
    )

    val incTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = inpOption,
      notInOption = serverOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numSwitchTasks,
      resources = new SwitchResource(
        createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), randomManager),
        properties = SwitchProps.typeHarmonia
      ),
      coLocateOnSameMachine = false
    )

    val serversToIncArc: TaskGroupConnection = new TaskGroupConnection(
      src = serverTaskGroup,
      dst = incTaskGroup
    )
    val serversArc: TaskGroupConnection = new TaskGroupConnection(
      src = incTaskGroup,
      dst = serverTaskGroup
    )

    new Job(
      submitted = submissionTime,
      isHighPriority = false,
      taskGroups = Array[TaskGroup](serverTaskGroup, incTaskGroup),
      arcs = Array(serversToIncArc, serversArc)
    )
  }

  def createPseudoTypeBIncLikeJobWithTwoFlavors(numServerTasks: Int, numSwitchTasks: Int,
                                                submissionTime: SimTypes.simTime, randomManager: RandomManager
                                                , cell: Cell): Job = {
    val duration = randomManager.getInt(constantMaxTaskDuration) + 1

    /** The minimum number of server tasks will be 2, since they have to be split in two subgroups */
    var actualNumServerTasks: Int = numServerTasks
    if (numServerTasks <= 1) {
      actualNumServerTasks = 2
    }
    val (inpOption: immutable.BitSet,
    serverOption: immutable.BitSet,
    jobOptions: immutable.BitSet) = WorkloadProvider.simpleJobIncServerAllFlavorBits

    val numProducerTasks: Int = actualNumServerTasks / 2

    val producerTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numProducerTasks,
      resources = new ServerResource(createArrayWithRangeValues(cell.serverResources(0).map(x => x / 4), randomManager))
    )

    val taskGroupSwitchesOnly: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = inpOption,
      notInOption = serverOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numSwitchTasks,
      resources = new SwitchResource(
        createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), randomManager),
        properties = SwitchProps.typeSharp
      ),
      coLocateOnSameMachine = false
    )
    val taskGroupSwitchesOnly2ndDummy: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = inpOption,
      notInOption = serverOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numSwitchTasks,
      resources = new SwitchResource(
        createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), randomManager),
        properties = SwitchProps.typeSharp
      ),
      coLocateOnSameMachine = false
    )

    val taskGroupSwitchesOnly2ndDummyEverywhere: TaskGroup = new TaskGroup(
      isSwitch = true,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numSwitchTasks,
      resources = new SwitchResource(
        createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), randomManager),
        properties = SwitchProps.typeSharp
      ),
      coLocateOnSameMachine = false
    )

    val taskGroupServersOnly: TaskGroup = new TaskGroup(
      isSwitch = false,
      isDaemonOfJob = false,
      inOption = serverOption,
      notInOption = inpOption,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime,
      numTasks = numServerTasks,
      resources = new ServerResource(createArrayWithRangeValues(cell.serverResources(0), randomManager))
    )

    val consumerTaskGroup: TaskGroup = new TaskGroup(
      isSwitch = false,
      isDaemonOfJob = false,
      inOption = WorkloadProvider.emptyFlavor,
      notInOption = WorkloadProvider.emptyFlavor,
      duration = duration,
      statisticsOriginalDuration = duration,
      submitted = submissionTime + 20,
      numTasks = numServerTasks,
      resources = new ServerResource(createArrayWithRangeValues(cell.serverResources(0), randomManager))
    )

    val producerWithSwitchesOnlyArc: TaskGroupConnection = new TaskGroupConnection(
      src = producerTaskGroup,
      dst = taskGroupSwitchesOnly
    )

    val producerWithServersOnlyArc: TaskGroupConnection = new TaskGroupConnection(
      src = producerTaskGroup,
      dst = taskGroupServersOnly
    )

    val incWithSwitchesOnlyToProducerArc: TaskGroupConnection = new TaskGroupConnection(
      src = taskGroupSwitchesOnly,
      dst = producerTaskGroup
    )

    val incWithServersOnlyToProducerArc: TaskGroupConnection = new TaskGroupConnection(
      src = taskGroupServersOnly,
      dst = producerTaskGroup
    )

    val incWithSwitchesOnlyToConsumerArc: TaskGroupConnection = new TaskGroupConnection(
      src = taskGroupSwitchesOnly,
      dst = consumerTaskGroup
    )

    val incWithServersOnlyToConsumerArc: TaskGroupConnection = new TaskGroupConnection(
      src = taskGroupServersOnly,
      dst = consumerTaskGroup
    )

    val consumerToIncWithSwitchesOnlyArc: TaskGroupConnection = new TaskGroupConnection(
      src = consumerTaskGroup,
      dst = taskGroupSwitchesOnly
    )

    val consumerToIncWithServersOnlyArc: TaskGroupConnection = new TaskGroupConnection(
      src = consumerTaskGroup,
      dst = taskGroupServersOnly
    )

    new Job(
      submitted = submissionTime,
      isHighPriority = false,
      taskGroups = Array[TaskGroup](
        producerTaskGroup,
        taskGroupSwitchesOnly,
        taskGroupServersOnly,
        consumerTaskGroup,
        taskGroupSwitchesOnly2ndDummy,
        taskGroupSwitchesOnly2ndDummyEverywhere
      ),
      arcs = Array(
        producerWithSwitchesOnlyArc,
        producerWithServersOnlyArc,
        incWithSwitchesOnlyToProducerArc,
        incWithServersOnlyToProducerArc,
        incWithSwitchesOnlyToConsumerArc,
        incWithServersOnlyToConsumerArc,
        consumerToIncWithSwitchesOnlyArc,
        consumerToIncWithServersOnlyArc
      )
    )
  }

  def withRandom(testCode: (RandomManager) => Any): Unit = {
    testCode(new RandomManager(constantSeed))

  }

  def withCell(testCode: (Cell) => Any): Unit = {
    val cell: Cell = CellFactory.newCell(
      k = 4,
      serverResources = Array[NumericalResource](100, 100),
      switchResources = new SwitchResource(Array(100, 100), SwitchProps.all),
      switchMaxActiveInpTypes = 2,
      cellType = "ft"
    )
    testCode(cell)
  }

  def withWorkload(withInp: Boolean, randomManager: RandomManager, cell: Cell, withSharedMode: SharedResourceMode,
                   testCode: (WorkloadProvider) => Any): Unit = {
    val wlRand = randomManager.copy
    val jobs: mutable.ListBuffer[Job] = mutable.ListBuffer()
    SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = withSharedMode

    SharedResources.initializeWithCell(cell)
    SharedResources.setPropertyResourceDemand(SwitchProps.typeSharp,
      createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), wlRand))
    SharedResources.setPropertyResourceDemand(SwitchProps.typeHarmonia,
      createArrayWithRangeValues(cell.switchResources(0).numericalResources.map(x => x / 4), wlRand))

    (0 until totalJobs).foreach(t => {
      val time = t * (constantSimTime / totalJobs)
      if (withInp && wlRand.getDouble < 0.9) {
        if (wlRand.getDouble > 0.5) {
          jobs.addOne(createPseudoTypeAIncLikeJob(
            wlRand.getInt(5) + 3,
            wlRand.getInt(15) + 3, time, wlRand, cell))
        } else {
          jobs.addOne(createPseudoTypeBIncLikeJobWithTwoFlavors(
            wlRand.getInt(5) + 3,
            wlRand.getInt(15) + 3, time, wlRand, cell))
        }
      } else {
        jobs.addOne(createServerJob(wlRand.getInt(3) + 3, time, wlRand, cell))
      }
    })

    val workload = new OneTimeWorkloadProvider(randomManager, jobs)

    testCode(workload)
  }

  def withSimSetup(withInp: Boolean, withSharedMode: SharedResourceMode, testCode: (Cell, RandomManager, Simulator) => Any): Unit = {
    withRandom(random => {
      withCell(cell => {
        withWorkload(withInp, random.copy, cell, withSharedMode, workload => {
          val simulator: Simulator = new Simulator(workload :: Nil, random.copy)

          CellINPLoadStatistics.activate(
            simulator = simulator,
            cell = cell,
            inp_types = SwitchProps.allTypes,
            file = None,
            interval = 0
          )

          testCode(cell, random.copy, simulator)
        })
      })
    })
  }


  test("determinismSharedResourceYarnWithINP") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_FOREACH, (cell, random, simulator) => {
      val scheduler: Scheduler = new YarnCapacityScheduler(
        name = "yarn",
        cell = cell,
        simulator = simulator,
        node_locality_delay = 10,
        recheck_delay = 10,
        flavorSelector = new SchedulerDecidesFlavorSelector()
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||            Target|                                   Statistic|                                                                    Value|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           @300001|                                   TotalJobs|                                                                       60|
          ||              yarn|                               Jobs with Inp|                                                               54 (90.0%)|
          ||isScheduling:false|                               TotalJobsDone|                                                                       45|
          ||                  |                            TotalJobsDoneInp|                                                                       14|
          ||                  |                     TotalJobsFullyScheduled|                                                                       50|
          ||                  |                  TotalJobsFullyScheduledInp|                                                                       15|
          ||                  |                          Scheduling Success|                                                                   83.33%|
          ||                  |                                 Preemptions|                                                                        0|
          ||                  |                              TotalThinkTime|                                                                    12901|
          ||                  |                          SchedulingAttempts|                                                                      929|
          ||                  |                         TotalScheduledTasks|                                                                     1127|
          ||                  |                        PerformedAllocations|                                                                      929|
          ||                  |              TG Placement Latency (last 1k)|                0h:00:01.973 median |0h:00:10.131 mean | 0h:01:54.566 max|
          ||                  |                              TasksScheduled|                                                                     1127|
          ||                  |                        TotalThinkTimeWasted|                                                                        0|
          ||                  |not fully scheduled, with future submissions|                                                                       10|
          ||                  |               not fully scheduled, till now|                                                                       10|
          ||                  |                  pending 'scheduling items'|                                                                       15|
          ||                  |                   fully scheduled, till now|                                                                        0|
          ||                  |                             take inp flavor|                                                               20 (37.0%)|
          ||                  |                          take server flavor|                                                                       34|
          ||                  |                    server fallback resubmit|                                                                        0|
          ||                  |                      cell server load (now)|                                                            82.50, 70.06%|
          ||                  |                cell switch active INP (now)|                                                                   70.00%|
          ||                  |                      cell switch load (now)|                                                            74.25, 84.80%|
          ||                  |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                  |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(2)|[   52.45,   62.65,     100,    11.6,    53.4,    77.2,     100,     7.6]|
          ||                  |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(7)|[      29,      29,      40,  51.125,      19,      19,      40,  63.625]|
          ||                  |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }


  test("determinismSharedResourceCoCoWithINP") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_FOREACH, (cell, random, simulator) => {
      val scheduler: Scheduler = new CoCoScheduler(
        name = "CoCo",
        cell = cell,
        simulator = simulator,
        flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(configuredServerFallbackDelay = -10),
        solver = new SuccessiveShortestSolver(stopWhenAllAllocationsFound = true, stopAtFirstUnscheduledFlow = false)
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||            Target|                                   Statistic|                                                                    Value|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           @300001|                                   TotalJobs|                                                                       92|
          ||              CoCo|                               Jobs with Inp|                                                               54 (58.7%)|
          ||isScheduling:false|                               TotalJobsDone|                                                                       43|
          ||                  |                            TotalJobsDoneInp|                                                                       18|
          ||                  |                     TotalJobsFullyScheduled|                                                                       49|
          ||                  |                  TotalJobsFullyScheduledInp|                                                                       21|
          ||                  |                          Scheduling Success|                                                                   81.67%|
          ||                  |                                 Preemptions|                                                                      562|
          ||                  |                              TotalThinkTime|                                                                    29737|
          ||                  |                          SchedulingAttempts|                                                                      296|
          ||                  |                         TotalScheduledTasks|                                                                     1482|
          ||                  |                        PerformedAllocations|                                                                     1482|
          ||                  |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:03.260 mean | 0h:02:38.865 max|
          ||                  |                              TasksScheduled|                                                                     1482|
          ||                  |                        TotalThinkTimeWasted|                                                                        0|
          ||                  |not fully scheduled, with future submissions|                                                                       11|
          ||                  |               not fully scheduled, till now|                                                                       11|
          ||                  |                  pending 'scheduling items'|                                                                       11|
          ||                  |                   fully scheduled, till now|                                                                        0|
          ||                  |                             take inp flavor|                                                               21 (38.9%)|
          ||                  |                          take server flavor|                                                                        1|
          ||                  |                    server fallback resubmit|                                                                       32|
          ||                  |                      cell server load (now)|                                                            89.63, 69.06%|
          ||                  |                cell switch active INP (now)|                                                                   95.00%|
          ||                  |                      cell switch load (now)|                                                            76.80, 57.60%|
          ||                  |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                  |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(2)|[ 32.6667,      34,      90, 46.3889, 33.2222, 36.3333,      90, 24.7222]|
          ||                  |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(7)|[    34.2,    46.2,     100,    30.6,    19.9,    24.9,     100,    32.7]|
          ||                  |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceSparrowTryIncResubmitWithINP") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_FOREACH, (cell, random, simulator) => {
      val scheduler: Scheduler = new SparrowLikeQueueScheduler(
        name = "Sparrorw",
        cell = cell,
        simulator = simulator,
        flavorSelector = new SchedulerDecidesFlavorSelector()
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           Target|                                   Statistic|                                                                    Value|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||          @300001|                                   TotalJobs|                                                                       72|
          ||         Sparrorw|                               Jobs with Inp|                                                               54 (75.0%)|
          ||isScheduling:true|                               TotalJobsDone|                                                                       45|
          ||                 |                            TotalJobsDoneInp|                                                                       19|
          ||                 |                     TotalJobsFullyScheduled|                                                                       49|
          ||                 |                  TotalJobsFullyScheduledInp|                                                                       20|
          ||                 |                          Scheduling Success|                                                                   81.67%|
          ||                 |                                 Preemptions|                                                                      243|
          ||                 |                              TotalThinkTime|                                                                   255000|
          ||                 |                          SchedulingAttempts|                                                                     1083|
          ||                 |                         TotalScheduledTasks|                                                                     1239|
          ||                 |                        PerformedAllocations|                                                                     1083|
          ||                 |              TG Placement Latency (last 1k)|                0h:00:00.070 median |0h:00:05.456 mean | 0h:03:04.207 max|
          ||                 |                              TasksScheduled|                                                                     1239|
          ||                 |                        TotalThinkTimeWasted|                                                                        0|
          ||                 |not fully scheduled, with future submissions|                                                                       11|
          ||                 |               not fully scheduled, till now|                                                                       11|
          ||                 |                  pending 'scheduling items'|                                                                      137|
          ||                 |                   fully scheduled, till now|                                                                        0|
          ||                 |                             take inp flavor|                                                               23 (45.1%)|
          ||                 |                          take server flavor|                                                                       16|
          ||                 |                    server fallback resubmit|                                                                       12|
          ||                 |                      cell server load (now)|                                                            84.69, 66.25%|
          ||                 |                cell switch active INP (now)|                                                                   47.50%|
          ||                 |                      cell switch load (now)|                                                            43.55, 61.45%|
          ||                 |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                 |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(2)|[ 38.8947, 45.8421,      95,       0, 48.4737, 64.6842,      95,       0]|
          ||                 |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(7)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }


  test("determinismSharedResourceSparrowCellUtilWithINP") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_FOREACH, (cell, random, simulator) => {
      val scheduler: Scheduler = new SparrowLikeQueueScheduler(
        name = "Sparrorw",
        cell = cell,
        simulator = simulator,
        flavorSelector = new SchedulerDecidesFlavorSelector()
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           Target|                                   Statistic|                                                                    Value|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||          @300001|                                   TotalJobs|                                                                       72|
          ||         Sparrorw|                               Jobs with Inp|                                                               54 (75.0%)|
          ||isScheduling:true|                               TotalJobsDone|                                                                       45|
          ||                 |                            TotalJobsDoneInp|                                                                       19|
          ||                 |                     TotalJobsFullyScheduled|                                                                       49|
          ||                 |                  TotalJobsFullyScheduledInp|                                                                       20|
          ||                 |                          Scheduling Success|                                                                   81.67%|
          ||                 |                                 Preemptions|                                                                      243|
          ||                 |                              TotalThinkTime|                                                                   255000|
          ||                 |                          SchedulingAttempts|                                                                     1083|
          ||                 |                         TotalScheduledTasks|                                                                     1239|
          ||                 |                        PerformedAllocations|                                                                     1083|
          ||                 |              TG Placement Latency (last 1k)|                0h:00:00.070 median |0h:00:05.456 mean | 0h:03:04.207 max|
          ||                 |                              TasksScheduled|                                                                     1239|
          ||                 |                        TotalThinkTimeWasted|                                                                        0|
          ||                 |not fully scheduled, with future submissions|                                                                       11|
          ||                 |               not fully scheduled, till now|                                                                       11|
          ||                 |                  pending 'scheduling items'|                                                                      137|
          ||                 |                   fully scheduled, till now|                                                                        0|
          ||                 |                             take inp flavor|                                                               23 (45.1%)|
          ||                 |                          take server flavor|                                                                       16|
          ||                 |                    server fallback resubmit|                                                                       12|
          ||                 |                      cell server load (now)|                                                            84.69, 66.25%|
          ||                 |                cell switch active INP (now)|                                                                   47.50%|
          ||                 |                      cell switch load (now)|                                                            43.55, 61.45%|
          ||                 |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                 |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(2)|[ 38.8947, 45.8421,      95,       0, 48.4737, 64.6842,      95,       0]|
          ||                 |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(7)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceK8WithINP") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_FOREACH, (cell, random, simulator) => {
      val scheduler: Scheduler = new KubernetesScheduler(
        name = "k8",
        cell = cell,
        simulator = simulator,
        flavorSelector = new SchedulerDecidesFlavorSelector()
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           Target|                                   Statistic|                                                                    Value|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||          @300001|                                   TotalJobs|                                                                       60|
          ||               k8|                               Jobs with Inp|                                                               54 (90.0%)|
          ||isScheduling:true|                               TotalJobsDone|                                                                       41|
          ||                 |                            TotalJobsDoneInp|                                                                        9|
          ||                 |                     TotalJobsFullyScheduled|                                                                       46|
          ||                 |                  TotalJobsFullyScheduledInp|                                                                        9|
          ||                 |                          Scheduling Success|                                                                   76.67%|
          ||                 |                                 Preemptions|                                                                        0|
          ||                 |                              TotalThinkTime|                                                                     1842|
          ||                 |                          SchedulingAttempts|                                                                     1054|
          ||                 |                         TotalScheduledTasks|                                                                     1071|
          ||                 |                        PerformedAllocations|                                                                      853|
          ||                 |              TG Placement Latency (last 1k)|                0h:00:00.023 median |0h:00:10.764 mean | 0h:01:40.093 max|
          ||                 |                              TasksScheduled|                                                                     1071|
          ||                 |                        TotalThinkTimeWasted|                                                                     3200|
          ||                 |not fully scheduled, with future submissions|                                                                       14|
          ||                 |               not fully scheduled, till now|                                                                       14|
          ||                 |                  pending 'scheduling items'|                                                                       19|
          ||                 |                   fully scheduled, till now|                                                                        0|
          ||                 |                             take inp flavor|                                                               19 (35.2%)|
          ||                 |                          take server flavor|                                                                       35|
          ||                 |                    server fallback resubmit|                                                                        0|
          ||                 |                      cell server load (now)|                                                            87.00, 73.81%|
          ||                 |                cell switch active INP (now)|                                                                   50.00%|
          ||                 |                      cell switch load (now)|                                                            57.55, 72.85%|
          ||                 |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                 |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(2)|[   47.05,   57.55,     100,       0,   48.35,   72.85,     100,       0]|
          ||                 |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(7)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceHireCostNoPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-cost-no-preempt",
        cell = cell,
        simulator = simulator,
        solver = new CostScalingSolver,
        minQueuingTimeBeforePreemption = constantSimTime
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+--------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||              Target|                                   Statistic|                                                                    Value|
          |+--------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||             @300001|                                   TotalJobs|                                                                       60|
          ||hire-cost-no-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          ||  isScheduling:false|                               TotalJobsDone|                                                                       51|
          ||                    |                            TotalJobsDoneInp|                                                                       35|
          ||                    |                     TotalJobsFullyScheduled|                                                                       57|
          ||                    |                  TotalJobsFullyScheduledInp|                                                                       39|
          ||                    |                          Scheduling Success|                                                                   95.00%|
          ||                    |                                 Preemptions|                                                                        0|
          ||                    |                              TotalThinkTime|                                                                    33206|
          ||                    |                          SchedulingAttempts|                                                                      546|
          ||                    |                         TotalScheduledTasks|                                                                     1279|
          ||                    |                        PerformedAllocations|                                                                     1279|
          ||                    |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:03.069 mean | 0h:01:20.180 max|
          ||                    |                              TasksScheduled|                                                                     1279|
          ||                    |                        TotalThinkTimeWasted|                                                                    21400|
          ||                    |not fully scheduled, with future submissions|                                                                        3|
          ||                    |               not fully scheduled, till now|                                                                        3|
          ||                    |                  pending 'scheduling items'|                                                                        3|
          ||                    |                   fully scheduled, till now|                                                                        0|
          ||                    |                             take inp flavor|                                                               41 (75.9%)|
          ||                    |                          take server flavor|                                                                       13|
          ||                    |                    server fallback resubmit|                                                                        0|
          ||                    |                      cell server load (now)|                                                            91.50, 64.56%|
          ||                    |                cell switch active INP (now)|                                                                   85.00%|
          ||                    |                      cell switch load (now)|                                                            85.00, 78.50%|
          ||                    |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                    |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(2)|[    58.5,    58.5,     100,    26.5,    62.5,    62.5,     100,      16]|
          ||                    |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(7)|[ 37.8571, 37.8571,      70,    44.5, 22.8571, 22.8571,      70, 57.1429]|
          ||                    |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                    |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+--------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceHireRelaxNoPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-relax-no-preempt",
        cell = cell,
        simulator = simulator,
        solver = new RelaxationSolver,
        minQueuingTimeBeforePreemption = constantSimTime
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+---------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||               Target|                                   Statistic|                                                                    Value|
          |+---------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||              @300001|                                   TotalJobs|                                                                       60|
          ||hire-relax-no-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          ||   isScheduling:false|                               TotalJobsDone|                                                                       50|
          ||                     |                            TotalJobsDoneInp|                                                                       33|
          ||                     |                     TotalJobsFullyScheduled|                                                                       57|
          ||                     |                  TotalJobsFullyScheduledInp|                                                                       38|
          ||                     |                          Scheduling Success|                                                                   95.00%|
          ||                     |                                 Preemptions|                                                                        0|
          ||                     |                              TotalThinkTime|                                                                    32709|
          ||                     |                          SchedulingAttempts|                                                                      552|
          ||                     |                         TotalScheduledTasks|                                                                     1269|
          ||                     |                        PerformedAllocations|                                                                     1269|
          ||                     |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:03.314 mean | 0h:01:20.180 max|
          ||                     |                              TasksScheduled|                                                                     1269|
          ||                     |                        TotalThinkTimeWasted|                                                                    22500|
          ||                     |not fully scheduled, with future submissions|                                                                        3|
          ||                     |               not fully scheduled, till now|                                                                        3|
          ||                     |                  pending 'scheduling items'|                                                                        3|
          ||                     |                   fully scheduled, till now|                                                                        0|
          ||                     |                             take inp flavor|                                                               40 (74.1%)|
          ||                     |                          take server flavor|                                                                       14|
          ||                     |                    server fallback resubmit|                                                                        0|
          ||                     |                      cell server load (now)|                                                            90.06, 66.44%|
          ||                     |                cell switch active INP (now)|                                                                   92.50%|
          ||                     |                      cell switch load (now)|                                                            89.55, 79.40%|
          ||                     |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                     |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(2)|[    58.5,    58.5,     100,   31.05,    62.5,    62.5,     100,    16.9]|
          ||                     |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(7)|[ 36.5294, 36.5294,      85, 51.8824, 19.8824, 19.8824,      85, 59.4118]|
          ||                     |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                     |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+---------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceHireSSPNoPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-ssp-no-preempt",
        cell = cell,
        simulator = simulator,
        solver = new SuccessiveShortestSolver(stopWhenAllAllocationsFound = true, stopAtFirstUnscheduledFlow = false),
        minQueuingTimeBeforePreemption = constantSimTime
      )

      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||             Target|                                   Statistic|                                                                    Value|
          |+-------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||            @300001|                                   TotalJobs|                                                                       60|
          ||hire-ssp-no-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          || isScheduling:false|                               TotalJobsDone|                                                                       51|
          ||                   |                            TotalJobsDoneInp|                                                                       35|
          ||                   |                     TotalJobsFullyScheduled|                                                                       58|
          ||                   |                  TotalJobsFullyScheduledInp|                                                                       39|
          ||                   |                          Scheduling Success|                                                                   96.67%|
          ||                   |                                 Preemptions|                                                                        0|
          ||                   |                              TotalThinkTime|                                                                    29606|
          ||                   |                          SchedulingAttempts|                                                                      510|
          ||                   |                         TotalScheduledTasks|                                                                     1279|
          ||                   |                        PerformedAllocations|                                                                     1279|
          ||                   |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:02.246 mean | 0h:01:23.513 max|
          ||                   |                              TasksScheduled|                                                                     1279|
          ||                   |                        TotalThinkTimeWasted|                                                                    21400|
          ||                   |not fully scheduled, with future submissions|                                                                        2|
          ||                   |               not fully scheduled, till now|                                                                        2|
          ||                   |                  pending 'scheduling items'|                                                                        2|
          ||                   |                   fully scheduled, till now|                                                                        0|
          ||                   |                             take inp flavor|                                                               41 (75.9%)|
          ||                   |                          take server flavor|                                                                       13|
          ||                   |                    server fallback resubmit|                                                                        0|
          ||                   |                      cell server load (now)|                                                            92.63, 68.00%|
          ||                   |                cell switch active INP (now)|                                                                   85.00%|
          ||                   |                      cell switch load (now)|                                                            85.00, 78.50%|
          ||                   |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                   |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(2)|[    58.5,    58.5,     100,    26.5,    62.5,    62.5,     100,      16]|
          ||                   |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(7)|[ 37.8571, 37.8571,      70, 49.1429, 22.8571, 22.8571,      70, 58.9286]|
          ||                   |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                   |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceHireCostWithPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-cost-preempt",
        cell = cell,
        simulator = simulator,
        solver = new CostScalingSolver,
        minQueuingTimeBeforePreemption = queueingTimeBeforePreemption
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           Target|                                   Statistic|                                                                    Value|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||          @300001|                                   TotalJobs|                                                                       60|
          ||hire-cost-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          ||isScheduling:true|                               TotalJobsDone|                                                                       48|
          ||                 |                            TotalJobsDoneInp|                                                                       35|
          ||                 |                     TotalJobsFullyScheduled|                                                                       55|
          ||                 |                  TotalJobsFullyScheduledInp|                                                                       40|
          ||                 |                          Scheduling Success|                                                                   91.67%|
          ||                 |                                 Preemptions|                                                                       31|
          ||                 |                              TotalThinkTime|                                                                    33206|
          ||                 |                          SchedulingAttempts|                                                                      550|
          ||                 |                         TotalScheduledTasks|                                                                     1263|
          ||                 |                        PerformedAllocations|                                                                     1263|
          ||                 |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:02.906 mean | 0h:01:20.180 max|
          ||                 |                              TasksScheduled|                                                                     1263|
          ||                 |                        TotalThinkTimeWasted|                                                                    21700|
          ||                 |not fully scheduled, with future submissions|                                                                        5|
          ||                 |               not fully scheduled, till now|                                                                        5|
          ||                 |                  pending 'scheduling items'|                                                                        5|
          ||                 |                   fully scheduled, till now|                                                                        0|
          ||                 |                             take inp flavor|                                                               43 (79.6%)|
          ||                 |                          take server flavor|                                                                       11|
          ||                 |                    server fallback resubmit|                                                                        0|
          ||                 |                      cell server load (now)|                                                            94.19, 47.94%|
          ||                 |                cell switch active INP (now)|                                                                   85.00%|
          ||                 |                      cell switch load (now)|                                                            84.50, 77.60%|
          ||                 |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                 |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(2)|[    58.5,    58.5,     100,      26,    62.5,    62.5,     100,    15.1]|
          ||                 |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(7)|[ 37.1429, 37.1429,      70, 44.7143, 21.5714, 21.5714,      70, 60.4286]|
          ||                 |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

  test("determinismSharedResourceHireRelaxWithPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-relax-preempt",
        cell = cell,
        simulator = simulator,
        solver = new RelaxationSolver,
        minQueuingTimeBeforePreemption = queueingTimeBeforePreemption
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||            Target|                                   Statistic|                                                                    Value|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           @300001|                                   TotalJobs|                                                                       60|
          ||hire-relax-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          ||isScheduling:false|                               TotalJobsDone|                                                                       48|
          ||                  |                            TotalJobsDoneInp|                                                                       34|
          ||                  |                     TotalJobsFullyScheduled|                                                                       55|
          ||                  |                  TotalJobsFullyScheduledInp|                                                                       39|
          ||                  |                          Scheduling Success|                                                                   91.67%|
          ||                  |                                 Preemptions|                                                                       39|
          ||                  |                              TotalThinkTime|                                                                    30106|
          ||                  |                          SchedulingAttempts|                                                                      498|
          ||                  |                         TotalScheduledTasks|                                                                     1264|
          ||                  |                        PerformedAllocations|                                                                     1264|
          ||                  |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:02.797 mean | 0h:01:45.300 max|
          ||                  |                              TasksScheduled|                                                                     1264|
          ||                  |                        TotalThinkTimeWasted|                                                                    19700|
          ||                  |not fully scheduled, with future submissions|                                                                        5|
          ||                  |               not fully scheduled, till now|                                                                        5|
          ||                  |                  pending 'scheduling items'|                                                                        5|
          ||                  |                   fully scheduled, till now|                                                                        0|
          ||                  |                             take inp flavor|                                                               42 (77.8%)|
          ||                  |                          take server flavor|                                                                       12|
          ||                  |                    server fallback resubmit|                                                                        0|
          ||                  |                      cell server load (now)|                                                            96.75, 49.88%|
          ||                  |                cell switch active INP (now)|                                                                   87.50%|
          ||                  |                      cell switch load (now)|                                                            85.20, 77.20%|
          ||                  |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                  |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(2)|[    58.5,    58.5,     100,    26.7,    62.5,    62.5,     100,    14.7]|
          ||                  |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(7)|[    35.6,    35.6,      75,    46.4,    19.6,    19.6,      75,    56.4]|
          ||                  |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                  |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+------------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }


  test(s"determinismSharedResourceHireSSPWithPreemption") {
    withSimSetup(withInp = true, withSharedMode = SharedResourceMode.CONSIDER_ONCE, (cell, random, simulator) => {
      val scheduler: Scheduler = new HireScheduler(
        name = "hire-ssp-preempt",
        cell = cell,
        simulator = simulator,
        solver = new SuccessiveShortestSolver(stopWhenAllAllocationsFound = true, stopAtFirstUnscheduledFlow = false),
        minQueuingTimeBeforePreemption = queueingTimeBeforePreemption
      )
      simulator.setScheduler(scheduler)
      simulator.runSim(constantSimTime)

      val result = simulator.printQuickStats(false, false)
      println(result)
      result.shouldBe(
        """+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||           Target|                                   Statistic|                                                                    Value|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+
          ||          @300001|                                   TotalJobs|                                                                       60|
          || hire-ssp-preempt|                               Jobs with Inp|                                                               54 (90.0%)|
          ||isScheduling:true|                               TotalJobsDone|                                                                       50|
          ||                 |                            TotalJobsDoneInp|                                                                       36|
          ||                 |                     TotalJobsFullyScheduled|                                                                       56|
          ||                 |                  TotalJobsFullyScheduledInp|                                                                       39|
          ||                 |                          Scheduling Success|                                                                   93.33%|
          ||                 |                                 Preemptions|                                                                       42|
          ||                 |                              TotalThinkTime|                                                                    31406|
          ||                 |                          SchedulingAttempts|                                                                      523|
          ||                 |                         TotalScheduledTasks|                                                                     1273|
          ||                 |                        PerformedAllocations|                                                                     1273|
          ||                 |              TG Placement Latency (last 1k)|                0h:00:00.200 median |0h:00:03.110 mean | 0h:01:20.690 max|
          ||                 |                              TasksScheduled|                                                                     1273|
          ||                 |                        TotalThinkTimeWasted|                                                                    20800|
          ||                 |not fully scheduled, with future submissions|                                                                        4|
          ||                 |               not fully scheduled, till now|                                                                        4|
          ||                 |                  pending 'scheduling items'|                                                                        4|
          ||                 |                   fully scheduled, till now|                                                                        0|
          ||                 |                             take inp flavor|                                                               42 (77.8%)|
          ||                 |                          take server flavor|                                                                       12|
          ||                 |                    server fallback resubmit|                                                                        0|
          ||                 |                      cell server load (now)|                                                            90.31, 57.19%|
          ||                 |                cell switch active INP (now)|                                                                   90.00%|
          ||                 |                      cell switch load (now)|                                                            88.05, 79.95%|
          ||                 |                       detailed switch stats|[  d0 use, % alloc, % total,% others,  d1 use, % alloc, % total,% others]|
          ||                 |                                   BitSet(1)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(2)|[    59.4,    59.4,     100,   28.65,    63.4,    63.4,     100,   16.55]|
          ||                 |                                   BitSet(3)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(4)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(5)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(6)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(7)|[ 35.8125, 35.8125,      80,  52.375, 20.6875, 20.6875,      80,  60.875]|
          ||                 |                                   BitSet(8)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          ||                 |                                   BitSet(9)|[       0,       0,       0,       0,       0,       0,       0,       0]|
          |+-----------------+--------------------------------------------+-------------------------------------------------------------------------+""".stripMargin)
    })
  }

}
