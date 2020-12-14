package hiresim.experiments

import hiresim.cell.factory.CellFactory
import hiresim.cell.factory.resources.MachineResourceProvider
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.scheduler._
import hiresim.scheduler.flow.coco.CoCoScheduler
import hiresim.scheduler.flow.hire.costs.HireLocalityCostCalculator
import hiresim.scheduler.flow.hire.{HireLocalityHandlingMode, HireScheduler}
import hiresim.scheduler.flow.solver.mcmf.util.MCMFSolverStatistics
import hiresim.scheduler.flow.solver.mcmf.{CostScalingSolver, RelaxationSolver, SuccessiveShortestSolver}
import hiresim.scheduler.flow.solver.{MultiThreadSolver, Solver}
import hiresim.shared.TimeIt
import hiresim.simulation._
import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.simulation.statistics._
import hiresim.tenant.SharedResources
import hiresim.workload.AlibabaClusterTraceWorkload

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.util.Try

case class SimConfig(var scheduler: Option[mutable.ListBuffer[List[String]]] = Some(mutable.ListBuffer()),
                     var runId: Option[Int] = Some(-1),
                     var simulationTime: Option[SimTypes.simTime] = None, // should be less than 694879 * 1000 (appx 8 days), which is the last event of the workload
                     var workloadTime: Option[SimTypes.simTime] = None,
                     var skipJobsBeforeTime: Option[SimTypes.simTime] = Some(-1),
                     var verbose: Option[Int] = Some(0),
                     var statusReportMessage: Option[Int] = Some(3600),
                     var cellType: Option[String] = Some("ft"),
                     var cellSwitchHomogeneous: Option[String] = Some("homogeneous"),
                     var cellK: Option[Int] = Some(4),
                     var maxActiveInp: Option[Int] = Some(2),

                     var seed: Option[Int] = Some(0),
                     var precision: Option[Long] = Some(100L),

                     var useSimpleTwoStateInpServerFlavorOptions: Option[Boolean] = Some(false),

                     var tgMaxDuration: Option[Int] = None,

                     var hireINPLocDecayFactor: Option[BigInt] = None,
                     var hireINPLocInitGain: Option[BigInt] = None,
                     var hireServerPenaltyCost: Option[Double] = Some(3.0),
                     var hireServerPenaltyWaitingLower: Option[Int] = Some(10000),
                     var hireServerPenaltyWaitingUpper: Option[Int] = Some(100000),

                     var softLimitProducersInGraph: Option[Int] = Some(200),
                     var softLimitSupplyInGraph: Option[Int] = Some(-1),
                     var backlogDisableTime: Option[Int] = None,

                     var sharedResourceMode: Option[SharedResourceMode] = Some(SharedResourceMode.DEACTIVATED),

                     var maxCellServerPressure: Option[Double] = Some(1.0),
                     var minQueuingTimeBeforePreemption: Option[Int] = Some(8000),
                     var maxInpFlavorDecisionsPerRound: Option[Int] = Some(100),

                     var hireShortcutsMaxSearchSpace: Option[Int] = Some(200),
                     var hireShortcutsMaxSelection: Option[Int] = Some(50),

                     var maxResourceUnitServer: Option[NumericalResource] = Some(1000000),
                     var maxResourceUnitSwitches: Option[NumericalResource] = Some(1000000),
                     var scaleCellServerCapacity: Option[Array[Double]] = Some(Array(1, 1)),
                     var scaleCellSwitchCapacity: Option[Array[Double]] = Some(Array(1, 1)),

                     var createInpFlavorStartingFromTime: Option[Int] = Some(-1),

                     var kappaRuntime: Option[Double] = Some(1.0),
                     var kappaServers: Option[Double] = Some(1.0),
                     var kappaDrawRandoms: Option[Boolean] = Some(true),
                     var muInpFlavor: Option[Double] = Some(0.3),
                     var ratioOfIncTaskGroups: Option[Double] = Some(0.25),
                     var inpTypes: Option[mutable.ListBuffer[SwitchProps]] = Some(mutable.ListBuffer()),

                     // Statistics Options
                     var statisticsCell: Option[Boolean] = Some(false),
                     var statisticsTenant: Option[(Boolean, Boolean)] = Some(Tuple2(false, false)),
                     var statisticsScheduler: Option[Boolean] = Some(false),
                     var statisticsSolver: Option[Boolean] = Some(false),
                     var statisticsInterval: Option[Int] = Some(60000),
                     var targetStatisticsDir: Option[String] = Some("."),
                     var statisticsStartTimestamp: Option[Int] = Some(0), // Defaults to immediately
                     var statisticsJobsExtraSnapShotsHours: Array[Int] = Array(),
                     var statisticsTiming: Option[Boolean] = Some(false),
                     var statisticsINPCell: Option[Boolean] = Some(false),

                     var sanityChecks: Option[Boolean] = Some(false),
                     var activateTimeIt: Option[Boolean] = Some(false),
                     var thinkTimeScaling: Option[Double] = Some(0.0),
                     var flowSchedulerPostponeSchedulingIfStuck: Option[Int] = Some(1000),
                     var sspMaxSearchTimeSeconds: Option[Long] = Some(300L)
                    ) {

  // from https://stackoverflow.com/a/23129035
  override def toString: String = {
    val fields: Map[String, Any] = getClass.getDeclaredFields.foldLeft(Map[String, Any]()) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(this))
    }
    s"${
      this.getClass.getName
    }(${
      fields.mkString(", ")
    })"
  }

}

object SimRunnerFromCmdArguments {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new AssertionError("Example usage: --sim-time 3600000 --verbose  --inp-types netchain,harmonia --scheduler greedy,fallback,10000")
    }

    val argList: Seq[String] = args.toList

    val config = SimConfig()

    def parseArgs(arglist: Seq[String]): Unit = {
      val left: Seq[String] = arglist match {
        case Nil => Nil
        case "--verbose" :: level :: tail =>
          config.verbose = Some(level.toInt)
          tail
        case "-v" :: tail =>
          config.verbose = Some(config.verbose.get + 1)
          tail
        case "--sanity" :: tail =>
          config.sanityChecks = Some(true)
          tail
        case "--time-it" :: enable :: tail =>
          config.activateTimeIt = Some(Try(enable.toBoolean).getOrElse(false))
          tail
        case "--id" :: run_id :: tail =>
          config.runId = Some(run_id.toInt)
          tail
        case "--scheduler" :: sched :: tail =>
          config.scheduler.get.addOne(sched.split(",").toList)
          tail
        case "--sim-time" :: time :: tail =>
          config.simulationTime = Some(time.toInt)
          tail
        case "--skip-jobs-before-time" :: time :: tail =>
          config.skipJobsBeforeTime = Some(time.toInt)
          tail
        case "--workload-time" :: time :: tail =>
          if (time.toInt > 0)
            config.workloadTime = Some(time.toInt)
          else
            config.workloadTime = None
          tail
        case "--status-report-message" :: time :: tail =>
          config.statusReportMessage = Some(time.toInt)
          tail
        case "--seed" :: seed :: tail =>
          config.seed = Some(seed.toInt)
          tail
        case "--precision" :: precision :: tail =>
          config.precision = Some(precision.toLong)
          tail
        case "--useSimpleTwoStateInpServerFlavorOptions" :: enable :: tail =>
          config.useSimpleTwoStateInpServerFlavorOptions = Some(enable.toBoolean)
          tail
        case "--tgMaxDuration" :: max :: tail =>
          if (max.toInt > 0)
            config.tgMaxDuration = Some(max.toInt)
          else
            config.tgMaxDuration = None
          tail
        case "--softLimitProducersInGraph" :: softLimit :: tail =>
          config.softLimitProducersInGraph = Some(softLimit.toInt)
          tail
        case "--softLimitSupplyInGraph" :: limit :: tail =>
          config.softLimitSupplyInGraph = Some(limit.toInt)
          tail
        case "--maxServerPressure" :: limit :: tail =>
          config.maxCellServerPressure = Some(limit.toDouble)
          tail
        case "--disableLimits" :: time :: tail =>
          config.backlogDisableTime = Some(time.toInt)
          tail
        case "--flowSchedulerPostponeSchedulingIfStuck" :: limit :: tail =>
          config.flowSchedulerPostponeSchedulingIfStuck = Some(limit.toInt)
          tail
        case "--hireShortcutsMaxSearchSpace" :: limit :: tail =>
          config.hireShortcutsMaxSearchSpace = Some(limit.toInt)
          tail
        case "--hireShortcutsMaxSelection" :: limit :: tail =>
          config.hireShortcutsMaxSelection = Some(limit.toInt)
          tail
        case "--minQueuingTimeBeforePreemption" :: limit :: tail =>
          config.minQueuingTimeBeforePreemption = Some(limit.toInt)
          tail
        case "--think-time-scaling" :: scaling :: tail =>
          config.thinkTimeScaling = Some(scaling.toDouble)
          tail
        case "--sspMaxSearchTimeSeconds" :: timeout :: tail =>
          config.sspMaxSearchTimeSeconds = Some(timeout.toLong)
          tail
        case "--maxInpFlavorDecisionsPerRound" :: limit :: tail =>
          config.maxInpFlavorDecisionsPerRound = Some(limit.toInt)
          tail

        case "--hireINPLocDecayFactor" :: factor :: tail =>
          config.hireINPLocDecayFactor = Some(BigInt(factor.toLong))
          tail
        case "--hireINPLocInitGain" :: gain :: tail =>
          config.hireINPLocInitGain = Some(BigInt(gain.toLong))
          tail

        case "--hireInpServerPenaltyCost" :: penalty :: tail =>
          config.hireServerPenaltyCost = Some(penalty.toDouble)
          tail
        case "--hireInpServerPenaltyWaitingLower" :: wait :: tail =>
          config.hireServerPenaltyWaitingLower = Some(wait.toInt)
          tail
        case "--hireInpServerPenaltyWaitingUpper" :: wait :: tail =>
          config.hireServerPenaltyWaitingUpper = Some(wait.toInt)
          tail

        case "--celltype" :: cell :: tail =>
          config.cellType = Some(cell)
          tail
        case "--cellSwitchHomogeneous" :: cell :: tail =>
          config.cellSwitchHomogeneous = Some(cell)
          tail
        case "--cell-k" :: k :: tail =>
          config.cellK = Some(k.toInt)
          tail
        case "--cell-max-active-inp" :: maxActiveInp :: tail =>
          config.maxActiveInp = Some(maxActiveInp.toInt)
          tail

        case "--scale-cell-switches" :: factors :: tail =>
          config.scaleCellSwitchCapacity = Some(factors.split(",").map(_.toDouble))
          tail
        case "--scale-cell-servers" :: factors :: tail =>
          config.scaleCellServerCapacity = Some(factors.split(",").map(_.toDouble))
          tail


        case "--max-switch-capacity" :: unit :: tail =>
          config.maxResourceUnitSwitches = Some(unit.toInt)
          tail
        case "--max-server-capacity" :: unit :: tail =>
          config.maxResourceUnitServer = Some(unit.toInt)
          tail

        case "--shared-resource-mode" :: mode :: tail =>
          config.sharedResourceMode = Some(SharedResourceMode.getByName(mode))
          tail


        case "--kappa-tasks" :: kappa :: tail =>
          config.kappaServers = Some(kappa.toDouble)
          tail
        case "--kappa-runtime" :: kappa :: tail =>
          config.kappaRuntime = Some(kappa.toDouble)
          tail
        case "--kappa-draw-random" :: flag :: tail =>
          config.kappaDrawRandoms = Some(flag.toBoolean)
          tail
        case "--mu-inp" :: mu :: tail =>
          config.muInpFlavor = Some(mu.toDouble)
          tail
        case "--ratioOfIncTaskGroups" :: ratio :: tail =>
          config.ratioOfIncTaskGroups = Some(ratio.toDouble)
          tail
        case "--create-inp-starting-time" :: time :: tail =>
          config.createInpFlavorStartingFromTime = Some(time.toInt)
          tail

        case "--statistics-tenant-extra-snapshots-hours" :: extra :: tail =>
          if (extra != "/") {
            config.statisticsJobsExtraSnapShotsHours = extra.split(",").map(_.toInt)
          }
          tail
        case "--statistics-tenant-job" :: flag :: tail =>
          config.statisticsTenant = Some(Tuple2(flag.toBoolean, config.statisticsTenant.get._2))
          tail
        case "--statistics-tenant-tg" :: flag :: tail =>
          config.statisticsTenant = Some(Tuple2(config.statisticsTenant.get._1, flag.toBoolean))
          tail
        case "--statistics-scheduler" :: flag :: tail =>
          config.statisticsScheduler = Some(flag.toBoolean)
          tail
        case "--statistics-cell" :: flag :: tail =>
          config.statisticsCell = Some(flag.toBoolean)
          tail
        case "--statistics-interval" :: interval :: tail =>
          config.statisticsInterval = Some(interval.toInt)
          tail
        case "--statistics-solver" :: flag :: tail =>
          config.statisticsSolver = Some(flag.toBoolean)
          tail
        case "--statistics-start" :: time :: tail =>
          config.statisticsStartTimestamp = Some(time.toInt)
          tail
        case "--output-dir" :: dir :: tail =>
          config.targetStatisticsDir = Some(dir)
          tail
        case "--statistics-timing" :: flag :: tail =>
          config.statisticsTiming = Some(flag.toBoolean)
          tail
        case "--statistics-inp-cell" :: flag :: tail =>
          config.statisticsINPCell = Some(flag.toBoolean)
          tail

        case "--inp-types" :: flavors :: tail =>
          flavors.split(",").foreach(flavor => {
            config.inpTypes.get.addOne(SwitchProps.valueOf(flavor))
          })
          tail
        case unknown =>
          System.err.println(s"Invalid sim config: ${unknown}")
          throw new RuntimeException
      }
      if (left.nonEmpty)
        parseArgs(left)
    }


    parseArgs(argList)

    println(config)
    startSimulationWithConfig(config)
  }

  def startSimulationWithConfig(simConfig: SimConfig, doNotStartSimulation: Boolean = false): Simulator = {
    val random = new RandomManager(seed = simConfig.seed.get)

    SimulationConfigurationHelper.setDefaultSimulationConfiguration(withSanityChecks = false)

    SimulationConfiguration.ONGOING_RUN_ID = simConfig.runId.get

    SimulationConfiguration.TIME_IT_ACTIVATE = simConfig.activateTimeIt.get || simConfig.statisticsTiming.get
    SimulationConfiguration.TIME_IT_ACTIVATE_PRINT = simConfig.activateTimeIt.get

    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.LOGGING_VERBOSE_CELL = false
    SimulationConfiguration.LOGGING_VERBOSE_SIM = false
    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false

    println(s"Set verbose logging to level: ${simConfig.verbose.get}")

    def applyVerboseConfig(verbose: Int): Unit = {
      if (verbose > 0)
        SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = true
      if (verbose > 1)
        SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED = true
      if (verbose > 2)
        SimulationConfiguration.LOGGING_VERBOSE_SIM = true
      if (verbose > 3)
        SimulationConfiguration.LOGGING_VERBOSE_OTHER = true
      if (verbose > 4)
        SimulationConfiguration.LOGGING_VERBOSE_CELL = true
      if (verbose > 5) {
        SimulationConfiguration.SANITY_CHECKS_CELL = true
        SimulationConfiguration.SANITY_CHECK_JOB_CHECK_RESOURCE_REQ = true
        SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK = true
      }
    }

    applyVerboseConfig(simConfig.verbose.get)

    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK = simConfig.sanityChecks.get
    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_CYCLE_FREE = simConfig.sanityChecks.get
    SimulationConfiguration.SANITY_CHECK_HIRE_CHECK_IF_UNSCHEDULED = simConfig.sanityChecks.get
    SimulationConfiguration.SANITY_CHECKS_HIRE = simConfig.sanityChecks.get
    SimulationConfiguration.SANITY_CHECKS_CELL = simConfig.sanityChecks.get
    SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD = 0.95
    SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION = 0.01

    SimulationConfiguration.HIRE_POSTPONE_SCHEDULING_IF_STUCK = simConfig.flowSchedulerPostponeSchedulingIfStuck.get

    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP = simConfig.hireShortcutsMaxSearchSpace.get
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP = simConfig.hireShortcutsMaxSelection.get
    SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME = true

    SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT = simConfig.softLimitProducersInGraph.get
    SimulationConfiguration.HIRE_BACKLOG_MAX_NEW_JOBS = simConfig.softLimitProducersInGraph.get
    SimulationConfiguration.HIRE_BACKLOG_SUPPLY_SOFT_LIMIT = simConfig.softLimitSupplyInGraph.get
    SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS = false
    SimulationConfiguration.PRECISION = simConfig.precision.get
    SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE = simConfig.maxCellServerPressure.get
    SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = simConfig.sharedResourceMode.get

    SimulationConfiguration.MULTICORE_MCMF_SOLVER_ACTIVE_AFTER_TIME = -1

    SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR = simConfig.hireServerPenaltyCost.get
    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER = simConfig.hireServerPenaltyWaitingLower.get
    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER = simConfig.hireServerPenaltyWaitingUpper.get

    if (simConfig.hireINPLocInitGain.isDefined)
      HireLocalityCostCalculator.InitialLocalityGain = simConfig.hireINPLocInitGain.get
    if (simConfig.hireINPLocDecayFactor.isDefined)
      HireLocalityCostCalculator.DecayFactor = simConfig.hireINPLocDecayFactor.get

    if (simConfig.workloadTime.isEmpty)
      simConfig.workloadTime = Some(simConfig.simulationTime.get)

    var calledI = 0
    val cellRandom = random.copy

    val p4 = SwitchProps.typeNetCache.or(
      SwitchProps.typeDistCache).or(
      SwitchProps.typeHarmonia).or(
      SwitchProps.typeHovercRaft).or(
      SwitchProps.typeR2p2).or(
      SwitchProps.typeNetChain)
    val customAsic = SwitchProps.typeSharp
    val accel = SwitchProps.typeIncBricks

    def getSwitchProperties: SwitchProps = {
      simConfig.cellSwitchHomogeneous.get match {
        case "homogeneous" => {
          calledI += 1
          SwitchProps.all
        }
        case "random2" => {
          calledI += 1
          cellRandom.getChoicesOfSeq(2, ArraySeq.unsafeWrapArray(SwitchProps.allTypes)).
            foldLeft(SwitchProps.none)((a, b) => a.or(b))
        }
        case "random4" => {
          calledI += 1
          cellRandom.getChoicesOfSeq(4, ArraySeq.unsafeWrapArray(SwitchProps.allTypes)).
            foldLeft(SwitchProps.none)((a, b) => a.or(b))
        }
        case "split" => {
          calledI += 1

          // all can do p4, but 1/3 can also do accel and another 1/3 can also do custom asic
          if (calledI % 3 == 0) {
            p4
          } else if (calledI % 3 == 1) {
            p4.or(accel)
          } else {
            p4.or(customAsic)
          }

        }
      }
    }

    val switches_scaling: Array[Double] = simConfig.scaleCellSwitchCapacity.get

    assert(switches_scaling.length == 3 || switches_scaling.length == 9,
      "switch dimensions must be 3, so scaling must be of dimension 3 or 9")

    val switch_tor_resource = switches_scaling.indices.map(i =>
      (BigDecimal(switches_scaling(i)) * BigDecimal(simConfig.maxResourceUnitSwitches.get)).toInt).toArray

    var switch_fabric_resource = switch_tor_resource
    var switch_spine_resource = switch_tor_resource

    if (switches_scaling.length == 9) {

      switch_fabric_resource = switches_scaling.indices.map(i =>
        (BigDecimal(switches_scaling(i + 3)) * BigDecimal(simConfig.maxResourceUnitSwitches.get)).toInt).toArray

      switch_spine_resource = switches_scaling.indices.map(i =>
        (BigDecimal(switches_scaling(i + 6)) * BigDecimal(simConfig.maxResourceUnitSwitches.get)).toInt).toArray

    }

    val server_resources: Array[NumericalResource] = simConfig.scaleCellServerCapacity.get.indices.map(i =>
      (BigDecimal(simConfig.scaleCellServerCapacity.get(i)) * BigDecimal(simConfig.maxResourceUnitServer.get)).toInt).toArray


    val resource_provider = new MachineResourceProvider {

      override def getSpineSwitchResources(spineSwitchId: NumericalResource): SwitchResource = {
        new SwitchResource(switch_spine_resource.clone(), getSwitchProperties)
      }

      override def getFabricSwitchResources(fabricSwitchId: NumericalResource): SwitchResource = {
        new SwitchResource(switch_fabric_resource.clone(), getSwitchProperties)
      }

      override def getToRSwitchResources(torSwitchId: NumericalResource): SwitchResource = {
        new SwitchResource(switch_tor_resource.clone(), getSwitchProperties)
      }

      override def getServerResources(torSwitchId: NumericalResource, serverId: NumericalResource): Array[NumericalResource] = {
        server_resources.clone()
      }
    }

    println("create large cell")
    val cell = CellFactory.getCell(
      k = simConfig.cellK.get,
      cellType = simConfig.cellType.get,
      resource_provider = resource_provider,
      switchMaxActiveInpTypes = simConfig.maxActiveInp.get
    )

    SharedResources.initializeWithCell(cell)

    assert(calledI == cell.numSwitches, s"Something goes wrong.. switch res should be called exactly as often as switches are there... (Was ${calledI} but should have been ${cell.numSwitches})")

    println("load alibaba trace")
    val workloads = new AlibabaClusterTraceWorkload(
      random = random.copy,
      partialLoadTillTime = simConfig.workloadTime.get,
      skipJobsBeforeTime = simConfig.skipJobsBeforeTime.get,
      traceJobFile = "traces/alibaba_trace_2018/hire_workload_jobs.csv",
      traceClusterMachines = "traces/alibaba_trace_2018/hire_cluster.csv",
      maxServerResources = Array(simConfig.maxResourceUnitServer.get, simConfig.maxResourceUnitServer.get),
      maxSwitchNumericalResource = simConfig.maxResourceUnitSwitches.get,
      setOfSwitchResourceTypes = simConfig.inpTypes.get.toArray,
      minKappaRuntime = simConfig.kappaRuntime.get,
      minKappaServers = simConfig.kappaServers.get,
      muInpFlavor = simConfig.muInpFlavor.get,
      ratioOfIncTaskGroups = simConfig.ratioOfIncTaskGroups.get,
      useRandomForKappa = simConfig.kappaDrawRandoms.get,
      createInpFlavorStartingFromTime = simConfig.createInpFlavorStartingFromTime.get,
      tgMaxDuration = simConfig.tgMaxDuration,
      useSimpleTwoStateInpServerFlavorOptions = simConfig.useSimpleTwoStateInpServerFlavorOptions.get) :: Nil
    val workloadsFillStart = Nil

    val sim = new Simulator(
      workloadProvider = workloads,
      randomManager = random.copy)

    // !!! this should be activated before creating the scheduler -
    // so that we track only those inp types we are interested in
    CellINPLoadStatistics.activate(
      simulator = sim,
      cell = cell,
      inp_types = simConfig.inpTypes.get.toArray,
      file = Some(Paths.get(simConfig.targetStatisticsDir.get, "cell-inp.csv").toAbsolutePath.toString),
      interval = simConfig.statisticsInterval.get
    )

    sim.scheduleEvent(event = new RepeatingEvent(repeatingAction = s => {
      s.logInfo(s"Status report ---- ${sim.msToHuman(sim.currentTime())}")
      TimeIt.printStatistics
      Simulator.gc()
      s.printQuickStats()
    }, interval = simConfig.statusReportMessage.get * 1000, startTime = 0))


    def genFlavorSelector(configName: List[String]): FlavorSelector = {
      configName match {
        case "resubmit" :: timeout :: Nil => new ForceInpButDelayedServerFallbackFlavorSelector(
          configuredServerFallbackDelay = timeout.toInt,
          replaceJobAndResubmitIfNecessary = true)
        case "greedy" :: Nil => new SchedulerDecidesFlavorSelector()
      }
    }

    def parseSolver(s: String, stopSolverWhenUnscheduleFound: Boolean): Solver = {
      val ParallelSolverPattern = "(parallel-[0-9]*-[a-zA-Z-]*)".r

      s match {
        case "ssp" => new SuccessiveShortestSolver(
          stopWhenAllAllocationsFound = true,
          stopAtFirstUnscheduledFlow = stopSolverWhenUnscheduleFound,
          maxSecondsToSearch = simConfig.sspMaxSearchTimeSeconds.get)
        case "relax" => new RelaxationSolver
        case "cost" => new CostScalingSolver
        case ParallelSolverPattern(sub) =>
          val options = sub.substring("parallel-".length).split("-")
          new MultiThreadSolver(
            nestedSolver = options.tail.map(sub1 => parseSolver(sub1, stopSolverWhenUnscheduleFound)),
            thresholdMsRuntimeParallelExecution = options.head.toLong)
      }
    }


    var newSched: Scheduler = null
    simConfig.scheduler.get.foreach(f = (sched: Seq[String]) => {
      newSched = sched match {
        case "kubernetes#" :: tail =>
          val flavorSel: FlavorSelector = genFlavorSelector(tail)
          new KubernetesScheduler(
            name = s"Kubi# $tail µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            simulator = sim,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            takeServerFallbackBeforeBackoff = true,
            takeServerFallbackBeforeUnsched = true,
            flavorSelector = flavorSel)

        case "kubernetes" :: tail =>
          val flavorSel: FlavorSelector = genFlavorSelector(tail)
          new KubernetesScheduler(
            name = s"Kubi $tail µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            simulator = sim,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            takeServerFallbackBeforeBackoff = false,
            takeServerFallbackBeforeUnsched = false,
            flavorSelector = flavorSel)

        case "yarn" :: tail =>
          val flavorSel: FlavorSelector = genFlavorSelector(tail)
          new YarnCapacityScheduler(
            name = s"Yarn Guy $tail " +
              s"µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            node_locality_delay = 100,
            recheck_delay = 50,
            rack_locality_additional_delay = -1,
            simulator = sim,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            flavorSelector = flavorSel,
            takeServerFallbackBeforeBackoff = false)

        case "yarn#" :: tail =>
          val flavorSel: FlavorSelector = genFlavorSelector(tail)
          new YarnCapacityScheduler(
            name = s"Yarn# Guy $tail " +
              s"µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            node_locality_delay = 100,
            recheck_delay = 50,
            rack_locality_additional_delay = -1,
            serverFallbackTimeout = 60000,
            simulator = sim,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            flavorSelector = flavorSel,
            takeServerFallbackBeforeBackoff = true)

        case "sparrow" :: sampling :: recheck :: maxSamplingWhenReCheck :: serverFallback :: tail =>
          val flavorSel: FlavorSelector = genFlavorSelector(tail)
          new SparrowLikeQueueScheduler(
            name = s"Sparrow Guy $tail " +
              s"µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            simulator = sim,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            flavorSelector = flavorSel,
            samplingM = sampling.toInt,
            recheckTimeout = recheck.toInt,
            maxSamplingWhenReCheck = maxSamplingWhenReCheck.toDouble,
            applyServerFallback = serverFallback.toBoolean)

        case "hire" :: solver_name :: Nil =>
          new HireScheduler(
            name = s"Hire $solver_name µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            simulator = sim,
            minQueuingTimeBeforePreemption = simConfig.minQueuingTimeBeforePreemption.get,
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            maxInpFlavorDecisionsPerRound = simConfig.maxInpFlavorDecisionsPerRound.get,
            solver = parseSolver(solver_name, stopSolverWhenUnscheduleFound = false),
            sanityChecks = simConfig.sanityChecks.get)

        case "coco" :: solver_name :: tail =>
          new CoCoScheduler(
            name = s"CoCo $solver_name µ${simConfig.muInpFlavor.get} |${simConfig.maxActiveInp.get}" replaceAll(",", "|"),
            cell = cell,
            simulator = sim,
            solver = parseSolver(solver_name, stopSolverWhenUnscheduleFound = false),
            thinkTimeScaling = simConfig.thinkTimeScaling.get,
            flavorSelector = genFlavorSelector(tail)
          )

      }
    })
    assert(newSched != null, s"Could not determine scheduler type to be used! ${simConfig.scheduler.get.mkString(", ")}")
    sim.setScheduler(newSched)

    if (simConfig.backlogDisableTime.isDefined) {
      sim.scheduleEvent(new ClosureEvent(s => {
        SimulationConfiguration.HIRE_BACKLOG_ENABLE = false
      }, Math.max(0, simConfig.backlogDisableTime.get)))
    }

    // Enable statistics for the flow solver if desired
    if (simConfig.statisticsSolver.get) {
      MCMFSolverStatistics.activate(
        simulator = sim,
        path = Paths.get(simConfig.targetStatisticsDir.get, "solver.csv").toAbsolutePath.toString
      )
    }

    if (simConfig.statisticsScheduler.get) {
      sim.scheduleEvent(new ClosureEvent(s => {
        SchedulerStatistics.activate(
          simulator = s,
          scheduler = newSched,
          file_path = Paths.get(simConfig.targetStatisticsDir.get, "scheduler.csv").toAbsolutePath.toString,
          interval = simConfig.statisticsInterval.get
        )
      }, simConfig.statisticsStartTimestamp.get))
    }

    if (simConfig.statisticsTenant.get._1 || simConfig.statisticsTenant.get._2) {
      sim.scheduleEvent(new ClosureEvent(s => {
        TenantStatistics.activate(
          simulator = s,
          scheduler = newSched,
          path_jobs = Paths.get(simConfig.targetStatisticsDir.get, "jobs.csv").toAbsolutePath.toString,
          path_taskgroups = Paths.get(simConfig.targetStatisticsDir.get, "taskgroups.csv").toAbsolutePath.toString,
          write_job_details = simConfig.statisticsTenant.get._1,
          write_taskgroup_details = simConfig.statisticsTenant.get._2
        )
        for (extra <- simConfig.statisticsJobsExtraSnapShotsHours) {
          Console.out.println(s"Adding Job statistics for snapshot @$extra hours")
          TenantStatistics.activate(
            simulator = s,
            scheduler = newSched,
            path_jobs = Paths.get(simConfig.targetStatisticsDir.get, s"jobs-${extra}h.csv").toAbsolutePath.toString,
            path_taskgroups = Paths.get(simConfig.targetStatisticsDir.get, s"taskgroups-${extra}h.csv").toAbsolutePath.toString,
            write_job_details = simConfig.statisticsTenant.get._1,
            write_taskgroup_details = simConfig.statisticsTenant.get._2,
            termination_timestamp = Some(3600000 * extra)
          )
        }

      }, simConfig.statisticsStartTimestamp.get))
    }

    if (simConfig.statisticsCell.get) {
      sim.scheduleEvent(new ClosureEvent(s => {
        CellStatistics.activate(
          simulator = s,
          scheduler = newSched,
          file = Paths.get(simConfig.targetStatisticsDir.get, "cell.csv").toAbsolutePath.toString,
          interval = simConfig.statisticsInterval.get
        )
      }, simConfig.statisticsStartTimestamp.get))
    }

    if (simConfig.statisticsTiming.get) {
      sim.scheduleEvent(new ClosureEvent(s => {
        TimingStatistics.activate(
          simulator = s,
          path = Paths.get(simConfig.targetStatisticsDir.get, "timings.csv").toAbsolutePath.toString,
          buffer = 200
        )
      }, simConfig.statisticsStartTimestamp.get))
    }


    if (!doNotStartSimulation) {
      Files.writeString(Paths.get(simConfig.targetStatisticsDir.get, "START"),
        s"$simConfig \n${LocalDateTime.now}")

      println("start simulation")
      sim.runSim(simConfig.simulationTime.get)
      println("done")

      Files.writeString(Paths.get(simConfig.targetStatisticsDir.get, "DONE"),
        LocalDateTime.now.toString)
    }
    sim
  }


}
