package hiresim.simulation.configuration

import hiresim.cell.machine.Identifiable
import hiresim.scheduler.flow.hire.{HireLocalityHandlingMode, HireScheduler}
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.tenant.Allocation

object SimulationConfigurationHelper {

  /**
   * Sets the `SimulationConfiguration` to the default values
   */
  def setDefaultSimulationConfiguration(withSanityChecks: Boolean = true): Unit = {
    Allocation.lastId = -1L
    Identifiable.previousId = 0L
    HireScheduler.round = 0


    SimulationConfiguration.SANITY_CHECKS_FAST_ALLOCATION_CHECK = withSanityChecks
    SimulationConfiguration.SANITY_CHECKS_GRAPH = withSanityChecks
    SimulationConfiguration.SANITY_CHECKS_GRAPH_COSTS = withSanityChecks
    SimulationConfiguration.SANITY_CHECKS_SCHEDULER = withSanityChecks
    SimulationConfiguration.SANITY_CHECKS_CELL = withSanityChecks
    SimulationConfiguration.SANITY_CHECKS_HIRE = withSanityChecks
    SimulationConfiguration.SANITY_CHECK_HIRE_CHECK_IF_UNSCHEDULED = withSanityChecks
    SimulationConfiguration.SANITY_CHECK_JOB_CHECK_RESOURCE_REQ = withSanityChecks
    SimulationConfiguration.SANIYY_MCMF_CHECKS = withSanityChecks
    SimulationConfiguration.SANITY_MCMF_COST_SCALING_SANITY_CHECKS = withSanityChecks
    SimulationConfiguration.SANITY_MCMF_CHECK_ZERO_SUPPLY_NETWORK = withSanityChecks
    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK = withSanityChecks
    SimulationConfiguration.SANITY_ALGO_DIJKSTRA_CHECK_CYCLE_FREE = withSanityChecks
    SimulationConfiguration.SANITY_HIRE_PARALLEL_SOLVER_SANITY_RUN_ALL_COMPARE_EQUALITY = withSanityChecks
    SimulationConfiguration.SANITY_HIRE_SHORTCUTS_FEASIBILITY_CHECK = withSanityChecks

    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED = false
    SimulationConfiguration.LOGGING_VERBOSE_CELL = false
    SimulationConfiguration.LOGGING_VERBOSE_SIM = false

    SimulationConfiguration.TIME_IT_ACTIVATE = false
    SimulationConfiguration.TIME_IT_ACTIVATE_PRINT = false
    SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS = true

    SimulationConfiguration.SIM_GC_NOT_WITHIN_SECONDS = 300
    SimulationConfiguration.ONGOING_RUN_ID = -1

    SimulationConfiguration.SIM_TRIGGER_GC_TICKS = 100000

    SimulationConfiguration.PRECISION = 100L

    SimulationConfiguration.SCHEDULER_CONSIDER_THINK_TIME = true

    SimulationConfiguration.KUBERNETES_ENABLE_AGGRESSIVE_BACKOFF_CONSUME = false
    SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = SharedResourceMode.DEACTIVATED
    SimulationConfiguration.SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE = 0.98

    SimulationConfiguration.HIRE_INITIAL_NODE_CONTAINER_SIZE = 100000
    SimulationConfiguration.HIRE_SHORTCUTS_CACHE_THRESHOLD = 1.1
    SimulationConfiguration.HIRE_POSTPONE_SCHEDULING_IF_STUCK = 250

    SimulationConfiguration.MULTICORE_MCMF_SOLVER_ACTIVE_AFTER_TIME = -1
    SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR = 3D
    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER = 500
    SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER = 2000
    SimulationConfiguration.HIRE_MAX_QUEUING_TIME_TO_PREEMPT = 180000

    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP = 50
    SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP = 50
    SimulationConfiguration.HIRE_BACKLOG_ENABLE = true
    SimulationConfiguration.HIRE_BACKLOG_MAX_NEW_JOBS = 800
    SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT = 800
    SimulationConfiguration.HIRE_BACKLOG_SUPPLY_SOFT_LIMIT = -1
    SimulationConfiguration.HIRE_MAX_CONNECTED_TASK_GROUPS_TO_CHECK_LOCALITY = 10
    SimulationConfiguration.HIRE_LOCALITY_MODE_M2S = HireLocalityHandlingMode.DEFAULT
    SimulationConfiguration.HIRE_LOCALITY_MODE_STG2N = HireLocalityHandlingMode.DEFAULT
    SimulationConfiguration.HIRE_LOCALITY_MODE_NTG2N = HireLocalityHandlingMode.DEFAULT
    SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD = 0.95
    SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION = 0.01

    // reset static fields
    CellINPLoadStatistics.resetStaticField()
  }


}
