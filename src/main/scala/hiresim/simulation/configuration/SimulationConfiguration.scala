package hiresim.simulation.configuration

import hiresim.scheduler.flow.hire.HireLocalityHandlingMode

/**
 * This object provides various options on controlling the simulation execution
 * flow. For example the use of sanity checks and logging output can be regulated.
 */
object SimulationConfiguration {
  var SANITY_CHECKS_FAST_ALLOCATION_CHECK: Boolean = false
  var SANITY_CHECKS_GRAPH: Boolean = false
  var SANITY_CHECKS_GRAPH_COSTS: Boolean = false
  var SANITY_CHECKS_SCHEDULER = false
  var SANITY_CHECKS_CELL: Boolean = false
  var SANITY_CHECKS_HIRE: Boolean = false
  var SANITY_CHECK_HIRE_CHECK_IF_UNSCHEDULED: Boolean = false
  var SANITY_CHECK_JOB_CHECK_RESOURCE_REQ = true
  var SANIYY_MCMF_CHECKS = false
  var SANITY_MCMF_COST_SCALING_SANITY_CHECKS = false
  var SANITY_MCMF_CHECK_ZERO_SUPPLY_NETWORK = false
  var SANITY_ALGO_DIJKSTRA_CHECK_SINGLE_SINK: Boolean = false
  var SANITY_ALGO_DIJKSTRA_CHECK_CYCLE_FREE: Boolean = false
  var SANITY_HIRE_PARALLEL_SOLVER_SANITY_RUN_ALL_COMPARE_EQUALITY: Boolean = false
  var SANITY_HIRE_SHORTCUTS_FEASIBILITY_CHECK: Boolean = false

  var LOGGING_VERBOSE_OTHER: Boolean = false
  var LOGGING_VERBOSE_SCHEDULER: Boolean = false
  var LOGGING_VERBOSE_SCHEDULER_DETAILED: Boolean = false
  var LOGGING_VERBOSE_CELL: Boolean = false
  var LOGGING_VERBOSE_SIM: Boolean = false

  var TIME_IT_ACTIVATE: Boolean = false
  var TIME_IT_ACTIVATE_PRINT: Boolean = false
  var PRINT_FLOW_SOLVER_STATISTICS: Boolean = true

  // trigger extra GC call not withing x seconds
  var SIM_GC_NOT_WITHIN_SECONDS = 300
  var ONGOING_RUN_ID: Int = -1

  // trigger GC after ticks
  var SIM_TRIGGER_GC_TICKS: Integer = 100000

  // used for int calculation divisions
  var PRECISION: Long = 100L

  // de/activates think time of schedulers
  var SCHEDULER_CONSIDER_THINK_TIME = true

  val DEFAULT_CELL_TYPE: String = "ft"

  var KUBERNETES_ENABLE_AGGRESSIVE_BACKOFF_CONSUME: Boolean = false
  var SCHEDULER_SHARED_RESOURCE_MODE: SharedResourceMode = SharedResourceMode.DEACTIVATED
  var SCHEDULER_ACTIVE_MAXIMUM_CELL_SERVER_PRESSURE = 0.98

  // initial internal array size of node container
  var HIRE_INITIAL_NODE_CONTAINER_SIZE: Int = 100000
  // threshold for creating new lookup caches
  var HIRE_SHORTCUTS_CACHE_THRESHOLD: Double = 1.1
  // postpone scheduling by x time units,
  // if the scheduling problem remains same (nothing added job/tg or no resource changed)
  var HIRE_POSTPONE_SCHEDULING_IF_STUCK: Int = 250

  var MULTICORE_MCMF_SOLVER_ACTIVE_AFTER_TIME = -1
  var HIRE_SERVER_PENALTY_COST_FACTOR: Double = 3D
  var HIRE_SERVER_PENALTY_WAITING_LOWER = 500
  var HIRE_SERVER_PENALTY_WAITING_UPPER = 2000
  var HIRE_MAX_QUEUING_TIME_TO_PREEMPT = 180000

  var HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP: Int = 50
  var HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP: Int = 50
  var HIRE_BACKLOG_ENABLE = true
  var HIRE_BACKLOG_MAX_NEW_JOBS = 800
  var HIRE_BACKLOG_PRODUCER_SOFT_LIMIT = 800
  var HIRE_BACKLOG_SUPPLY_SOFT_LIMIT: Int = -1
  var HIRE_MAX_CONNECTED_TASK_GROUPS_TO_CHECK_LOCALITY: Int = 10
  /**
   * Options to enable/disable each of the 3 locality cost terms used by hire
   */
  var HIRE_LOCALITY_MODE_M2S: HireLocalityHandlingMode = HireLocalityHandlingMode.DEFAULT
  var HIRE_LOCALITY_MODE_STG2N: HireLocalityHandlingMode = HireLocalityHandlingMode.DEFAULT
  var HIRE_LOCALITY_MODE_NTG2N: HireLocalityHandlingMode = HireLocalityHandlingMode.DEFAULT
  /**
   * if any of the resource dimensions is overloaded, add only a small portion of TGs of the affected resources to the network
   */
  var HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD: Double = 0.95
  var HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION: Double = 0.01
}
