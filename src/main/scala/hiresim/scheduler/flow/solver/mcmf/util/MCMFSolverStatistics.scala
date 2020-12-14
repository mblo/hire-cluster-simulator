package hiresim.scheduler.flow.solver.mcmf.util

import hiresim.scheduler.flow.solver.Solver
import hiresim.scheduler.flow.solver.graph.FlowGraph
import hiresim.shared.Logging
import hiresim.simulation.Simulator
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.{SimStatsWriter, TimingStatistics}

object MCMFSolverStatistics {

  private var initialized: Boolean = false
  private var writer: SimStatsWriter = _
  private var simulator: Simulator = _

  def getIsInitialized: Boolean = {
    initialized
  }

  def activate(simulator: Simulator, path: String): Unit = {

    // The writer which will handle the I/O job
    writer = new SimStatsWriter(
      filePath = path,
      flushBuffer = 15
    )

    // Add a sim done hook to close the file handle when simulation has finished
    simulator.registerSimDoneHook(simulator => {
      writer.close()
    })

    // Dump the header to the file
    writer.addEntry(
      "RunID",
      "Solver",
      "ParallelRun",
      "SimulationTime",
      "StartTimeNanos",
      "EndTimeNanos",
      "ProducerCnt",
      "NonProducerCnt",
      "Supply",
      "ServerLoad",
      "SwitchLoad",
      "CleanUpTime"
    )

    // Remember needed organizing classes
    this.simulator = simulator
    // Finally mark this Statistics collector as intialized and thus usable
    this.initialized = true

    // Signal success to console
    Logging.verbose("Statistics initialized for solvers.")

  }

  private var current_take_id: Long = 0
  private var start_time_nanos: Long = _
  private var producer_cnt: Long = _
  private var non_producer_cnt: Long = _
  private var supply: Long = _

  private var load_server: Double = -1D
  private var load_switch: Double = -1D
  private var time_cleanup: Long = -1L

  def load(server: Double, switch: Double): Unit = {
    load_server = server
    load_switch = switch
  }

  def cleanup(delta: Long): Unit = {
    time_cleanup = delta
  }

  def start(graph: FlowGraph): Unit = {
    producer_cnt = graph.nodes.producerIds.size
    non_producer_cnt = graph.nodes.nonProducerIds.size
    supply = math.abs(graph.nodes(graph.sinkNodeId).supply)
    start_time_nanos = System.nanoTime()
  }

  def end(solver: Solver,
          graph: FlowGraph,
          wasParallelRun: Boolean = false): Unit = {

    val end_time_nanos = System.nanoTime()
    val elapsed_time_nanos = end_time_nanos - start_time_nanos

    // Post to timing stats
    TimingStatistics.onPostMeasurement("SolverTime", elapsed_time_nanos)

    if (SimulationConfiguration.PRINT_FLOW_SOLVER_STATISTICS)
      Logging.verbose(s"Solved ${solver.name} in ${elapsed_time_nanos / 1000000}ms for ${producer_cnt} producers and ${supply} supply.")

    // If initialized we dump the results to a file for later analysis
    if (initialized) {
      writer.addEntry(
        current_take_id.toString,
        solver.name,
        wasParallelRun.toString,
        simulator.currentTime().toString,
        start_time_nanos.toString,
        end_time_nanos.toString,
        producer_cnt.toString,
        non_producer_cnt.toString,
        supply.toString,
        load_server.toString,
        load_switch.toString,
        time_cleanup.toString
      )
    }

    // Increment take for next run of the solver
    current_take_id += 1

  }

}
