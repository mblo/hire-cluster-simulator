package hiresim.scheduler.flow.solver

import hiresim.scheduler.flow.solver.graph.FlowGraph
import hiresim.scheduler.flow.solver.mcmf.util.MCMFSolverStatistics
import hiresim.shared.TimeIt
import hiresim.simulation.configuration.SimulationConfiguration

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}
import scala.collection.mutable

/** Interface for scheduling problem solvers */
trait Solver {

  protected var runCount: Int = 0

  @inline def getRunCount: Int = runCount

  /**
   * Provides the name of this solver. This could be e.g.
   * the name of the used algorithm
   *
   * @return the name of this solver
   */
  def name: String

  def solveInline(graph: FlowGraph, maxFlowsOnMachines: Long): Unit


  def checkGraph(graph: FlowGraph): Unit = {
    var totalSupply: Long = 0L
    var expectedProducers = graph.nodes.producerIds.size

    assert(expectedProducers > 0, "No producer node in graph")

    graph.nodes.foreach(n => {
      // track total supply
      totalSupply += n.supply
      // check for producers, if they have enough outgoing capacity
      if (n.isProducer) {
        expectedProducers -= 1
        var leftCapacity = n.supply
        n.outgoing.foreach(arc => {
          leftCapacity -= arc.capacity
        })
        assert(leftCapacity <= 0, s"A producer node (${n.id}) is missing outgoing arc capacity: $leftCapacity")
      }
    })

    assert(expectedProducers == 0, s"There are $expectedProducers producers in the graph which have no supply, " +
      s"but are flagged as producers")

    assert(totalSupply == 0, s"Total supply of graph is imbalanced: $totalSupply")
  }

}

trait ParallelSolver extends Solver {

  def allowParallelProcessing(allow: Boolean): Unit

  def solveInlineOrCloned(graph: FlowGraph, maxFlowsOnMachines: Long): (FlowGraph, Solver)

  override def solveInline(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = throw new IllegalArgumentException()

  def solveWithAll(graph: FlowGraph, maxFlowsOnMachines: Long): Array[(FlowGraph, Solver)]

}

abstract class TrackedSolver extends Solver {

  /**
   * Solves a transportation problem instance and tracks
   * the runtime of the solver.
   *
   * @param graph              the input graph, containing arcs and nodes.
   * @param maxFlowsOnMachines the max number of flows possible to pass nodes of type isMachine
   */
  override final def solveInline(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = {
    runCount += 1

    // check graph here
    if (SimulationConfiguration.SANITY_CHECKS_GRAPH) {
      checkGraph(graph)
    }

    MCMFSolverStatistics.start(graph)
    solveInline0(graph, maxFlowsOnMachines)
    MCMFSolverStatistics.end(this, graph)
  }

  @inline final def solveInlineWithoutStatistics(graph: FlowGraph, maxFlowsOnMachines: Long): Unit = {
    solveInline0(graph, maxFlowsOnMachines)
  }

  protected def solveInline0(graph: FlowGraph, maxFlowsOnMachines: Long): Unit

}


class MultiThreadSolver(nestedSolver: Array[Solver],
                        thresholdMsRuntimeParallelExecution: Long = 300L,
                        testOtherSolversAfterXRuns: Int = 300,
                        testOtherSolversWhenSlowerBy: Double = 1.2,
                        allowStickWithLastSolverChoice: Boolean = false,
                        thresholdMsRuntimeForceParallelExecution: Int = 5000) extends ParallelSolver {

  class SolverHistoryEntry(val solver: Solver,
                           val runtime: Long)

  object SolverHistoryEntry {
    def apply(solver: Solver, runtime: Long): SolverHistoryEntry = new SolverHistoryEntry(solver, runtime)
  }

  class SolverHistory(private val _queue: mutable.ArrayDeque[SolverHistoryEntry] = mutable.ArrayDeque(),
                      val windowSize: Int = 5) {

    private var runsInCurrentMode: Int = 0
    private var switchOverSolver: Solver = nestedSolver(0)
    private var switchOverRuntime: Long = thresholdMsRuntimeParallelExecution
    private var cachedAvgRuntime: Long = -1

    @inline def getRunsInCurrentMode(): Int = runsInCurrentMode

    @inline def getLastRuntime(): Long = _queue.last.runtime

    @inline def getLastSolver(): Solver = _queue.last.solver

    @inline def getSwitchOverRuntime(): Long = switchOverRuntime

    @inline def getSwitchOverSolver(): Solver = switchOverSolver

    def getAvgWindowRuntime(): Long = {
      if (cachedAvgRuntime == -1) {
        var avg = BigInt(0)
        for (entry <- _queue) {
          avg += entry.runtime
        }
        cachedAvgRuntime = (avg / _queue.size).toLong
      }
      cachedAvgRuntime
    }

    def add(solver: Solver, runtime: Long): Unit = {
      _queue.addOne(SolverHistoryEntry(solver, runtime))
      runsInCurrentMode += 1
      cachedAvgRuntime = -1
      while (_queue.size > windowSize)
        _queue.removeHead(false)
    }

    def makeSwitchOver(): Unit = {
      assert(_queue.nonEmpty)
      val last = _queue.last
      switchOverRuntime = getAvgWindowRuntime()
      switchOverSolver = last.solver
      cachedAvgRuntime = -1
      _queue.clear()
      runsInCurrentMode = 0
    }

    @inline def windowIsFull(): Boolean = _queue.size == windowSize

    def sameSolverInFullWindow(): Boolean = {
      if (_queue.size < windowSize) false
      else {
        val testSolver = _queue.head
        var foundOther = false
        for (otherCandidate <- _queue) {
          if (otherCandidate.solver != testSolver.solver) foundOther = true
        }
        !foundOther
      }
    }

  }

  override def name: String = "Threading Solver"

  assert(nestedSolver.length > 1, "this solver expects to have more than 1 nested solvers")

  private var parallelProcessingEnabled: Boolean = true

  private var inSingleProcessingMode: Boolean = thresholdMsRuntimeParallelExecution > 0

  private val solverHistory = new SolverHistory()

  private val executor: ExecutorService = Executors.newCachedThreadPool()


  override def solveWithAll(graph: FlowGraph, maxFlowsOnMachines: Long): Array[(FlowGraph, Solver)] = {
    runCount += 1

    val callableTasks: util.ArrayList[Callable[(FlowGraph, TimeIt, Solver)]] = prepareParallelSolvers(
      graph = graph,
      maxFlowsOnMachines = maxFlowsOnMachines,
      finishAllSolvers = true)
    val results: util.List[Future[(FlowGraph, TimeIt, Solver)]] = executor.invokeAll(callableTasks)
    var i = 0
    val out: Array[(FlowGraph, Solver)] = Array.ofDim(nestedSolver.length)
    results.forEach(fut => {
      val singleResult: (FlowGraph, TimeIt, Solver) = fut.get()
      out(i) = (singleResult._1, singleResult._3)
      i += 1
    })
    out
  }

  def prepareParallelSolvers(graph: FlowGraph, maxFlowsOnMachines: Long, finishAllSolvers: Boolean): util.ArrayList[Callable[(FlowGraph, TimeIt, Solver)]] = {
    val callableTasks: util.ArrayList[Callable[(FlowGraph, TimeIt, Solver)]] =
      new util.ArrayList[Callable[(FlowGraph, TimeIt, Solver)]](nestedSolver.length)

    val done = new AtomicBoolean(false)

    val timeit = TimeIt("Clone Solver Graph", ignore = true)
    for (s <- nestedSolver) {
      // we do cloning sequentially!
      val clonedG = graph.clone()

      val runnableTask = new Callable[(FlowGraph, TimeIt, Solver)] {
        override def call(): (FlowGraph, TimeIt, Solver) = {
          s match {
            case solver: TrackedSolver =>
              solver.solveInlineWithoutStatistics(clonedG, maxFlowsOnMachines)
            case _ =>
              s.solveInline(clonedG, maxFlowsOnMachines)
          }

          if (finishAllSolvers || !done.getAndSet(true)) {
            (clonedG, timeit, s)
          } else {
            throw new InterruptedException()
          }
        }
      }

      callableTasks.add(runnableTask)
    }
    timeit.stop

    callableTasks
  }


  /**
   * Solves a transportation problem instance.
   *
   * @param graph              the input graph, containing arcs and nodes. This object will not be modified
   * @param maxFlowsOnMachines the max number of flows possible to pass nodes of type isMachine
   * @return the modified graph
   */
  override def solveInlineOrCloned(graph: FlowGraph, maxFlowsOnMachines: Long): (FlowGraph, Solver) = {
    runCount += 1
    val timeEffective = TimeIt("Parallel Solver - effective time")

    // check graph here
    if (SimulationConfiguration.SANITY_CHECKS_GRAPH) {
      checkGraph(graph)
    }

    var endedSolver: Option[Solver] = None
    var cloneGraphTimeMs: Long = 0
    val finalResultGraph: FlowGraph = if (inSingleProcessingMode) {
      // do single thread execution
      endedSolver = Some(solverHistory.getSwitchOverSolver())
      MCMFSolverStatistics.start(graph)

      solverHistory.getSwitchOverSolver() match {
        case solver: TrackedSolver =>
          solver.solveInlineWithoutStatistics(graph, maxFlowsOnMachines)
        case _ =>
          solverHistory.getSwitchOverSolver().solveInline(graph, maxFlowsOnMachines)
      }

      graph
    } else {
      // do parallel execution

      MCMFSolverStatistics.start(graph)
      val callableTasks: util.ArrayList[Callable[(FlowGraph, TimeIt, Solver)]] = prepareParallelSolvers(
        graph = graph,
        maxFlowsOnMachines = maxFlowsOnMachines,
        finishAllSolvers = false)
      val (resultGraph: FlowGraph, timeItCopyGraph: TimeIt, actualSolver: Solver) = executor.invokeAny(callableTasks)

      endedSolver = Some(actualSolver)
      timeItCopyGraph.writeNow()
      cloneGraphTimeMs = timeItCopyGraph.diff / 1000000L

      resultGraph
    }

    MCMFSolverStatistics.end(
      solver = endedSolver.get,
      graph = finalResultGraph,
      wasParallelRun = !inSingleProcessingMode)
    timeEffective.stop

    solverHistory.add(endedSolver.get, timeEffective.diff / 1000000L)

    if (thresholdMsRuntimeParallelExecution <= 0) {
      inSingleProcessingMode = false
    } else {

      // shall we switch mode for next solver run?
      if (inSingleProcessingMode) {
        if (parallelProcessingEnabled) {
          // switch from single to parallel?
          if (
          // every x runs, check if there is some faster solver
            solverHistory.getRunsInCurrentMode() > testOtherSolversAfterXRuns ||
              // maybe last switchover time was slow, so there is a threshold which enables parallel solver in such cases
              solverHistory.getAvgWindowRuntime() > thresholdMsRuntimeForceParallelExecution ||
              // or simply check the previous runs, if it is likely that parallel solver will run faster
              (solverHistory.getAvgWindowRuntime() > thresholdMsRuntimeParallelExecution
                &&
                solverHistory.getAvgWindowRuntime() > testOtherSolversWhenSlowerBy * solverHistory.getSwitchOverRuntime())
          ) {
            // next, try parallel solver
            //          println(s"#### single -> parallel, runs:${solverHistory.getRunsInCurrentMode()} " +
            //            s"compare speed: ${solverHistory.getAvgWindowRuntime()}" +
            //            s">${testOtherSolversWhenSlowerBy * solverHistory.getSwitchOverRuntime()}")

            inSingleProcessingMode = false
            solverHistory.makeSwitchOver()
          }
        }
      } else {
        // test if we should change from parallel to single
        if (solverHistory.getAvgWindowRuntime() < thresholdMsRuntimeForceParallelExecution &&
          ((allowStickWithLastSolverChoice || solverHistory.getLastSolver() != nestedSolver.last) &&
            // switch if we found a strictly faster solver
            (solverHistory.sameSolverInFullWindow() ||
              // or switch if we have a full test window and ...
              (solverHistory.windowIsFull() &&
                // ... the current runs are shorter than the threshold
                (solverHistory.getAvgWindowRuntime() < thresholdMsRuntimeParallelExecution
                  // ... or previous single solver + clone time is faster than parallel solver
                  || solverHistory.getSwitchOverRuntime() + cloneGraphTimeMs < solverHistory.getAvgWindowRuntime()
                  // ... or we failed to find a faster solver, but the parallel run is slower compared to single solver
                  || solverHistory.getAvgWindowRuntime() > testOtherSolversWhenSlowerBy * solverHistory.getSwitchOverRuntime()
                  // ... or we found faster solver (although it is not strictly faster)
                  || solverHistory.getAvgWindowRuntime() < testOtherSolversWhenSlowerBy * solverHistory.getSwitchOverRuntime()))
              ))) {
          //        println(s"#### parallel -> single, same solver:${solverHistory.sameSolverInFullWindow()} " +
          //          s"window:${solverHistory.windowIsFull()} " +
          //          s"avg:${solverHistory.getAvgWindowRuntime()}" +
          //          s"<${testOtherSolversWhenSlowerBy * solverHistory.getSwitchOverRuntime()}")

          inSingleProcessingMode = true
          solverHistory.makeSwitchOver()
        }
      }

    }

    (finalResultGraph, endedSolver.get)
  }

  override def allowParallelProcessing(allow: Boolean): Unit = parallelProcessingEnabled = allow

}