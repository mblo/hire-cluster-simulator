package hiresim.scheduler

import hiresim.cell.Cell
import hiresim.scheduler.SchedulerUtils.solvers
import hiresim.scheduler.flow.coco.CoCoScheduler
import hiresim.scheduler.flow.hire.HireScheduler
import hiresim.scheduler.flow.solver.{MultiThreadSolver, Solver}
import hiresim.scheduler.flow.solver.mcmf.{CostScalingSolver, RelaxationSolver, SuccessiveShortestSolver}
import hiresim.simulation.Simulator

import scala.collection.mutable

object SchedulerUtils {

  private val solvers: mutable.ListBuffer[Solver] = mutable.ListBuffer()
  solvers.append(new SuccessiveShortestSolver(false, false))
  solvers.append(new RelaxationSolver())
  solvers.append(new CostScalingSolver())


  // When activating this guy, we compare all solver results when running the test cases
  solvers.append(new MultiThreadSolver(solvers.toArray))

  private val schedulers: mutable.Map[String, String => ((Cell, Simulator) => Scheduler)] = mutable.HashMap()
  schedulers.put("Hire.SSP",
    name => (cell: Cell,
             sim: Simulator) => new HireScheduler(name, cell, new SuccessiveShortestSolver(), sim))
  schedulers.put("Hire.CS",
    name => (cell: Cell,
             sim: Simulator) => new HireScheduler(name, cell, new CostScalingSolver, sim))
  schedulers.put("Hire.RELAX",
    name => (cell: Cell,
             sim: Simulator) => new HireScheduler(name, cell, new RelaxationSolver(), sim))
  schedulers.put("K8-Greedy",
    name => (cell: Cell,
             sim: Simulator) => new KubernetesScheduler(name, cell, sim,
      flavorSelector = new SchedulerDecidesFlavorSelector()))
  schedulers.put("K8-Resubmit",
    name => (cell: Cell, sim: Simulator) => new KubernetesScheduler(name, cell, sim,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))

  schedulers.put("Yarn-Greedy",
    name => (cell: Cell, sim: Simulator) => new YarnCapacityScheduler(name, cell, sim,
      recheck_delay = 15, node_locality_delay = 1, rack_locality_additional_delay = 1,
      flavorSelector = new SchedulerDecidesFlavorSelector()))
  schedulers.put("Yarn-Resubmit",
    name => (cell: Cell, sim: Simulator) => new YarnCapacityScheduler(name, cell, sim,
      recheck_delay = 15, rack_locality_additional_delay = 1, node_locality_delay = 1,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))
  schedulers.put("Sparrow-Greedy",
    name => (cell: Cell, sim: Simulator) => new SparrowLikeQueueScheduler(name, cell, sim,
      flavorSelector = new SchedulerDecidesFlavorSelector()))
  schedulers.put("Sparrow-Resubmit",
    name => (cell: Cell, sim: Simulator) => new SparrowLikeQueueScheduler(name, cell, sim,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))

  schedulers.put("CoCo.SSP",
    name => (cell: Cell, sim: Simulator) => new CoCoScheduler(name, cell,
      new SuccessiveShortestSolver(stopAtFirstUnscheduledFlow = false), sim,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))
  schedulers.put("CoCo.CS",
    name => (cell: Cell, sim: Simulator) => new CoCoScheduler(name, cell, new CostScalingSolver(), sim,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))
  schedulers.put("CoCo.RELAX",
    name => (cell: Cell, sim: Simulator) => new CoCoScheduler(name, cell, new RelaxationSolver(), sim,
      flavorSelector = new ForceInpButDelayedServerFallbackFlavorSelector(1000,
        replaceJobAndResubmitIfNecessary = true)))

  def onExecuteForeachSolver(function: Solver => Unit): Unit = {
    solvers.foreach(function(_))
  }

  def onExecuteForSpecificSolver(solver: Solver, function: Solver => Unit): Unit = {
    function(solver)
  }

  def onExecuteWithEachScheduler(function: (String, (Cell, Simulator) => Scheduler) => Unit): Unit = {
    schedulers.foreachEntry((name: String, fnc) => function(name, fnc(name)))
  }

}
