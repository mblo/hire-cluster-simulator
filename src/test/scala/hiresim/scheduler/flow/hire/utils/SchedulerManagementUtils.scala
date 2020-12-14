package hiresim.scheduler.flow.hire.utils

import hiresim.cell.{Cell, CellFactory}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.scheduler.flow.hire.HireScheduler
import hiresim.scheduler.flow.solver.Solver
import hiresim.scheduler.flow.solver.mcmf.SuccessiveShortestSolver
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.{RandomManager, Simulator}
import hiresim.tenant.Job
import hiresim.workload.AlibabaClusterTraceWorkload.{RawJobSpec, RawTaskGroupSpec}
import hiresim.workload.{AlibabaClusterTraceWorkload, WorkloadProvider}

import scala.collection.{immutable, mutable}

object SchedulerManagementUtils {

  private var tgCounter = 0

  def createTestEnvironment(k: Int = 4,
                            randomSeed: Long = 0,
                            cellType: String = SimulationConfiguration.DEFAULT_CELL_TYPE,
                            serverResource: Array[NumericalResource] = Array[NumericalResource](20, 20),
                            switchResource: SwitchResource = new SwitchResource(Array(5), SwitchProps.all),
                            workloadProvider: Seq[WorkloadProvider] = Nil,
                            switchMaxActiveInpTypes: Int = 2): (Cell, Simulator) = {
    // Data center physical cell initialization
    val cell = CellFactory.newCell(
      k = k,
      cellType = cellType,
      serverResources = serverResource,
      switchResources = switchResource,
      switchMaxActiveInpTypes = switchMaxActiveInpTypes
    )

    val random = new RandomManager(randomSeed)

    // Creating the simulator
    val simulator = new Simulator(
      workloadProvider = workloadProvider,
      randomManager = random,
      workloadBatchSize = 1000000
    )
    (cell, simulator)
  }

  def createSchedulerTestEnvironment(k: Int = 4,
                                     randomSeed: Long = 0,
                                     cellType: String = SimulationConfiguration.DEFAULT_CELL_TYPE,
                                     serverResource: Array[NumericalResource] = Array[NumericalResource](20, 20),
                                     switchResource: SwitchResource = new SwitchResource(Array(5), SwitchProps.all),
                                     solver: Solver = new SuccessiveShortestSolver(),
                                     workloadProvider: Seq[WorkloadProvider] = Nil): (Cell, Simulator, HireScheduler) = {

    // Data center physical cell initialization
    val (cell, simulator) = createTestEnvironment(k, randomSeed, cellType, serverResource, switchResource, workloadProvider)

    // Creating the scheduler
    val scheduler = new HireScheduler(
      name = "hire",
      cell,
      solver = solver,
      simulator
    )
    simulator.setScheduler(scheduler)

    // Enable sanity checks as this is for testing
    scheduler.graphManager.graph.sanityChecks = true

    // Return the 3 basic components
    (cell, simulator, scheduler)
  }

  def createAlibabaLikeJob(submittedTime: NumericalResource,
                           duration: NumericalResource,
                           cpu: Double,
                           mem: Double,
                           instances: NumericalResource,
                           hasInpFlavor: Boolean): Job = {
    createAlibabaLikeJob(Array(submittedTime), Array(duration), Array(cpu), Array(mem), Array(instances), hasInpFlavor)
  }


  def createAlibabaLikeJob(submittedTime: Array[NumericalResource],
                           duration: Array[NumericalResource],
                           cpu: Array[Double],
                           mem: Array[Double],
                           instances: Array[NumericalResource],
                           hasInpFlavor: Boolean,
                           setOfSwitchResourceTypes: Array[SwitchProps] = SwitchProps.allTypes,
                           wlRandom: RandomManager = new RandomManager(0),
                           switchPropsKappaTsValues: immutable.HashMap[SwitchProps, (Double, Double)] = immutable.HashMap.from(SwitchProps.allTypes.map(p => {
                             (p, p match {
                               case _ => (1.0, 1.0)
                             })
                           })),
                           ratioOfIncTaskGroups: Double = .25): Job = {


    val inpDistribution = {
      Array.fill(setOfSwitchResourceTypes.length)(wlRandom.getInt(2))
    }

    val j = AlibabaClusterTraceWorkload.buildJob(jobSpec = new RawJobSpec(mutable.ListBuffer.from(submittedTime.indices.map(idx => {
      tgCounter += 1
      new RawTaskGroupSpec(
        submittedTime = submittedTime(idx),
        jobType = "container",
        duration = duration(idx),
        cpu = cpu(idx),
        mem = mem(idx),
        instances = instances(idx), links = None, taskGroupName = s"T$tgCounter")
    }))),
      hasInpFlavor = hasInpFlavor,
      ratioOfIncTaskGroups = ratioOfIncTaskGroups,
      random = wlRandom,
      useRandomForKappa = true,
      switchPropsToKappaTSValues = switchPropsKappaTsValues,
      maxServerResources = Array(100, 100),
      maxSwitchNumericalResource = 100,
      setOfSwitchResourceTypes = setOfSwitchResourceTypes,
      inpDistribution = inpDistribution)
    if (hasInpFlavor)
      assert(WorkloadProvider.flavorHasInp(j.allPossibleOptions))
    else {
      assert(!WorkloadProvider.flavorHasInp(j.allPossibleOptions))
      assert(j.getTasksCount == instances.sum, s"Job reports ${j.getTasksCount} tasks, but it should be ${instances.sum}")
    }
    j
  }

  def setAvailableServerResources(affectedGraphNodeID: NodeID,
                                  newAmount: Array[NumericalResource],
                                  cell: Cell,
                                  scheduler: HireScheduler): Unit =
    setAvailableServerResources(Seq(affectedGraphNodeID), newAmount, cell, scheduler)

  def setAvailableSwitchResources(affectedGraphNodeID: NodeID,
                                  newAmount: Array[NumericalResource],
                                  cell: Cell,
                                  scheduler: HireScheduler): Unit =
    setAvailableSwitchResources(Seq(affectedGraphNodeID), newAmount, cell, scheduler)


  def setAvailableServerResources(affectedGraphNodeIDs: Seq[NodeID],
                                  newAmount: Array[NumericalResource],
                                  cell: Cell,
                                  scheduler: HireScheduler): Unit = {

    val mapping = scheduler.graphManager.mapping
    val nodes = scheduler.graphManager.graph.nodes

    for (affected <- affectedGraphNodeIDs) {
      // First change resources
      cell.servers(mapping.getServerMachineIDFromNodeID(affected)) = newAmount.clone()
      // then flag that machine as dirty
      nodes(affected).dirty = true
    }

  }

  def setAvailableSwitchResources(affectedGraphNodeIDs: Seq[NodeID],
                                  newAmount: Array[NumericalResource],
                                  cell: Cell,
                                  scheduler: HireScheduler): Unit = {

    val mapping = scheduler.graphManager.mapping
    val nodes = scheduler.graphManager.graph.nodes

    for (affected <- affectedGraphNodeIDs) {
      // First change resources
      cell.switches(mapping.getSwitchMachineIDFromNetworkID(affected)).numericalResources = newAmount.clone()
      // then flag that machine as dirty
      nodes(affected).dirty = true
    }

  }

}
