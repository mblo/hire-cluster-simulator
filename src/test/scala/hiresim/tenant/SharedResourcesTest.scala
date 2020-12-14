package hiresim.tenant

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.cell.{Cell, CellFactory}
import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration, SimulationConfigurationHelper}
import hiresim.workload.WorkloadProvider
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.{immutable, mutable}

class SharedResourcesTest extends AnyFunSuite with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    SimulationConfigurationHelper.setDefaultSimulationConfiguration()
  }

  def getTestEnvironment(mode: SharedResourceMode,
                         demands: mutable.Map[immutable.BitSet, Array[NumericalResource]],
                         tg_demand: Array[NumericalResource],
                         tg_tasks: Int = 1,
                         tg_options: immutable.BitSet,
                         switch_props: SwitchProps = SwitchProps.all,
                         switch_resources: Array[NumericalResource] = Array[NumericalResource](100, 100)): (Cell, Job, TaskGroup) = {

    SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE = mode

    val cell: Cell = CellFactory.newCell(
      k = 4,
      serverResources = Array[NumericalResource](100, 100),
      switchResources = new SwitchResource(switch_resources, switch_props)
    )

    def informReleaseResources(alloc: Allocation) = {
      // do nothing
    }

    cell.setHookInformOnResourceRelease(informReleaseResources)

    demands.foreachEntry((property, demand) => {
      SharedResources.setPropertyResourceDemand(new SwitchProps(property), demand)
    })

    val (job, tg) = getTestTG(
      tg_demand = tg_demand,
      tg_tasks = tg_tasks,
      tg_options = tg_options
    )
    (cell, job, tg)
  }

  def getTestTG(tg_demand: Array[NumericalResource],
                tg_tasks: Int = 1,
                tg_options: immutable.BitSet): (Job, TaskGroup) = {

    val tg = new TaskGroup(
      isSwitch = true,
      inOption = SwitchProps.none.capabilities,
      notInOption = WorkloadProvider.emptyFlavor,
      numTasks = tg_tasks,
      submitted = 10,
      coLocateOnSameMachine = false,
      duration = 100,
      statisticsOriginalDuration = 100,
      resources = new SwitchResource(tg_demand, new SwitchProps(tg_options))
    )
    val job = new Job(
      submitted = 10,
      isHighPriority = true,
      taskGroups = Array(tg),
      arcs = Array()
    )

    (job, tg)
  }

  // We use those demand values for this test
  val demand_map: mutable.Map[immutable.BitSet, Array[NumericalResource]] = mutable.Map()
  demand_map += (SwitchProps.typeSharp.capabilities -> Array(10, 10))
  demand_map += (SwitchProps.typeHarmonia.capabilities -> Array(15, 15))
  demand_map += (SwitchProps.typeIncBricks.capabilities -> Array(20, 20))

  def executeTest_Alloc_1Property(mode: SharedResourceMode,
                                  point1: Array[NumericalResource],
                                  point2: Array[NumericalResource]): Unit = {

    val (cell, job, tg) = getTestEnvironment(
      mode = mode,
      tg_demand = Array(20, 20),
      tg_tasks = 5,
      tg_options = SwitchProps.typeHarmonia.capabilities,
      demands = demand_map
    )

    // Check initial conditions
    cell.switches.foreach(resource => {
      resource.numericalResources.foreach(element => {
        assert(element == 100)
      })
      assert(resource.properties.capabilities.equals(SwitchProps.all.capabilities))
    })

    // Make an allocation (will claim resources)
    val a1 = new Allocation(
      taskGroup = tg,
      numberOfTasksToStart = 1,
      machine = 2,
      job = job,
      cell = cell,
      startTime = 10
    )
    // Make sure individual and shared demand where taken into account
    assert(cell.switches(2).numericalResources.sameElements(point1))

    // Make a 2nd allocation
    val a2 = new Allocation(
      taskGroup = tg,
      numberOfTasksToStart = 1,
      machine = 2,
      job = job,
      cell = cell,
      startTime = 10
    )
    // As we are in foreach mode we should hhave subtracted the shared resources
    // a second time
    assert(cell.switches(2).numericalResources.sameElements(point2), s"Was: ${cell.switches(2).numericalResources.mkString(", ")} Should: ${point2.mkString(", ")}")

    // Complete alloc 2
    a2.release()
    assert(cell.switches(2).numericalResources.sameElements(point1))
    // Complete alloc 1
    a1.release()
    assert(cell.switches(2).numericalResources.sameElements(Array(100, 100)))
    assert(cell.switches(2).properties.capabilities.equals(SwitchProps.all.capabilities))

  }

  test("sharedResourceAlloc_foreach") {
    executeTest_Alloc_1Property(
      mode = SharedResourceMode.CONSIDER_FOREACH,
      point1 = Array(65, 65),
      point2 = Array(30, 30)
    )
  }

  test("sharedResourceAlloc_once") {
    executeTest_Alloc_1Property(
      mode = SharedResourceMode.CONSIDER_ONCE,
      point1 = Array(65, 65),
      point2 = Array(45, 45)
    )
  }

  test("sharedResourceAlloc_deactivated") {
    executeTest_Alloc_1Property(
      mode = SharedResourceMode.DEACTIVATED,
      point1 = Array(80, 80),
      point2 = Array(60, 60)
    )
  }

  def executeTest_Alloc_2Property(mode: SharedResourceMode,
                                  point1: Array[NumericalResource],
                                  point2: Array[NumericalResource]): Unit = {

    val (cell, job_1, tg_1) = getTestEnvironment(
      mode = mode,
      tg_demand = Array(20, 20),
      tg_tasks = 5,
      tg_options = SwitchProps.typeHarmonia.capabilities,
      demands = demand_map
    )

    val (job_2, tg_2) = getTestTG(
      tg_demand = Array(30, 30),
      tg_tasks = 5,
      tg_options = SwitchProps.typeSharp.or(SwitchProps.typeHarmonia).capabilities,
    )

    // Check initial conditions
    cell.switches.foreach(resource => {
      resource.numericalResources.foreach(element => {
        assert(element == 100)
      })
      assert(resource.properties.capabilities.equals(SwitchProps.all.capabilities))
    })

    // Make an allocation (will claim resources)
    val a1 = new Allocation(
      taskGroup = tg_1,
      numberOfTasksToStart = 1,
      machine = 2,
      job = job_1,
      cell = cell,
      startTime = 10
    )
    // Make sure individual and shared demand where taken into account
    assert(cell.switches(2).numericalResources.sameElements(point1))

    // Make a 2nd allocation
    val a2 = new Allocation(
      taskGroup = tg_2,
      numberOfTasksToStart = 1,
      machine = 2,
      job = job_2,
      cell = cell,
      startTime = 10
    )
    // As we are in foreach mode we should hhave subtracted the shared resources
    // a second time
    assert(cell.switches(2).numericalResources.sameElements(point2), s"Was: ${cell.switches(2).numericalResources.mkString(", ")} Should: ${point2.mkString(", ")}")

    // Complete alloc 2
    a2.release()
    assert(cell.switches(2).numericalResources.sameElements(point1))
    // Complete alloc 1
    a1.release()
    assert(cell.switches(2).numericalResources.sameElements(Array(100, 100)))
    assert(cell.switches(2).properties.capabilities.equals(SwitchProps.all.capabilities))

  }

  test("sharedResourcesAlloc_multiple_props_foreach") {
    executeTest_Alloc_2Property(
      mode = SharedResourceMode.CONSIDER_FOREACH,
      point1 = Array(65, 65),
      point2 = Array(10, 10)
    )
  }

  test("sharedResource2Alloc2_multiple_props_once") {
    executeTest_Alloc_2Property(
      mode = SharedResourceMode.CONSIDER_ONCE,
      point1 = Array(65, 65),
      point2 = Array(25, 25)
    )
  }

  test("sharedResource2Alloc2_multiple_props_deactivated") {
    executeTest_Alloc_2Property(
      mode = SharedResourceMode.DEACTIVATED,
      point1 = Array(80, 80),
      point2 = Array(50, 50)
    )
  }

}
