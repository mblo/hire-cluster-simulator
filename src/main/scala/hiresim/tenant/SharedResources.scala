package hiresim.tenant

import hiresim.cell.Cell
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.SwitchProps
import hiresim.simulation.configuration.{SharedResourceMode, SimulationConfiguration}

import scala.collection.{immutable, mutable}

object SharedResources {

  private val resources_cache: mutable.Map[immutable.BitSet, Array[NumericalResource]] = mutable.Map[immutable.BitSet, Array[NumericalResource]]()
  private val atomic_resource_cache: mutable.Map[Int, Array[NumericalResource]] = mutable.Map[Int, Array[NumericalResource]]()

  private var null_resource_demand: Array[NumericalResource] = Array(0)
  private var resource_dimensions: Int = -1

  def isActive: Boolean = {
    SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE != SharedResourceMode.DEACTIVATED
  }

  def initializeWithCell(cell: Cell): Unit = {
    resource_dimensions = cell.resourceDimSwitches
    null_resource_demand = Array.fill[NumericalResource](resource_dimensions)(0)
  }

  def setPropertyResourceDemand(property: SwitchProps, demand: Array[NumericalResource]): Unit = {

    // Must be initialized
    assert(resource_dimensions != -1, "Shared resources not initialized")
    // Only accept atomic properties
    assert(property.size == 1, "Only accepting atomic properties!")
    // Dimensions must match
    assert(demand.length == resource_dimensions, s"Dimension mismatch! Resource vector is ${demand.length} long but expected ${resource_dimensions}")

    // Store that property
    property.capabilities.foreach(property => {
      atomic_resource_cache += property -> demand
    })

    // Clear cache (maybe this was a overwrite)
    resources_cache.clear()
  }

  def getCellResourceDemandAndRealUsage(all_tg_properties: SwitchProps,
                                        changed_machine_properties: SwitchProps): (Array[NumericalResource], Array[NumericalResource]) = {
    if (!isActive) {
      (null_resource_demand, null_resource_demand)
    } else {
      val target_capabilities =
      // If we are in CONSIDER_ONCE mode we have to consider each prop only once
        if (SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE == SharedResourceMode.CONSIDER_ONCE)
          changed_machine_properties.capabilities
        // Otherwise we take all props this tg has
        else
          all_tg_properties.capabilities

      val cellResourceDemand =
      // If there is no property requested then there is also no demand
        if (target_capabilities.nonEmpty)
          getResourcesCached(target_capabilities)
        else
          null_resource_demand

      val realResourceUsage =
      // If the mode is once, we do not need to query again and can just take the target result
        if (SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE == SharedResourceMode.CONSIDER_ONCE)
          cellResourceDemand
        // Otherwise we must query for the difference
        else {
          if (changed_machine_properties.capabilities.nonEmpty)
            getResourcesCached(changed_machine_properties.capabilities)
          else
            null_resource_demand
        }

      (cellResourceDemand, realResourceUsage)
    }
  }

  private def getResourcesCached(properties: immutable.BitSet): Array[NumericalResource] = {
    resources_cache.getOrElseUpdate(properties, {

      val demand_vector: Array[NumericalResource] = new Array[NumericalResource](resource_dimensions)

      // Construct the demand vector by looking at each of the sub props
      properties.foreach(property => {
        // The sub property the current request has
        val property_demand = atomic_resource_cache(property)
        // Sum up every dimension
        (0 until resource_dimensions).foreach(res_index => {
          demand_vector(res_index) += property_demand(res_index)
        })
      })

      demand_vector

    })
  }

}
