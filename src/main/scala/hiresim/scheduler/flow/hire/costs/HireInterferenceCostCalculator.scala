package hiresim.scheduler.flow.hire.costs

import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.graph.NodeType
import hiresim.scheduler.flow.hire.{HireGraphManager, HireScheduler, NodeLinker}
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowNode}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.configuration.SimulationConfiguration.PRECISION
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.{Job, TaskGroup}

import java.math.MathContext
import scala.collection.mutable

class HireInterferenceCostCalculator(graph_manager: HireGraphManager)(implicit scheduler: HireScheduler) extends HireCostDimensionCalculator {

  private lazy val mapping = graph_manager.mapping
  private lazy val graph = graph_manager.graph

  // stores for each flavor node -> a map of flavor->cost, which gives for each flavor the cost term
  private val cachedFlavorSelector2TaskGroupFlavorCost: mutable.HashMap[NodeID, mutable.HashMap[Int, Cost]] = mutable.HashMap()
  // stores for each flavor node -> a map of flavor->boolean, which tells whether there is at
  // least 1 tg involved which is likely to do not get all the requested resources
  private val cachedFlavorSelector2TempDeactivatedFlavors: mutable.HashMap[NodeID, mutable.HashMap[Int, Boolean]] = mutable.HashMap()


  override def onClearCaches(): Unit = {
    cachedFlavorSelector2TaskGroupFlavorCost.clear()
    cachedFlavorSelector2TempDeactivatedFlavors.clear()
  }


  override def getServerToSinkCost(server: FlowNode): Cost = PRECISION

  override def getSwitchToSinkCost(switch: FlowNode): Cost = {
    getSwitchToSinkCost(switch, graph)
  }

  def getSwitchToSinkCost(switchNetworkNode: FlowNode,
                          graph: FlowGraph): Cost = {

    // The machine id of this switch
    val machine = mapping.getSwitchMachineIDFromNetworkID(switchNetworkNode.id)

    // The amount of active properties of that switch
    val active: Int = scheduler.cell.getActiveInpPropsOfSwitch(machine).size
    // The maximum number of active properties on a switch machine
    val max: Int = scheduler.cell.switchMaxActiveInpTypes

    // Only calculate the costs, if there some properties already running
    if (max != 0)
      (PRECISION * active) / max
    else
      PRECISION
  }

  override def getSwitchTaskGroupToNodeCost(taskGroup: TaskGroup,
                                            target: FlowNode,
                                            isPartOfFlavorSelectorPart: Boolean): Cost = {

    val maxActiveInpProps = scheduler.cell.switchMaxActiveInpTypes
    // The properties that are required by the TaskGroup to run
    val required_properties: SwitchProps = taskGroup.resources.asInstanceOf[SwitchResource].properties

    val out =
      if (maxActiveInpProps == 0)
        PRECISION
      else {
        if (target.isMachine) {

          // The id of the switch
          val switch = mapping.getSwitchMachineIDFromNetworkID(target.id)
          // Retrieve the node active properties of that switch
          val properties = scheduler.cell.getActiveInpPropsOfSwitch(switch)

          // If the TaskGroup uses a property that is already active, there is no cost in scheduling here
          if (properties.containsFully(required_properties))
            0L
          else
            (PRECISION * PRECISION) / ((PRECISION * properties.size) / maxActiveInpProps + PRECISION)

        } else {

          // check the max value of the switch, and give the ratio of requested compared to maxAvailable
          // this prefers subtrees, that have more in common with the requested task
          val switchMax: Int = target.maxSwitchResourcesInSubtree.properties.size

          if (switchMax == 0)
            PRECISION
          else
            (PRECISION * PRECISION) / ((PRECISION * required_properties.size) / switchMax + PRECISION)

        }
      }
    assert(out >= 0 && out <= PRECISION)
    out
  }

  override def getFlavorSelectorToTaskGroupCost(flavorNodeLinker: NodeLinker,
                                                taskGroup: TaskGroup,
                                                taskGroupNode: FlowNode): Cost = {

    // Make sure the cached values about costs are fresh
    updateFlavorSelectorCaches(flavorNodeLinker, taskGroup.job.get)
    //  Now simply get the cached value from the map
    val cached = cachedFlavorSelector2TaskGroupFlavorCost(flavorNodeLinker.flavorNodeId)

    var sum: Long = 0L
    var cnt: Int = 0

    taskGroup.inOption.foreach(flavor => {
      sum += cached(flavor)
      cnt += 1
      assert(sum >= 0, "long overflow")
    })

    scheduler.logDetailedVerbose(s"Flavor-TG, cache for ${taskGroup}=" +
      s"${taskGroup.inOption.map(cached(_)).mkString("|")}, and result is $sum / $cnt = ${sum / cnt}")

    sum / cnt
  }


  override def capacityFlavorSelector2TaskGroup(linker: NodeLinker, taskGroup: TaskGroup): Capacity = {
    1L
  }

  private def updateFlavorSelectorCaches(linker: NodeLinker, job: Job): Unit = {
    if (!cachedFlavorSelector2TaskGroupFlavorCost.contains(linker.flavorNodeId)) {

      // update the cache for each of the possible flavors the flavor selector could reach
      val flavorCache: mutable.HashMap[Int, Cost] = mutable.HashMap()
      cachedFlavorSelector2TaskGroupFlavorCost.put(linker.flavorNodeId, flavorCache)

      val flavorDeactivated: mutable.HashMap[Int, Boolean] = mutable.HashMap()
      cachedFlavorSelector2TempDeactivatedFlavors.put(linker.flavorNodeId, flavorDeactivated)

      val allFlavor = job.allPossibleOptions

      val summedCostTerms: Array[BigInt] = Array.fill(allFlavor.max + 1)(BigInt(0))
      val involvedTaskGroups: Array[Int] = Array.fill(allFlavor.max + 1)(0)

      // Check all the task groups of the flavor selector
      linker.forEachFlavorFlowNode(taskGroupNode => {

        if (SimulationConfiguration.SANITY_CHECKS_HIRE)
          assert(taskGroupNode.nodeType == NodeType.SERVER_TASK_GROUP || taskGroupNode.nodeType == NodeType.NETWORK_TASK_GROUP)

        val taskGroup = taskGroupNode.taskGroup.get
        val taskGroupFlavors = taskGroup.inOption

        // find cheapest edge of this other tg
        var cheapest: Cost = PRECISION // this is the max value such an edge could have
        var totalCapacity: Long = 0

        taskGroupNode.outgoing.foreach(arc => {
          if (cheapest > arc.cost)
            cheapest = arc.cost

          totalCapacity += arc.capacity
        })

        // The following assertion does not hold
        // assert(totalCapacity >= taskGroup.maxAllocationsInOngoingSolverRound)
        assert(cheapest >= 0 && cheapest <= PRECISION, s"there is a task group with an invalid cost term, ${cheapest}")

        val pending_tasks = taskGroupNode.taskGroup.get.notStartedTasks
        // Calculate the primary cost term as described in the paper.
        val scaling: Long =
          (if (totalCapacity == 0 || (totalCapacity < pending_tasks && taskGroup.isSwitch)) {
            PRECISION.toDouble
          } else {
            ((PRECISION * pending_tasks).toDouble / totalCapacity.toDouble)
          }).toLong min PRECISION

        var atLeastOneFlavorMatch = false
        allFlavor.foreach(flavor =>
          // Only respect those flavors this TaskGroup is actually in
          if (taskGroupFlavors.contains(flavor)) {
            atLeastOneFlavorMatch = true
            scheduler.logVerbose(s"\t  tgNodeid:$taskGroupNode  fl:$flavor  scaling:$scaling  " +
              s"cheapest:$cheapest  #:$pending_tasks vs c:$totalCapacity  = " +
              s"${cheapest * pending_tasks * scaling} ${taskGroupNode.taskGroup.get.detailedToString()}")


            // this is a task group which is part of the flavor, so consider this for the cost term
            summedCostTerms(flavor) += cheapest * pending_tasks * scaling
            involvedTaskGroups(flavor) += pending_tasks

          })
        assert(atLeastOneFlavorMatch, s"there is a flavor flow node, but this guy does not match on any flavor! ${taskGroup.detailedToString()}")

      })

      // Transform the summed cost terms to the per task group average of all the cost terms
      summedCostTerms.indices.foreach(flavor => {
        if (involvedTaskGroups(flavor) > 0)
          summedCostTerms(flavor) = summedCostTerms(flavor)./(involvedTaskGroups(flavor))
      })

      var largestSum: BigInt = 0
      var smallestSum: BigInt = Long.MaxValue

      // get min max, but ignore 0
      allFlavor.foreach(flavor => {

        val v = summedCostTerms(flavor)

        // Update biggest cost term we found
        if (v > largestSum)
          largestSum = v

        // Update smallest cost term we found
        if (v > 0 && v < smallestSum)
          smallestSum = v

      })

      // check for the case if there is no sum term at all
      if (smallestSum > largestSum)
        smallestSum = largestSum

      // if there is no entry (e.g. none of the task groups shows a possible shortcut, maybe cluster is fully utilized..),
      // or if there is only a single entry
      if (largestSum.equals(smallestSum)) {
        allFlavor.foreach(flavor => flavorCache.put(flavor, PRECISION / 2))
        scheduler.logVerbose(s"Flavor-TG cache, largest sum = smallest sum = ${largestSum}")
      } else {
        // we scale the terms F-G from PRECISION/2 to PRECISION, so that the cheapest flavor is more expensive than the common part (i.e. first we do the common part selection
        allFlavor.foreach(flavor => {

          if (summedCostTerms(flavor).equals(0))
            flavorCache.put(flavor, PRECISION)
          else {
            // scale term with respect to smallest and largest sum, so that the values go from PRECISION/2 to PRECISION

            val newEntry: Capacity = PRECISION / 2 + ((PRECISION / 2) * BigDecimal(summedCostTerms(flavor) - smallestSum, MathContext.DECIMAL32).
              /(BigDecimal(largestSum - smallestSum, MathContext.DECIMAL32))).
              toLong

            assert(newEntry >= PRECISION / 2 && newEntry <= PRECISION)
            flavorCache.put(flavor, newEntry)
          }

          scheduler.logVerbose(s"Flavor-TG cache, for flavor:${flavor}=${flavorCache(flavor)} " +
            s"based on sum=${summedCostTerms(flavor)} compared to max=$largestSum min=$smallestSum")

        })
      }

    }
  }

}
