package hiresim.cell

import hiresim.cell.factory.util.CellFactoryIntegrityValidator
import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchProps, SwitchResource}
import hiresim.shared.{ArrayUtils, Logging, Tabulator}
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.tenant.Graph.NodeID
import hiresim.tenant.{Allocation, SharedResources, TaskGroup}

import java.math.MathContext
import scala.collection.mutable

/** Physical data center cell using a tree topology.
 *
 * @param servers      servers available resources.
 *                     First index: server ID.
 *                     Second index: dimension.
 * @param switches     switches available resources.
 * @param switchLevels for each physical switch, store its level
 *                     in the physical topology.
 *                     Leaves (ToR switches) are at level 0.
 */
class Cell(val servers: Array[Array[NumericalResource]],
           val switches: Array[SwitchResource],
           val links: Array[CellEdge],
           val numTorSwitches: Int,
           val numRootSwitches: Int,
           val serversPerRack: Int,
           val switchLevels: Array[Int],
           val switchMaxActiveInpTypes: Int = 2,
           val name: String) {

  def getMaxDistanceBetween(leftGroupSwitches: mutable.BitSet, rightGroupServers: mutable.BitSet, rightGroupSwitches: mutable.BitSet): Int = {
    var maxDistance = 0
    for (leftSwitch <- leftGroupSwitches) {
      for (rightSwitch <- rightGroupSwitches) {
        if (leftSwitch != rightSwitch) {
          val dist = getDistanceOf(src = leftSwitch, srcIsSwitch = true, dst = rightSwitch, dstIsSwitch = true)
          if (dist > maxDistance) maxDistance = dist
        }
      }

      for (rightSwitch <- rightGroupServers.map(lookup_ServerNoOffset2ToR)) {
        if (leftSwitch != rightSwitch) {
          val dist = 1 + getDistanceOf(src = leftSwitch, srcIsSwitch = true, dst = rightSwitch, dstIsSwitch = true)
          if (dist > maxDistance) maxDistance = dist
        }
      }
    }
    maxDistance
  }


  def getAdditionalSwitchLevelToReachServers(switches: mutable.BitSet, servers: mutable.BitSet): Int = {
    var maxLevel = Integer.MIN_VALUE
    val reachableServers: mutable.BitSet = mutable.BitSet()

    assert(switches.nonEmpty)
    assert(servers.nonEmpty)

    for (switch <- switches) {
      val level = this.switchLevels(switch)
      if (level > maxLevel) maxLevel = level
      reachableServers.addAll(lookup_Switch2Servers(switch))
    }

    if (reachableServers.intersect(servers).size == servers.size) {
      // we reach all servers with these switches :)
      0
    } else {
      // we need additional level
      val considerSwitches = switches.clone()
      var additionalLevel = 0

      while (maxLevel < this.highestLevel && (reachableServers.intersect(servers).size < servers.size)) {
        // we could try to add more switches
        additionalLevel += 1

        val addSwitches = mutable.BitSet()
        // add all the parents of the top layer
        for (currentSwitch <- considerSwitches) {
          if (switchLevels(currentSwitch) == maxLevel)
            addSwitches.addAll(lookup_Switch2Parents(currentSwitch))
        }

        assert(addSwitches.nonEmpty)
        considerSwitches.addAll(addSwitches)

        for (switch <- addSwitches) {
          reachableServers.addAll(lookup_Switch2Servers(switch))
        }

        maxLevel += 1
      }

      // we either found all switches, or reached top layer
      if (reachableServers.intersect(servers).size == servers.size) {
        additionalLevel
      } else {
        // we did not reach all, so we need to do a detour by going from a middle layer to a different top layer
        additionalLevel + 2
      }
    }
  }


  // enforce INP statistics to refresh with this cell
  CellINPLoadStatistics.activated = false

  /* `NodeID`s
     *
     * +---------------------+                 +--------------------+
     * |    Root switches    |                 |    ToR switches    |
     * +---------------------------------------+--------------------+---------------------+
     * |                          Switches                          |       Servers       |
     * +------------------------------------------------------------+---------------------+
     * <--------------------- switches.length ----------------------><-- servers.length -->
     * <-- numRootSwitches -->                 <-- numTorSwitches -->
     */

  /** Number of servers and switches */
  val numServers: Int = servers.length
  val numSwitches: Int = switches.length
  val numMachines: Int = numSwitches + numServers

  val inpPropsOfCell: SwitchProps = switches.foldLeft(SwitchProps.none)((other, sw) => other.or(sw.properties))

  // Make sure the links are correctly setup if desired
  if (SimulationConfiguration.SANITY_CHECKS_CELL)
    CellFactoryIntegrityValidator.checkLinks(links, numSwitches)

  assert(numSwitches > 0)
  assert(numServers > 0)
  assert(numTorSwitches > 0)

  val backrefAllocationsServer: Array[mutable.TreeSet[Allocation]] = servers.map(_ => mutable.TreeSet())
  val backrefAllocationsSwitches: Array[mutable.TreeSet[Allocation]] = switches.map(_ => mutable.TreeSet())

  private val activeInpPropsPerSwitch: Array[SwitchProps] = switches.map(_ => SwitchProps.none)

  /* Holding the information about the maximum resource capabilities */
  val serverResources: Array[Array[NumericalResource]] = servers.map(_.clone())
  val switchResources: Array[SwitchResource] = switches.map(_.clone())

  // this assumes all switches of same res dim
  val resourceDimSwitches: Int = {
    val newDim = switches(0).numericalResources.length
    assert(!switches.exists(s => s.numericalResources.length != newDim), "there are switches with different resource dimensions")
    newDim
  }

  // this assumes all servers of same res dim
  val resourceDimServer: Int = {
    val newDim = servers(0).length
    assert(!servers.exists(s => s.length != newDim), "there are severs with different resource dimensions")
    newDim
  }

  SharedResources.initializeWithCell(this)

  val totalMaxSwitchCapacity: Array[Array[NumericalResource]] = switches.map(sw => sw.numericalResources.clone())
  val totalMaxServerCapacity: Array[Array[NumericalResource]] = servers.map(sr => sr.clone())

  // The claimed resources as thought by the scheduler
  var statistics_switch_claimed_resources_scheduler: Array[BigInt] = Array.fill(resourceDimSwitches)(BigInt(0))
  var statistics_server_claimed_resources_scheduler: Array[BigInt] = Array.fill(resourceDimServer)(BigInt(0))
  // The actual claimed resources
  var statistics_switch_claimed_resources_actual: Array[BigInt] = Array.fill(resourceDimSwitches)(BigInt(0))


  val maxCapacitySwitches: Array[BigInt] = (0 until resourceDimSwitches).map(d => {
    totalMaxSwitchCapacity.map(x => BigInt(x(d))).sum
  }).toArray


  val maxCapacityServers: Array[BigInt] = (0 until resourceDimServer).map(d => {
    totalMaxServerCapacity.map(x => BigInt(x(d))).sum
  }).toArray

  def currentSwitchLoad(): BigDecimal = {
    currentSwitchLoads().sum / resourceDimSwitches
  }

  def currentSwitchLoadMax(): BigDecimal = {
    currentSwitchLoads().max
  }

  def currentSwitchLoads(): IndexedSeq[BigDecimal] = {
    (0 until resourceDimSwitches).map(d => {
      BigDecimal(statistics_switch_claimed_resources_scheduler(d)) / BigDecimal(maxCapacitySwitches(d))
    })
  }

  def currentServerLoad(): BigDecimal = {
    currentServerLoads().sum / resourceDimServer
  }

  def currentServerLoadMax(): BigDecimal = {
    currentServerLoads().max
  }

  def currentServerLoads(): IndexedSeq[BigDecimal] = {
    (0 until resourceDimServer).map(d => {
      BigDecimal(statistics_server_claimed_resources_scheduler(d)) / BigDecimal(maxCapacityServers(d))
    })
  }

  def getMachineLoad(machine: NodeID, isSwitch: Boolean, scaling: Long): Array[Long] = {
    val (machineFree, machineMax) = if (isSwitch)
      (switches(machine).numericalResources, totalMaxSwitchCapacity(machine)) else
      (servers(machine), totalMaxServerCapacity(machine))
    val length = machineFree.length

    //    val out: Array[Long] = Array.copyAs[Long](machineFree, machineFree.length)
    val out: Array[Long] = new Array(length)
    var i = 0
    while (i < length) {
      out(i) = (scaling * (machineMax(i) - machineFree(i))) / machineMax(i)
      i += 1
    }
    out
  }

  private val endToEndHops: Array[Array[Int]] = {

    /**
     * Finds all shortest end to end paths by using floyd warshall internally (which is O(n power 3))
     * but with some optimizations so that we do not have to run FloydW on the whole topology ..
     *
     * complexity =  O(Switches pow 3 + switch*tor*server + tor*server + tor*server*server)
     *
     * IMPORTANT: The current implementation only works if each server has exactly one ToR!
     *
     */

    val timeStart = System.nanoTime()
    val distances = Array.fill(numMachines)(Array.fill(numMachines)(-1))

    Logging.verbose_cell("Calculating all end-to-end shortest path distances ... (This might take a while)")

    def runFloydWarshallForSwitches(intermediates: Range,
                                    sources: Range,
                                    destinations: Range): Unit = {

      // runs floyd warshall for switches only!
      for (intermediate <- intermediates) {

        if (intermediate % 100 == 0)
          Logging.verbose_cell(".", ident = false, break = false)

        // Now check every source/destination pair to route via the current intermediate
        for (source <- sources) {
          for (destination <- destinations) {

            if (source != destination) {
              if (source != intermediate && destination != intermediate
                && distances(source)(intermediate) > 0 && distances(intermediate)(destination) > 0) {

                val newCost = distances(source)(intermediate) + distances(intermediate)(destination)
                val oldCost = distances(source)(destination)

                // If we haven't seen this machine (oldCost is -1 in this case) or the cost routing via the current intermediate is better we update the cost
                if (oldCost == -1 || oldCost > newCost)
                  distances(source)(destination) = newCost

              }
            } else {
              // The case where source and destination are equal. We habe 0 cost in routing to the same machine.
              distances(source)(destination) = 0
            }

          }
        }

      }

      Logging.verbose_cell(" Complete", ident = false)
    }

    // Init the distance table with the known links. As we have uniform distances. Every connected node is 1 unit apart from the other.
    links.foreach(link => {
      distances(link.srcWithOffset)(link.dstWithOffset) = 1
      distances(link.dstWithOffset)(link.srcWithOffset) = 1
    })

    val range_switches = 0 until numSwitches
    val range_tor_switches = numSwitches - numTorSwitches until numSwitches
    val range_servers = numSwitches until numMachines


    Logging.verbose_cell("Calculating switch to switche distances .", break = false)
    // Measure distances between the switches
    runFloydWarshallForSwitches(
      intermediates = range_switches,
      sources = range_switches,
      destinations = range_switches
    )

    val torStartOffset: NodeID = numSwitches - numTorSwitches

    // now copy the ToR connections to its children, but adding +1 for each
    Logging.verbose_cell("Calculating server to switch distances ... ")
    // check all ToR and connected Servers
    val serverAndSwitches = numMachines
    //    val serverPointer: Array[Array[Int]] = new Array(numTorSwitches)
    var tor = torStartOffset // 1st id of ToR switch
    while (tor < numSwitches) {
      // find the first server
      var server = numSwitches // this is the offset, so the id of the very first server
      // simply copy the tor dst array, but add everywhere a +1
      val dstArrayOfAnyServer: Array[Int] = {
        val tmp = distances(tor).clone()
        assert(tmp.hashCode() != distances(tor).hashCode())

        var i = 0
        while (i < serverAndSwitches) {
          if (tmp(i) != -1) {
            tmp(i) += 1
          }
          i += 1
        }

        tmp(tor) = 1 // servers have a connection of one to this ToR

        tmp
      }

      // now check all servers of this ToR
      while (server < serverAndSwitches) {
        // is this a server of this tor?
        if (distances(tor)(server) == 1) {
          // overwrite the dst vector of this server
          distances(server) = dstArrayOfAnyServer.clone()
          distances(server)(server) = 0
        }
        server += 1
      }
      tor += 1
    }


    Logging.verbose_cell("Calculating switches to server distances .", break = false)
    // Now figure out the distances from the switches to the servers via the tor switches.
    runFloydWarshallForSwitches(
      sources = range_switches,
      intermediates = range_tor_switches,
      destinations = range_servers
    )

    Logging.verbose_cell("Calculating server to server distances .", break = false)
    // Now all switches have all hops to all switches and to all servers but servers do not have all connections
    // to all other servers, so calculate the server to server distances finally.
    runFloydWarshallForSwitches(
      sources = range_servers,
      intermediates = range_tor_switches,
      destinations = range_servers)

    // Final message will give us some information on the total stats
    Logging.verbose_cell(s"Done building all to all shortest path for $numMachines nodes in ${(System.nanoTime() - timeStart) / 1000000}ms")
    // Return the computed distance table
    distances

  }

  val maxDistance: Int = endToEndHops.map(_.max).max

  // Range of the tor switches
  val torSwitches: Range = (switches.length - numTorSwitches).until(switches.length)

  // Lookup table to quickly find the ToR switch of a Server
  val lookup_ServerNoOffset2ToR: Array[NodeID] = {

    val generate: Array[NodeID] = new Array[NodeID](numServers)

    links.foreach(edge => {
      if (edge.srcSwitch && !edge.dstSwitch)
        generate(edge.dstWithOffset - numSwitches) = edge.srcWithOffset
    })

    generate
  }

  // Lookup table to quickly find all members of rack
  val lookup_ToR2MembersWithoutOffset: mutable.Map[NodeID, mutable.BitSet] = mutable.HashMap()

  val lookup_Switch2Parents: mutable.Map[NodeID, mutable.BitSet] = mutable.HashMap()

  // Lookup table to quickly find all members of rack
  val lookup_Switch2Servers: mutable.Map[NodeID, mutable.BitSet] = {
    val switchToSwitches: mutable.Map[NodeID, mutable.BitSet] = mutable.Map()
    val switchToServers: mutable.Map[NodeID, mutable.BitSet] = mutable.Map()

    // build switch to switch map, and tor switch to machines
    links.foreach((edge: CellEdge) => {
      if (edge.srcSwitch && edge.dstSwitch) {
        val entry: mutable.BitSet = switchToSwitches.getOrElseUpdate(edge.srcWithOffset, mutable.BitSet())
        entry.add(edge.dstWithOffset)

        // update switch to parents update
        val childSwitch: mutable.BitSet = lookup_Switch2Parents.getOrElseUpdate(edge.dstWithOffset, mutable.BitSet())
        childSwitch.add(edge.srcWithOffset)

      } else if (edge.srcSwitch && !edge.dstSwitch) {
        // To get the server id we need to take care of the offset
        val serverWithoutOffset = edge.dstWithOffset - numSwitches

        val entry: mutable.BitSet = lookup_ToR2MembersWithoutOffset.getOrElseUpdate(edge.srcWithOffset, mutable.BitSet())
        // if entry is empty, this is the 1st entry of this tor, so add it also to the switch to server map
        if (entry.isEmpty)
          switchToServers.addOne(edge.srcWithOffset, entry)

        entry.add(serverWithoutOffset)
      }
    })

    // now we have all tor switches with machines
    val finalizedSwitches: mutable.BitSet = mutable.BitSet()
    finalizedSwitches.addAll(switchToServers.keys)
    while (switchToServers.size < numSwitches) {
      assert(finalizedSwitches.nonEmpty)
      for (entry: (NodeID, mutable.BitSet) <- switchToSwitches) {
        if (!finalizedSwitches.contains(entry._1) && finalizedSwitches.intersect(entry._2).size == entry._2.size) {
          // entry is a switch for which we already visited all children
          assert(!switchToServers.contains(entry._1))
          val reachableMachines = mutable.BitSet()
          for (child <- entry._2) {
            // add all of its machines
            reachableMachines.addAll(switchToServers(child))
          }
          switchToServers.addOne(entry._1, reachableMachines)
          finalizedSwitches.add(entry._1)
        }
      }
    }

    switchToServers
  }

  def sanityPrintEnd2EndMatrix(): Unit = {
    logVerbose(Tabulator.format({
      val out: Array[Array[String]] = Array.fill(endToEndHops.length + 1)(Array.fill(endToEndHops.length + 1)("____"))
      (0 until numMachines).foreach(i => {
        val j: Int = if (i < numSwitches) i else i - numSwitches
        val t: String = if (i < numSwitches) if (i >= numSwitches - numTorSwitches) "TN" else "NN" else "SN"
        out(i + 1)(0) = t + "%02d".format(j)
        out(0)(i + 1) = t + "%02d".format(j)
      })

      for (src <- 0 until numMachines;
           dst <- 0 until numMachines) {
        out(src + 1)(dst + 1) = "%04d".format(endToEndHops(src)(dst))
      }

      out.map(_.toIterable)
    }))
  }

  if (SimulationConfiguration.SANITY_CHECKS_CELL) {
    assert(switchLevels.length == switches.length)
    servers.foreach(s => {
      assert(s.length == resourceDimServer)
      s.foreach(r => assert(r >= 0))
    })
    switches.foreach(s => {
      assert(s.numericalResources.length == resourceDimSwitches)
      s.numericalResources.foreach(r => assert(r >= 0))
    })
  }

  /**
   * @return hop count or -1 if there is no connection
   */
  @inline def getDistanceOf(src: NodeID, srcIsSwitch: Boolean, dst: NodeID, dstIsSwitch: Boolean): Int = {
    endToEndHops(if (srcIsSwitch) src else src + numSwitches)(if (dstIsSwitch) dst else dst + numSwitches)
  }

  def highestLevel: Int = switchLevels.max

  protected var informHookOnResourceRelease: Option[Allocation => Unit] = None

  def setHookInformOnResourceRelease(hook: Allocation => Unit): Unit = {
    assert(informHookOnResourceRelease.isEmpty)
    informHookOnResourceRelease = Some(hook)
  }

  def releaseResources(allocation: Allocation): Unit = {
    updateResourcesDelta(allocation, freeAlloc = true)
    informHookOnResourceRelease.get(allocation)
  }

  def claimResources(allocation: Allocation): Unit = {
    updateResourcesDelta(allocation, freeAlloc = false)
  }

  private def updateResourcesDelta(allocation: Allocation, freeAlloc: Boolean): Unit = {
    val taskGroup = allocation.taskGroup
    val machineId = allocation.machine

    // The amount of tasks to start (negative if freeing)
    val numberOfTasks =
      if (freeAlloc)
        -allocation.numberOfTasksToStart
      else
        allocation.numberOfTasksToStart

    if (SimulationConfiguration.SANITY_CHECKS_CELL) {
      if (!freeAlloc) {
        assert(checkMaxTasksToAllocate(taskGroup, machineId, ignoreRunningTasksOfTaskGroup = true) >= numberOfTasks,
          s"Starting more tasks than what can be hosted on the target machine (MaxStartable: ${checkMaxTasksToAllocate(taskGroup, machineId, ignoreRunningTasksOfTaskGroup = true)} Starting: $numberOfTasks)!")
      } else {
        // check max resources
        for (checkMachine <- switches.indices) {
          for (resIndex <- 0 until resourceDimSwitches) {
            assert(totalMaxSwitchCapacity(checkMachine)(resIndex) >= switches(checkMachine).numericalResources(resIndex))
          }
        }
        for (checkMachine <- servers.indices) {
          for (resIndex <- 0 until resourceDimServer) {
            assert(totalMaxServerCapacity(checkMachine)(resIndex) >= servers(checkMachine)(resIndex))
          }
        }
      }
    }

    if (taskGroup.isSwitch) {
      assert(numberOfTasks.abs == 1, s"running multiple switch tasks of the same task group " +
        s"on the same switch is not supported (${numberOfTasks} tasks requested)")

      if (!freeAlloc)
        assert(backrefAllocationsSwitches(machineId).add(allocation), s"looks like allocation ${allocation} is applied twice")
      else
        assert(backrefAllocationsSwitches(machineId).remove(allocation), s"looks like we try to remove an allocation ${allocation} twice")

      val taskGroupRes = taskGroup.resources.asInstanceOf[SwitchResource]
      val switchesRes = taskGroupRes.numericalResources

      // if this is a free alloc action, we need to refresh the active props of the switch
      val old_active_set = activeInpPropsPerSwitch(machineId)
      if (freeAlloc) {
        var new_active_set = SwitchProps.none
        // Figure out whats still needed on that switch
        backrefAllocationsSwitches(machineId).foreach(otherAlloc => {
          assert(otherAlloc.isConsumingResources)
          new_active_set = new_active_set.or(otherAlloc.taskGroup.resources.asInstanceOf[SwitchResource].properties)
        })
        // Update the active props set
        activeInpPropsPerSwitch(machineId) = new_active_set
      }

      // !!! be aware the active inc props are now w/o considering the props of the allocation..
      val demand: SwitchTasksDemand = calculateEffectiveSwitchDemand(taskGroup = taskGroup,
        activeProperties = activeInpPropsPerSwitch(machineId))

      if (freeAlloc) {
        // Which props have been disabled?
        val deactivated_set = old_active_set.remove(activeInpPropsPerSwitch(machineId))
        if (deactivated_set.size > 0) {

          val blocked = ArrayUtils.subtract(totalMaxSwitchCapacity(machineId), switches(machineId).numericalResources)
          ArrayUtils.subtractInplace(blocked, demand.firstTaskReserve)

          // Post the change to the total available resources (maybe some props got deactivated)
          CellINPLoadStatistics.onTotalResourcesChanged(
            total_resources = totalMaxSwitchCapacity(machineId),
            // take the current resource usage (which does reflect the current allocation)
            // of the switch and treat it as blocked resource by other properties
            // so we need to remove the resource usage of the current allocation
            blocked_resources_by_others = blocked,
            changed_props = deactivated_set,
            freeing = true
          )
        }
      }

      // activate new switch props?
      if (!freeAlloc) {
        activeInpPropsPerSwitch(machineId) = activeInpPropsPerSwitch(machineId).or(taskGroup.resources.asInstanceOf[SwitchResource].properties)
        val new_activated_set: SwitchProps = activeInpPropsPerSwitch(machineId).remove(old_active_set)
        if (new_activated_set.capabilities.nonEmpty) {
          // Post the change to the total available resources (maybe some props got deactivated)
          val blocked: Array[NumericalResource] = ArrayUtils.subtract(totalMaxSwitchCapacity(machineId), switches(machineId).numericalResources)
          CellINPLoadStatistics.onTotalResourcesChanged(
            total_resources = totalMaxSwitchCapacity(machineId),
            // take the current resource usage (which does not reflect the current allocation)
            // of the switch and treat it as blocked resource by other properties
            blocked_resources_by_others = blocked,
            changed_props = new_activated_set,
            freeing = false
          )
        }
      }


      (0 until resourceDimSwitches).foreach(resIndex => {
        val deltaReserve = numberOfTasks * demand.firstTaskReserve(resIndex)
        val deltaReal = numberOfTasks * demand.firstTaskUsage(resIndex)
        statistics_switch_claimed_resources_scheduler(resIndex) += deltaReserve
        statistics_switch_claimed_resources_actual(resIndex) += deltaReal
        switches(machineId).numericalResources(resIndex) -= deltaReserve
        assert(switches(machineId).numericalResources(resIndex) >= 0)
        assert(statistics_switch_claimed_resources_scheduler(resIndex) >= 0)
        assert(statistics_switch_claimed_resources_actual(resIndex) >= 0)
      })


      // Post the change to the switch load (caused by the tg)
      CellINPLoadStatistics.onResourcesChanged(
        resourcesReserve = demand.firstTaskReserve,
        resourcesReal = demand.firstTaskUsage,
        used_props = taskGroupRes.properties,
        col_located_props = old_active_set,
        freeing = freeAlloc
      )


      if (SimulationConfiguration.SANITY_CHECKS_CELL) {

        // Make sure the resources are still in bounds
        (0 until resourceDimSwitches).foreach(res_index => {
          assert(switches(machineId).numericalResources(res_index) >= 0)
          assert(switches(machineId).numericalResources(res_index) <= totalMaxSwitchCapacity(machineId)(res_index))
        })

        // Sanity
        var activeProps = SwitchProps.none
        for (alloc <- backrefAllocationsSwitches(machineId)) {
          assert(activeInpPropsPerSwitch(machineId).containsFully(alloc.taskGroup.resources.asInstanceOf[SwitchResource].properties))
          activeProps = activeProps.or(alloc.taskGroup.resources.asInstanceOf[SwitchResource].properties)
        }
        assert(activeProps.equals(activeInpPropsPerSwitch(machineId)))
      }

    } else {
      val serverRes = taskGroup.resources.asInstanceOf[ServerResource].numericalResources
      for (resIndex <- 0 until resourceDimServer) {
        val delta = numberOfTasks * serverRes(resIndex)
        statistics_server_claimed_resources_scheduler(resIndex) += delta
        servers(machineId)(resIndex) -= delta

        assert(servers(machineId)(resIndex) >= 0)
        assert(servers(machineId)(resIndex) <= totalMaxServerCapacity(machineId)(resIndex))
      }
      if (!freeAlloc) {
        assert(backrefAllocationsServer(machineId).add(allocation))
      } else {
        assert(backrefAllocationsServer(machineId).remove(allocation))
      }

    }
  }

  def checkIfSwitchHasMaxActivePropsRunning(machine: Int): Boolean =
    switchMaxActiveInpTypes == activeInpPropsPerSwitch(machine).size

  def checkIfSwitchHasCapabilityActiveOrHasFreeSlot(machine: Int, capability: Int): (Boolean, Boolean) = {
    val active = activeInpPropsPerSwitch(machine)
    if (active.capabilities.contains(capability))
      (true, true)
    else (false, active.size < switchMaxActiveInpTypes)
  }

  def getActiveInpPropsOfSwitch(machine: Int): SwitchProps = activeInpPropsPerSwitch(machine)

  def getActiveInpNormalizedLoadOfAllSwitches: BigDecimal = {
    if (switchMaxActiveInpTypes == 0)
      BigDecimal(0)
    else {
      getActiveInpLoadOfAllSwitches / BigDecimal(switchMaxActiveInpTypes, MathContext.DECIMAL32)
    }
  }

  /**
   * returns for each property the ratio of active switches with this property,
   * including switches which cannot be used for this property anymore (considering multiplexing contraints)
   */
  def getDetailedInpLoadPerPropOfAllSwitches: Array[Double] = {
    val out: Array[Double] = Array.fill(inpPropsOfCell.size + 1)(0.0)
    val allProps: Array[Int] = inpPropsOfCell.capabilities.toArray
    var switch: Int = 0
    while (switch < activeInpPropsPerSwitch.length) {
      val switchFull = activeInpPropsPerSwitch(switch).capabilities.size == switchMaxActiveInpTypes
      if (switchFull) {
        allProps.foreach(prop => out(prop) += 1)
      } else {
        activeInpPropsPerSwitch(switch).capabilities.foreach(prop => out(prop) += 1)
      }
      switch += 1
    }
    var i = 0
    while (i < out.length) {
      out(i) /= activeInpPropsPerSwitch.length.toDouble
      assert(out(i) >= 0.0 && out(i) <= 1.0)
      i += 1
    }
    out
  }

  def getActiveSwitches: Int = {
    var total = 0
    var switch: Int = 0
    while (switch < activeInpPropsPerSwitch.length) {
      if (activeInpPropsPerSwitch(switch).size > 0)
        total += 1

      switch += 1
    }
    total
  }

  def getActiveInpLoadOfAllSwitches: BigDecimal = {
    if (switchMaxActiveInpTypes == 0)
      BigDecimal(0)
    else {
      // expensive - only for statistics!
      var total = BigInt(0)
      var switch: Int = 0
      while (switch < activeInpPropsPerSwitch.length) {
        total += activeInpPropsPerSwitch(switch).size
        switch += 1
      }
      BigDecimal(total, MathContext.DECIMAL32) / BigDecimal(activeInpPropsPerSwitch.length, MathContext.DECIMAL32)
    }
  }


  class SwitchTasksDemand(val firstTaskReserve: Array[NumericalResource],
                          val firstTaskUsage: Array[NumericalResource],
                          //                         val  additionalTasksReserved: Array[NumericalResource],
                          //                         val  additionalTasksReal: Array[NumericalResource]
                         )

  def calculateEffectiveSwitchDemand(taskGroup: TaskGroup, activeProperties: SwitchProps): SwitchTasksDemand = {
    assert(taskGroup.isSwitch)
    val tgRes = taskGroup.resources.asInstanceOf[SwitchResource]
    val new_props_to_be_activated = tgRes.properties.remove(activeProperties)
    // The shared resource demand associated with the new props
    val (cell_demand, real_usage) = SharedResources.getCellResourceDemandAndRealUsage(tgRes.properties, new_props_to_be_activated)

    val firstTaskReserve: Array[NumericalResource] = tgRes.numericalResources.clone()
    val firstTaskUsage: Array[NumericalResource] = tgRes.numericalResources.clone()
    var i = 0
    while (i < firstTaskUsage.length) {
      assert(cell_demand(i) >= real_usage(i))
      firstTaskReserve(i) += cell_demand(i)
      firstTaskUsage(i) += real_usage(i)
      i += 1
    }

    if (taskGroup.coLocateOnSameMachine && taskGroup.numTasks > 1 && SharedResources.isActive) {
      throw new NotImplementedError()
    } else {
      new SwitchTasksDemand(firstTaskUsage = firstTaskUsage, firstTaskReserve = firstTaskReserve)
    }
  }

  /**
   * @param ignoreRunningTasksOfTaskGroup if true, returns the max number of tasks you could start on this machine, no matter how many tasks the task group has
   * @param checkWithMaxMachineResources  if true, consider the total resources of this machine neglecting all active allocations
   * @return
   */
  def checkMaxTasksToAllocate(taskGroup: TaskGroup, machineId: Int,
                              ignoreRunningTasksOfTaskGroup: Boolean = false,
                              checkWithMaxMachineResources: Boolean = false): Int = {
    var tasks =
      if (ignoreRunningTasksOfTaskGroup) {
        if (taskGroup.coLocateOnSameMachine) Int.MaxValue else 1
      } else {
        if (taskGroup.coLocateOnSameMachine)
          taskGroup.notStartedTasks
        else
          taskGroup.notStartedTasks min 1
      }

    // check for already co-located tasks of same task group
    if (!taskGroup.coLocateOnSameMachine && !ignoreRunningTasksOfTaskGroup) {
      val existingAllocs = if (taskGroup.isSwitch) backrefAllocationsSwitches(machineId) else backrefAllocationsServer(machineId)
      if (existingAllocs.exists(checkAlloc => checkAlloc.taskGroup == taskGroup)) {
        tasks = 0
      }
    }

    if (tasks > 0)
      if (taskGroup.isSwitch) {
        val taskGroupRes: SwitchResource = taskGroup.resources.asInstanceOf[SwitchResource]
        assert(taskGroupRes.numericalResources.length == resourceDimSwitches, s"tg $taskGroup with wrong switch dimensions, " +
          s"${
            taskGroupRes.numericalResources.length
          }" + " vs " + s"$resourceDimSwitches")

        val machineRes: Array[NumericalResource] = if (checkWithMaxMachineResources) {
          totalMaxSwitchCapacity(machineId)
        } else {
          switches(machineId).numericalResources
        }


        if (switches(machineId).properties.containsFully(taskGroupRes.properties)) {
          // if the new inp type is not active, check if we then would exceed the limit
          if (!activeInpPropsPerSwitch(machineId).containsFully(taskGroupRes.properties) && activeInpPropsPerSwitch(machineId).or(taskGroupRes.properties).size > switchMaxActiveInpTypes) {
            // yes, so do not allow to start any additional task
            tasks = 0
          } else {
            val demand: SwitchTasksDemand = calculateEffectiveSwitchDemand(taskGroup = taskGroup,
              activeProperties = activeInpPropsPerSwitch(machineId))

            (0 until resourceDimSwitches).foreach(resIndex => {
              if (demand.firstTaskReserve(resIndex) > 0) {
                val fit = machineRes(resIndex) / demand.firstTaskReserve(resIndex)
                if (fit < tasks)
                  tasks = fit
              }
            })
          }
        } else {
          tasks = 0
        }
      } else {
        val serverRes = taskGroup.resources.asInstanceOf[ServerResource].numericalResources
        assert(serverRes.length == resourceDimServer)
        val machineRes: Array[NumericalResource] = if (checkWithMaxMachineResources) {
          totalMaxServerCapacity(machineId)
        } else {
          servers(machineId)
        }
        (0 until resourceDimServer)
          .foreach(resIndex => if (tasks > 0 && serverRes(resIndex) > 0) {
            val newLim = machineRes(resIndex) / serverRes(resIndex)
            if (newLim < tasks) {
              tasks = newLim
            }
          })
      }

    tasks
  }

  def santyCheckCellIsFree(): Boolean = {
    var result = true
    for (server <- servers.indices if result) {
      for (res <- servers(server).indices) {
        if (servers(server)(res) != totalMaxServerCapacity(server)(res)) {
          result = false
        }
      }
    }
    for (switch <- switches.indices if result) {
      for (res <- switches(switch).numericalResources.indices) {
        if (switches(switch).numericalResources(res) != totalMaxSwitchCapacity(switch)(res)) {
          result = false
        }
      }
    }
    result
  }

  def sanityTotalFreeServerCapacity(): Array[BigInt] = {
    val res: Array[BigInt] = Array.fill(resourceDimServer)(BigInt(0))
    for (s <- servers) {
      for (r <- 0 until resourceDimServer) {
        res(r) += BigInt(s(r))
      }
    }
    res
  }

  def sanityTotalFreeSwitchCapacity(): Array[BigInt] = {

    val res: Array[BigInt] = Array.fill(resourceDimSwitches)(BigInt(0))
    for (s <- switches) {
      for (r <- 0 until resourceDimSwitches) {
        res(r) += BigInt(s.numericalResources(r))
      }
    }
    res
  }

  @inline final def logVerbose(str: => String, linebreak: Boolean = true): Unit =
    if (SimulationConfiguration.LOGGING_VERBOSE_CELL) {
      if (linebreak) {
        Console.out.println(str)
      } else {
        Console.out.print(str)
        Console.out.flush()
      }
    }

  override def toString: String =
    s"Cell: $numServers servers, $numSwitches switches ($numTorSwitches ToR)"
}


