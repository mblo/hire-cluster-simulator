package hiresim.scheduler.flow

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{NumericalResource, ServerResource, SwitchProps, SwitchResource}
import hiresim.graph.NodeType
import hiresim.scheduler.flow.solver.graph.{FlowGraph, FlowGraphUtils, FlowNode}
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.tenant.TaskGroup

import scala.collection.mutable

class PhysicalResourceHelper(implicit scheduler: FlowBasedScheduler) {

  private lazy val mapping: FlowGraphStructure = scheduler.graphManager.mapping
  private lazy val graph: FlowGraph = scheduler.graphManager.graph
  private lazy val cell = scheduler.cell

  // per resource dimension a lookup cache for a target resource capacity, and the matching machines
  private lazy val subtreeLookupCacheServerMasks: Array[mutable.TreeMap[NumericalResource, mutable.BitSet]] =
    Array.fill(cell.resourceDimServer)(mutable.TreeMap())

  // per switch property, per resource dimension a lookup cache for a target resource capacity, and the matching machines
  private lazy val subtreeLookupCacheSwitchMasks: Array[Array[mutable.TreeMap[NumericalResource, mutable.BitSet]]] =
    Array.fill(cell.inpPropsOfCell.capabilities.max + 1)(Array.fill(cell.resourceDimSwitches)(mutable.TreeMap()))

  private lazy val subtreeLookupCacheSwitchActivePropsMasks: Array[Array[mutable.TreeMap[NumericalResource, mutable.BitSet]]] =
    Array.fill(cell.inpPropsOfCell.capabilities.max + 1)(Array.fill(cell.resourceDimSwitches)(mutable.TreeMap()))


  private lazy val subtreeLookupCacheServerAllPossible: mutable.BitSet = {
    val out = mutable.BitSet()

    def visit(n: NodeID): Unit = {
      if (out.add(n)) {
        val node: FlowNode = graph.nodes(n)
        node.nodeType match {
          case NodeType.MACHINE_SERVER => // already added by calling visit ;)
          case NodeType.NETWORK_NODE_FOR_SERVERS => node.outgoing.foreach(arc => visit(arc.dst))
        }
      }
    }

    mapping.getServerNetworkRootNodes.foreach(visit)
    out
  }

  private val subtreeLookupCacheSwitchNowAvailableActive: Array[mutable.BitSet] = Array.ofDim(cell.inpPropsOfCell.capabilities.max + 1)
  private val subtreeLookupCacheSwitchNowAvailableInactive: Array[mutable.BitSet] = Array.ofDim(cell.inpPropsOfCell.capabilities.max + 1)
  private val subtreeLookupCacheSwitchNowAvailableInvalidPlaceholder = mutable.BitSet(99)

  private def getAllPossibleAndAllActiveSubtreeLookupSwitches(singleSwitchProp: Int): (mutable.BitSet, mutable.BitSet) = {
    // do we need to update the cache?
    if (subtreeLookupCacheSwitchNowAvailableActive(singleSwitchProp) == subtreeLookupCacheSwitchNowAvailableInvalidPlaceholder) {
      // initialize
      val active = mutable.BitSet()
      val inactive = mutable.BitSet()
      subtreeLookupCacheSwitchNowAvailableActive(singleSwitchProp) = active
      subtreeLookupCacheSwitchNowAvailableInactive(singleSwitchProp) = inactive
      // now filter
      subtreeLookupCacheSwitchAllPossible(singleSwitchProp).foreach(machineId => {
        val machine = graph.nodes(machineId)
        machine.nodeType match {
          case NodeType.NETWORK_NODE_FOR_INC =>
            if (machine.minSwitchResourcesInSubtree.properties.capabilities.contains(singleSwitchProp))
              active.add(machineId)
          case NodeType.MACHINE_NETWORK =>
            val (isActive, hasFreeSlots) = cell.checkIfSwitchHasCapabilityActiveOrHasFreeSlot(mapping.getSwitchMachineIDFromNetworkID(machineId), singleSwitchProp)
            if (isActive) active.add(machineId)
            else if (hasFreeSlots) inactive.add(machineId)
        }

      })
      if (SimulationConfiguration.SANITY_CHECKS_HIRE)
        assert(active.intersect(inactive).isEmpty)
    }
    (subtreeLookupCacheSwitchNowAvailableInactive(singleSwitchProp).clone(),
      subtreeLookupCacheSwitchNowAvailableActive(singleSwitchProp).clone())
  }

  private lazy val subtreeLookupCacheSwitchAllPossible: Array[mutable.BitSet] = {
    val out: Array[mutable.BitSet] = Array.fill(cell.inpPropsOfCell.capabilities.max + 1)(mutable.BitSet())

    val checked = mutable.BitSet()

    def visit(n: NodeID): Unit = {
      if (checked.add(n)) {
        val node: FlowNode = graph.nodes(n)

        node.nodeType match {
          case NodeType.MACHINE_NETWORK =>
            val switch: NodeID = mapping.getSwitchMachineIDFromNetworkID(node.id)
            cell.switches(switch).properties.capabilities.foreach(cap => out(cap).add(n))

          case NodeType.NETWORK_NODE_FOR_INC =>
            // do we also need to add this root node?
            node.minSwitchResourcesInSubtree.properties.capabilities.foreach(cap => out(cap).add(n))
            // check all siblings
            node.outgoing.foreach(arc => visit(arc.dst))
        }
      }
    }

    mapping.getSwitchNetworkRootNodes.foreach(visit)

    out
  }

  def prepareSubtreeLookupCacheForCurrentRound(): Unit = {
    subtreeLookupCacheServerMasks.foreach(_.clear())

    subtreeLookupCacheSwitchMasks.foreach(_.foreach(_.clear()))
    subtreeLookupCacheSwitchActivePropsMasks.foreach(_.foreach(_.clear()))

    subtreeLookupCacheSwitchNowAvailableActive.indices.foreach(subtreeLookupCacheSwitchNowAvailableActive(_) = subtreeLookupCacheSwitchNowAvailableInvalidPlaceholder)
    subtreeLookupCacheSwitchNowAvailableInactive.indices.foreach(subtreeLookupCacheSwitchNowAvailableInactive(_) = subtreeLookupCacheSwitchNowAvailableInvalidPlaceholder)
  }

  def selectAllocatableSubtreesUsingCaches(taskGroup: TaskGroup): mutable.ArrayDeque[FlowNode] = {
    val resLookup: Array[mutable.BitSet] = if (taskGroup.isSwitch) Array.ofDim(cell.resourceDimSwitches) else Array.ofDim(cell.resourceDimServer)
    val resLookupForNowAvailableProps: Array[mutable.BitSet] = Array.ofDim(resLookup.length)

    val resLookupOrigin: Array[NumericalResource] = if (taskGroup.isSwitch) Array.ofDim(cell.resourceDimSwitches) else Array.ofDim(cell.resourceDimServer)
    val resLookupOriginForNowAvailableProps: Array[NumericalResource] = Array.ofDim(resLookupOrigin.length)

    val (tgRes: Array[NumericalResource], tgResForSharedAllActive: Array[NumericalResource]) = if (taskGroup.isSwitch) {
      (cell.calculateEffectiveSwitchDemand(taskGroup, SwitchProps.none).firstTaskReserve,
        cell.calculateEffectiveSwitchDemand(taskGroup, SwitchProps.all).firstTaskReserve)
    } else {
      val out = taskGroup.resources.asInstanceOf[ServerResource].numericalResources
      (out, out)
    }

    // holds for switch tgs the property that must match
    var singleSwitchProp = -1

    var cacheSharedInpActive: Array[mutable.TreeMap[NumericalResource, mutable.BitSet]] = Array.empty
    val cache: Array[mutable.TreeMap[NumericalResource, mutable.BitSet]] = if (taskGroup.isSwitch) {
      val switchProp = taskGroup.resources.asInstanceOf[SwitchResource]
      assert(switchProp.properties.capabilities.size == 1, "subtree lookup expects a switch tg to use exactly 1 switch prop :)")
      singleSwitchProp = switchProp.properties.capabilities.head
      cacheSharedInpActive = subtreeLookupCacheSwitchActivePropsMasks(singleSwitchProp)
      subtreeLookupCacheSwitchMasks(singleSwitchProp)
    } else subtreeLookupCacheServerMasks

    assert(cache.length == resLookup.length, s"cache has ${cache.length} entries, but tg res has ${resLookup.length}, for tg:${taskGroup.detailedToString()}")

    /**
     * filters the machine candidates with resource demand
     * @param cacheOrigin [dim]->[numerical resource] the numerical resource which was checked previously
     * @param cache [dim]->[numerical resource]->[bit set of possible machines] for each res dim, the lookup cache map with machines that offer the specified numerical resources
     */
    def checkResourcesAndUpdateCaches(dim: Int,
                                      machineCandidates: Array[mutable.BitSet],
                                      resourceDemand: Array[NumericalResource],
                                      cacheOrigin: Array[NumericalResource],
                                      cache: Array[mutable.TreeMap[NumericalResource, mutable.BitSet]]): Unit = {
      for (checkId <- machineCandidates(dim).toArray) {
        val node = graph.nodes(checkId)
        node.nodeType match {
          case NodeType.NETWORK_NODE_FOR_SERVERS =>
            assert(!taskGroup.isSwitch)
            // check caches, only check for ALWAYS fit
            if (node.minServerResourcesInSubtree.numericalResources(dim) < resourceDemand(dim)) {
              // subtree does not always fit, ignore this tree
              machineCandidates(dim).remove(checkId)
            }

          case NodeType.NETWORK_NODE_FOR_INC =>
            assert(taskGroup.isSwitch)
            // check caches, only check for ALWAYS fit
            if (node.minSwitchResourcesInSubtree.numericalResources(dim) < resourceDemand(dim)) {
              // subtree does not always fit, ignore this tree
              machineCandidates(dim).remove(checkId)
            }

          case NodeType.MACHINE_SERVER =>
            assert(!taskGroup.isSwitch)
            // ask cell for resources
            if (cell.servers(mapping.getServerMachineIDFromNodeID(checkId))(dim) < resourceDemand(dim)) {
              // machine does not fit, remove index
              machineCandidates(dim).remove(checkId)
            }

          case NodeType.MACHINE_NETWORK =>
            assert(taskGroup.isSwitch)
            // ask cell for resources
            if (cell.switches(mapping.getSwitchMachineIDFromNetworkID(checkId)).numericalResources(dim) < resourceDemand(dim)) {
              // machine does not fit, remove index
              machineCandidates(dim).remove(checkId)
            }
        }
      }

      // resLookup(dim) is no up to date, shall we store it for other caches?
      if (cacheOrigin(dim) == -1 || ((SimulationConfiguration.HIRE_SHORTCUTS_CACHE_THRESHOLD * cacheOrigin(dim)).toInt < resourceDemand(dim))) {
        // store it either because there was no other cache entry, or this tg res vs cached differ >x%
        cache(dim)(resourceDemand(dim)) = machineCandidates(dim).clone()
      }
    }

    // get for each dimension the cached entry with largest resources <= the one of this task group
    resLookup.indices.foreach(dim => {
      // is there any cached?
      cache(dim).maxBefore(tgRes(dim) + 1) match {
        case Some(found: (NumericalResource, mutable.BitSet)) =>
          resLookup(dim) = found._2.clone()
          resLookupOrigin(dim) = found._1
        case None =>
          resLookup(dim) = if (taskGroup.isSwitch) getAllPossibleAndAllActiveSubtreeLookupSwitches(singleSwitchProp)._1 else subtreeLookupCacheServerAllPossible.clone()
          resLookupOrigin(dim) = -1
      }

      // now we have the best pre-cached set, so now check for exact resources
      // this is the only check for servers
      // for switches, this checks the high resources demand considering switches where none of the reqs are running now
      checkResourcesAndUpdateCaches(dim = dim,
        machineCandidates = resLookup,
        resourceDemand = tgRes,
        cacheOrigin = resLookupOrigin,
        cache = cache)

      if (taskGroup.isSwitch) {

        assert(tgRes(dim) >= tgResForSharedAllActive(dim))

        cacheSharedInpActive(dim).maxBefore(tgResForSharedAllActive(dim) + 1) match {
          case Some(found: (NumericalResource, mutable.BitSet)) =>
            resLookupForNowAvailableProps(dim) = found._2.clone()
            resLookupOriginForNowAvailableProps(dim) = found._1
          case None =>
            resLookupForNowAvailableProps(dim) = getAllPossibleAndAllActiveSubtreeLookupSwitches(singleSwitchProp)._2
            resLookupOriginForNowAvailableProps(dim) = -1
        }

        checkResourcesAndUpdateCaches(dim = dim,
          machineCandidates = resLookupForNowAvailableProps,
          resourceDemand = tgResForSharedAllActive,
          cacheOrigin = resLookupOriginForNowAvailableProps,
          cache = cacheSharedInpActive)
      }
    })

    // now summarize all
    def allIntersection(in: Array[mutable.BitSet]): mutable.BitSet = {
      var out = in(0)
      var i = 1
      while (i < in.length) {
        out = out.intersect(in(i))
        i += 1
      }
      out
    }

    val allMatchingMachines = if (taskGroup.isSwitch) {
      allIntersection(resLookup).union(allIntersection(resLookupForNowAvailableProps))
    } else {
      allIntersection(resLookup)
    }

    val out: mutable.ArrayDeque[FlowNode] = mutable.ArrayDeque()

    // how many entries shall we return finally?
    var maxEntries = SimulationConfiguration.HIRE_SHORTCUTS_MAX_SEARCH_SPACE_PER_TASK_GROUP
    if (maxEntries == -1)
      maxEntries = allMatchingMachines.size

    for (nodeId <- allMatchingMachines) {
      if (maxEntries > 0) {
        val node = graph.nodes(nodeId)
        // now we need to check for co-location
        if (taskGroup.coLocateOnSameMachine || taskGroup.runningTasks == 0 || node.allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0) == 0) {
          maxEntries -= 1
          out.addOne(node)
        }
      }
    }

    // sanity checks
    if (SimulationConfiguration.SANITY_CHECKS_HIRE) {
      for (node <- out)
        if (node.isMachine) {
          if (node.isServerMachine) {
            val resAvailable = cell.servers(mapping.getServerMachineIDFromNodeID(node.id))
            tgRes.indices.foreach(i => assert(resAvailable(i) >= tgRes(i)))
          } else if (node.isSwitchMachine) {
            val resAvailable = cell.switches(mapping.getSwitchMachineIDFromNetworkID(node.id))
            if (resAvailable.properties.containsFully(taskGroup.resources.asInstanceOf[SwitchResource].properties))
              tgRes.indices.foreach(i => assert(resAvailable.numericalResources(i) >= tgResForSharedAllActive(i)))
            else
              tgRes.indices.foreach(i => assert(resAvailable.numericalResources(i) >= tgRes(i)))
          }
        }
    }

    out
  }

  /** The following is taking care of updating the metric on running tasks in subtree */

  private[flow] def updateRunningTasksInSubtreeCountFn(taskGroups: Iterable[TaskGroup],
                                                       propagateMaxOfChildInsteadOfExactTaskCount: Boolean = false): (FlowNode => Unit) =
    (node: FlowNode) => {
      if (!node.isMachine) {
        for (taskGroup <- taskGroups) {
          // Keeping track of how many tasks there are running in the subtree starting at this node
          var tasks_in_subtree = 0
          if (taskGroup.runningTasks > 0) {
            // Go through all of the children to check for running tasks
            node.outgoing.foreach(arc => {
              // And accumulate the running tasks
              if (propagateMaxOfChildInsteadOfExactTaskCount) {
                val tmp = graph.nodes(arc.dst).allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0)
                tasks_in_subtree = tasks_in_subtree max tmp
              } else
                tasks_in_subtree += graph.nodes(arc.dst).allocatedTaskGroupsInSubtree.getOrElse(taskGroup, 0)
            })
          }

          //          scheduler.logDetailedVerbose(f" .. tree update .. tg:$taskGroup - @$node|${node.nodeType}|lvl:${node.level} --> $tasks_in_subtree")

          // Finally update entry
          if (tasks_in_subtree == 0)
            node.allocatedTaskGroupsInSubtree.remove(taskGroup)
          else {
            assert(taskGroup.numTasks >= tasks_in_subtree, f"there are more tasks (${tasks_in_subtree}) " +
              f"in this subtree, than running in total for this task group ${taskGroup.detailedToString()}!")
            if (!propagateMaxOfChildInsteadOfExactTaskCount) {
              assert(taskGroup.runningTasks >= tasks_in_subtree)
            }
            node.allocatedTaskGroupsInSubtree.update(taskGroup, tasks_in_subtree)
          }
        }
      } else {
        //        scheduler.logDetailedVerbose(f"  ... tree update, @ machine ${node}|${node.nodeType}|lvl:${node.level}, " +
        //          f"going up to parents (${node.incoming.size})")
      }
    }

  /** The following is taking care of updating the server resource statistics * */

  private[flow] def updateServerResourceStatistics(start: NodeID): Unit = updateServerResourceStatistics(start, graph)

  private[flow] def updateServerResourceStatistics(start: Iterable[NodeID]): Unit = updateServerResourceStatistics(start, graph)

  private[flow] def updateServerResourceStatistics(start: NodeID,
                                                   graph: FlowGraph): Unit = updateServerResourceStatistics(Iterable.single(start), graph)

  private[flow] def updateServerResourceStatistics(start: Iterable[NodeID],
                                                   graph: FlowGraph): Unit = {

    val minServerResources: mutable.ArrayBuffer[Array[NumericalResource.NumericalResource]] = mutable.ArrayBuffer()
    val maxServerResources: mutable.ArrayBuffer[Array[NumericalResource.NumericalResource]] = mutable.ArrayBuffer()

    FlowGraphUtils.visitPhysicalParentsBottomUp(start, graph, includeStart = false, node => {

      // Clear of caches first as we only need min/max of the current nodes children
      minServerResources.clear()
      maxServerResources.clear()

      for (arc <- node.outgoing) {
        // The child we are now looking at
        val child: FlowNode = graph.nodes(arc.dst)
        // Depending on the type of the child there are 2 ways of extracting the available resources
        child.nodeType match {

          // The case, where the child is a basic allocatable server
          case NodeType.MACHINE_SERVER =>

            val server: NodeID = mapping.getServerMachineIDFromNodeID(child.id)
            val resources: Array[NumericalResource.NumericalResource] = cell.servers(server)

            // Min and Max resources are equal in this case
            minServerResources += resources
            maxServerResources += resources

          // The case, where the child is a network node (modelling the topology)
          case NodeType.NETWORK_NODE_FOR_SERVERS =>

            if (child.minServerResourcesInSubtree != null)
              minServerResources += child.minServerResourcesInSubtree.numericalResources

            if (child.maxServerResourcesInSubtree != null)
              maxServerResources += child.maxServerResourcesInSubtree.numericalResources

        }
      }

      assert(minServerResources.nonEmpty && maxServerResources.nonEmpty, s"Did not find any child nodes to collect min/max resource values from")

      // Initialize both min and max to neutral available resources in each dimension, so that it will be overwitten in first run
      val min: Array[NumericalResource.NumericalResource] = Array.fill[NumericalResource](cell.resourceDimServer)(elem = NumericalResource.`NumericalResource.MaxValue`)
      val max: Array[NumericalResource.NumericalResource] = Array.fill[NumericalResource](cell.resourceDimServer)(elem = 0)

      // Now construct the min/max resource available
      for (i <- 0 until cell.resourceDimServer) {

        for (elem <- minServerResources)
          min(i) = Math.min(min(i), elem(i))

        for (elem <- maxServerResources)
          max(i) = Math.max(max(i), elem(i))

      }

      node.minServerResourcesInSubtree = new ServerResource(min)
      node.maxServerResourcesInSubtree = new ServerResource(max)

    })

  }

  /** The following is taking care of updating the switch resource statistics * */

  private[flow] def updateSwitchResourceStatistics(start: NodeID): Unit = updateSwitchResourceStatistics(start, graph)

  private[flow] def updateSwitchResourceStatistics(start: Iterable[NodeID]): Unit = updateSwitchResourceStatistics(start, graph)

  private[flow] def updateSwitchResourceStatistics(start: NodeID,
                                                   graph: FlowGraph): Unit = updateSwitchResourceStatistics(Iterable.single(start), graph)

  private[flow] def updateSwitchResourceStatistics(start: Iterable[NodeID],
                                                   graph: FlowGraph): Unit = {

    val minSwitchResources: mutable.ArrayBuffer[SwitchResource] = mutable.ArrayBuffer()
    val maxSwitchResources: mutable.ArrayBuffer[SwitchResource] = mutable.ArrayBuffer()

    FlowGraphUtils.visitPhysicalParentsBottomUp(start, graph, includeStart = false, node => {

      // Clear of caches first as we only need min/max of the current nodes children
      minSwitchResources.clear()
      maxSwitchResources.clear()

      for (arc <- node.outgoing) {
        // The child we are now looking at
        val child: FlowNode = graph.nodes(arc.dst)
        // Depending on the type of the child there are 2 ways of extracting the available resources
        child.nodeType match {

          case NodeType.MACHINE_NETWORK =>

            val switch: NodeID = mapping.getSwitchMachineIDFromNetworkID(child.id)
            val resources: SwitchResource = {

              val available: SwitchResource = cell.switches(switch).clone()
              // We need to copy in the active inp props like this as they are tracked in their own array
              available.properties = cell.getActiveInpPropsOfSwitch(switch)

              available

            }

            // Min and max are same for actual machines
            minSwitchResources += resources
            maxSwitchResources += resources

          // If we have a physical switch (network structure node) we can take the underlying min/max resources
          case NodeType.NETWORK_NODE_FOR_INC =>

            if (child.minSwitchResourcesInSubtree != null)
              minSwitchResources += child.minSwitchResourcesInSubtree

            if (child.maxSwitchResourcesInSubtree != null)
              maxSwitchResources += child.maxSwitchResourcesInSubtree

          case _ =>
            throw new AssertionError(s"Got illegal node type while propagating switch resource update (was ${child.nodeType})")

        }


      }


      assert(minSwitchResources.nonEmpty && maxSwitchResources.nonEmpty, s"Did not find any child nodes to collect min/max resource values from")

      // Initialize both min and max to neutral available resources in each dimension, so that it will be overwritten in first run
      val min: SwitchResource = new SwitchResource(Array.fill[NumericalResource](cell.resourceDimSwitches)(elem = NumericalResource.`NumericalResource.MaxValue`), SwitchProps.all)
      val max: SwitchResource = new SwitchResource(Array.fill[NumericalResource](cell.resourceDimSwitches)(elem = 0), SwitchProps.none)

      // Now construct the min/max resource available
      for (i <- 0 until cell.resourceDimSwitches) {

        for (elem <- minSwitchResources)
          min.numericalResources(i) = Math.min(min.numericalResources(i), elem.numericalResources(i))

        for (elem <- maxSwitchResources)
          max.numericalResources(i) = Math.max(max.numericalResources(i), elem.numericalResources(i))

      }

      // Update properties, such that min.properties holds an overview over all available properties in the subtree.
      for (elem <- minSwitchResources)
        min.properties = min.properties.and(elem.properties)

      // Also update properties, such that max.properties holds an overview over all common properties in the subtree.
      for (elem <- maxSwitchResources)
        max.properties = max.properties.or(elem.properties)

      // And set the updated bounds
      node.minSwitchResourcesInSubtree = min
      node.maxSwitchResourcesInSubtree = max

    })
  }

  private[flow] def initializeStaticResources(): Unit = {
    initializeStaticResources(graph)
  }

  private[flow] def initializeStaticResources(graph: FlowGraph): Unit = {

    // All the machine nodes in the graph
    val machines = mapping.getSwitchNodes ++ mapping.getServerNodes
    // Temporary buffer to store permanent max switch resources
    val maxSwitchResources: mutable.ArrayBuffer[SwitchResource] = mutable.ArrayBuffer()

    FlowGraphUtils.visitPhysicalParentsBottomUp(machines, graph, includeStart = false, node => {

      // Clear of caches first as we only need min of the current nodes children
      maxSwitchResources.clear()
      // Accumulating how many slots there are to allocate to below this node
      var taskSlots = 0

      for (arc <- node.outgoing) {
        // The child we are now looking at
        val child: FlowNode = graph.nodes(arc.dst)
        // Depending on the type of the child there are 2 ways of extracting the available resources
        child.nodeType match {

          case NodeType.MACHINE_NETWORK =>

            // Get the cell id of that switch machine
            val switch: NodeID = mapping.getSwitchMachineIDFromNetworkID(child.id)
            // As we are running this method at init cell.switches holds the total max capabilities of the switch
            maxSwitchResources += cell.switches(switch).clone()

            // Also account for the slot this switch offers
            taskSlots += 1

          // If we have a physical switch (network structure node) we can take the underlying max resources
          case NodeType.NETWORK_NODE_FOR_INC =>

            taskSlots += child.maxTaskFlows

            if (child.maxStaticSwitchResourcesInSubtree != null)
              maxSwitchResources += child.maxStaticSwitchResourcesInSubtree

          // If this is a machine we are only interested in setting up taskSlots correctly
          case NodeType.MACHINE_SERVER =>
            taskSlots += 1

          // Analogous for the server network
          case NodeType.NETWORK_NODE_FOR_SERVERS =>
            taskSlots += child.maxTaskFlows

          // Everything else is an error!
          case _ =>
            throw new AssertionError(s"Got illegal node type while propagating switch resource update (was ${child.nodeType})")

        }
      }

      node.maxTaskFlows = taskSlots

      // The following only needs to be done for the switch network
      if (node.nodeType == NodeType.NETWORK_NODE_FOR_INC) {
        // Make sure there are elements to calculate the static max from
        assert(maxSwitchResources.nonEmpty, s"Did not find any child nodes to collect min/max resource values from")

        // Initialize both max to neutral available resources in each dimension, so that it will be overwritten in first run
        val max: SwitchResource = new SwitchResource(Array.fill[NumericalResource](cell.resourceDimSwitches)(elem = 0), SwitchProps.none)
        // Now construct the min/max resource available
        for (i <- 0 until cell.resourceDimSwitches) {

          for (elem <- maxSwitchResources)
            max.numericalResources(i) = Math.max(max.numericalResources(i), elem.numericalResources(i))

        }

        // Update properties, such that min.properties holds an overview over all available properties in the subtree.
        for (elem <- maxSwitchResources)
          max.properties = max.properties.or(elem.properties)

        // And set the updated bounds
        node.maxStaticSwitchResourcesInSubtree = max

      }

    })

  }
}
