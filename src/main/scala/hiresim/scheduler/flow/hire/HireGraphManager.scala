package hiresim.scheduler.flow.hire

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchResource}
import hiresim.graph.NodeType
import hiresim.scheduler.flow.hire.costs.{HireCostModel, HireLocalityCostCalculator}
import hiresim.scheduler.flow.solver.graph.FlowArc.{Capacity, Cost}
import hiresim.scheduler.flow.solver.graph.FlowNode.Supply
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowGraphUtils, FlowNode}
import hiresim.scheduler.flow.{ArcDescriptor, FlowGraphManager}
import hiresim.shared.TimeIt
import hiresim.shared.graph.FixedIndexElementStore
import hiresim.shared.graph.Graph.NodeID
import hiresim.simulation.RandomManager
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation.statistics.CellINPLoadStatistics
import hiresim.tenant.{Job, TaskGroup}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HireGraphManager(random: RandomManager,
                       maxInpFlavorDecisionsPerRound: NodeID)(implicit scheduler: HireScheduler) extends FlowGraphManager {


  def runGraphSanityCheck() = {
    // for each producer (SF, TGs)
    val now = scheduler.simulator.currentTime()
    val PRECISION = SimulationConfiguration.PRECISION

    def extractFlavorCosts(flavorNode: FlowNode): (Job, Cost, ArrayBuffer[(Cost, FlowNode)], ArrayBuffer[(Cost, FlowNode)]) = {
      var costU = -1L
      val costInc: ArrayBuffer[(Cost, FlowNode)] = ArrayBuffer()
      val costServer: ArrayBuffer[(Cost, FlowNode)] = ArrayBuffer()
      var job: Option[Job] = None
      flavorNode.outgoing.foreach(arc => {
        val dst = graph.nodes(arc.dst)
        dst.nodeType match {
          case NodeType.TASK_GROUP_POSTPONE => costU = arc.cost
          case NodeType.SERVER_TASK_GROUP | NodeType.NETWORK_TASK_GROUP =>
            val tg = dst.taskGroup.get
            if (tg.belongsToInpFlavor) {
              costInc.addOne((arc.cost, dst))
            } else {
              costServer.addOne((arc.cost, dst))
            }
            if (job.isEmpty) job = Some(dst.taskGroup.get.job.get)
        }
      })
      (job.get, costU, costInc, costServer)
    }

    graph.nodes.producerIds.foreach(producerId => {
      val producerNode = graph.nodes(producerId)

      producerNode.nodeType match {
        case NodeType.SUPER_JOB_FLAVOR_SELECTOR =>

          // for each flavor node
          producerNode.outgoing.foreach(flavorArc => {
            val flavorNode = graph.nodes(flavorArc.dst)

            assert(flavorNode.nodeType == NodeType.JOB_FLAVOR_SELECTOR, s"wrong node found after SFJ: ${flavorNode.nodeType}")

            if (flavorNode.outgoing.size > 1) {

              val (
                job: Job,
                partialPostponeCost: Cost,
                partialIncCost: ArrayBuffer[(Cost, FlowNode)],
                partialServerCost: ArrayBuffer[(Cost, FlowNode)]) = extractFlavorCosts(flavorNode)

              val linker = jobNodes(job)
              // U cost.. check with LOWER and UPPER thresholds
              val jobWaitTime = {
                now - (linker.oldestSubmissionTimeOfFlavorNode)
              }
              assert(partialPostponeCost >= 3 * PRECISION && partialPostponeCost <= 4 * PRECISION,
                s"F-P postpone must be > 3* && <= 4*, but is $partialPostponeCost")
              if (jobWaitTime >= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER)
                assert(partialPostponeCost == 4 * PRECISION, s"F-P postpone must be 4* (${4 * PRECISION}), " +
                  s"since waiting time is >= lower bound of server penalty waiting" +
                  s", but it is $partialPostponeCost")

              // INC cost -> check within bounds
              for ((incCost: Cost, incNode: FlowNode) <- partialIncCost) {
                assert(incCost <= PRECISION, s"inc cost is:$incCost, but should be <= $PRECISION")
              }

              // non INC cost -> check within bounds, wrt. SERVER penalty
              for ((serverCost: Cost, serverNode: FlowNode) <- partialServerCost) {
                val tgWait = now - (serverNode.taskGroup.get.submitted max
                  linker.oldestSubmissionTimeOfFlavorNode)
                assert(serverCost <= PRECISION * (1 + SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR))
                if (tgWait <= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_LOWER) {
                  assert(serverCost >= PRECISION * SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR &&
                    serverCost <= PRECISION * (1.0 + SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR),
                    s"server flavor cost: $serverCost, should be with max penalty, " +
                      s"so ${PRECISION * SimulationConfiguration.HIRE_SERVER_PENALTY_COST_FACTOR}")
                } else if (tgWait >= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER) {
                  assert(serverCost <= PRECISION)
                }
              }

              if (partialServerCost.nonEmpty) {
                val smallestServerCost: Cost = partialServerCost.map(_._1).min
                if (smallestServerCost > partialPostponeCost) {
                  assert(jobWaitTime <= SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER)
                }
              }
            }
          })
        case NodeType.NETWORK_TASK_GROUP =>
        case NodeType.SERVER_TASK_GROUP =>
        case _ => throw new RuntimeException(s"invalid producer node type, ${producerNode.nodeType}")
      }
    })
  }

  private[flow] lazy val costs: HireCostModel = new HireCostModel(this)(scheduler)
  private[flow] lazy val capacities: HireCapacityModel = new HireCapacityModel()(scheduler)

  private[hire] lazy val locality: HireLocalityCostCalculator = new HireLocalityCostCalculator(this)
  private[flow] lazy val mapping: TopologyGraphStructure = new TopologyGraphStructure()

  /** Keeping track of key graph nodes for every `TaskGroup` */
  private[hire] val jobNodes: mutable.TreeMap[Job, NodeLinker] = mutable.TreeMap()(new Ordering[Job] {
    override def compare(x: Job, y: Job): Int = if (x.submitted == y.submitted) (x.id - y.id).toInt else x.submitted - y.submitted
  })
  private[hire] var taskGroupNodesInCommonPart: FlowNodeContainer = new FlowNodeContainer()

  private[hire] var skippedTaskGroupsBecauseOverloaded: Int = 0
  private[hire] var skippedFlavorSelectorBecauseOverloaded: Int = 0
  private[hire] val overloadedSwitchProp: Array[Boolean] = Array.fill(scheduler.cell.inpPropsOfCell.capabilities.max + 1)(false)

  // Create the super flow node and add it to the graph
  private[hire] lazy val super_flavor_node_id: NodeID = graph.addNode(new FlowNode(
    nodeType = NodeType.SUPER_JOB_FLAVOR_SELECTOR,
    level = scheduler.cell.highestLevel + 4,
    supply = 0L // Supply will be set by cleanup
  ))

  override def getEstimatedSchedulingOptions: Int =
    (getProducerCount max taskGroupNodesInCommonPart.size) + graph.nodes(super_flavor_node_id).outgoing.size

  def cleanup(): Supply = {
    val cleanupTimer = TimeIt("Cleanup Graph")

    var notScheduledTasksOnMaschines = 0L
    var notScheduledTasks = 0L

    // clear cache of matching machines
    resources.prepareSubtreeLookupCacheForCurrentRound()

    // Disconnect the TaskGroups from the topology graph. We will reconnect them
    // after we finished updating the resource availability information
    taskGroupNodesInCommonPart.foreach(node => if (node.isProducer)
      disconnectAggregatorFromGraph(node_id = node.id, exclude = node.usedUnscheduleId))

    // Retrieve all the dirty servers
    val dirty_servers: Array[NodeID] = mapping.serverNodes.filter(id => graph.nodes(id).dirty).toArray
    // And propagate their new available resources through the tree
    resources.updateServerResourceStatistics(dirty_servers)

    // Retrieve all the dirty switches
    val dirty_switches: Array[NodeID] = mapping.switchNodes.filter(id => graph.nodes(id).dirty).toArray
    // And propagate their new available resources through the graph
    resources.updateSwitchResourceStatistics(dirty_switches)

    // All the machines that experienced some change
    dirty_servers ++ dirty_switches

    // Update Machine -> Sink arcs on dirty machines
    def updateMachineSinkEdge(machineNetworkID: NodeID): Unit = {
      // The concrete flow node representing the current machine
      val machine: FlowNode = graph.nodes(machineNetworkID)

      graph.updateArc(
        src = machineNetworkID,
        dst = graph.sinkNodeId,
        // The updated cost depending on whether we are dealing with a server or a switch
        newCost =
          if (machine.isServerMachine)
            costs.getServerToSink(machine)
          else
            costs.getSwitchToSink(machine))
    }

    dirty_servers.foreach(updateMachineSinkEdge)
    dirty_switches.foreach(updateMachineSinkEdge)

    def createListOfDirtyServerAndSwitchTaskGroups: (mutable.ArrayDeque[TaskGroup], mutable.ArrayDeque[TaskGroup]) = {
      val outServer: mutable.ArrayDeque[TaskGroup] = mutable.ArrayDeque()
      val outSwitch: mutable.ArrayDeque[TaskGroup] = mutable.ArrayDeque()

      // First consider all the TaskGroups that are still in for scheduling
      taskGroupNodesInCommonPart.foreach(taskGroupNode => {
        val taskgroup = taskGroupNode.taskGroup.get

        // Only execute if that TaskGroup is dirty
        if (taskgroup.dirty) {
          if (taskgroup.isSwitch) outSwitch.append(taskgroup)
          else outServer.append(taskgroup)
        }

      })

      // Then consider the TaskGroups that have been removed since the last run
      removed_taskgroup_backlog.foreach((taskGroup: TaskGroup) => {
        if (taskGroup.isSwitch) outSwitch.append(taskGroup)
        else outServer.append(taskGroup)
      })

      (outServer, outSwitch)
    }

    val (dirtyServerTaskgroups: Iterable[TaskGroup], dirtySwitchTaskgroups: Iterable[TaskGroup]) =
      createListOfDirtyServerAndSwitchTaskGroups

    var dirtyServerMachines = mutable.BitSet()
    dirtyServerTaskgroups.foreach(tg => dirtyServerMachines.|=(tg.dirtyMachines))
    var dirtySwitchMachines = mutable.BitSet()
    dirtySwitchTaskgroups.foreach(tg => dirtySwitchMachines.|=(tg.dirtyMachines))

    var requiredSinkCapacity = 0L

    // Recalculate the amount of running tasks in every affected node for every changed TaskGroup
    val fnUpdateServerTaskCount: FlowNode => Unit = resources.updateRunningTasksInSubtreeCountFn(dirtyServerTaskgroups)
    val fnUpdateSwitchTaskCount: FlowNode => Unit = resources.updateRunningTasksInSubtreeCountFn(dirtySwitchTaskgroups)
    // Update the local gains for each dirty server in a dirty taskgroup
    val fnUpdateLocalityGain: FlowNode => Unit = locality.updateServerLocalGainsFn(dirtyServerTaskgroups)

    FlowGraphUtils.visitPhysicalParentsBottomUp(start = dirtyServerMachines,
      graph = graph, function = (node: FlowNode) => {
        fnUpdateServerTaskCount(node)
        fnUpdateLocalityGain(node)
      }, includeStart = true)

    FlowGraphUtils.visitPhysicalParentsBottomUp(start = dirtySwitchMachines,
      graph = graph, function = (node: FlowNode) => {
        fnUpdateSwitchTaskCount(node)
      }, includeStart = true)

    dirtyServerTaskgroups.foreach(_.dirtyMachines.clear())
    dirtySwitchTaskgroups.foreach(_.dirtyMachines.clear())

    // Clear caches at this point as we are now constructing the next rounds tg connections
    costs.onClearCaches()
    // And clear the backlog of removed TaskGroups
    removed_taskgroup_backlog.clear()

    overloadedSwitchProp.indices.foreach(i => overloadedSwitchProp(i) = false)

    var limitServerTgs = false


    val switchLoad: Array[Double] = scheduler.cell.getDetailedInpLoadPerPropOfAllSwitches
    // check more detailed for the specific inc type
    for (property: Int <- scheduler.cell.inpPropsOfCell.capabilities) {
      // check detailed load only if there are many switches already "blocked for INC"
      if (switchLoad(property) >= SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD) {
        // get load of inp property
        val loadOfProperty: Array[Double] = CellINPLoadStatistics.getSwitchLoadStatOfProperty(property)
        for (load: Double <- loadOfProperty) {
          // check each dimension if below threshold
          if (load > SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD) {
            overloadedSwitchProp(property) = true
            scheduler.logVerbose(s"Overload - flag switch prop $property")
          }
        }
      }
    }

    // check server load
    if (scheduler.cell.currentServerLoadMax().toDouble > SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_UTILIZATION_THRESHOLD) {
      limitServerTgs = true
      scheduler.logVerbose(s"Overload - flag servers")
    }

    val sortedTaskGroupsInCommon: List[FlowNode] =
      taskGroupNodesInCommonPart.asSortedList((n1, n2) => {
        if (n1.taskGroup.get.submitted == n2.taskGroup.get.submitted) {
          n1.taskGroup.get.id < n2.taskGroup.get.id
        } else n1.taskGroup.get.submitted < n2.taskGroup.get.submitted
      })

    val maxSwitchTaskGroupsWhenOverloaded: Array[Int] = Array.fill(overloadedSwitchProp.length)(
      (SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION * scheduler.cell.numSwitches).toInt max 10)

    var (maxServerTaskGroupsToConnect: Int, maxFlavorNodes: Int) = if (limitServerTgs) {
      ((SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION * scheduler.cell.numServers).toInt max 10,
        ((SimulationConfiguration.HIRE_PREVENT_HIGH_LOAD_SOLVER_TG_PORTION * scheduler.cell.numServers).toInt min maxInpFlavorDecisionsPerRound) max 10)
    } else {
      (SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT max 10,
        (SimulationConfiguration.HIRE_BACKLOG_PRODUCER_SOFT_LIMIT min maxInpFlavorDecisionsPerRound) max 10)
    }

    val now = scheduler.simulator.currentTime()

    // Now reconnect all of the TaskGroups to the topology graph. This will also
    // update the TaskGroup -> Unschedule Arc
    skippedTaskGroupsBecauseOverloaded = 0
    var connectedTaskGroupsCommonPart = 0
    sortedTaskGroupsInCommon.foreach(taskGroupNode => {
      val taskGroup = taskGroupNode.taskGroup.get

      assert(taskGroupNode.outgoing.size == 1)

      val considerTaskGroupForScheduling: Boolean = {
        if (taskGroup.isSwitch) {
          // if there is any switch property involved, which seems to be overloaded, check if we are still ok with adding this tg
          var ok = true
          taskGroup.resources.asInstanceOf[SwitchResource].properties.capabilities.foreach(prop => {
            if (overloadedSwitchProp(prop)) {
              if (maxSwitchTaskGroupsWhenOverloaded(prop) > 0) {
                maxSwitchTaskGroupsWhenOverloaded(prop) -= 1
              } else {
                ok = false
              }
            }
          })
          ok
        } else {
          if (maxServerTaskGroupsToConnect > 0) {
            maxServerTaskGroupsToConnect -= 1
            true
          } else false
        }
      }

      if (considerTaskGroupForScheduling) {
        connectedTaskGroupsCommonPart += 1
        // do we need to reactivate this guy as a producer node?
        if (!taskGroupNode.isProducer) {
          graph.nodes.migrateNonProducerToProducerNode(taskGroupNode.id)
        }

        // The TaskGroup to update for now
        // Connect the TaskGroup to the Graph again
        val addedShortcuts = connectTaskGroupToGraph(taskgroup = taskGroup, taskgroup_node = taskGroupNode.id,
          unschedule_node = taskGroupNode.usedUnscheduleId)

        // Keep track of how many tasks we are scheduling in this round
        notScheduledTasks += taskGroup.notStartedTasks
        notScheduledTasksOnMaschines += taskGroup.maxAllocationsInOngoingSolverRound

        // We preempt this TaskGroup if it can't be allocated somewhere and the timing allows for that
        if (taskGroup.maxAllocationsInOngoingSolverRound == 0) {
          scheduler.addPreemptionCandidate(taskGroup)
        }
        if (taskGroup.isSwitch &&
          (now - taskGroup.consideredForSchedulingSince) > scheduler.minQueuingTimeBeforePreemption) {
          scheduler.addPreemptionCandidate(taskGroup)
        }

        assert(taskGroupNode.supply > 0)
        requiredSinkCapacity += taskGroupNode.supply

        taskGroup.dirty = false
      } else {
        skippedTaskGroupsBecauseOverloaded += 1
        // do we need to make this task group temporary a non producer?
        if (taskGroupNode.isProducer) {
          taskGroupNode.supply = 0L
          graph.nodes.migrateProducerToNonProducerNode(taskGroupNode.id)
        }

        // postpone preemption?
        if (taskGroup.isSwitch && (now - taskGroup.consideredForSchedulingSince) > scheduler.minQueuingTimeBeforePreemption) {
          scheduler.addPreemptionCandidate(taskGroup)
        }
      }

      if (SimulationConfiguration.SANITY_CHECKS_GRAPH) {
        if (taskGroupNode.isProducer)
          assert(graph.nodes.producerIds.contains(taskGroupNode.id))
        else
          assert(graph.nodes.nonProducerIds.contains(taskGroupNode.id))
      }

    })

    val super_flow_node = graph.nodes(super_flavor_node_id)
    val producersInGraph = graph.nodes.producerIds.size
    val expectedProducers = connectedTaskGroupsCommonPart + (if (graph.nodes.producerIds.contains(super_flavor_node_id)) 1 else 0)
    if (producersInGraph != expectedProducers) {
      for (prod <- graph.nodes.producerIds) if (prod != super_flavor_node_id) {
        val p = graph.nodes(prod)
        assert(p.isProducer, s"there is a producer, which is no producer!!! ${p.nodeType}")
        println(p.taskGroup.get.detailedToString())
      }
      assert(false, s"there are more producers ($producersInGraph) in the graph than expected ($expectedProducers)!")

    }

    skippedFlavorSelectorBecauseOverloaded = 0
    var connected_cnt = 0
    var flavor_nodes_hint_overloaded_cell = false
    val timeThresholdBeforeForceServer = now - SimulationConfiguration.HIRE_SERVER_PENALTY_WAITING_UPPER
    jobNodes.foreachEntry((job, linker) => {
      if (linker.flavorNodeId != -1) {

        scheduler.logVerbose(s"consider jf j:${job.id} submitted at:${job.submitted}, " +
          s"lastPreemptTime${job.lastPreemptionTime} flavor possible since:${job.flavorDecisionPossibleSince}")

        if (maxFlavorNodes > 0) {
          // Make sure the FlavorSelector is connected to the graph (if possible)

          val (connected_options, inpOptionAvailable, anyTaskGroupReady) = connectFlavorSelectorToGraph(job, linker)

          if (anyTaskGroupReady && connected_options == 0) {
            // there are flavor nodes, but we do not find an option, so the cell is overloaded
            scheduler.logVerbose("flavor node does not find an option, looks like cell is overloaded")
            flavor_nodes_hint_overloaded_cell = true
          }

          // Also if there there are options, this FlavorSelector is able to push one flow through a machine node
          if (connected_options > 0) {
            maxFlavorNodes -= 1
            connected_cnt += 1
          } else {
            skippedFlavorSelectorBecauseOverloaded += 1
            // this flavor selector has no option available, simply check if there is a taskgroup in the flavor part
            // .. if there is no task group in the flavor node, this means there is currently no active TG in the flow network,
            // but some when later there will be task group ;)
            if (anyTaskGroupReady) {
              scheduler.logDetailedVerbose(s" do not connect flavor part of j:$job, " +
                s"inp available:$inpOptionAvailable, connected Options:$connected_options")

            }
          }

        } else {
          skippedFlavorSelectorBecauseOverloaded += 1

          linker.forEachFlavorFlowNode(tgNode => {
            // The TaskGroup associated with that option
            // First disconnect it from the graph (i.e. remove arcs)
            val ttsub = TimeIt("connectFlavor p1.disconnect")
            if (tgNode.outgoing.nonEmpty)
              disconnectAggregatorFromGraph(tgNode.id)
            ttsub.stop
          })

          val flavor_node = graph.nodes(linker.flavorNodeId)

          val flavor_node_is_connected_to_sfn = flavor_node.incoming.existsArcWith(super_flavor_node_id)
          if (flavor_node_is_connected_to_sfn) {
            graph.removeArc(flavor_node.incoming.get(super_flavor_node_id).reverseArc)
          }

        }
      }
    })

    if (connected_cnt == 0 && flavor_nodes_hint_overloaded_cell) {
      scheduler.logDetailedVerbose("Looks like no flavor node finds a valid allocation, so mark graph as overloaded")
      skippedFlavorSelectorBecauseOverloaded += 1
    }

    // How many individual flavor selectors are there connected to the super flavor selector?
    super_flow_node.dirty = false
    // Whats the supply of the super flavor node
    val super_flow_node_supply = math.min(connected_cnt, maxInpFlavorDecisionsPerRound)
    // Update the supply
    super_flow_node.supply = super_flow_node_supply
    assert(super_flow_node.supply >= 0L)
    requiredSinkCapacity += super_flow_node_supply
    if (graph.nodes.containsNonProducer(super_flavor_node_id) && super_flow_node_supply > 0) {
      scheduler.logVerbose(s"make super flavor node a producer, with ${super_flow_node_supply} options")
      graph.nodes.migrateNonProducerToProducerNode(super_flavor_node_id)
    } else if (graph.nodes.containsProducer(super_flavor_node_id) && super_flow_node_supply == 0) {
      scheduler.logVerbose(s"make super flavor node a non producer, with ${super_flow_node_supply} options")
      graph.nodes.migrateProducerToNonProducerNode(super_flavor_node_id)
    } else {
      assert(super_flow_node.isProducer == graph.nodes.containsProducer(super_flavor_node_id))
      scheduler.logVerbose(s"super flavor node is not changed, being a producer==${super_flow_node.isProducer}, " +
        s"with ${super_flow_node_supply} options")
    }

    // The FlavorSelector accounts for one new task to be scheduled
    notScheduledTasks += super_flow_node_supply
    // We can only startup as many machines as the supply and the connected tgs allow
    notScheduledTasksOnMaschines += math.min(connected_cnt, super_flow_node_supply)

    // Finally reset dirty flag and potentials for next solver run
    graph.nodes.foreach(node => {
      node.dirty = false
      node.potential = 0L
    })

    // Recalculate the supply of the sink node (most solvers need a net flow of 0)
    val sink = graph.nodes(graph.sinkNodeId)
    // The new supply is the negative amount of tasks to schedule (from FlavorSelectors and TaskGroups)
    sink.supply = -requiredSinkCapacity

    cleanupTimer.stop

    notScheduledTasksOnMaschines
  }

  private def createFlavorNodeAndReturnId(usingUnscheduleId: NodeID): NodeID = {
    val flavor_node = new FlowNode(
      supply = 0L,
      nodeType = NodeType.JOB_FLAVOR_SELECTOR,
      taskGroup = None, // The FlavorSelector does not belong to a specific TaskGroup
      level = scheduler.cell.highestLevel + 4
    )
    val flavor_id = graph.addNode(flavor_node)

    // Connect the FlavorSelector to the unschedule node
    graph.addArc(new FlowArc(
      src = flavor_id,
      dst = usingUnscheduleId,
      capacity = capacities.getFlavorSelectorToUnschedule,
      cost = 0L // cost will be updated anyway later before scheduling
    ))
    flavor_id
  }

  /** The following methods are used to add/remove/update TaskGroups in the Graph * */

  override def addTaskGroup(taskGroup: TaskGroup): Option[FlowNode] = {

    def getNodeLinker(job: Job): NodeLinker = {
      jobNodes.getOrElseUpdate(job, {

        // Create the unschedule node of that job
        val unschedule_node = new FlowNode(
          supply = 0L,
          nodeType = NodeType.TASK_GROUP_POSTPONE,
          taskGroup = Some(taskGroup),
          level = scheduler.cell.highestLevel + 1
        )
        val unschedule_id: NodeID = graph.addNode(unschedule_node)

        // Connect the unschedule node to the sink
        graph.addArc(new FlowArc(
          src = unschedule_id,
          dst = graph.sinkNodeId,
          capacity = capacities.getJobUnscheduleToSink(job),
          cost = ArcDescriptor.NoCosts
        ))

        val flavorNodeId =
        // If the flavor has already been chosen, we don't need a selector!
          if (job.flavorHasBeenChosen)
            -1
          else {
            scheduler.logVerbose(s"create flavor node for job $job because flavor is not yet set, options: ${job.allPossibleOptions.mkString("|")}")
            createFlavorNodeAndReturnId(usingUnscheduleId = unschedule_id)
          }

        // Create the NodeLinker
        new NodeLinker(
          unschedule = unschedule_node,
          flavorNodeId = flavorNodeId,
          flavorPartGroups =
            if (job.flavorHasBeenChosen)
              Array.empty
            else
              new Array(job.taskGroupsCountWithFlavorOptions)
        )
      })
    }

    val job = taskGroup.job.get

    graph.nodes(super_flavor_node_id).dirty = true

    val tgIsInCommonPart = job.checkIfTaskGroupIsDefinitelyInFlavorSelection(taskGroup)
    val tgIsInFlavorPart = (!tgIsInCommonPart && job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup))

    if ((tgIsInCommonPart || tgIsInFlavorPart) && taskGroup.notStartedTasks > 0) {

      // Get the linker storing all participating nodes in that job. If not present, create it.
      val linker = getNodeLinker(job)

      // Create the NTG/STG node representing the TaskGroup
      val taskgroup_node = new FlowNode(
        supply = 0L,
        nodeType =
          if (taskGroup.isSwitch)
            NodeType.NETWORK_TASK_GROUP
          else
            NodeType.SERVER_TASK_GROUP,
        taskGroup = Some(taskGroup),
        level = scheduler.cell.highestLevel + 2
      )
      // And add the new node instance to the graph
      val taskGroupNodeId = graph.addNode(taskgroup_node)

      // should the task group belong to the common flavor part?
      if (tgIsInCommonPart) {

        // Remember the unschedule node used by this TaskGroup
        taskgroup_node.usedUnscheduleId = linker.unschedule.id
        // Enqueue this node as a common option
        linker.commonTaskGroups.add(taskgroup_node)
        taskGroupNodesInCommonPart.add(taskgroup_node)

        // Connect the TaskGroup to the unschedule node
        graph.addArc(new FlowArc(
          src = taskGroupNodeId,
          dst = linker.unschedule.id,
          capacity = capacities.getTaskGroupToUnschedule(taskGroup),
          cost = 0L // will be updated later before scheduling
        ))

        // The flavor has not jet been chosen. So
      } else {
        assert(tgIsInFlavorPart)
        // Enqueue this node as a flavor option
        linker.addFlowNodeInFlavorPart(taskgroup_node)
      }

      Some(taskgroup_node)
    } else {
      scheduler.logDetailedVerbose(s"addTaskGroup tgId:${taskGroup.id}, do not add," +
        s" ${if (taskGroup.notStartedTasks > 0) "tasks left, but not part of job" else "no tasks left"}")
      None
    }
  }

  private def connectFlavorSelectorToGraph(job: Job, linker: NodeLinker): (Int, Boolean, Boolean) = {
    val t = TimeIt("connectFlavor")

    // The nodes participating in this flavor selection
    val unschedule_id: NodeID = linker.unschedule.id
    val flavor_node_id: NodeID = linker.flavorNodeId

    var shortcuts = 0
    var anyTaskGroupAvailable = false

    // Re-add all the TaskGroups registered to it to the Graph
    linker.forEachFlavorFlowNode(tgNode => {
      // The TaskGroup associated with that option
      val taskGroup = tgNode.taskGroup.get
      // First disconnect it from the graph (i.e. remove arcs)
      disconnectAggregatorFromGraph(tgNode.id)
      assert(tgNode.outgoing.isEmpty)

      // And then reconnect it with updated arcs, capacities and costs.
      val added = connectAggregatorToGraph(taskGroup = taskGroup, taskGroupNodeId = tgNode.id, isPartOfFlavorSelectorPart = true)
      assert(tgNode.outgoing.size == added, s"broken? added:$added outgoing:${tgNode.outgoing.size} ${taskGroup.detailedToString()}")
      shortcuts += added

      assert(tgNode.supply == 0, s"there is a broken task group node, ${tgNode.taskGroup.get.detailedToString()}")
    })
    var total_options_connected = 0
    var inpOptionAvailable: Boolean = false
    var mostExpensiveOption = 0L

    // Update the edges from the FlavorSelector to the TaskGroups (has to be executed after connecting the TG's
    // to the graph, in order for the interference score to be calculated correctly
    linker.forEachFlavorFlowNode(tgNode => {
      anyTaskGroupAvailable = true

      // The TaskGroup associated with that option
      val taskGroup = tgNode.taskGroup.get
      // Calculate the capacity of the arc connecting the flavor node to the taskgroup node
      val capacity: Capacity = if (tgNode.outgoing.isEmpty) 0L else
        capacities.getFlavorSelectorToTaskGroup(flavorNodeLinker = linker, taskGroup = taskGroup)

      // this edge might be not there anymore - e.g. when in the previous round, a decision has been made
      if (capacity > 0) {
        // The cost this edge will have
        val cost = costs.getFlavorSelectorToTaskGroup(flavorNodeLinker = linker, taskGroup = taskGroup, taskGroupNode = tgNode)

        // since we removed all the tg nodes, we can simply add the arc, w/o checking for existing arcs...
        if (SimulationConfiguration.SANITY_CHECKS_HIRE)
          assert(!graph.nodes(flavor_node_id).outgoing.existsArcWith(tgNode.id))

        graph.addArc(new FlowArc(
          src = flavor_node_id,
          dst = tgNode.id,
          capacity = capacity,
          cost = cost
        ))

        if (cost > mostExpensiveOption)
          mostExpensiveOption = cost

        if (taskGroup.belongsToInpFlavor && taskGroup.isSwitch) {
          scheduler.logVerbose(s"seems INP is available for ${taskGroup.detailedToString()}")

          inpOptionAvailable = true
        }

        // Account for this option
        total_options_connected += 1
      }
    })

    if (total_options_connected > 0)
      assert(shortcuts > 0)

    // connect F node with U node, but !after updating all the task groups.. so that we can easily use the node linker state
    val unschedCost = costs.getFlavorSelectorToUnschedule(job, linker)
    graph.updateArc(
      src = flavor_node_id,
      dst = unschedule_id,
      newCost = unschedCost
    )

    val flavor_node = graph.nodes(flavor_node_id)
    val flavor_node_is_connected_to_sfn = flavor_node.incoming.existsArcWith(super_flavor_node_id)
    if (total_options_connected > 0) {
      if (!flavor_node_is_connected_to_sfn) {
        // Connect the SuperFlavorSelector to this FlavorSelector
        graph.addArc(new FlowArc(
          src = super_flavor_node_id,
          dst = flavor_node_id,
          capacity = 1L, // Representing 1 flavor decision
          cost = costs.getSuperFlavorSelectorToFlavorSelector
        ))
      }
    } else {
      if (flavor_node_is_connected_to_sfn) {
        graph.removeArc(flavor_node.incoming.get(super_flavor_node_id).reverseArc)
      }
    }

    t.stop

    // Return how many option nodes we have connected, and if there is an inp option available
    (total_options_connected, inpOptionAvailable, anyTaskGroupAvailable)
  }

  private def connectTaskGroupToGraph(taskgroup: TaskGroup,
                                      taskgroup_node: NodeID,
                                      unschedule_node: NodeID): Int = {
    // First update the arc connecting the TaskGroup to it's unschedule node
    graph.updateArc(
      src = taskgroup_node,
      dst = unschedule_node,
      newCost = costs.getTaskGroupToUnschedule(taskgroup)
    )

    // And connect that TaskGroup to all feasible subtrees that can host tasks
    connectAggregatorToGraph(taskGroup = taskgroup, taskGroupNodeId = taskgroup_node, isPartOfFlavorSelectorPart = false)
  }

  def debugGetPossibleShortcutCount(taskGroup: TaskGroup): Int = {
    resources.prepareSubtreeLookupCacheForCurrentRound()
    resources.selectAllocatableSubtreesUsingCaches(taskGroup).size
  }

  private def connectAggregatorToGraph(taskGroup: TaskGroup,
                                       taskGroupNodeId: NodeID,
                                       isPartOfFlavorSelectorPart: Boolean): Int = {

    if (SimulationConfiguration.SANITY_CHECKS_HIRE)
      assert(graph.nodes.contains(taskGroupNodeId))

    val taskGroupNode = graph.nodes(taskGroupNodeId)

    assert(taskGroupNode.shouldHaveUnscheduledConnection == !isPartOfFlavorSelectorPart,
      "node is not part of flavor area, but also does not have a u node!?")

    // A set of node indexes under which at least one of tasks of the TaskGroup can be allocated
    val allocatable: mutable.ArrayDeque[FlowNode] =
      resources.selectAllocatableSubtreesUsingCaches(taskGroup)

    /** Checking whether these subtree indexes actually make sense */
    if (SimulationConfiguration.SANITY_HIRE_SHORTCUTS_FEASIBILITY_CHECK) {

      allocatable.foreach((node: FlowNode) => {

        /** Keeping track of already-visited `Node`s */
        val visited: mutable.Set[FlowNode] = mutable.HashSet()
        /** Keeping track of `Node`s to visit */
        val toVisit: mutable.Queue[FlowNode] = mutable.Queue()

        /** The algorithm starts with the `start`ing machine */
        toVisit += graph.nodes(node.id)

        /** Visiting all its children */
        while (toVisit.nonEmpty) {
          val currentNode: FlowNode = toVisit.dequeue()
          if (SimulationConfiguration.SANITY_CHECKS_HIRE) assert(currentNode.isPhysical)
          val currentNodeType: NodeType = currentNode.nodeType

          /** Considering only NNDs and SNs */
          if (currentNodeType == NodeType.MACHINE_NETWORK || currentNodeType == NodeType.MACHINE_SERVER) {

            if (SimulationConfiguration.SANITY_CHECKS_HIRE) assert((currentNodeType == NodeType.MACHINE_NETWORK && taskGroup.isSwitch) ||
              (currentNodeType == NodeType.MACHINE_SERVER && !taskGroup.isSwitch))

            /** Checking switches */
            if (taskGroup.isSwitch) {

              assert(currentNodeType == NodeType.MACHINE_NETWORK)

              val switchResources = scheduler.cell.switches(mapping.getSwitchMachineIDFromNetworkID(currentNode.id))
              val producerRequirements = scheduler.cell.calculateEffectiveSwitchDemand(taskGroup, scheduler.cell.getActiveInpPropsOfSwitch(mapping.getSwitchMachineIDFromNetworkID(currentNode.id))).firstTaskReserve

              // There exists a feasible place for the task group if every resource dimensions has enough space available and the properties do align
              switchResources.numericalResources.indices.foreach(dim => {
                assert(switchResources.numericalResources(dim) >= producerRequirements(dim))
              })
              assert(switchResources.properties.containsFully(taskGroup.resources.asInstanceOf[SwitchResource].properties))
            }

            /** Checking servers */
            else {

              assert(currentNodeType == NodeType.MACHINE_SERVER)

              val serverResources: Array[NumericalResource] =
                scheduler.cell.servers(mapping.getServerMachineIDFromNodeID(currentNode.id))
              val producerRequirements = taskGroup.resources.asInstanceOf[ServerResource].numericalResources

              val existsOneFeasibleServer: Boolean = serverResources.zip(producerRequirements).forall {
                case (serverResource: NumericalResource, producerRequirement: NumericalResource) =>
                  serverResource >= producerRequirement
              }
              assert(existsOneFeasibleServer)
            }
          }

          // For each child node
          currentNode.outgoing.foreach(arc => {
            val childNode: FlowNode = graph.nodes(arc.dst)

            if (visited.add(childNode) && childNode.isPhysical)
              toVisit.enqueue(childNode)

          })

        }
      })
    }

    // Reset to 0, so we can use this variable temporarily to accumulate outbound capacity
    taskGroup.maxAllocationsInOngoingSolverRound = 0L

    val targets: Array[mutable.ArrayBuffer[FlowNode]] = new Array(SimulationConfiguration.PRECISION.toInt + 1)

    for (index: Int <- targets.indices)
      targets(index) = mutable.ArrayBuffer()

    // Now map all of the options we got to their corresponding cost.
    allocatable.foreach((destination: FlowNode) => {
      val cost: Cost =
        if (taskGroup.isSwitch) {
          val c = costs.getSwitchTaskGroupToNode(taskGroup = taskGroup, targetNode = destination,
            isPartOfFlavorSelectorPart = isPartOfFlavorSelectorPart)
          c
        } else {
          val c = costs.getServerTaskGroupToNode(taskGroup = taskGroup, targetNode = destination,
            isPartOfFlavorSelectorPart = isPartOfFlavorSelectorPart)
          c
        }
      targets(cost.toInt).append(destination)
    })

    // Is the amount of shortcuts to be added unlimited?
    val shortcuts_unlimited = SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP == -1
    // Keep track of how many arcs we already added
    var shortcuts_added: Int = 0
    // Current cost index
    var current_cost_index = 0

    while (current_cost_index <= SimulationConfiguration.PRECISION
      && (shortcuts_unlimited || shortcuts_added < SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP)) {
      val current_targets: mutable.ArrayBuffer[FlowNode] = targets(current_cost_index)

      if (current_targets.nonEmpty) {
        var current_index = 0

        while (current_index < current_targets.size
          && (shortcuts_unlimited || shortcuts_added < SimulationConfiguration.HIRE_SHORTCUTS_MAX_SELECTION_PER_TASK_GROUP)) {

          val destination: FlowNode = current_targets(current_index)
          val capacity: Capacity = capacities.getTaskGroupToNode(taskGroup, destination)

          graph.addArc(new FlowArc(
            src = taskGroupNodeId,
            dst = destination.id,
            capacity = capacity,
            cost = current_cost_index
          ))

          // Keep track of the current outgoing total capacity, which might constrain how many tasks we can start from
          // this TG in the next round.
          taskGroup.maxAllocationsInOngoingSolverRound += capacity
          assert(taskGroup.maxAllocationsInOngoingSolverRound > 0L)
          // Also remember how many shortcuts we already added
          shortcuts_added += 1
          // Advance to next option
          current_index += 1
        }
      }

      current_cost_index += 1
    }

    // The maximum number of allocations is bound by the accumulated outbound capacity and the number of not started tasks
    taskGroup.maxAllocationsInOngoingSolverRound = math.min(
      if (taskGroup.isSwitch)
        scheduler.cell.numSwitches else
        scheduler.cell.numServers,
      math.min(
        taskGroup.maxAllocationsInOngoingSolverRound,
        taskGroup.notStartedTasks))

    if (!isPartOfFlavorSelectorPart) {
      // refresh node supply, with either the remaining task count,
      // or with the found possible allocations candidate capacity + 1 (so this lower the supply to the smallest number we can use

      taskGroupNode.supply =
        math.min(taskGroup.notStartedTasks,
          taskGroup.maxAllocationsInOngoingSolverRound + 1L)

      assert(taskGroupNode.supply >= 1L)

      if (SimulationConfiguration.SANITY_CHECKS_GRAPH)
        assert(graph.nodes.producerIds.contains(taskGroupNode.id))
    }
    shortcuts_added
  }

  def removeNode(nodeId: NodeID): Unit = {

    if (SimulationConfiguration.SANITY_CHECKS_HIRE)
      assert(nodeId != -1, "Can not remove node with unset id tag!")

    val node: FlowNode = graph.nodes(nodeId)

    // If this node represents a TaskGroup of the common part (not in flavor selector), we need to drop references to this
    if (node.isTaskGroup && node.shouldHaveUnscheduledConnection) {

      val taskgroup = node.taskGroup.get
      // The linker holding all information about the nodes participating in the TaskGroup's job
      val linker: NodeLinker = jobNodes(taskgroup.job.get)

      // And drop references in corresponding collections
      linker.commonTaskGroups.remove(node)
      taskGroupNodesInCommonPart.remove(node)

      // Remember that we have deleted this TaskGroup. We still need to clear references to this TaskGroup
      // in the next round of 'clear'!!!
      removed_taskgroup_backlog += taskgroup
    }

    // remove all outgoing arcs and also clear parent/child relations
    graph.removeArcsWithSource(nodeId)
    graph.nodes.remove(nodeId)
  }

  def checkFlavorSelectorAfterFlavorAllocationAndUpdateGraph(job: Job, afterPreemption: Boolean = false): Unit = {
    // this function will be called after the flavor selector did an allocation, but also after preempting a job

    scheduler.logDetailedVerbose(s"Check job:${job} after ${if (afterPreemption) "preemption" else "alloc"}")

    // before removing any of the edges from F-G, we need to check if the edge still exists (we might
    // have removed it previously, simply when there was a unfeasible flavor part

    val linker = jobNodes(job)
    val now = scheduler.simulator.currentTime()

    def onPushTaskGroupToCommonPart(taskGroup: TaskGroup,
                                    node: FlowNode): Unit = {

      scheduler.logDetailedVerbose(s"   [Update Flavor] Removing TG ${taskGroup} (${node}) and push to " +
        s"common part because task group flavor ${taskGroup.inOption.mkString(",")} is in ${job.chosenJobOption.mkString(",")}")

      // Disconnect that TaskGroup first
      disconnectAggregatorFromGraph(node.id)

      taskGroupNodesInCommonPart.add(node)
      node.usedUnscheduleId = linker.unschedule.id
      node.supply = taskGroup.notStartedTasks
      assert(node.supply > 0)
      graph.nodes.migrateNonProducerToProducerNode(node.id)
      linker.commonTaskGroups.add(node)

      // Connect the TaskGroup to the unschedule node
      graph.addArc(new FlowArc(
        src = node.id,
        dst = linker.unschedule.id,
        capacity = capacities.getTaskGroupToUnschedule(taskGroup),
        cost = 0L // will be updated later before scheduling
      ))
    }

    def createFlavorNodeAgain(linker: NodeLinker) = {
      // create flavor node again
      linker.flavorNodeId = {
        createFlavorNodeAndReturnId(usingUnscheduleId = linker.unschedule.id)
      }
      scheduler.logDetailedVerbose(s"  [create flavor node] with id ${linker.flavorNodeId} " +
        s"options: ${job.allPossibleOptions.mkString("|")}")
    }

    def removeTaskGroup(taskGroup: TaskGroup,
                        taskGroupNode: FlowNode): Unit = {
      scheduler.logDetailedVerbose(s"   Remove tgId:${taskGroup} (${taskGroupNode}) because remaining tasks:${taskGroup.notStartedTasks} or" +
        s" flavor ${taskGroup.inOption.mkString(",")} not in chosen flavor ${job.chosenJobOption.mkString(",")}")

      removeNode(taskGroupNode.id)
    }

    if (afterPreemption) {
      // at this point, there is no allocation which conflicts with the flavor, however, there might a TG which conflicts!!!
      // maybe there is a TG missing in the graph because we previously removed it

      // flavor may be chosen, but could be also undecided

      // is there still a flavor node?
      val flavorNodeExists = linker.flavorNodeId != -1

      val taskGroupChecked: mutable.HashSet[TaskGroup] = mutable.HashSet()

      graph.nodes(super_flavor_node_id).dirty = true

      // flavor chosen?
      if (job.flavorHasBeenChosen) {
        scheduler.logDetailedVerbose(s" [Flavor is chosen] ${job.chosenJobOption}")
        // if so, check for the remaining TGs in the flavor node part, whether we need to migrate them or delete
        if (flavorNodeExists) {
          linker.forEachFlavorFlowNode(tgNode => {
            val tg = tgNode.taskGroup.get
            taskGroupChecked.add(tg)
            if (job.checkIfTaskGroupIsDefinitelyInFlavorSelection(tg)) {
              onPushTaskGroupToCommonPart(tg, tgNode)
            } else {
              removeTaskGroup(tg, tgNode)
            }
          })
          // invalidate the flavor part
          linker.resetFlavorPartNodes()
        }
      } else {
        scheduler.logDetailedVerbose(s" [Flavor not chosen] exists:$flavorNodeExists fid:${linker.flavorNodeId}")
        // flavor not chosen
        // reset the flavor part
        if (!flavorNodeExists) {
          linker.resetFlavorPartNodes()
          createFlavorNodeAgain(linker)
        } else {
          // flavor node was in graph and preemption was running, so it is likely that we need to add a flavor node.. so we need to reset this data structure
          scheduler.logDetailedVerbose(s" [prepare new flavor part] remove all existing flavor nodes to prepare for re-insertion")
          linker.forEachFlavorFlowNode(tgNode => {
            val tg = tgNode.taskGroup.get
            removeTaskGroup(tg, tgNode)
          })
          linker.resetFlavorPartNodes()
        }
      }
      // now the flavor part is for sure empty
      // common part may have TGs which need to be moved to flavor part
      // common part / flavor part may miss some TGs

      // check the existing common part, what needs to be removed/moved to flavor part
      // we need to take a copy.. will be manipulated while iterating
      scheduler.logDetailedVerbose(" [start checking common TG nodes] ...")
      Array.from(linker.commonTaskGroups).foreach(taskGroupFlowNode => {
        val taskGroup = taskGroupFlowNode.taskGroup.get
        if (taskGroupChecked.add(taskGroup)) {
          // check if this tg is still part of the job
          if (job.checkIfTaskGroupIsDefinitelyInFlavorSelection(taskGroup)) {
            // simply keep it here, all fine
            // supply will be updated later in cleanup
            scheduler.logDetailedVerbose(s" [tgId:${taskGroup.id}] keep in common")
          } else if (job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup)) {
            scheduler.logDetailedVerbose(s" [tgId:${taskGroup.id}] push from common to flavor")
            // remove this guy from the common part - needs to be in flavor selection part
            removeTaskGroup(taskGroup, taskGroupFlowNode)
            assert(taskGroupFlowNode.id == -1)
            // simply call add logic, which will add it to the flavor part
            val newTgNode: FlowNode = addTaskGroup(taskGroup).get
            scheduler.logDetailedVerbose(s"  [tgId:${taskGroup.id}] has a new flavor tgNodeId:${newTgNode.id}")
          } else {
            scheduler.logDetailedVerbose(s" [tgId:${taskGroup.id}] is not part of job, remove")
            // TG will be definitely not in this job anymore
            removeTaskGroup(taskGroup, taskGroupFlowNode)
          }
        }
      })

      // now the common and flavor part contains only valid entries
      // however, both might miss nodes

      scheduler.logDetailedVerbose(" [check all other tg if not already considered] ...")
      // now check each task group, if we need to add it
      for (tg <- job.taskGroups) {
        // consider only TGs that were submitted already, and TGs we did not check yet
        if (tg.submitted <= now && taskGroupChecked.add(tg)) {
          scheduler.logDetailedVerbose(s" [tgId:${tg.id}] re-insert")
          addTaskGroup(tg)
        }
      }
    }

    // called after allocation
    else {
      assert(linker.flavorNodeId != -1)
      scheduler.logDetailedVerbose(s" [Update Flavor] check ${linker.expensiveFlavorFlowNodes2List.size} nodes:" +
        s" ${linker.expensiveFlavorFlowNodes2List.mkString("|")}")

      // Now look at each TaskGroup participating in the flavor part
      linker.forEachFlavorFlowNode(taskGroupNode => {

        val taskGroup = taskGroupNode.taskGroup.get

        // Handling the case where the flavor has been fully chosen.
        if (job.flavorHasBeenChosen) {

          // Remove the TaskGroup if either all Tasks have been started or it is not in the flavor selection.
          if (taskGroup.notStartedTasks == 0 || !job.checkIfTaskGroupIsDefinitelyInFlavorSelection(taskGroup))
            removeTaskGroup(taskGroup, taskGroupNode)
          // Otherwise the TaskGroup belongs to the flavor and still has more tasks to schedule
          else
            onPushTaskGroupToCommonPart(taskGroup, taskGroupNode)
        } else {
          // The TaskGroup has no more tasks to start or is not by any chance in the flavor selection... So we remove it
          if (taskGroup.notStartedTasks == 0 || !job.checkIfTaskGroupMightBeInFlavorSelection(taskGroup))
            removeTaskGroup(taskGroup, taskGroupNode)
          // If the TaskGroup is definitely in the flavor part and has more tasks to schedule, we want to push it to the common part
          else if (job.checkIfTaskGroupIsDefinitelyInFlavorSelection(taskGroup))
            onPushTaskGroupToCommonPart(taskGroup, taskGroupNode)
          // Leaving the TaskGroup  in the flavor selector as the jobs flavor has not jet been decided
          else
            scheduler.logDetailedVerbose(s" [Update Flavor] keep TG ${taskGroup} (${taskGroupNode}) in flavor area " +
              s"since it is not clear whether  flavor ${taskGroup.inOption.mkString(",")} will be in ${job.chosenJobOption.mkString(",")} in future")

        }
      })
    }

    if (job.flavorHasBeenChosen) {

      if (job.statisticsInpChosen)
        scheduler.statisticsFlavorTakeInp += 1
      else
        scheduler.statisticsFlavorTakeServer += 1

      scheduler.logDetailedVerbose(s" [Update Flavor] Flavor selection is done for job $job! " +
        s"Group size common:${linker.commonTaskGroups.size} flavor:${linker.approximateFlavorPartNodeCount}")

      if (linker.flavorNodeId != -1) {
        // Finally remove the flavor selector itself
        removeNode(linker.flavorNodeId)
        linker.flavorNodeId = -1
      }
    } else {
      assert(linker.flavorNodeId != -1, s"flavor node is missing, but the flavor is not chosen")
    }

    // sanity checks
    if (SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER_DETAILED ||
      SimulationConfiguration.SANITY_CHECKS_GRAPH) {
      val linker: NodeLinker = jobNodes(job)
      var fail = false
      val commonTaskGroups: Map[TaskGroup, FlowNode] = linker.commonTaskGroups.map(fn => {
        assert(fn.isPresentInGraph)
        (fn.taskGroup.get, fn)
      }).toMap
      val flavorTaskGroups: mutable.HashMap[TaskGroup, FlowNode] = mutable.HashMap()
      linker.forEachFlavorFlowNode(fn => {
        val tg = fn.taskGroup.get
        flavorTaskGroups.addOne(tg, fn)
      }, ignoreFlowNodeStatus = true)

      assert(flavorTaskGroups.isEmpty == job.flavorHasBeenChosen)

      // we check if all submitted TGs that match to the job or that could match, are present in graph.
      for (tg <- job.taskGroups) {
        if (tg.submitted <= now && tg.notStartedTasks > 0) {
          if (job.checkIfTaskGroupIsDefinitelyInFlavorSelection(tg)) {
            scheduler.logDetailedVerbose(s"    in common:${tg.shortToString()}")
            if (!commonTaskGroups.contains(tg)) {
              fail = true
              scheduler.simulator.logWarning(s"tg found which is - for whatever reason, ready for schedule, but not in common part?, ${tg.detailedToString()}, appx:${linker.approximateFlavorPartNodeCount} common:${linker.commonTaskGroups}")
            }
          } else if (!job.flavorHasBeenChosen && job.checkIfTaskGroupMightBeInFlavorSelection(tg)) {

            scheduler.logDetailedVerbose(s"    in flavor:${tg.shortToString()}")
            if (!flavorTaskGroups.contains(tg) || flavorTaskGroups(tg).shouldHaveUnscheduledConnection) {
              fail = true
              scheduler.simulator.logWarning(s"tg found which is - for whatever reason, ready for schedule, " +
                s"but not in flavor part - " +
                s"has U node:${flavorTaskGroups.contains(tg) && flavorTaskGroups(tg).shouldHaveUnscheduledConnection}?, ${tg.detailedToString()}, appx:${linker.approximateFlavorPartNodeCount} common:${linker.commonTaskGroups}")
            }
          }
        } else {
          if (tg.submitted > now) {
            scheduler.logDetailedVerbose(s"    future:${tg.shortToString()}")
          } else {
            scheduler.logDetailedVerbose(s"    finished:${tg.shortToString()}")
          }
        }
      }

      if (fail) {
        scheduler.simulator.logWarning(s"common:${commonTaskGroups}")
        scheduler.simulator.logWarning(s"flavor:${flavorTaskGroups}")
        assert(!fail, s"failed preemption: ${job.detailedToString()}")
      }
    }
  }
}

/** Stores fundamental graph nodes for each TaskGroup
 * and FlavorSelector object.
 *
 * @param unschedule the TaskGroup's / FlavorSelector's unscheduled node (U)
 */
class NodeLinker(val unschedule: FlowNode,
                 var flavorNodeId: Int = -1,
                 flavorPartGroups: Array[FlowNode] = Array.empty,
                 val commonTaskGroups: mutable.HashSet[FlowNode] = mutable.HashSet()) {

  assert(unschedule.id >= 0)
  var lastFlavorTaskGroupInsertId: Int = -1

  private var flavorPartNodeCount = 0
  private var _oldestSubmissionTimeOfFlavorNode: Int = -1

  @inline def approximateFlavorPartNodeCount: Int = flavorPartNodeCount

  @inline def oldestSubmissionTimeOfFlavorNode: Int =
    if (_oldestSubmissionTimeOfFlavorNode == -1) Int.MaxValue else _oldestSubmissionTimeOfFlavorNode


  def forEachFlavorFlowNode(fn: FlowNode => Unit,
                            ignoreFlowNodeStatus: Boolean = false): Unit = {
    if (flavorNodeId != -1) {
      var i = 0
      flavorPartNodeCount = lastFlavorTaskGroupInsertId
      _oldestSubmissionTimeOfFlavorNode = -1
      while (i <= lastFlavorTaskGroupInsertId) {
        val node = flavorPartGroups(i)
        if (node.id != -1 && (ignoreFlowNodeStatus || !node.shouldHaveUnscheduledConnection)) {
          val tmp = node.taskGroup.get.submitted
          if (_oldestSubmissionTimeOfFlavorNode == -1 || _oldestSubmissionTimeOfFlavorNode > tmp)
            _oldestSubmissionTimeOfFlavorNode = tmp

          fn(node)
        } else {
          flavorPartNodeCount -= 1
        }
        i += 1
      }
    } else {
      _oldestSubmissionTimeOfFlavorNode = -1
      flavorPartNodeCount = 0
    }
  }

  def expensiveFlavorFlowNodes2List: mutable.Seq[FlowNode] = {
    val out: mutable.ListBuffer[FlowNode] = mutable.ListBuffer()
    forEachFlavorFlowNode(out.addOne)
    out
  }

  def resetFlavorPartNodes(): Unit = {
    flavorPartNodeCount = 0
    _oldestSubmissionTimeOfFlavorNode = -1
    lastFlavorTaskGroupInsertId = -1
  }

  def addFlowNodeInFlavorPart(flowNode: FlowNode): Unit = {
    lastFlavorTaskGroupInsertId += 1
    if (_oldestSubmissionTimeOfFlavorNode == -1)
      _oldestSubmissionTimeOfFlavorNode = flowNode.taskGroup.get.submitted
    else
      _oldestSubmissionTimeOfFlavorNode = _oldestSubmissionTimeOfFlavorNode min flowNode.taskGroup.get.submitted
    flavorPartNodeCount += 1
    flavorPartGroups(lastFlavorTaskGroupInsertId) = flowNode
  }

  override def toString: String = s"U: ${unschedule.id}, fTgs: ${flavorPartGroups.size}"

}

class FlowNodeContainer(indices: mutable.BitSet = mutable.BitSet(),
                        startNodeArray: Array[FlowNode] = Array.empty)
  extends FixedIndexElementStore[FlowNode, FlowNodeContainer](indices = indices, startNodeArray = startNodeArray)
    with collection.IterableOnce[FlowNode] {
  override def getArray(size: NodeID): Array[FlowNode] = new Array[FlowNode](size)

  def add(item: FlowNode): FlowNode = super.add(item, item.id)

  def remove(item: FlowNode): Boolean = super.remove(item.id)

  def asSortedList(cmp: (FlowNode, FlowNode) => Boolean): List[FlowNode] = {
    List.from(this).sortWith(cmp)
  }

  override def shimCopy: FlowNodeContainer = new FlowNodeContainer(indices = this.indices.clone(), startNodeArray = Array.from(allNodes))

  override def iterator: Iterator[FlowNode] = new Iterator[FlowNode] {
    private val i = indices.iterator

    override def hasNext: Boolean = i.hasNext

    override def next(): FlowNode = allNodes(i.next())
  }
}