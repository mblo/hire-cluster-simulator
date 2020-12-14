package hiresim.scheduler.flow.solver.mcmf

import hiresim.graph.NodeType
import hiresim.scheduler.flow.solver.graph.{FlowArc, FlowArcContainer, FlowGraph, FlowNode}
import hiresim.simulation.RandomManager
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Callable, ExecutorService, Executors}
import scala.collection.mutable
import scala.util.matching.Regex

class FlowNetworkMicroTests extends AnyFunSuite with BeforeAndAfterEach {


  def withRandom(seed: Int, testCode: (RandomManager) => Any): Unit = {
    testCode(new RandomManager(seed))
  }


  def withFlowNetwork(size: Int, connections: Int, rand: RandomManager,
                      testCode: (FlowGraph, mutable.TreeMap[Int, mutable.TreeSet[Int]]) => Any): Unit = {
    val graph: FlowGraph = new FlowGraph()

    // create some nodes
    (0 until size - 1).foreach(_ =>
      graph.addNode(new FlowNode(rand.getIntBetween(0, 4),
        NodeType.NETWORK_TASK_GROUP, None, 3))
    )


    val sink = new FlowNode(-3L, NodeType.SINK, None, 0)
    graph.addNode(sink)

    // all nodeIds
    val allNodes: Array[Int] = graph.nodes.nonProducerIds.|(graph.nodes.producerIds).toArray
    assert(allNodes.size == size)

    // do some random connections
    val connectedMap: mutable.TreeMap[Int, mutable.TreeSet[Int]] = mutable.TreeMap()
    (0 until connections).foreach(_ => {
      // shuffle arrays and find a valid connection
      val (fromNode: Int, toNode: Int) = {
        val fromChoices: mutable.ArraySeq[Int] = rand.shuffle(allNodes)
        val toChoices: mutable.ArraySeq[Int] = rand.shuffle(allNodes)
        var found = false
        var i = 0
        var j = 0
        while (!found && i < fromChoices.length) {
          j = 0
          while (!found && j < toChoices.length) {
            if (fromChoices(i) != toChoices(j)) {
              val m = connectedMap.getOrElseUpdate(fromChoices(i), mutable.TreeSet.empty)
              if (m.add(toChoices(j))) found = true
            }
            if (!found)
              j += 1
          }
          if (!found)
            i += 1
        }
        assert(found, "cannot find a valid connection... probably you asked for too many connections")
        (fromChoices(i), toChoices(j))
      }

      val arc = new FlowArc(fromNode, toNode, 2L, 0L, 10L)
      graph.addArc(arc)
    })

    testCode(graph, connectedMap)
  }


  def getRandomConnection(rand: RandomManager,
                          connectedMap: mutable.TreeMap[Int, mutable.TreeSet[Int]]) = {
    // choose a src which has at least one connection
    val possibleSources: IndexedSeq[(Int, mutable.TreeSet[Int])] = connectedMap.toIndexedSeq.filter(p => p._2.nonEmpty)
    assert(possibleSources.nonEmpty)
    val from: (Int, mutable.TreeSet[Int]) = possibleSources(rand.getIntBetween(0, possibleSources.size))
    val possibleTargets = from._2.toIndexedSeq
    assert(possibleTargets.nonEmpty)
    val to: Int = possibleTargets(rand.getIntBetween(0, possibleTargets.size))

    (from._1, to)
  }


  def replayDump(longLog: String, safeMode: Boolean) = {
    val container = FlowArcContainer(true, 0, true)
    val hashSet: mutable.HashSet[Int] = mutable.HashSet()

    val executor: ExecutorService = Executors.newCachedThreadPool()
    var j = 0
    val pattern: Regex = "\\((true|false),([0-9]+)\\),?".r
    pattern.findAllIn(longLog.substring(1)).matchData foreach {
      m => {
        val add = m.group(1).toBoolean
        val id = m.group(2).toInt
        j += 1
        if (add) {
          container.addOne(new FlowArc(0, id, 0L))
          assert(hashSet.add(id))
        } else {
          val arc = container.get(id)
          container.-=(arc)
          assert(hashSet.remove(id))
        }

      }


    }

    // now simulate the solver
    def doThreadedCloning() = {
      val parallelGuys = 4
      val callableTasks: util.ArrayList[Callable[FlowArcContainer]] =
        new util.ArrayList[Callable[FlowArcContainer]](parallelGuys)

      val done = new AtomicBoolean(false)

      for (s <- 0 until parallelGuys) {
        val runnableTask = new Callable[FlowArcContainer] {
          override def call(): (FlowArcContainer) = {

            val clonedG: FlowArcContainer = FlowArcContainer(true, 0, true)
            clonedG.cloneEverythingFromOther(container)

            clonedG
          }
        }

        callableTasks.add(runnableTask)
      }
      val resultContainer: FlowArcContainer = executor.invokeAny(callableTasks)
    }


    println(s" .. replayed ${j} actions, ${longLog.substring(0, 500 min longLog.length)}")
    println(s" .. now simulate threaded cloning")

    if (safeMode) {
      println(s" .. activate safe mode, container has some elements:${container.iterator.nonEmpty}")
    }
    doThreadedCloning()

    for (entry <- hashSet) assert(container.existsArcWith(entry))
    container.foreach(entry => assert(hashSet.contains(entry.dst)))
  }

  test("fromLogState") {
    val longLog: String = "[(true,99783),(true,99788),(true,99789),(true,99790),(true,99791),(true,99785),(true,99792),(true,99793),(false,99783),(false,99788),(false,99789),(false,99790),(false,99791),(false,99785),(false,99792),(false,99793)]"
    replayDump(longLog, false)
  }

  test("basicOperations") {
    (0 until 30).foreach(seed =>
      withRandom(seed = seed, rand => {
        withFlowNetwork(size = 30, connections = 500 + rand.getInt(370), rand = rand,
          (graph: FlowGraph, connectedMap: mutable.TreeMap[Int, mutable.TreeSet[Int]]) => {

            // is there already an arc to the final dst?
            if (!connectedMap.head._2.contains(29)) {
              val arc = new FlowArc(connectedMap.head._1, 29, 2L, 0L, 10L)
              graph.addArc(arc)
              // println(s"add an arc | Arc:${arc} ConnectedMapHead: ${connectedMap.head._1}")
              connectedMap(arc.src).add(arc.dst)
            }

            def checkArc(from: Int, to: Int, graph: FlowGraph) = {
              val fromNode = graph.nodes(from)
              val toNode = graph.nodes(to)
              assert(fromNode.outgoing.get(to) != null, s"$fromNode is missing an outgoing arc to $to")
              assert(toNode.incoming.get(from) != null)
              // check forward and backward arcs
              val arc: FlowArc = fromNode.outgoing.get(to)
              assert(arc.fwd)
              val backArc: FlowArc = toNode.incoming.get(from)
              assert(!backArc.fwd)
              assert(arc.reverseArc == backArc)
              assert(backArc.reverseArc == arc)
            }

            def checkSomeArcs(graph: FlowGraph): Unit = {
              // check if forward and backward links are correct
              (0 until 10).foreach(_ => {
                val (from, to) = getRandomConnection(rand, connectedMap)
                checkArc(from, to, graph)
              })
            }

            def checkAllArcs(graph: FlowGraph): Unit = {
              connectedMap.foreachEntry((fr, toList) => {
                for (to <- toList) {
                  checkArc(fr, to, graph)
                }
              })
              graph.nodes.foreach(node => {
                assert(connectedMap.contains(node.id))
                node.outgoing.foreach(arc => {
                  assert(connectedMap(node.id).contains(arc.dst), s"there is an arc (${node.id}->${arc.dst}) " +
                    s"in the graph which should not be there according to the connectedMap")
                })
              })
            }

            checkSomeArcs(graph)
            checkAllArcs(graph)

            // clone the graph and check if this guy has all items in there (correctly)
            var cloned = graph.clone()
            checkAllArcs(cloned)

            // delete some arcs
            (0 until 400).foreach(i => {
              val (from, to) = getRandomConnection(rand, connectedMap)
              assert(connectedMap(from).remove(to))
              // use any of the APIs
              if (i % 2 == 0)
                graph.removeOutgoingArc(from, to)
              else
                graph.removeArc(graph.getArc(from, to))
            })
            checkAllArcs(graph)
            // check cloned
            cloned = graph.clone()
            checkAllArcs(cloned)

            // use the remove with exclude method
            (0 until 30).foreach(i => {
              val (from, to) = getRandomConnection(rand, connectedMap)

              // println(s"remove all edges of node $from, except connections with $to")
              graph.removeArcsOfNodeExcept(from, exclude = to)
              connectedMap(from).filterInPlace(test => test == to)
              // now all arcs to this guy
              connectedMap.foreachEntry((src, destinations) => {
                if (src != from && src != to) {
                  destinations.filterInPlace(test => test != from)
                }
              })
              checkAllArcs(graph)
            })

            // check cloned
            cloned = graph.clone()
            checkAllArcs(cloned)


          })
      }))
  }

}
