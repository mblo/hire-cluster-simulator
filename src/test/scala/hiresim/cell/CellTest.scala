package hiresim.cell

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{SwitchProps, SwitchResource}
import hiresim.shared.Tabulator
import hiresim.simulation.RandomManager
import hiresim.simulation.configuration.{SimulationConfiguration, SimulationConfigurationHelper}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers.be

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CellTest extends AnyFunSuite with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    SimulationConfigurationHelper.setDefaultSimulationConfiguration()

    SimulationConfiguration.LOGGING_VERBOSE_OTHER = false
    SimulationConfiguration.LOGGING_VERBOSE_CELL = false
    SimulationConfiguration.LOGGING_VERBOSE_SCHEDULER = false
    SimulationConfiguration.SANITY_CHECKS_CELL = true
    SimulationConfiguration.SANITY_CHECKS_GRAPH = true
  }

  def foreachCell(k: Int, testCode: (Cell) => Any): Unit = {
    Array("binary-tree", "fat-tree").foreach(cellName => {
      withCell(k, cellName, testCode)
    })
  }

  def withCell(k: Int, name: String, testCode: (Cell) => Any): Unit = {
    val cell: Cell = CellFactory.newCell(
      k = k,
      serverResources = Array[NumericalResource](100, 100),
      switchResources = new SwitchResource(Array(100, 100), SwitchProps.all),
      switchMaxActiveInpTypes = 2,
      cellType = name
    )
    testCode(cell)
  }

  foreachCell(4, (cell: Cell) => {
    test(s"testGetAdditionalSwitchLevelToReachServers.${cell.name}") {
      def getXAdditionalParentSwitches(switches: mutable.BitSet, additionalLevels: Int): mutable.BitSet = {
        val out = mutable.BitSet()
        var toCheck = mutable.BitSet().addAll(switches)
        var remainingLevels = additionalLevels
        while (toCheck.nonEmpty && (remainingLevels > 0 || additionalLevels == -1)) {
          remainingLevels -= 1
          toCheck = toCheck.filter(s => cell.switchLevels(s) < cell.highestLevel).flatMap(s => cell.lookup_Switch2Parents(s))
          out.addAll(toCheck)
        }
        out
      }

      val toCheck: Array[(mutable.BitSet, mutable.BitSet)] = Array(
        (mutable.BitSet().addOne(0), (mutable.BitSet().addOne(0))),
        (mutable.BitSet().addOne(0), (mutable.BitSet().addOne(16))),
        (mutable.BitSet().addOne(0), (mutable.BitSet().addOne(19))),
        (mutable.BitSet().addOne(15), (mutable.BitSet().addOne(0))),
        (mutable.BitSet().addOne(15), (mutable.BitSet().addOne(19))),
      ) ++ (0 until 200).map(seed => {
        val random = new RandomManager(seed)
        val servers: mutable.BitSet = mutable.BitSet().addAll(random.getChoicesOfRange(random.getIntBetween(1, 2 max (seed min cell.numServers % 10)), cell.numServers))
        val switches: mutable.BitSet = mutable.BitSet().addAll(random.getChoicesOfRange(random.getIntBetween(1, 2 max (seed min cell.numSwitches % 10)), cell.numSwitches))
        (servers, switches)
      })

      toCheck.foreach(entry => {
        val (servers, switches) = entry

        val additionalLevelReported: Int = cell.getAdditionalSwitchLevelToReachServers(switches, servers)
        additionalLevelReported should be >= (0)
        additionalLevelReported should be <= (cell.highestLevel + 2)

        var additionalLevelRequired = 0
        // check all servers
        for (server <- servers) {
          // start with the tor of this server and all of its parents
          val switchesThatCoverTheServer = mutable.BitSet().
            addOne(cell.lookup_ServerNoOffset2ToR(server)).
            addAll(
              getXAdditionalParentSwitches(
                switches = mutable.BitSet().addOne(cell.lookup_ServerNoOffset2ToR(server)),
                additionalLevels = -1))
          var switchesToCheck = mutable.BitSet().addAll(switches)
          var goUp = true
          var levelThisRound = 0
          // now check if we find a switch by going upstream the topology

          while (goUp && !switchesToCheck.exists(s => switchesThatCoverTheServer.contains(s))) {
            // filter switches to contain only those which are not top level
            switchesToCheck = getXAdditionalParentSwitches(switchesToCheck, 1)
            if (switchesToCheck.isEmpty) {
              goUp = false
            } else {
              // go one step up
              levelThisRound += 1
            }
          }
          // did we find something?
          if (goUp) {
            // yes
          } else {
            // we did not find all switches, so add intermediates steps
            levelThisRound += 2
          }
          additionalLevelRequired = additionalLevelRequired max levelThisRound
        }
        additionalLevelReported should be(additionalLevelRequired)
      })
    }
  })

  foreachCell(8, (cell: Cell) => {
    test(s"testGetMaxDistanceBetween.${cell.name}") {
      (0 until 200).foreach(seed => {
        val random = new RandomManager(seed)
        val rightServers: mutable.BitSet = mutable.BitSet().addAll(random.getChoicesOfRange(random.getIntBetween(1, 2 max (seed min cell.numServers % 6)), cell.numServers))
        val leftSwitches: mutable.BitSet = mutable.BitSet().addAll(random.getChoicesOfRange(random.getIntBetween(1, 2 max (seed min cell.numSwitches % 6)), cell.numSwitches))
        val rightSwitches: mutable.BitSet = mutable.BitSet().addAll(random.getChoicesOfRange(random.getIntBetween(1, 2 max (seed min cell.numSwitches % 6)), cell.numSwitches))

        val maxDist = cell.getMaxDistanceBetween(leftGroupSwitches = leftSwitches,
          rightGroupServers = rightServers,
          rightGroupSwitches = rightSwitches)

        var foundMax = 0
        for (leftSwitch <- leftSwitches) {
          for (rightSwitch <- rightSwitches) {
            val found = cell.getDistanceOf(
              src = leftSwitch,
              srcIsSwitch = true,
              dst = rightSwitch,
              dstIsSwitch = true)
            foundMax = foundMax max found
          }

          for (rightServer <- rightServers) {
            val found = cell.getDistanceOf(
              src = leftSwitch,
              srcIsSwitch = true,
              dst = rightServer,
              dstIsSwitch = false)
            foundMax = foundMax max found
          }
        }

        maxDist should be(foundMax)
      })
    }
  })

  foreachCell(8, (cell: Cell) => {
    test(s"cell.lookup_Server2ToR.${cell.name}") {
      cell.lookup_ServerNoOffset2ToR.size should be(cell.numServers)
      cell.torSwitches.size should be(cell.numServers / cell.serversPerRack)
      for (serverNoOffset <- cell.servers.indices) {
        val tor = cell.lookup_ServerNoOffset2ToR(serverNoOffset)

        cell.torSwitches.exists(sw => sw == tor) should be(true)
        cell.links.exists(edge =>
          (edge.srcWithOffset == tor
            && edge.srcSwitch
            && !edge.dstSwitch
            && edge.dstWithOffset == cell.numSwitches + serverNoOffset)) should be(true)
      }
    }
  })

  foreachCell(8, (cell: Cell) => {
    test(s"testLookup_ToR2Members.${cell.name}") {
      // do we have the expected set of tor switches?
      cell.torSwitches.size should be(cell.numServers / cell.serversPerRack)
      val seenMachines = mutable.BitSet()
      for (tor <- cell.torSwitches) {
        val members = cell.lookup_ToR2MembersWithoutOffset(tor)
        for (memberWithoutOffset <- members) {
          cell.servers.indices.contains(memberWithoutOffset) should be(true)
          seenMachines.add(memberWithoutOffset) should be(true)
          cell.links.exists(edge =>
            (edge.srcWithOffset == tor
              && edge.srcSwitch
              && !edge.dstSwitch
              && edge.dstWithOffset == cell.numSwitches + memberWithoutOffset)) should be(true)
        }
      }
      // did we visit all servers?
      seenMachines.size should be(cell.numServers)
    }
  })

}
