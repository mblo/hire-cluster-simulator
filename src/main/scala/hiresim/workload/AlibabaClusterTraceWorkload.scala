package hiresim.workload

import hiresim.cell.machine.NumericalResource.NumericalResource
import hiresim.cell.machine.{ServerResource, SwitchProps, SwitchResource}
import hiresim.simulation.SimTypes.simTime
import hiresim.simulation.configuration.SimulationConfiguration
import hiresim.simulation._
import hiresim.tenant.{Job, SharedResources, TaskGroup, TaskGroupConnection}
import hiresim.workload.AlibabaClusterTraceWorkload.{RawJobSpec, RawTaskGroupSpec}

import scala.collection.immutable.BitSet
import scala.collection.{immutable, mutable}

class AlibabaClusterTraceWorkload(random: RandomManager,
                                  partialLoadTillTime: SimTypes.simTime,
                                  traceJobFile: String,
                                  traceClusterMachines: String,
                                  maxServerResources: Array[NumericalResource],
                                  maxSwitchNumericalResource: NumericalResource,
                                  setOfSwitchResourceTypes: Array[SwitchProps], // the set of available switch resource types
                                  minKappaServers: Double, // minimum factor by which INP flavor reduces required server tasks of related task group,
                                  // each TG will draw randomly the exact kappa value based
                                  minKappaRuntime: Double, // minimum factor by which INP flavor reduces runtime of task group
                                  muInpFlavor: Double, // ratio of jobs getting an INP flavor
                                  val skipJobsBeforeTime: SimTypes.simTime = -1,
                                  useRandomForKappa: Boolean = true,
                                  createInpFlavorStartingFromTime: SimTypes.simTime = -1,
                                  makespan: Boolean = false,
                                  tgMaxDuration: Option[Int] = None,
                                  ratioOfIncTaskGroups: Double = 0.25,
                                  useSimpleTwoStateInpServerFlavorOptions: Boolean = false
                                 ) extends WorkloadProvider(random = random) {

  assert(maxServerResources.length == 2)

  assert(ratioOfIncTaskGroups >= 0.0 && ratioOfIncTaskGroups <= 1.0)

  // AlibabaClusterTraceWorkload expects the SharedResources Struct to be initialized already

  AlibabaClusterTraceWorkload.setupSharedResourceDemand(maxSwitchNumericalResource)

  protected var jobsWithInpFlavor: Int = 0
  protected val inpDistribution: Array[Int] = Array.fill(setOfSwitchResourceTypes.length)(0)


  val switchPropsToKappaTSValues: immutable.HashMap[SwitchProps, (Double, Double)] =
    immutable.HashMap.from(SwitchProps.allTypes.map(p => {
      (p, p match {
        case _ => (Math.max(minKappaRuntime, 0.8), Math.max(minKappaServers, 0.8))
      })
    }))


  override def getNextBatchTillTime(requestTillTime: Int, simulator: Simulator): Iterator[Event] = {
    new Iterator[Event] {
      override def hasNext: Boolean = pendingJobs.nonEmpty && timeOfLastEvent < requestTillTime

      override def next(): Event = {
        val job = pendingJobs.pop()
        timeOfLastEvent = job.submitted
        new ClosureEvent(_ => simulator.addJob(job), job.submitted)
      }
    }
  }

  private var pendingJobs: mutable.Stack[Job] = mutable.Stack()

  // load all jobs
  load()

  var maxCpuFound = 0.0
  var maxMemFound = 0.0

  var jobCounterINPoffset = 0

  protected def buildJob(parsedLines: mutable.ListBuffer[Array[String]]): Unit = {
    val taskGroups: mutable.ListBuffer[RawTaskGroupSpec] = mutable.ListBuffer()
    var (submissionEarliest: simTime, submissionLatest: simTime) = (Integer.MAX_VALUE, Integer.MIN_VALUE)
    parsedLines.foreach(parsed => {

      val original_submission_time: Int = parsed(2).toInt * 1000
      val target_submission_time =
        if (makespan)
          0
        else
          original_submission_time

      val original_duration: Int = parsed(3).toInt * 1000
      val target_duration =
        if (tgMaxDuration.isDefined)
          original_duration min tgMaxDuration.get
        else
          original_duration

      val tgSpec = new RawTaskGroupSpec(
        jobType = parsed(1),
        submittedTime = target_submission_time,
        duration = target_duration,
        taskGroupName = AlibabaClusterTraceWorkload.taskGroupPrefixTraceNames + parsed(4),
        instances = parsed(5).toInt,
        mem = parsed(6).toDouble / 100,
        cpu = parsed(7).toDouble / 9600.0,
        links = if (parsed.length == 9) Some(parsed(8)) else None
      )
      if (original_submission_time <= partialLoadTillTime && original_submission_time >= skipJobsBeforeTime) {
        taskGroups.append(tgSpec)
        if (original_submission_time < submissionEarliest)
          submissionEarliest = original_submission_time
        if (original_submission_time > submissionLatest)
          submissionLatest = original_submission_time
        if (tgSpec.mem > maxMemFound)
          maxMemFound = tgSpec.mem
        if (tgSpec.cpu > maxCpuFound)
          maxCpuFound = tgSpec.cpu
      }
    })

    // do we need to ignore this job?
    if (taskGroups.nonEmpty) {
      jobCounter += 1
      val hasInpFlavor = {
        if ((submissionEarliest min submissionLatest) <= createInpFlavorStartingFromTime) {
          false
        } else {
          if (jobCounterINPoffset == 0) {
            jobCounterINPoffset = jobCounter
          }

          val targetStatus = (jobsWithInpFlavor * 1000 / ((jobCounter - jobCounterINPoffset) max 1)) - (muInpFlavor * 1000).toInt
          // println(s"$targetStatus with $jobsWithInpFlavor and $jobCounter")
          if (targetStatus > 20) {
            false
          } else if (targetStatus < -20) {
            true
          } else random.getDouble < muInpFlavor
        }
      }


      val job = AlibabaClusterTraceWorkload.buildJob(
        jobSpec = new RawJobSpec(taskGroups),
        hasInpFlavor = hasInpFlavor,
        ratioOfIncTaskGroups = ratioOfIncTaskGroups,
        random = random,
        useRandomForKappa = useRandomForKappa,
        switchPropsToKappaTSValues = switchPropsToKappaTSValues,
        maxServerResources = maxServerResources,
        maxSwitchNumericalResource = maxSwitchNumericalResource,
        setOfSwitchResourceTypes = setOfSwitchResourceTypes,
        inpDistribution = inpDistribution,
        useSimpleTwoStateInpServerFlavorOptions = useSimpleTwoStateInpServerFlavorOptions)

      pendingJobs.append(job)

      if (WorkloadProvider.flavorHasInp(job.allPossibleOptions)) {
        jobsWithInpFlavor += 1
      }
    }
  }

  protected def load(): Unit = {
    val jobFile = io.Source.fromFile(traceJobFile)
    val lines = jobFile.getLines()
    val header = lines.next()
    if (header != "job,type,submit,duration,task_group,instances,mem,cpu,connections") {
      throw new RuntimeException("wrong csv schema")
    }

    val jobDefinitions: mutable.ListBuffer[Array[String]] = mutable.ListBuffer()
    var currentJob: Option[String] = None
    var i = 0
    while (lines.hasNext) {
      val line = lines.next()
      i += 1
      if (SimulationConfiguration.LOGGING_VERBOSE_OTHER) {
        if (i % 10000 == 0) {
          Console.out.print(".")
          Console.out.flush()
          if (i % 1000000 == 0) {
            Console.out.println(" " + (i.toDouble / 14231084.0 * 100).round + "%")
          }
        }
      }

      val parsedLine: Array[String] = line.split(",")

      val jobId = parsedLine(0)

      if (currentJob.isEmpty) {
        currentJob = Some(jobId)
      }

      if (currentJob.get != jobId) {
        // we see already the next job, so build it now
        buildJob(jobDefinitions)
        jobDefinitions.clear()
        currentJob = Some(jobId)
        jobDefinitions.addOne(parsedLine)
      } else {
        // still seeing the current job, simply append
        jobDefinitions.addOne(parsedLine)
      }
    }

    if (currentJob.isDefined) {
      buildJob(jobDefinitions)
    }

    pendingJobs = pendingJobs.sortWith((j1, j2) => {
      if (j1.submitted == j2.submitted) j1.id < j2.id else j1.submitted < j2.submitted
    })

    Console.out.print(s". done loading trace (maxMem:$maxMemFound maxCpu:$maxCpuFound)\n")
    Console.out.flush()
  }

}

object AlibabaClusterTraceWorkload {

  final protected val taskGroupPrefixTraceNames = "fromtrace-"
  final protected val taskGroupPrefixInp = "inp-"
  final protected val taskGroupPrefixInpServer = "inpserver-"

  val maxSram = 22
  val maxStages = 48


  def setupSharedResourceDemand(maxSwitchNumericalResource: NumericalResource) = {
    SharedResources.setPropertyResourceDemand(SwitchProps.typeDaiet,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 18,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeSharp,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 0,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeIncBricks,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 0,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeNetCache,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 8,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeDistCache,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 8,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeNetChain,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 8,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeHarmonia,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 3,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeHovercRaft,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 18,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

    SharedResources.setPropertyResourceDemand(SwitchProps.typeR2p2,
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = 0,
        stages = 0,
        sramMb = 0,
        maxSwitchNumericalResource = maxSwitchNumericalResource))

  }


  class RawTaskGroupSpec(val submittedTime: Int,
                         val jobType: String,
                         val duration: Int,
                         val cpu: Double,
                         val mem: Double,
                         val instances: Int,
                         val links: Option[String],
                         val taskGroupName: String) {
    assert(cpu >= 0.0 && cpu <= 1.0, s"invalid cpu resource: $cpu")
    assert(mem >= 0.0 && mem <= 1.0, s"invalid mem resource: $mem")
    assert(instances > 0)
    assert(jobType == "container" || jobType == "DAG")
    assert(duration > 0)
  }

  class RawJobSpec(val taskGroups: mutable.ListBuffer[RawTaskGroupSpec])


  def drawDurationAndTaskCount(inpType: SwitchProps,
                               normalDuration: Int, normalTaskCount: Int,
                               random: Option[RandomManager],
                               switchPropsToKappaTSValues: immutable.HashMap[SwitchProps, (Double, Double)]):
  (Int, Int) = {
    val (lowerBoundKappaDuration: Double, lowerBoundKappaServers: Double) = switchPropsToKappaTSValues(inpType)

    def randKappa: Double = {
      if (random.isDefined)
        random.get.getDouble
      else
        1.0
    }

    val targetDuration = (normalDuration * (randKappa * (1.0 - lowerBoundKappaDuration) + lowerBoundKappaDuration)).ceil.toInt
    val targetTaskCount = (normalTaskCount * (randKappa * (1.0 - lowerBoundKappaServers) + lowerBoundKappaServers)).ceil.toInt
    assert(targetDuration > 0 && targetDuration <= normalDuration && targetDuration >= normalDuration * lowerBoundKappaDuration)
    assert(targetTaskCount > 0 && targetTaskCount <= normalTaskCount && targetTaskCount >= normalTaskCount * lowerBoundKappaServers)
    (targetDuration, targetTaskCount)
  }

  def translateSwitchResources(reservedRecirculationCapacityPercentage: Double,
                               stages: Int,
                               sramMb: Double,
                               maxSwitchNumericalResource: NumericalResource): Array[Int] = {

    assert(maxSram >= sramMb)
    assert(maxStages >= stages)

    Array(
      (reservedRecirculationCapacityPercentage * maxSwitchNumericalResource).toInt / 100,
      (stages * maxSwitchNumericalResource) / maxStages,
      (sramMb * maxSwitchNumericalResource.toDouble).toInt / maxSram)
  }

  def buildJob(jobSpec: RawJobSpec,
               hasInpFlavor: Boolean,
               ratioOfIncTaskGroups: Double,
               random: RandomManager,
               useRandomForKappa: Boolean,
               switchPropsToKappaTSValues: immutable.HashMap[SwitchProps, (Double, Double)],
               maxServerResources: Array[NumericalResource],
               maxSwitchNumericalResource: NumericalResource,
               setOfSwitchResourceTypes: Array[SwitchProps],
               inpDistribution: Array[Int],
               useSimpleTwoStateInpServerFlavorOptions: Boolean = false
              ): Job = {
    /**
     * this list will be mapped to the task group names in the map of in and out connections
     */
    val arcs: mutable.ListBuffer[(String, String)] = mutable.ListBuffer()

    /**
     * this map will not be pumped with in out combinations
     */
    val flatArcs: mutable.ListBuffer[(String, String)] = mutable.ListBuffer()

    var jobSubmission = -1

    // get the affected task groups that should have an INP flavor
    val taskGroupsWithInp: mutable.Set[NumericalResource] = mutable.Set()
    val maxTaskGroupsWithInp = (jobSpec.taskGroups.size.toDouble * ratioOfIncTaskGroups).ceil.toInt max 1

    // holds the connections according to the trace
    val taskGroups: mutable.Map[String, TaskGroup] = mutable.Map()
    var allJobsFlavorOptions: immutable.BitSet = WorkloadProvider.emptyFlavor

    def translateSwitchResources(reservedRecirculationCapacityPercentage: Double,
                                 stages: Int,
                                 sramMb: Double): Array[Int] = {
      AlibabaClusterTraceWorkload.translateSwitchResources(
        reservedRecirculationCapacityPercentage = reservedRecirculationCapacityPercentage,
        stages = stages,
        sramMb = sramMb,
        maxSwitchNumericalResource = maxSwitchNumericalResource)
    }

    /*
    holds for each task group id, a list of input and output ids
    the key is simply the original name of the task group like it is given in the trace
     */
    val taskGroupConnectionInAndOut: mutable.HashMap[String, (mutable.ListBuffer[String], mutable.ListBuffer[String])] = mutable.HashMap()

    def buildInpComposite(inpType: SwitchProps,
                          traceTaskGroupName: String,
                          submittedTime: SimTypes.simTime,
                          normalDuration: SimTypes.simTime,
                          normalTaskCount: Int,
                          normalAllToAll: Boolean,
                          serverResource: ServerResource
                         ): Unit = {

      assert(normalTaskCount > 0)
      assert(normalDuration > 0)
      val (reducedDuration, reducedTaskCount) = drawDurationAndTaskCount(
        inpType = inpType, normalDuration = normalDuration, normalTaskCount = normalTaskCount,
        random = if (useRandomForKappa) Some(random) else None,
        switchPropsToKappaTSValues = switchPropsToKappaTSValues)

      val (newInpOption: BitSet, newServerOption: BitSet) = {
        if (useSimpleTwoStateInpServerFlavorOptions) {
          val (inp, server, _) = WorkloadProvider.simpleJobIncServerAllFlavorBits
          (inp, server)
        } else {
          (WorkloadProvider.newInpFlavorBit(allJobsFlavorOptions), WorkloadProvider.newServerFlavorBit(allJobsFlavorOptions))
        }
      }

      allJobsFlavorOptions |= newInpOption
      allJobsFlavorOptions |= newServerOption

      inpType match {

        case SwitchProps.typeDaiet | SwitchProps.typeSharp =>
          val inpNumRes = inpType match {
            case SwitchProps.typeDaiet => translateSwitchResources(
              reservedRecirculationCapacityPercentage = 10,
              stages = 1,
              sramMb = 10)
            case SwitchProps.typeSharp => translateSwitchResources(
              reservedRecirculationCapacityPercentage = 0,
              stages = 0,
              sramMb = random.getIntBetween(1, 8))
          }
          val switchNum = Math.log(normalTaskCount).ceil.toInt max 1

          val nameOfInpServer = taskGroupPrefixInpServer + traceTaskGroupName
          val nameOfRootDaietSwitch = "root " + taskGroupPrefixInp + traceTaskGroupName

          // daiet changes the in out relation.. so inputs are always the servers, but output is either the server when running w/o inp, or the root switch
          taskGroupConnectionInAndOut(traceTaskGroupName) = (mutable.ListBuffer(traceTaskGroupName, nameOfInpServer),
            mutable.ListBuffer(traceTaskGroupName, nameOfRootDaietSwitch))

          taskGroups.put(traceTaskGroupName, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newServerOption,
            notInOption = newInpOption,
            duration = normalDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = normalTaskCount,
            allToAll = normalAllToAll,
            resources = serverResource
          ))

          taskGroups.put(nameOfInpServer, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = reducedTaskCount,
            allToAll = false,
            resources = serverResource
          ))

          taskGroups.put(nameOfRootDaietSwitch, new TaskGroup(
            isSwitch = true,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = switchNum,
            allToAll = true,
            resources = new SwitchResource(
              numericalResources = inpNumRes,
              properties = inpType
            ),
            coLocateOnSameMachine = false))

          var remainingSwitches = switchNum - 1
          var predecessors: mutable.ListBuffer[String] = mutable.ListBuffer(nameOfRootDaietSwitch)
          while (remainingSwitches > 0) {
            val nextLevel: mutable.ListBuffer[String] = mutable.ListBuffer()
            predecessors.foreach(parent => {
              (0 until 2).foreach(child => {
                val nameOfThisGuy = s"child$remainingSwitches" + taskGroupPrefixInp + traceTaskGroupName

                taskGroups.put(nameOfThisGuy, new TaskGroup(
                  isSwitch = true,
                  isDaemonOfJob = false,
                  inOption = newInpOption,
                  notInOption = newServerOption,
                  duration = reducedDuration,
                  statisticsOriginalDuration = normalDuration,
                  submitted = submittedTime,
                  numTasks = 1,
                  allToAll = false,
                  resources = new SwitchResource(
                    numericalResources = inpNumRes,
                    properties = inpType
                  ),
                  coLocateOnSameMachine = false))


                remainingSwitches -= 1
                flatArcs += Tuple2(parent, nameOfThisGuy)
                nextLevel += nameOfThisGuy
              })
            })
            predecessors = nextLevel
          }

          predecessors.foreach(firstDaietSwitchAfterServer => {
            flatArcs.addOne((nameOfInpServer, firstDaietSwitchAfterServer))
          })

        case SwitchProps.typeIncBricks | SwitchProps.typeDistCache | SwitchProps.typeNetCache =>
          val inpNumRes: Array[Int] = inpType match {
            case SwitchProps.typeIncBricks => translateSwitchResources(
              reservedRecirculationCapacityPercentage = 40,
              stages = random.getIntBetween(4, 8),
              sramMb = random.getIntBetween(3, 12))
            case SwitchProps.typeDistCache => translateSwitchResources(
              reservedRecirculationCapacityPercentage = random.getInt(10),
              stages = random.getIntBetween(0, 8),
              sramMb = random.getIntBetween(6, 12))
            case SwitchProps.typeNetCache => translateSwitchResources(
              reservedRecirculationCapacityPercentage = random.getInt(10),
              stages = random.getIntBetween(0, 8),
              sramMb = random.getIntBetween(6, 12))
          }

          assert(inpNumRes.max > 0, s"switch num resources should be higher")
          val switchNum: NumericalResource = math.max(3, Math.log(normalTaskCount).ceil.toInt)
          // we need only a single inp task group, since all switches communicate always directly with a server

          val nameOfSwitch = taskGroupPrefixInp + traceTaskGroupName

          taskGroups.put(nameOfSwitch, new TaskGroup(
            isSwitch = true,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = switchNum,
            allToAll = if (inpType.equals(SwitchProps.typeDistCache)) true else false,
            resources = new SwitchResource(
              numericalResources = inpNumRes,
              properties = inpType
            ),
            coLocateOnSameMachine = false))

          // change the in-out connections
          taskGroupConnectionInAndOut(traceTaskGroupName) = (mutable.ListBuffer(traceTaskGroupName, nameOfSwitch), mutable.ListBuffer(traceTaskGroupName, nameOfSwitch))

          // so here we create the required nodes if duration and or server task count is different when running INP
          taskGroups.put(traceTaskGroupName, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newServerOption,
            notInOption = newInpOption,
            duration = normalDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = normalTaskCount,
            allToAll = normalAllToAll,
            resources = serverResource
          ))

          val nameOfInpServer = taskGroupPrefixInpServer + traceTaskGroupName
          taskGroups.put(nameOfInpServer, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = reducedTaskCount,
            allToAll = false,
            resources = serverResource
          ))

          flatArcs.addOne((nameOfInpServer, nameOfSwitch))
          flatArcs.addOne((nameOfSwitch, nameOfInpServer))

        case SwitchProps.typeHovercRaft |
             SwitchProps.typeR2p2 |
             SwitchProps.typeHarmonia =>

          val inpNumRes: Array[Int] = inpType match {
            case SwitchProps.typeHovercRaft => translateSwitchResources(
              reservedRecirculationCapacityPercentage = random.getInt(11),
              stages = random.getIntBetween(1, 19),
              sramMb = (1 + random.getInt(128)).toDouble / 1000.0)
            case SwitchProps.typeR2p2 => translateSwitchResources(
              reservedRecirculationCapacityPercentage = random.getInt(31),
              stages = random.getIntBetween(1, 2 max (maxStages min normalTaskCount)),
              sramMb = (random.getIntBetween(1, 64)).toDouble / 1000.0)
            case SwitchProps.typeHarmonia => translateSwitchResources(
              reservedRecirculationCapacityPercentage = 0,
              stages = random.getIntBetween(1, 4),
              sramMb = (768 + random.getInt(1280)).toDouble / 1000.0)
          }

          assert(inpNumRes.max > 0, s"switch num resources should be higher, received: ${inpNumRes.mkString("[", ",", "]")}")
          val switchNum: NumericalResource = math.max(1, normalTaskCount / 9000)

          // we need only a single inp task group, since the switches do not communicate to each other
          val nameOfHovercRaftSwitch = taskGroupPrefixInp + traceTaskGroupName

          taskGroups.put(nameOfHovercRaftSwitch, new TaskGroup(
            isSwitch = true,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = switchNum,
            allToAll = false,
            resources = new SwitchResource(
              numericalResources = inpNumRes,
              properties = inpType
            ),
            coLocateOnSameMachine = false))

          // HovercRaft will not change the in-out connections
          taskGroupConnectionInAndOut(traceTaskGroupName) = (mutable.ListBuffer(traceTaskGroupName), mutable.ListBuffer(traceTaskGroupName))

          // so here we create the required nodes if duration and or server task count is different when running INP
          taskGroups.put(traceTaskGroupName, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newServerOption,
            notInOption = newInpOption,
            duration = normalDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = normalTaskCount,
            allToAll = normalAllToAll,
            resources = serverResource
          ))

          val nameOfInpServer = taskGroupPrefixInpServer + traceTaskGroupName
          taskGroups.put(nameOfInpServer, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = reducedTaskCount,
            allToAll = false,
            resources = serverResource
          ))

          // add the reduced server to in and out points of this
          taskGroupConnectionInAndOut(traceTaskGroupName)._1 += nameOfInpServer
          taskGroupConnectionInAndOut(traceTaskGroupName)._2 += nameOfInpServer

          flatArcs.addOne((nameOfInpServer, nameOfHovercRaftSwitch))
          flatArcs.addOne((nameOfHovercRaftSwitch, nameOfInpServer))


        case SwitchProps.typeNetChain =>
          val inpNumRes = translateSwitchResources(
            reservedRecirculationCapacityPercentage = random.getInt(11),
            stages = random.getInt(9),
            sramMb = random.getIntBetween(6, 12))
          assert(inpNumRes.max > 0, s"switch num resources should be higher")
          val switchNum: NumericalResource = math.max(3, (3 * normalTaskCount) / 1000)

          // we need only a single inp task group, since all switches communicate too all others
          val nameOfNetChainSwitch = taskGroupPrefixInp + traceTaskGroupName

          taskGroups.put(nameOfNetChainSwitch, new TaskGroup(
            isSwitch = true,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = switchNum,
            allToAll = true,
            resources = new SwitchResource(
              numericalResources = inpNumRes,
              properties = inpType
            ),
            coLocateOnSameMachine = false))

          // NetChain will not change the in-out connections
          taskGroupConnectionInAndOut(traceTaskGroupName) = (mutable.ListBuffer(traceTaskGroupName), mutable.ListBuffer(traceTaskGroupName))

          // so here we create the required nodes if duration and or server task count is different when running INP
          taskGroups.put(traceTaskGroupName, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newServerOption,
            notInOption = newInpOption,
            duration = normalDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = normalTaskCount,
            allToAll = normalAllToAll,
            resources = serverResource
          ))

          val nameOfInpServer = taskGroupPrefixInpServer + traceTaskGroupName
          taskGroups.put(nameOfInpServer, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = newInpOption,
            notInOption = newServerOption,
            duration = reducedDuration,
            statisticsOriginalDuration = normalDuration,
            submitted = submittedTime,
            numTasks = reducedTaskCount,
            allToAll = false,
            resources = serverResource
          ))

          // add the reduced server to in and out points of this
          taskGroupConnectionInAndOut(traceTaskGroupName)._1 += nameOfInpServer
          taskGroupConnectionInAndOut(traceTaskGroupName)._2 += nameOfInpServer

          flatArcs.addOne((nameOfInpServer, nameOfNetChainSwitch))
          flatArcs.addOne((nameOfNetChainSwitch, nameOfInpServer))


      }
    }


    try {

      // build the job

      var jobIsInp: Boolean = false

      var jobHighPrio: Boolean = false

      var serverGroupIndex = -1
      jobSpec.taskGroups.foreach((taskGroupSpec: RawTaskGroupSpec) => {
        serverGroupIndex += 1
        // update Job time
        if (jobSubmission < 0 || jobSubmission > taskGroupSpec.submittedTime) {
          jobSubmission = taskGroupSpec.submittedTime
        }

        val allToAll: Boolean = taskGroupSpec.jobType match {
          case "DAG" => true
          case "container" => false
        }

        // the task group with the highest priority sets the job priority
        jobHighPrio = taskGroupSpec.jobType match {
          case "DAG" => jobHighPrio
          case "container" => true
        }

        // add edges?
        if (taskGroupSpec.links.isDefined && taskGroupSpec.links.get != "") {
          taskGroupSpec.links.get.split("_").foreach(dest => {
            arcs.addOne(Tuple2(taskGroupSpec.taskGroupName, taskGroupPrefixTraceNames + dest))
          })
        }

        val tgCpu: Int = (taskGroupSpec.cpu * maxServerResources(0)).toInt
        val tgMem: Int = (taskGroupSpec.mem * maxServerResources(1)).toInt


        if (!(tgCpu <= maxServerResources(0) && tgCpu > 0 && tgMem <= maxServerResources(1) && tgMem > 0)) {
          throw new RuntimeException(s"resources of task group are corrupted: " +
            s"cpu:${taskGroupSpec.cpu} mem:${taskGroupSpec.mem} tgCpu:$tgCpu tgMem:$tgMem")
        }

        // is this server task group affected by INP?
        if (hasInpFlavor
          && taskGroupsWithInp.size < maxTaskGroupsWithInp
          && taskGroupSpec.instances >= WorkloadProvider.minServerTasksRequiredForInp) {
          taskGroupsWithInp += serverGroupIndex

          val randomSwitchProperty = {
            val minOcc = inpDistribution.min
            val options = inpDistribution.zipWithIndex.filter(_._1 == minOcc).toArray
            val incIndex = options(random.getInt(options.length))._2

            // increase counter
            inpDistribution(incIndex) += 1
            setOfSwitchResourceTypes(incIndex)
          }
          jobIsInp = true


          buildInpComposite(
            inpType = randomSwitchProperty,
            traceTaskGroupName = taskGroupSpec.taskGroupName,
            submittedTime = taskGroupSpec.submittedTime,
            normalDuration = taskGroupSpec.duration,
            normalTaskCount = taskGroupSpec.instances,
            normalAllToAll = allToAll,
            serverResource = new ServerResource(Array(tgCpu, tgMem)))


        } else {
          taskGroupConnectionInAndOut(taskGroupSpec.taskGroupName) = (mutable.ListBuffer(taskGroupSpec.taskGroupName),
            mutable.ListBuffer(taskGroupSpec.taskGroupName))
          taskGroups.put(taskGroupSpec.taskGroupName, new TaskGroup(
            isSwitch = false,
            isDaemonOfJob = false,
            inOption = WorkloadProvider.emptyFlavor,
            notInOption = WorkloadProvider.emptyFlavor,
            duration = taskGroupSpec.duration,
            statisticsOriginalDuration = taskGroupSpec.duration,
            submitted = taskGroupSpec.submittedTime,
            numTasks = taskGroupSpec.instances,
            allToAll = allToAll,
            resources = new ServerResource(Array(tgCpu, tgMem))
          ))
        }
      })

      val edges: mutable.ListBuffer[TaskGroupConnection] = mutable.ListBuffer()
      arcs.foreach(tuple => {
        val from = tuple._1
        val to = tuple._2

        if (taskGroups.contains(from) && taskGroups.contains(to)) {
          val outgoingOptionsOfFrom = taskGroupConnectionInAndOut(from)._2
          val incomingOptionsOfTo = taskGroupConnectionInAndOut(to)._1

          for (fromSpecific <- outgoingOptionsOfFrom;
               toSpecific <- incomingOptionsOfTo
               if taskGroups.contains(fromSpecific) && taskGroups.contains(toSpecific)) {
            edges.addOne(new TaskGroupConnection(src = taskGroups(fromSpecific), dst = taskGroups(toSpecific)))
          }
        }

      })

      // sanity check
      val usedFlavors = mutable.BitSet()
      taskGroups.values.foreach(tg =>
        usedFlavors |= tg.inOption)
      assert(usedFlavors.intersect(allJobsFlavorOptions).size == usedFlavors.size)

      flatArcs.foreach(arc => {
        edges.addOne(new TaskGroupConnection(src = taskGroups(arc._1), dst = taskGroups(arc._2)))
      })

      // deterministic tg order
      val orderedTaskGroups: Array[TaskGroup] = taskGroups.values.toArray.sortWith((m, n) => {
        if (m.isSwitch == n.isSwitch) {
          m.id < n.id
        } else if (m.isSwitch) true
        else false
      })
      val idx: Array[Int] = orderedTaskGroups.indices.toArray
      val shuffleIdx: mutable.ArraySeq[Int] = random.shuffle(idx)
      val shuffleTaskGroups: Array[TaskGroup] = Array.ofDim(orderedTaskGroups.length)

      var i = 0
      while (i < orderedTaskGroups.length) {
        shuffleTaskGroups(i) = orderedTaskGroups(shuffleIdx(i))
        i += 1
      }

      val job = new Job(
        isHighPriority = jobHighPrio,
        submitted = jobSubmission,
        taskGroups = shuffleTaskGroups,
        arcs = edges.toArray)
      job
    }

    catch {
      case e: Exception => {
        System.err.println(jobSpec)
        e.printStackTrace()
        throw e
      }
    }
  }


}

