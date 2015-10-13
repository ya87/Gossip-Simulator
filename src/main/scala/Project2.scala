import java.util.ArrayList
import scala.util.Random
import akka.actor.Actor
import akka.routing.BroadcastGroup
import akka.actor.ActorRef
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.RoundRobinGroup
import akka.routing.Broadcast
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import akka.actor.Cancellable

//Project2 Without Fault
object Gossip {

  val TOPOLOGY_LINE = "line"
  val TOPOLOGY_2D = "2d"
  val TOPOLOGY_IMPERFECT_2D = "imp2d"
  val TOPOLOGY_FULL = "full"

  val ALGORITHM_GOSSIP = "gossip"
  val ALGORITHM_PUSH_SUM = "pushsum"

  val MAX_RUMOUR_COUNT = 5
  val MAX_STOP_COUNT = 3
  val THRESHOLD = Math.pow(10, -10)

  case class AddNeighbours(neighbours: ArrayList[ActorRef])
  case object StartMonitor
  case class Start(algo: String)
  case object StartPushSum
  case class PushSum(s: Double, w: Double)
  case object SpreadRumour
  case object NeighbourDead
  case object ReceivedRumourOnce
  case object BuzzAlarmGossip
  case object BuzzAlarmPushSum
  case object WakeUpGossip
  case object WakeUpPushSum
  case object NodeDead

  def main(args: Array[String]) {

    val topologyList = List(TOPOLOGY_LINE, TOPOLOGY_2D, TOPOLOGY_IMPERFECT_2D, TOPOLOGY_FULL)
    val algoList = List(ALGORITHM_GOSSIP, ALGORITHM_PUSH_SUM)
    var numNodes: Int = 0
    var topology: String = ""
    var algo: String = ""

    var isValidInputArgs = true

    if (args.length < 3) {
      isValidInputArgs = false
    } else {
      try {
        numNodes = args(0).toInt
        if (numNodes <= 1) {
          isValidInputArgs = false
        }
      } catch {
        case ex: Exception => {
          isValidInputArgs = false
        }
      }

      topology = args(1)

      if (!topologyList.contains(topology)) {
        isValidInputArgs = false
      }

      algo = args(2)

      if (!algoList.contains(algo)) {
        isValidInputArgs = false
      }
    }

    if (!isValidInputArgs) {
      println("Invalid input arguments !!!")
      println("Run as: Project2 <numNodes> <topology> <algorithm>")
      println("1) numNodes should be greater than 1\n2) Topology should be one these: line, 2d, imp2d, full\n3) Algorithm should be one of these: gossip, pushsum")
      System.exit(1)
    }

    //if topology is 2D or Imperfect 2D then sure numNodes is perfect square
    var n: Int = -1
    if (topology.equalsIgnoreCase(TOPOLOGY_2D) || topology.equalsIgnoreCase(TOPOLOGY_IMPERFECT_2D)) {
      try {
        n = Math.ceil(Math.sqrt(numNodes.toDouble)).toInt
        numNodes = Math.pow(n.toDouble, 2.0).toInt
      } catch {
        case ex: Exception => {
          println(ex.getStackTrace())
        }
      }
    }

    val config = ConfigFactory.load()
    val system = ActorSystem("GossipSimulatorSystem", config)

    var networkMonitor = system.actorOf(Props(classOf[NetworkMonitor], numNodes, topology, algo), "networkMonitor")
    networkMonitor ! StartMonitor
  }

  class NetworkMonitor(pNumNodes: Int, pTopology: String, pAlgo: String) extends Actor {
    val numNodes = pNumNodes
    val topology = pTopology
    val algo = pAlgo
    var numNodesConverged = 0
    var startTime: Long = 0
    var endTime: Long = 0

    var nodes: ArrayList[ActorRef] = new ArrayList[ActorRef]()

    def receive = {
      case StartMonitor => {

        for (i <- 0 to (numNodes - 1)) {
          var nodeId = "node" + (i + 1)
          nodes.add(i, context.actorOf(Props(classOf[Node], nodeId, (i + 1).toDouble, 1.0), name = nodeId))
        }

        buildTopology(nodes, topology, algo)

        startTime = System.currentTimeMillis()
        getRandomNode(nodes) ! Start(algo)
      }

      case ReceivedRumourOnce => {
        numNodesConverged += 1
        if (numNodesConverged == numNodes) {
          endTime = System.currentTimeMillis()
          println("Gossip converged in time: " + (endTime - startTime) + " ms")
        }
      }

      case NodeDead => {
        if (nodes.contains(sender)) {
          nodes.remove(sender)
        }

        if (nodes.size() == 0) {
          if (algo.equalsIgnoreCase(ALGORITHM_GOSSIP)) {
            //if (numNodesConverged != numNodes)
              //println("All nodes dead. Not converged !!!")
            endTime = System.currentTimeMillis()
            println("Gossip finished in time: " + (endTime - startTime) + " ms")
          }

          if (algo.equalsIgnoreCase(ALGORITHM_PUSH_SUM)) {
            endTime = System.currentTimeMillis()
            println("PushSum converged in time: " + (endTime - startTime) + " ms")
          }

          context.system.shutdown()
        }
      }
    }
  }

  class Node(pNodeId: String, pS: Double, pW: Double) extends Actor {
    val nodeId: String = pNodeId
    var s: Double = pS
    var w: Double = pW

    var neighbours: ArrayList[ActorRef] = new ArrayList[ActorRef]()
    var stopCount = 0
    var nodeUp = true
    var nodeWorking = false

    var alarmClock: ActorRef = null
    var alarmClockScheduler: Cancellable = null
    var isAlarmCancelled = false

    def receive = {
      case AddNeighbours(pNeighbours) => {
        neighbours.addAll(pNeighbours)
      }

      case Start(algo) => {
        algo match {
          case ALGORITHM_GOSSIP => {
            self ! SpreadRumour
          }

          case ALGORITHM_PUSH_SUM => {
            self ! StartPushSum
          }
        }
      }

      case StartPushSum => {
        self ! PushSum(0, 0)
      }

      case PushSum(si, wi) => {
        if (nodeUp) {
          var prevRatio = ratio()

          s = s + si
          w = w + wi

          var curRatio = ratio()

          if ((prevRatio - curRatio) < THRESHOLD) {
            stopCount += 1
          } else {
            stopCount = 0
          }

          if (stopCount == MAX_STOP_COUNT) {
            //logger.info("Average computed by " + nodeId + ": " + curRatio)
            nodeUp = false
            context.parent ! NodeDead
          } else {
            s = s / 2.0
            w = w / 2.0
          }
        } 
        
        if(nodeUp){
          getRandomNode(neighbours) ! PushSum(s, w)
        }else{
          getRandomNode(neighbours) ! PushSum(si, wi)
        }
      }

      case SpreadRumour => {
        stopCount += 1

        if (stopCount == 1) {
          context.parent ! ReceivedRumourOnce
          setUpAlarm(ALGORITHM_GOSSIP)
        } else if (stopCount > MAX_RUMOUR_COUNT) {
          if (!isAlarmCancelled) {
            cancelAlarm()
          }
          sender ! NeighbourDead
          context.parent ! NodeDead
        }
      }

      case WakeUpGossip => {
        if (stopCount <= MAX_RUMOUR_COUNT && neighbours.size() > 0) {
          getRandomNode(neighbours) ! SpreadRumour
        } else if (stopCount > MAX_RUMOUR_COUNT) {
          if (!isAlarmCancelled) {
            cancelAlarm()
          }
          context.parent ! NodeDead
        }
      }

      case NeighbourDead => {

        neighbours.remove(sender)

        if (neighbours.size() == 0 && alarmClockScheduler != null) {
          cancelAlarm()
          context.parent ! NodeDead
        }
      }
    }

    def setUpAlarm(algo: String) {
      alarmClock = context.actorOf(Props(classOf[AlarmClock]), "alarm")

      val system = context.system
      import system.dispatcher

      algo match {
        case ALGORITHM_GOSSIP =>
          alarmClockScheduler = system.scheduler.schedule(Duration.Zero, Duration(500, TimeUnit.MILLISECONDS), alarmClock, BuzzAlarmGossip)
        case ALGORITHM_PUSH_SUM =>
          alarmClockScheduler = system.scheduler.schedule(Duration.Zero, Duration(500, TimeUnit.MILLISECONDS), alarmClock, BuzzAlarmPushSum)
      }
    }

    def cancelAlarm() {
      if (alarmClockScheduler != null && !isAlarmCancelled) {
        isAlarmCancelled = alarmClockScheduler.cancel()
      }
    }

    def ratio(): Double = {
      return (s / w)
    }
  }

  class AlarmClock extends Actor {
    def receive = {
      case BuzzAlarmGossip => {
        sender ! WakeUpGossip
      }

      case BuzzAlarmPushSum => {
        sender ! WakeUpPushSum
      }
    }
  }

  def buildTopology(nodes: ArrayList[ActorRef], topology: String, algo: String) {
    val numNodes: Int = nodes.size()
    val isPushSum = (algo.equalsIgnoreCase(ALGORITHM_PUSH_SUM))

    topology match {
      case "line" => {
        for (i <- 0 to (numNodes - 1)) {
          var neighbours: ArrayList[ActorRef] = new ArrayList[ActorRef]()
          var currNode = nodes.get(i)

          if (isNodeLeftmost(i, numNodes)) {
            neighbours.add(nodes.get(i + 1))
          } else if (isNodeRightmost(i, numNodes)) {
            neighbours.add(nodes.get(i - 1))
          } else {
            neighbours.add(nodes.get(i - 1))
            neighbours.add(nodes.get(i + 1))
          }

          currNode ! AddNeighbours(neighbours)
        }
      }

      case "2d" => {
        var n: Int = -1
        try {
          n = Math.sqrt(numNodes.toDouble).toInt
        } catch {
          case ex: Exception => {
            println(ex.getStackTrace())
          }
        }

        for (i <- 0 to (numNodes - 1)) {
          var neighbours: ArrayList[ActorRef] = new ArrayList[ActorRef]()
          var currNode = nodes.get(i)

          identify2dNeighbours(i, n, neighbours, nodes)
          currNode ! AddNeighbours(neighbours)
        }
      }

      case "imp2d" => {
        var n: Int = -1
        try {
          n = Math.sqrt(numNodes.toDouble).toInt
        } catch {
          case ex: Exception => {
            println(ex.getStackTrace())
          }
        }

        for (i <- 0 to (numNodes - 1)) {
          var tempNodes: ArrayList[ActorRef] = new ArrayList[ActorRef]()
          var neighbours: ArrayList[ActorRef] = new ArrayList[ActorRef]()
          var currNode = nodes.get(i)
          tempNodes.addAll(nodes)

          identify2dNeighbours(i, n, neighbours, nodes)
          tempNodes.removeAll(neighbours)
          tempNodes.remove(currNode)
          neighbours.add(getRandomNode(tempNodes))

          currNode ! AddNeighbours(neighbours)
        }
      }

      case "full" => {
        for (i <- 0 to (numNodes - 1)) {
          var neighbours: ArrayList[ActorRef] = new ArrayList[ActorRef]()
          var currNode = nodes.get(i)
          neighbours.addAll(nodes)
          neighbours.remove(i)

          currNode ! AddNeighbours(neighbours)
        }
      }
    }

    def identify2dNeighbours(i: Int, n: Int, neighbours: ArrayList[ActorRef], nodes: ArrayList[ActorRef]) {
      if (isNodeTopLeftmost(i, n)) {
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i + n))
      } else if (isNodeTopRightmost(i, n)) {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i + n))
      } else if (isNodeBottomLefttmost(i, n)) {
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i - n))
      } else if (isNodeBottomRightmost(i, n)) {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i - n))
      } else if (isNodeInTopRow(i, n)) {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i + n))
      } else if (isNodeInBottomRow(i, n)) {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i - n))
      } else if (isNodeInLeftColumn(i, n)) {
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i - n))
        neighbours.add(nodes.get(i + n))
      } else if (isNodeInRightColumn(i, n)) {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i - n))
        neighbours.add(nodes.get(i + n))
      } else {
        neighbours.add(nodes.get(i - 1))
        neighbours.add(nodes.get(i + 1))
        neighbours.add(nodes.get(i - n))
        neighbours.add(nodes.get(i + n))
      }
    }

    def isNodeLeftmost(i: Int, n: Int): Boolean = {
      if (i == 0)
        return true
      return false
    }

    def isNodeRightmost(i: Int, n: Int): Boolean = {
      if (i == (n - 1))
        return true
      return false
    }

    def isNodeTopLeftmost(i: Int, n: Int): Boolean = {
      if (i == 0)
        return true
      return false
    }

    def isNodeBottomRightmost(i: Int, n: Int): Boolean = {
      if (i == (n * n - 1))
        return true
      return false
    }

    def isNodeTopRightmost(i: Int, n: Int): Boolean = {
      if (i == (n - 1))
        return true
      return false
    }

    def isNodeBottomLefttmost(i: Int, n: Int): Boolean = {
      if (i == (n * n - n))
        return true
      return false
    }

    def isNodeInTopRow(i: Int, n: Int): Boolean = {
      if (i / n == 0)
        return true
      return false
    }

    def isNodeInBottomRow(i: Int, n: Int): Boolean = {
      if (i / n == (n - 1))
        return true
      return false
    }

    def isNodeInLeftColumn(i: Int, n: Int): Boolean = {
      if (i % n == 0)
        return true
      return false
    }

    def isNodeInRightColumn(i: Int, n: Int): Boolean = {
      if (i % n == (n - 1))
        return true
      return false
    }
  }

  def getRandomNode(nodes: ArrayList[ActorRef]): ActorRef = {
    val numNodes = nodes.size()
    if (numNodes == 1) {
      return nodes.get(0)
    } else {
      val rand = new Random()
      var randNum = rand.nextInt(numNodes)
      return nodes.get(randNum)
    }
  }
}