package nk.util

/**
  * Created by NK on 2016. 8. 25..
  */
object DebugUtil {
  var _numOfPhase = 0
  val _traceString = "(%s) PROGRESS_LOG [PHASE %s] - %s ..."
  val _progressStateString = "\t%s"

  def printCurrentPhase(stageDescription:String): Unit = {
      printCurrentPhase(_numOfPhase, stageDescription)
      _numOfPhase += 1
  }

  def printCurrentPhase(phase:Int, stageDescription:String): Unit = {
    println(String.format(_traceString, String.valueOf(System.currentTimeMillis()), String.valueOf(phase), stageDescription))
    _numOfPhase = phase
  }

  def printProgressState(log:Any) = {
    log match {
      case s:String => println(String.format(_progressStateString, s))
      case i:Int => println(String.format(_progressStateString, String.valueOf(i)))
    }
  }
}
