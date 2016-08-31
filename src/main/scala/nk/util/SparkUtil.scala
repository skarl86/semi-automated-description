package nk.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by NK on 2016. 8. 17..
  */
object SparkUtil {

  val DATAFRAME_OUTPUT_FORMAT = "com.databricks.spark.csv"

  /**
    * * RDD 를 파일로 저장할 때
    * 저장 경로를 중복되지 않도록
    * Unique한 주소를 생성 해주는 함수
    *
    * @param path 사용자가 직접 입력한 경로.
    * @return Unique 한 path
    */
  def generatePath(path:String = ""): String ={
    "output/" + List(StringUtil.getCurrentDate(), path).mkString("-")
  }

  /**
    *
    * @param level
    */
  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

  /**
    *
    * @return
    */
  def initSparkContext(): SparkContext = {
    setLogLevel(Level.WARN)
    val conf = new SparkConf().setAppName("Auto Rule").setMaster("local[*]")
    new SparkContext(conf)
  }

  /**
    *
    * @param sc
    * @return
    */
  def initSparkSQLContext(sc:SparkContext): SQLContext = { new SQLContext(sc) }
}
