package generator

import org.apache.commons.math.distribution.NormalDistributionImpl
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import nk.util.SparkUtil._

/**
  * Created by NK on 2016. 8. 17..
  */
object GeneratorDescription {

  val CLOSURE_AXIOM = "closure"
  val EXISTENTIAL_AXIOM = "existential"
  val UNIVERSAL_AXIOM = "universal"

  def generateDescription(sc:SparkContext, objectFrequencyResults:Array[(String, Int, (Int, scala.collection.mutable.ListMap[String, Int]))], writeFile:Boolean = false) = {
    val sqlContext = initSparkSQLContext(sc)

    objectFrequencyResults.foreach{ case(activity, numOfActivityContainedInShot,(numOfTotalObject, objectFrequencyMap)) =>
        println("=========================================================================================")
        println(String.format("Activity Name is [ %s ]", activity))
        println("=========================================================================================")
        val frequencyDF = createDataFrame(sc, sqlContext, objectFrequencyMap)
        val (result, rawDF) = calculateNormalDistribution(sqlContext, frequencyDF)

        if(writeFile)
        {
          rawDF.coalesce(1).write.format(DATAFRAME_OUTPUT_FORMAT).save(generatePath(activity))
        }

        for( (axiom, descs) <- result.collect() ){
          println(String.format("\nAxiom : [ %s ]\nCandidate Object : [ %s ]", axiom, descs.mkString(", ")))
        }
    }
  }

  def createDataFrame(sc:SparkContext, sqlContext:SQLContext, frequency:scala.collection.mutable.ListMap[String, Int]) = {
    val schema = StructType(
      Seq(StructField("object_name", StringType, true),
        StructField("count", IntegerType, true))
    )

    val rowList = frequency.map{case (activity, count) => Row(activity, count)}
    sqlContext.createDataFrame(sc.parallelize(rowList.toList),schema)
  }

  def calculateNormalDistribution(sqlContext:SQLContext, frequencyDF:DataFrame, alpha:Double = 0.2) = {
    // Event에 Object 출현 빈도값들의 표준편차 값(Standard Deviation)을 구한다.
    val stdd = frequencyDF.agg(stddev_pop("count")).map(row => row.getDouble(0)).collect()(0)

    // Event에 Object 출현 빈도값들의 평균 값을 구한다.
    val mean = frequencyDF.agg(avg("count")).map(row => row.getDouble(0)).collect()(0)

    println(String.format("Standard deviation = %s\nMean = %s", stdd.toString, mean.toString))

    // 정규편차(Normal Distribution)값을 구하기 위한 객체를 생성.
    val normDistCalculator = new NormalDistributionImpl(mean, stdd)
    // Event에 Object 출현 빈도 값의 누적 편차 값을 구한다.
    val rows = frequencyDF
      .map(row => Row(row(0).toString, normDistCalculator.cumulativeProbability(row.getInt(1).toDouble)))

    val schema = StructType(
      Seq(StructField("object_name", StringType),
        StructField("cdf_value", DoubleType))
    )

    // 표준 편차값과 평균값을 가지고 기준값을 정한다.
    // Cumulative Distribution Function (누적 분포 함수) 값.
    val threshold = normDistCalculator.cumulativeProbability(stdd + mean)
    val lambda = 0.2

    println("Threshold value = " + threshold)
    println("Alpha = " + alpha)
//    sqlContext.createDataFrame(rows, schema).show()
    val resultsDF = sqlContext.createDataFrame(rows, schema)
      .map(row => (row.getString(0), row.getDouble(1)))
      .map{case (objName, cdf) =>
        if(threshold <= cdf){
          (CLOSURE_AXIOM, objName)
        }else if((threshold - alpha) < cdf && cdf < threshold){
          (EXISTENTIAL_AXIOM, objName)
        }else{
          (UNIVERSAL_AXIOM, objName)
        }
      }
      .groupByKey()
      .map{case (axiom, objList) => (axiom, objList.toList)}

    (resultsDF, sqlContext.createDataFrame(rows, schema))
  }
}
