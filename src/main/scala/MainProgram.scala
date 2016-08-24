/**
  * Created by NK on 2016. 8. 17..
  */

import java.io.FileOutputStream
import java.nio.channels.Channels

import nk.util.SparkUtil._
import nk.util.parser.NKParser._

import classifier.ActivityClassifier._
import calculator.ObjectFrequencyCalculator._
import generator.GeneratorDescription._
import generator.TestDataSetGenerator._
import ontology.MediaOntologyManager

import scala.collection.JavaConverters._

import scala.xml.{XML, PrettyPrinter, Node}


object MainProgram {
  def main(args: Array[String]) {
//    val testList = Set("Shot1", "Shot2", "Shot3")
//    val inferredList = Set("Shot1","Shot2", "Shot3","Shot4")
//    println(testList--inferredList)

    /**
      *  0. Spark Setting
      */
    val sc = initSparkContext()
    val dataPath = "data/personalMedia.n3"
    val inputTripleRDD = sc.textFile(dataPath).mapPartitions(parseNTriple, true)

    val activityContainedShotArray = getActivityContainedInShotRDD(inputTripleRDD)
      .map{case (a, s) => (a, s.length, s)}.sortBy(_._2, false)
      .map{case(a, len, s) => (a, s.sorted)}.take(10)

    /**
      * 0-1. Test Set과 Training Set을 만든다.
      */
    val dataSetRDD = generateTrainingAndTestSet(inputTripleRDD)

    /**
      * 1. 미디어 파일에서 Activity별 Instance를 Clustering 해준다.
      */

    val classifiedActivity = classifyActivity(inputTripleRDD)

    /**
      * 2. Activity의 Instance를 기반으로 Object Frequency을 계산
      * 2-1. (Activity, (# of Total Object, Map(Object-> # of Object, ...))
      */

    val objectFrequencyPerActivity = calculateObejctFreqencyInActivity(classifiedActivity)

    /**
      * 2 -2. Activity가 포함된 shot 갯수를 구한다.
      * 예) (Activity1, # of Activity contained in Shot)
      */
    val activityContainedInShotRDD = getActivityContainedInShotRDD(inputTripleRDD).map{case (a, s) => (a, s.length)}

    /**
      * 예를 들어.
      * Object Frequency Per Activity 를 X라고 가정하고
      * Activity Contained In Shot을 Y라고 가정할 때,
      * X => (A1, ( ToC, Map(Object->Count) )
      * Y => (A1, Count)
      * 일때,
      * X join Y
      * ( A1, (Toc, Map(Object->Count), Count) )
      *
      * 2-3. RDD를 Activity contained In Shot 순으로 정렬한다.
      * (A1, # of Activity contained in Shot, (# of Total Object, Map(Object -> Count))
      * (Activity_자전거묘기하다_실외,79,(241,Map(tennis -> 1, flowerpot -> 1, building -> 1, slide -> 1,
      *                                       bus -> 1, road -> 1, motorcycle -> 1, helmet -> 1, step -> 2,
      *                                       rock -> 2, billboard -> 3, car -> 4, person -> 94, bicycle -> 105,
      *                                       ramp -> 8, tree -> 3, bench -> 2, apartment -> 2, clothes -> 1, can -> 1,
      *                                       wheel -> 1, bridge -> 1, flowerbed -> 1, parasol -> 1,
      *                                       carrier -> 1, hurdle -> 1)))
      */

    val objectFrequencyResultRDD = objectFrequencyPerActivity.join(activityContainedInShotRDD)
      .map{case (a, b) => (a, b._2, b._1)}.sortBy(_._2, false)

    /**
      * 3. Object Frequency를 기반으로 Description 모델을 생성.
      */
    val result = generateDescription(sc, objectFrequencyResultRDD.take(10), printResult = true)
    val newOwl = generateDescriptionToXML(result)

    /**
      * 3.1
      */
    val newOwlFileName = "data/owl/automated-personalMedia-object-5.owl"

//    XML.save(newOwlFileName,newOwl,"UTF-8", true, null)
//    save(newOwl, newOwlFileName)

    val inferredInstanceMap = scala.collection.mutable.Map[String, List[Any]]()
    result.foreach{case (activityName, rstMap) =>
        val modelActivityClassIRI = makeModelActivityName(activityName)
        inferredInstanceMap(modelActivityClassIRI) =
          MediaOntologyManager.reasoningOntologyUsingHermit(modelActivityClassIRI, newOwlFileName).asScala.toList
    }
    activityContainedShotArray.foreach{case(activityName, testInstances) =>
      println("======================================================================================")
        println(String.format("%s 의 정확도 : ",activityName))
        val inferredInstanceList = inferredInstanceMap(makeModelActivityName(activityName))
        println(String.format("Inferred Instance의 갯수 => %s \nTest Instance의 갯수 => %s",
          String.valueOf(inferredInstanceList.size), String.valueOf(testInstances.size)))
        println((testInstances.toSet--inferredInstanceList.toSet).size)
      println("======================================================================================")

    }
    println("======================================================================================")
    println(inferredInstanceMap)

  }
  /**
    *
    * @param node
    * @param fileName
    * @return
    */
  def save(node: Node, fileName: String) = {
    val Encoding = "UTF-8"
    val pp = new PrettyPrinter(80, 2)
    val fos = new FileOutputStream(fileName)
    val writer = Channels.newWriter(fos.getChannel(), Encoding)

    try {
      writer.write("<?xml version='1.0' encoding='" + Encoding + "'?>\n")
      writer.write(pp.format(node))
    } finally {
      writer.close()
    }

    fileName
  }
}
