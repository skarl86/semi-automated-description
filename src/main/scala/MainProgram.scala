/**
  * Created by NK on 2016. 8. 17..
  */

import java.io.FileOutputStream
import java.nio.channels.Channels

import nk.util.{StringUtil, DebugUtil}
import nk.util.SparkUtil._
import nk.util.parser.NKParser._

import experiment.MediaOntologyExperiment

import nk.util.matcher.MediaOntologyMatcher._

import classifier.ActivityClassifier._
import calculator.ObjectFrequencyCalculator._
import generator.GeneratorDescription._
import generator.TestDataSetGenerator._
import ontology.MediaOntologyManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.xml.{XML, PrettyPrinter, Node}
import scala.collection.JavaConverters._

object MainProgram {

  val NUM_OF_ACTIVITY_FOR_EXPERIMENT = 10
  val NUM_OF_OBJECT_FOR_MODEL = 10
  val ALPHA_OF_STANDARD_DISTRIBUTION = 0.2

  def main(args: Array[String]) {
    val alpha = List(0.0, 0.05, 0.1, 0.15, 0.20, 0.25)
    val nObject = NUM_OF_OBJECT_FOR_MODEL

    val sc = initSparkContext()
    val totalAccuracyList = scala.collection.mutable.ListBuffer[List[Double]]()

    alpha.foreach{ap =>
      totalAccuracyList += run(sc, nObject, ap)
    }

    totalAccuracyList.foreach{accList =>
      println(accList.sum / accList.size)
    }
  }

  def run(sc:SparkContext, nOfObject:Int, alpha:Double) = {
    /**
      *  0. Spark Setting
      */
    DebugUtil.printCurrentPhase("Spark Setting")

    val dataPath = "data/personalMedia.n3"
    val inputTripleRDD = sc.textFile(dataPath).mapPartitions(parseNTriple, true)

    //    val activityContainedShotArray = getActivityContainedInShotRDD(inputTripleRDD)
    //      .map{case (a, s) => (a, s.length, s)}.sortBy(_._2, false)
    //      .map{case(a, len, s) => (a, s.sorted)}.take(NUM_OF_ACTIVITY_FOR_EXPERIMENT)

    val activityContainedShotArray = getEventContainedInShotRDD(inputTripleRDD)
      .map{case (a, s) => (a, s.length, s)}.sortBy(_._2, false)
      .map{case(a, len, s) => (a, s.sorted)}.take(NUM_OF_ACTIVITY_FOR_EXPERIMENT)

    val testInstancesMap = scala.collection.mutable.Map[String, List[String]]()

    activityContainedShotArray.foreach{case (act, testInstance) =>
      testInstancesMap(act) = testInstance
    }

    /**
      * 0-1. Test Set과 Training Set을 만든다.
      */
    DebugUtil.printCurrentPhase("Create Test Set Training Set")
    val dataSetRDD = generateTrainingAndTestSet(inputTripleRDD)

    /**
      * 1. 미디어 파일에서 Activity별 Instance를 Clustering 해준다.
      */
    DebugUtil.printCurrentPhase("Clustering")
    //    val classifiedActivity = classifyActivity(inputTripleRDD)
    val classifiedActivity = classifyActivity2(inputTripleRDD)

    /**
      * 2. Activity의 Instance를 기반으로 Object Frequency을 계산
      * 2-1. (Activity, (# of Total Object, Map(Object-> # of Object, ...))
      */
    DebugUtil.printCurrentPhase("Calculating Object Frequency")
    val objectFrequencyPerActivity = calculateObejctFreqencyInActivity(classifiedActivity)

    /**
      * 2 -2. Activity가 포함된 shot 갯수를 구한다.
      * 예) (Activity1, # of Activity contained in Shot)
      */
    //    val activityContainedInShotRDD = getActivityContainedInShotRDD(inputTripleRDD)
    val activityContainedInShotRDD = getEventContainedInShotRDD(inputTripleRDD)
    val activityAndNumOfShotRDD = activityContainedInShotRDD.map{case (a, s) => (a, s.length)}

    /**
      * 예를 들어.
      * Object Frequency Per Activity 를 X라고 가정하고
      * Activity Contained In Shot을 Y라고 가정할 때,
      * X => (A1, ( ToC, Map(Object->Count) )
      * Y => (A1, Count)
      * 일때,
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

    val objectFrequencyResultRDD = objectFrequencyPerActivity.join(activityAndNumOfShotRDD)
      .map{case (a, b) => (a, b._2, b._1)}.sortBy(_._2, false)

    /**
      * 3. Object Frequency를 기반으로 Description 모델을 생성.
      */
    DebugUtil.printCurrentPhase(String.format("Generating Automated Description : # of Activity = %s, # of Top Object = %s",
      String.valueOf(NUM_OF_ACTIVITY_FOR_EXPERIMENT), String.valueOf(nOfObject)))
    val result = generateDescription(sc, objectFrequencyResultRDD.take(NUM_OF_ACTIVITY_FOR_EXPERIMENT),
      alpha = alpha, numberOfObject = nOfObject, printResult = true, writeFile = false)
    //    DebugUtil.printCurrentPhase("Generating OWL using Automated Description")
    val newOwl = generateDescriptionToXML(result)

    /**
      * 3.1
      */
    val newOwlFileName = "data/owl/new-automated-personalMedia-10-based-event.owl"

    DebugUtil.printCurrentPhase("Saving Automated OWL File  ")
    XML.save(newOwlFileName,newOwl,"UTF-8", true, null)
    //    save(newOwl, newOwlFileName)
    //
    /**
      * 4.
      */
    DebugUtil.printCurrentPhase("Reasoning Ontology Using Automated OWL ")
    val experimentResult = scala.collection.mutable.ListBuffer[Row]()
    val inferredInstanceMap = scala.collection.mutable.Map[String, List[Any]]()

    result.foreach{case (activityName, rstMap) =>
      val modelActivityClassIRI = makeModelActivityName(activityName)
      inferredInstanceMap(modelActivityClassIRI) =
        MediaOntologyManager.reasoningOntologyUsingHermit(modelActivityClassIRI, newOwlFileName).asScala.toList
      //          MediaOntologyManager.reasoningOnotologyUsingPellet(modelActivityClassIRI, newOwlFileName).asScala.toList
    }

    /**
      * 5. Experiment OWL
      */
    DebugUtil.printCurrentPhase("Experiment OWL")
    MediaOntologyExperiment.run(testInstancesMap, inferredInstanceMap)

    val nFoldTableRows = scala.collection.mutable.ListBuffer[Row]()
    val accuracyList = scala.collection.mutable.ListBuffer[Double]()

    inferredInstanceMap.foreach{case (activityName1, inferedInstaces) =>
      DebugUtil.printProgressState("======================================================================================")
      DebugUtil.printProgressState(String.format("%s 의 정확도 : ",activityName1))
      val nFoldValueList = scala.collection.mutable.ListBuffer[Any]()
      nFoldValueList += activityName1

      testInstancesMap.foreach{case (activtiyName2, testInstances) =>
        val resultList = inferedInstaces.intersect(testInstances)
        DebugUtil.printProgressState(String.format("[%s]와 [%s] 의 Matched Count : %s",
          activityName1.replace("http://www.co-ode.org/ontologies/ont.owl#", ""),
          activtiyName2.replace("http://www.co-ode.org/ontologies/ont.owl#", ""),
          String.valueOf(resultList.size)))
        nFoldValueList += resultList.size
      }

      /**
        * 별도에 정확도 구하기.
        */
      val actName = activityName1.replace("http://www.co-ode.org/ontologies/ont.owl#", "").split("_")(1)
      val testList = testInstancesMap(actName)
      val infList = inferredInstanceMap(activityName1)
      val acurracy = infList.intersect(testList).size
      accuracyList += acurracy.toDouble / infList.size.toDouble


      nFoldValueList += inferedInstaces.size
      nFoldTableRows += Row.fromSeq(nFoldValueList)

      val testInstancesList = testInstancesMap(activityName1.replace("http://www.co-ode.org/ontologies/ont.owl#", "").split("_")(1))
      val matchedCount = testInstancesList.intersect(inferedInstaces).size
      val accuracy = matchedCount.toDouble/testInstancesList.size.toDouble

      //      sc.parallelize(testInstances).coalesce(1).saveAsTextFile("output/test_data_" + activityName)
      //      sc.parallelize(inferredInstanceList).coalesce(1).saveAsTextFile("output/inferred_data_" + activityName)

      experimentResult += Row(
        activityName1,
        testInstancesList.size,
        inferedInstaces.size,
        matchedCount,
        accuracy
      )

      DebugUtil.printProgressState(String.format("Inferred Instance의 갯수 => %s", String.valueOf(inferedInstaces.size)))
      DebugUtil.printProgressState(String.format("Test Instance의 갯수 => %s", String.valueOf(testInstancesList.size)))
      //      DebugUtil.printProgressState((testInstances.toSet--inferedInstaces.toSet).size)

      DebugUtil.printProgressState("======================================================================================")
    }



    /*
    activityContainedShotArray.foreach{case(activityName, testInstances) =>
      DebugUtil.printProgressState("======================================================================================")
      DebugUtil.printProgressState(String.format("%s 의 정확도 : ",activityName))
      val nFoldValueList = scala.collection.mutable.ListBuffer[Any]()
      nFoldValueList += activityName

      inferredInstanceMap.foreach{case (activityName2, inferredInstances) =>
          val resultList = testInstances.intersect(inferredInstances)
          DebugUtil.printProgressState(String.format("[%s]와 [%s] 의 Matched Count : %s",
            activityName, activityName2.replace("http://www.co-ode.org/ontologies/ont.owl#", ""), String.valueOf(resultList.size)))
          nFoldValueList += resultList.size
      }
      nFoldTableRows += Row.fromSeq(nFoldValueList)

      val inferredInstanceList = inferredInstanceMap(makeModelActivityName(activityName))

      val matchedCount = testInstances.toSet.size - (testInstances.toSet--inferredInstanceList.toSet).size
      val accuracy = matchedCount.toDouble/testInstances.size.toDouble

//      sc.parallelize(testInstances).coalesce(1).saveAsTextFile("output/test_data_" + activityName)
//      sc.parallelize(inferredInstanceList).coalesce(1).saveAsTextFile("output/inferred_data_" + activityName)

      experimentResult += Row(
        activityName,
        testInstances.size,
        inferredInstanceList.size,
        matchedCount,
        accuracy
      )

      DebugUtil.printProgressState(String.format("Inferred Instance의 갯수 => %s", String.valueOf(inferredInstanceList.size)))
      DebugUtil.printProgressState(String.format("Test Instance의 갯수 => %s", String.valueOf(testInstances.size)))
      DebugUtil.printProgressState((testInstances.toSet--inferredInstanceList.toSet).size)

      DebugUtil.printProgressState("======================================================================================")
    }*/

    /**
      * 4-1. n-fold data generator
      */

    DebugUtil.printCurrentPhase("N-Fold Data Generating")
    val nFoldTableHeaderList = scala.collection.mutable.ListBuffer[StructField]()
    nFoldTableHeaderList += StructField("activity_name", StringType)
    //    nFoldTableHeaderList ++= inferredInstanceMap.keys.toList.map{case act => StructField(act, IntegerType)}
    nFoldTableHeaderList ++= testInstancesMap.map{case (act, testInst) => StructField(act, IntegerType)}.toList
    nFoldTableHeaderList += StructField("inferred_count", IntegerType)
    val nFoldTableSchema = StructType(nFoldTableHeaderList.toSeq)

    val sqlContext = initSparkSQLContext(sc)
    sqlContext.createDataFrame(sc.parallelize(nFoldTableRows) ,nFoldTableSchema)
      .coalesce(1).write.format(DATAFRAME_OUTPUT_FORMAT).option("header", "true")
      .save(generatePath(String.format("n-fold_result_alpha[%s]_dynamic[YES]_object[%s]"
        ,String.valueOf(alpha)
        ,String.valueOf(nOfObject))))

    val schema = StructType(
      Seq(StructField("activity_name", StringType),
        StructField("test_count", IntegerType),
        StructField("inferred_count", IntegerType),
        StructField("matched_count", IntegerType),
        StructField("accuracy", DoubleType)
      )
    )
    sqlContext.createDataFrame(sc.parallelize(experimentResult), schema)
      .coalesce(1).write.format(DATAFRAME_OUTPUT_FORMAT).option("header", "true").save(generatePath("experiment_result_using_pellet"))
    DebugUtil.printProgressState("======================================================================================")
    //    DebugUtil.printProgressState(inferredInstanceMap)

    accuracyList.toList
  }
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
