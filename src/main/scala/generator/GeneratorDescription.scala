package generator

import java.io.FileOutputStream
import java.nio.channels.Channels

import org.apache.commons.math.distribution.NormalDistributionImpl
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import scala.xml.{PrettyPrinter, Node, Elem, XML}
import scala.xml.transform._


import nk.util.SparkUtil._

import scala.xml.transform.RewriteRule

/**
  * Created by NK on 2016. 8. 17..
  */
object GeneratorDescription {

  val CLOSURE_AXIOM = "closure"
  val EXISTENTIAL_AXIOM = "existential"
  val UNIVERSAL_AXIOM = "universal"

  /**
    *
    * @param label
    * @param newChild
    */
  class AddChildrenTo(label: String, newChild: Node) extends RewriteRule {
    override def transform(n: Node) = n match {
      case n @ Elem(_, `label`, _, _, _*) => addChild(n, newChild)
      case other => other
    }
  }

  /**
    *
    * @param n
    * @param newChild
    * @return
    */
  def addChild(n: Node, newChild: Node) = n match {
    case Elem(prefix, label, attribs, scope, child @ _*) =>
      Elem(prefix, label, attribs, scope, child ++ newChild : _*)
    case _ => error("Can only add children to elements!")
  }

  /**
    *
    * @param sc
    * @param objectFrequencyResults
    * @param writeFile
    * @param printResult
    * @return
    */
  def generateDescription(sc:SparkContext, objectFrequencyResults:Array[(String, Int, (Int, scala.collection.mutable.ListMap[String, Int]))], writeFile:Boolean = false, printResult:Boolean = false) = {
    val sqlContext = initSparkSQLContext(sc)
    val resultDescriptionMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, List[String]]]()

    objectFrequencyResults.foreach{ case(activity, numOfActivityContainedInShot,(numOfTotalObject, objectFrequencyMap)) =>
        println("=========================================================================================")
        println(String.format("Activity Name is [ %s ]", activity))
        println("=========================================================================================")
        val frequencyDF = createDataFrame(sc, sqlContext, objectFrequencyMap)
        val (result, rawDF) = calculateNormalDistribution(sqlContext, frequencyDF)
        val resultMap = scala.collection.mutable.Map[String, List[String]]()

        if(writeFile)
        {
          rawDF.coalesce(1).write.format(DATAFRAME_OUTPUT_FORMAT).save(generatePath(activity))
        }

        for( (axiom, descs) <- result.collect() ){
          resultMap(axiom) = descs
          if(printResult)
          {
            println(String.format("\nAxiom : [ %s ]\nCandidate Object : [ %s ]", axiom, descs.mkString(", ")))
          }
        }
      resultDescriptionMap(activity) = resultMap
    }
    resultDescriptionMap
  }

  def generateDescriptionToXML(result: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, List[String]]]) = {
    var newOwl:Node = XML.load("data/owl/personalMedia_mod.owl")
    newOwl = generateDeclaration(newOwl, result)
    newOwl = generateOWLDescription(newOwl, result)
    newOwl
  }

  /**
    *
    * @param originalOwl
    * @param result
    * @return
    */
  def generateDeclaration(originalOwl:Node, result: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, List[String]]]) = {
    var newOwl:Node = originalOwl

    result.foreach{case (activityName, reslt) =>
      val classIRI = makeModelActivityName(activityName)
      val node =
        <Declaration>
          <Class IRI={classIRI}/>
      </Declaration>
      newOwl = new RuleTransformer(new AddChildrenTo("Ontology", node)).transform(newOwl).head
    }

    newOwl
  }

  def generateOWLDescription(owl:Node, result: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, List[String]]]) = {
    var equivalentClassElm:Node = null
    var subClassOfElm:Node = null
    var equivalentClassesList = scala.collection.mutable.ListBuffer[Node]()
    var subClassOfList = scala.collection.mutable.ListBuffer[Node]()

    var newOwl = owl
    result.foreach{case (activityName, reslt) =>

      val classIRI = makeModelActivityName(activityName)
      if(reslt.contains(CLOSURE_AXIOM)){

      }
      equivalentClassElm =
        <EquivalentClasses>
          <Class IRI={classIRI}/>
          <ObjectIntersectionOf></ObjectIntersectionOf>
        </EquivalentClasses>

      if(reslt.contains(CLOSURE_AXIOM)){
        val closureList = extractValidClass(reslt(CLOSURE_AXIOM))
        if(closureList.nonEmpty){
          closureList.foreach{case className =>
            if(isValidClass(className)){
              val propertyName = getPropertyName(className)
              val newClassName = getNewClassName(className)
              equivalentClassElm = new RuleTransformer(
                new AddChildrenTo("ObjectIntersectionOf", makeSomeValueFrom(propertyName, newClassName)))
                .transform(equivalentClassElm).head
              subClassOfElm =
                <SubClassOf>
                  <Class IRI={classIRI}/>
                  <ObjectIntersectionOf></ObjectIntersectionOf>
                </SubClassOf>
              subClassOfElm = new RuleTransformer(
                new AddChildrenTo("ObjectIntersectionOf", makeAllValuesFrom(propertyName, newClassName)))
                .transform(subClassOfElm).head
            }
          }
        }
      }
      if(reslt.contains(UNIVERSAL_AXIOM)){
        val universalList = reslt(UNIVERSAL_AXIOM)
        if(universalList.nonEmpty){
          universalList.foreach{case className =>
            if(isValidClass(className)){
              val propertyName = getPropertyName(className)
              val newClassName = getNewClassName(className)
              equivalentClassElm = new RuleTransformer(
                new AddChildrenTo("ObjectIntersectionOf", makeAllValuesFrom(propertyName, newClassName)))
                .transform(equivalentClassElm).head
            }
          }
        }
      }

      if(reslt.contains(EXISTENTIAL_AXIOM)){
        val existentialList = reslt(EXISTENTIAL_AXIOM)
        if(existentialList.nonEmpty){
          existentialList.foreach{case className =>
            if(isValidClass(className)){
              val propertyName = getPropertyName(className)
              val newClassName = getNewClassName(className)
              equivalentClassElm = new RuleTransformer(
                new AddChildrenTo("ObjectIntersectionOf", makeSomeValueFrom(propertyName, newClassName)))
                .transform(equivalentClassElm).head
            }
          }
        }
      }


      equivalentClassesList += equivalentClassElm
      if(subClassOfElm != null){
        subClassOfList += subClassOfElm
      }


    }

    equivalentClassesList.foreach{case node:Elem =>
      newOwl = new RuleTransformer(new AddChildrenTo("Ontology", node)).transform(newOwl).head
    }
    subClassOfList.foreach{case node:Elem =>
      newOwl = new RuleTransformer(new AddChildrenTo("Ontology", node)).transform(newOwl).head
    }
    newOwl

  }

  /**
    *
    * @param orginalActivityName
    * @return
    */
  def makeModelActivityName(orginalActivityName:String) = {
    String.format("http://www.co-ode.org/ontologies/ont.owl#M_%s",orginalActivityName)
  }

  /**
    *
    * @param property
    * @param className
    * @return
    */
  def makeSomeValueFrom(property:String, className:String) = {
    val propertyIRI = "p0:"+property
    val classIRI = "p0:"+className

    val someValue = <ObjectSomeValuesFrom>
      <ObjectProperty abbreviatedIRI={propertyIRI}/>
      <Class abbreviatedIRI={classIRI}/>
    </ObjectSomeValuesFrom>
    someValue
  }

  /**
    *
    * @param property
    * @param className
    * @return
    */
  def makeAllValuesFrom(property:String, className:String) = {
    val propertyIRI = "p0:"+property
    val classIRI = "p0:"+className

    val allValue = <ObjectAllValuesFrom>
      <ObjectProperty abbreviatedIRI={propertyIRI}/>
      <Class abbreviatedIRI={classIRI}/>
    </ObjectAllValuesFrom>
    allValue
  }

  def isValidClass(className:String) = {
    val exceptionList = List("WhatObject", "Position")
    !exceptionList.contains(className)
  }

  def extractValidClass(classList:List[String]) = {
    val exceptionList = Set("WhatObject", "Position")
    classList.filterNot(exceptionList)
  }
  def getNewClassName(className:String) = {
    if(className.equals("Who")){
      "Person"
    }else {
      className
    }
  }
  def getPropertyName(className:String) = {
    if(className.equals("Sound")){
      "hasAural"
    }else {
      "hasVisual"
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

  def calculateNormalDistribution(sqlContext:SQLContext, frequencyDF:DataFrame, numberOfObject:Int = 5, alpha:Double = 0.2) = {
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
      .orderBy(desc("cdf_value"))
      .limit(numberOfObject)
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
