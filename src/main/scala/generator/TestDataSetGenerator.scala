package generator

import classifier.ActivityClassifier._

import nk.util.parser.OntologyParser._
import nk.util.matcher.MediaOntologyMatcher._

import org.apache.spark.rdd.RDD
import stringutil.Unicode

/**
  * Created by NK on 2016. 8. 17..
  */
object TestDataSetGenerator {
  def generateTrainingAndTestSet(inputRDD:RDD[Triple]) = {
    val shotAndActivityPairRDD = inputRDD.filter{case (s, p ,o) =>
      p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (Unicode.decode(s), Unicode.decode(a))}
    val shotAndTriplePairRDD = inputRDD.filter{case (s, p ,o) =>
      s.contains("Shot")}.filter{case (s, p, o) => !p.contains(RDF_LABEL) || !p.contains(RDF_TYPE)}
      .map{case (s, p, a) => (Unicode.decode(s), (Unicode.decode(s), Unicode.decode(p), Unicode.decode(a)))}

    val dataSetRDD = shotAndActivityPairRDD.join(shotAndTriplePairRDD)
      .map{case (s ,(a, triple)) => (a, triple)}.groupByKey()
      .map{case (a, triples) => (a, triples.toList.distinct)}
      .map{case (a, triples) => (eraseURI(a), (triples.length, triples))}


    val activityContainedInShotRDD = getActivityContainedInShotRDD(inputRDD).map{case (a, s) => (a, s.length)}
    activityContainedInShotRDD.join(dataSetRDD).sortBy(_._2._1,false)
  }

  def generateSmallDataSet(inputRDD:RDD[Triple]) = {

  }
}
