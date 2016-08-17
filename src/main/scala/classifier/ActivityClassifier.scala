package classifier

import nk.util.parser.OntologyParser._
import nk.util.matcher.MediaOntologyMatcher._

import org.apache.spark.rdd.RDD
import stringutil.Unicode

/**
  * Created by NK on 2016. 8. 17..
  */
object ActivityClassifier {
//  def classifyActivity(inputRDD:RDD[Triple]) = {
//    val shotAndActivityPairRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
//    val shotAndObjectPairRDD = inputRDD.filter{case (s, p ,o) => isObject(p)}.map{case (s, p ,o) => (s, o)}.groupByKey()
//
//    val rdd1 = shotAndActivityPairRDD
//      .join(shotAndObjectPairRDD)
//      .map{case (s, (a, o)) => (a, o)}.groupByKey()
//      .map{case (a, o) => (Unicode.decode(eraseURI(a)), o.flatten.map(obj => eraseIndex(eraseVideoIDString(obj))))}
//    rdd1
//  }

  def classifyActivity(inputRDD:RDD[Triple]) = {
    val shotAndActivityPairRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
    val objectAndClassPairRDD = inputRDD.filter{case (s, p, o) => p.contains(RDF_TYPE)}.map{case (o, typep, c) => (o, c)}
    val shotAndObjectPairRDD = inputRDD.filter{case (s, p ,o) => isObject(p)}.map{case (s, p ,o) => (o, s)}
    val rdd2 = shotAndObjectPairRDD.join(objectAndClassPairRDD).map{case (o, (s, c)) => (s, c)}.groupByKey()
    val rdd1 = shotAndActivityPairRDD
      .join(rdd2)
      .map{case (s, (a, c)) => (a, c)}.groupByKey()
      .map{case (a, c) => (Unicode.decode(eraseURI(a)), c.flatten.map(clss => Unicode.decode(eraseURI(clss))))}
    rdd1
  }

  def classifyActivity2(inputRDD:RDD[Triple], trainingSet:RDD[Triple]) = {
    val shotAndActivityPairRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
    val objectAndClassPairRDD = inputRDD.filter{case (s, p, o) => p.contains(RDF_TYPE)}.map{case (o, typep, c) => (o, c)}
    val shotAndObjectPairRDD = inputRDD.filter{case (s, p ,o) => isObject(p)}.map{case (s, p ,o) => (o, s)}
    val rdd2 = shotAndObjectPairRDD.join(objectAndClassPairRDD).map{case (o, (s, c)) => (s, c)}.groupByKey()
    val rdd1 = shotAndActivityPairRDD
      .join(rdd2)
      .map{case (s, (a, c)) => (a, c)}.groupByKey()
      .map{case (a, c) => (Unicode.decode(eraseURI(a)), c.flatten.map(clss => Unicode.decode(eraseURI(clss))))}
    rdd1
  }

  def getActivityContainedInShotRDD(inputRDD:RDD[Triple]) = {
    val rdd1 = inputRDD.filter{case (s, p, a) => p.contains(HAS_ACTIVITY)}
      .map{case (s, p, a) => (Unicode.decode(eraseURI(a)), s)}.groupByKey()
      .map{case (activity, shots) => (activity, shots.toList)}
    rdd1
  }


}
