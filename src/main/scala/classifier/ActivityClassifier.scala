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
    val activityInstanceRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
    val typeInstanceRDD = inputRDD.filter{case (s, p, o) => p.contains(RDF_TYPE)}.map{case (o, typep, c) => (o, c)}
    val objectInstance = inputRDD.filter{case (s, p ,o) => isObject(p)}.map{case (s, p ,o) => (o, s)}
    val rdd1 = objectInstance.join(typeInstanceRDD).map{case (o, (s, c)) => (s, c)}.groupByKey()
    val rdd2 = activityInstanceRDD
      .join(rdd1)
      .map{case (s, (a, c)) => (a, c)}.groupByKey()
      .map{case (a, c) => (Unicode.decode(eraseURI(a)), c.flatten.map(clss => Unicode.decode(eraseURI(clss))))}
    rdd2
  }

  /**
    * Activity 기준으로 하는 것이 아닌.
    * Event를 기준으로 Classify 하도록 하는 Function.
    * @param inputRDD
    */
  def classifyActivity2(inputRDD:RDD[Triple]) = {
    val eventTypeRDD = inputRDD.filter{case (s, p, o) => s.contains("Video") && p.contains("type") && o.contains("Event")}.map{case (v, t, e) => (eraseURI(v) , e)}

    val activityInstanceRDD = inputRDD.filter{case (s, p ,o) => p.contains(HAS_ACTIVITY)}.map{case (s, p, a) => (s, a)}
    val typeInstanceRDD = inputRDD.filter{case (s, p, o) => p.contains(RDF_TYPE)}.map{case (o, typep, c) => (o, c)}
    val objectInstance = inputRDD.filter{case (s, p ,o) => isObject(p)}.map{case (s, p ,o) => (o, s)}
    val rdd1 = objectInstance.join(typeInstanceRDD).map{case (o, (s, c)) => (s, c)}.groupByKey()
    val rdd2 = activityInstanceRDD
      .join(rdd1)
      .map{case (s, (a, c)) => (eraseURI(s).split("_")(0), (a, c))}
      .join(eventTypeRDD)
      .map{case (v, ((a, c), e)) => (e, c)}.groupByKey()
      .map{case (e, c) => (eraseEventURI(e), c.flatten.map(clss => Unicode.decode(eraseURI(clss))))}
    rdd2
  }

  def getActivityContainedInShotRDD(inputRDD:RDD[Triple]) = {
    val rdd1 = inputRDD.filter{case (s, p, a) => p.contains(HAS_ACTIVITY)}
      .map{case (s, p, a) => (Unicode.decode(eraseURI(a)), s)}.groupByKey()
      .map{case (activity, shots) => (activity, shots.toList)}
    rdd1
  }

  def getEventContainedInShotRDD(inputRDD:RDD[Triple]) = {
    val eventTypeRDD = inputRDD.filter{isEvent}.map{case (v, t, e) => (eraseURI(v) , e)}
    val shotsRDD = inputRDD.filter(isShot).map{case (s, t, shot) => (eraseURI(s).split("_")(0), s)}

    eventTypeRDD.join(shotsRDD)
      .map{case (v, (e, s)) => (eraseEventURI(e), s)}.groupByKey()
      .map{case (event, shots) => (event, shots.toList)}
  }


}
