package calculator

import org.apache.spark.rdd.RDD

import nk.util.parser.OntologyParser._

import scala.collection.mutable

import classifier.ActivityClassifier._

/**
  * Created by NK on 2016. 8. 17..
  */
object ObjectFrequencyCalculator {
  def calculateObejctFreqencyInActivity(classifiedActivityRDD:RDD[(String, Iterable[String])]) ={
    val resultRDD = classifiedActivityRDD
      .map{case (a, o) =>
      {
        val map = scala.collection.mutable.Map[String, Int]()
        var count = 0
        for(obj <- o){
          if(map.contains(obj)){map(obj) += 1}
          else{map(obj) = 1}
          count += 1
        }
        (a, (count, mutable.ListMap(map.toSeq.sortWith(_._2 > _._2):_*)))
      }
      }
    resultRDD
  }
}
