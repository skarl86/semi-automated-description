package nk.util.parser

import org.apache.spark.rdd.RDD

/**
  * Created by NK on 2016. 8. 17..
  */
object OntologyParser {

  type Triple = (String, String, String)
  type Tuple = (String, String)

  val RDF_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>"
  val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  val WHAT_BEHAVIOR = "hasWhatBehavior"
  val HAS_VISUAL = "hasVisual"
  val HAS_AURAL = "hasAural"
  val WHAT_OBJECT = "hasWhatObject"
  val HAS_ACTIVITY = "hasActivity"

  val CLOSURE_AXIOM = "closure"
  val EXISTENTIAL_AXIOM = "existential"
  val UNIVERSAL_AXIOM = "universal"

  /**
    *
    * @param triple
    * @return
    */
  def getTypeTriple(triple: RDD[Triple]): RDD[Triple] = { getPredicate(triple, RDF_TYPE) }

  /**
    *
    * @param triple
    * @return
    */
  def getLabelTriple(triple: RDD[Triple]): RDD[Triple] ={ getPredicate(triple, RDF_LABEL) }

  /**
    *
    * @param triple
    * @param predicate
    * @return
    */
  def getPredicate(triple: RDD[Triple], predicate:String): RDD[Triple] = {
    triple
      .filter{case (s, p, o) => p.contains(predicate)}
  }

}
