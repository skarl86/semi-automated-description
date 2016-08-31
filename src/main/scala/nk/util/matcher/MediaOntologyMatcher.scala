package nk.util.matcher

import java.util.NoSuchElementException
import scala.util.matching.Regex

import nk.util.parser.OntologyParser._



/**
  * Created by NK on 2016. 8. 17..
  */
object MediaOntologyMatcher {

  def isObject(str:String) = {
    str.contains(HAS_AURAL) || str.contains(HAS_VISUAL) || str.contains(WHAT_OBJECT)
  }

  def isShot(spo:Triple) = {
    spo._3.contains("<http://www.personalmedia.org/soongsil-diquest#Shot>")
  }
  def isEvent(spo:Triple) = {
    spo._1.contains("Video") && spo._2.contains("type") && spo._3.contains("Event")
  }
  /**
    *
    * @param videoID
    * @param range
    * @return
    */
  def isEventVideo(videoID:String, range: Range): Boolean = {
    val reg = new Regex("([0-9]+)")
    try{
      val id = reg.findAllIn(videoID).matchData.next().group(1).toInt
      range.start <= id && range.end >= id
    }catch {
      case nse: NoSuchElementException => {
        false
      }
    }
  }

  /**
    *
    * @param videoID
    * @return
    */
  def eraseVideoIDString(videoID:String): String = {
    try{
      val reg = new Regex("_(.+)>")
      reg.findAllIn(videoID).matchData.next().group(1)
    } catch {
      case nse: NoSuchElementException => {
        videoID
      }
    }
  }

  /**
    *
    * @param objectName
    * @return
    */
  def eraseIndex(objectName:String): String = {
    try{
      val reg = new Regex("(\\w+[a-z]+)")
      reg.findAllIn(objectName).matchData.next().group(1)
    }catch {
      case nse: NoSuchElementException => {
        objectName
      }
    }
  }

  def eraseIndexWithoutPrefix(objectName:String): String ={
    try{
      val reg = new Regex("_(\\w+[a-z]+)")
      reg.findAllIn(objectName).matchData.next().group(1)
    }catch {
      case nse: NoSuchElementException => {
        objectName
      }
    }
  }


  /**
    *
    * @param str
    * @return
    */
  def eraseURI(str:String): String ={
    val reg = new Regex("([A-Z].+)>")
    if(reg == null)
    {
      str
    }else{
      reg.findAllIn(str).matchData.next().group(1)
    }
  }

  /**
    *
    */
  def eraseEventURI(str:String): String = {
    val reg = new Regex("#([a-zA-Z].+)>")
    if(reg == null)
    {
      str
    }else{
      reg.findAllIn(str).matchData.next().group(1)
    }
  }
  /**
    *
    * @param propertyURI
    * @return
    */
  def getProperty(propertyURI: String)= {
    val reg = new Regex("ontology\\/([a-zA-Z]+)")
    reg.findAllIn(propertyURI).matchData.next().group(1)
  }
}
