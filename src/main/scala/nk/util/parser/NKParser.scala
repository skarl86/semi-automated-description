package nk.util.parser

import java.util.NoSuchElementException

import scala.util.matching.Regex

/**
  * Created by NK on 2016. 8. 17..
  */
object NKParser {
  /**
    *
    * @param lines
    * @return
    */
  def parseNTriple(lines: Iterator[String]) = {
    val TripleParser = new Regex("(<[^\\s]*>)|(_:[^\\s]*)|(\".*\")")
    for (line <- lines) yield {
      try{
        val tokens = TripleParser.findAllIn(line)
        val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
        (s, p, o)

      }catch {
        case nse: NoSuchElementException => {
          ("ERROR", "ERROR", "ERROR")
        }
      }
    }

  }
}
