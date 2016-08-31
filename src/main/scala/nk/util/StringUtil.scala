package nk.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by NK on 2016. 8. 25..
  */
object StringUtil {
  def getCurrentDate() = {
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyyMMddhhmmss")
    val currentMinuteAsString = minuteFormat.format(now)
    currentMinuteAsString
  }
}
