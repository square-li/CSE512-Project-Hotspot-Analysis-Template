package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val rectangleInput: List[Double] = queryRectangle.split("\\,").map(_.trim.toDouble).toList
    val pointInput: List[Double] = pointString.split("\\,").map(_.trim.toDouble).toList
    val x1 = Math.min(rectangleInput(0), rectangleInput(2))
    val y1 = Math.max(rectangleInput(1), rectangleInput(3))
    val x2 = Math.max(rectangleInput(0), rectangleInput(2))
    val y2 = Math.min(rectangleInput(1), rectangleInput(3))
    val px = pointInput(0)
    val py = pointInput(1)
    if (x1 <= px && y1 >= py && x2 >= px && y2 <= py){
      return true
    }
    return false
  }

  // YOU NEED TO CHANGE THIS PART

}
