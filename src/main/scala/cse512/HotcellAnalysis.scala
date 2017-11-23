package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  var spaceTimeCube : Array[Array[Array[Int]]]

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX", (pickupPoint: String) => ((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY", (pickupPoint: String) => ((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ", (pickupTime: String) => ((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50 / HotcellUtils.coordinateStep
  val maxX = -73.70 / HotcellUtils.coordinateStep
  val minY = 40.50 / HotcellUtils.coordinateStep
  val maxY = 40.90 / HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val xDiff = (maxX - minX + 1).toInt
  val yDiff = (maxY - minY + 1).toInt
  val zDiff = maxZ - minZ + 1
  val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)
  println(numCells)

  val nextDfRDD = pickupInfo.rdd.map {
    r => (r.getInt(0) + "," + r.getInt(1) + "," + r.getInt(2), 1)
  }.reduceByKey(_ + _).sortBy(_._2)
  nextDfRDD.collect().foreach(println)
  spaceTimeCube = Array.ofDim[Int](xDiff, yDiff, zDiff)
  for ((coord, count) <- nextDfRDD.collect()) {
    val coordInp: List[Int] = coord.split(',').map(_.trim.toInt).toList
    println(coord)
    spaceTimeCube(coordInp.head - minX.toInt)(coordInp(1) - minY.toInt)(coordInp(2) - minZ.toInt) += count.toInt
  }
  val meanCube = Array.ofDim[Double](xDiff, yDiff, zDiff)
  val sdCube = Array.ofDim[Double](xDiff, yDiff, zDiff)
  for (i <- 0 to xDiff) {
    for (j <- 0 to yDiff) {
      for (k <- 0 to zDiff) {
        var cells = get26Cells(i, j, k, xDiff, yDiff, zDiff)
        meanCube(i)(j)(k) = computeMean(cells)
        sdCube(i)(j)(k) = computeSD(cells, meanCube(i)(j)(k))
      }
    }
  }

  return pickupInfo // YOU NEED TO CHANGE THIS PART
}

  def get26Cells(x: Int, y: Int, z: Int, xDiff: Int, yDiff: Int, zDiff: Int): Array[Int] = {
    var diffs = Array(-1, 0, 1)
    var outList: Array[Int] = new Array[Int](26)
    val ind = 0
    for(i <- diffs){
      for(j <- diffs){
        for(k <- diffs){
          if(i!=0 && j!=0 && k!=0) {
            if (isInBounds(x + i, y + j, z + k, xDiff, yDiff, zDiff)) {
              outList(ind) = spaceTimeCube(x + i)(y + j)(z + k)
              ind ++
            } else {
              outList(ind) = 0
              ind ++
            }
          }
        }
      }
    }
    return outList
  }

  def isInBounds(x: Int, y: Int, z: Int, xDiff: Int, yDiff: Int, zDiff: Int): Boolean = {
    return x >= 0 && x < xDiff && y >= 0 && y < yDiff && z >= 0 && z < zDiff
  }

  def computeMean(cells: Array[Int]): Double = {
    var sum =0.0
    for (i <- 0 to 25){
      sum += cells(i)
    }
    return sum/26
  }

  def computeSD(cells: Array[Int], mean: Double): Double = {
    var sum =0.0
    for (i <- 0 to 25){
      sum += Math.pow(cells(i),2)
    }
    return Math.sqrt((sum/26)- Math.pow(mean,2))
  }
}
