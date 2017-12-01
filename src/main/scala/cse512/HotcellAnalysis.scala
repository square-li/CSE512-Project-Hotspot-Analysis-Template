package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

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
  var spaceTimeCube = Array.ofDim[Int](xDiff, yDiff, zDiff)
  println(numCells)

  val nextDfRDD = pickupInfo.rdd.map {
    r => (r.getInt(0) + "," + r.getInt(1) + "," + r.getInt(2), 1)
  }.reduceByKey(_ + _).sortBy(_._2)
  //nextDfRDD.collect().foreach(println)
  var x = 0
  var y = 0
  var z = 0
  spaceTimeCube = Array.ofDim[Int](xDiff, yDiff, zDiff)
  for ((coord, count) <- nextDfRDD.collect()) {
    val coordInp: List[Int] = coord.split(',').map(_.trim.toInt).toList
    // println(coord)
    x = coordInp.head - minX.toInt
    y = coordInp(1)- minY.toInt
    z = coordInp(2) - minZ.toInt
    if(x > xDiff || y > yDiff || z > zDiff){
      println("Outlier: " + formCoordString(x, y, z, minX, minY, minZ))
    }
    spaceTimeCube(x)(y)(z) += count.toInt
  }
  val (mean, sd, totalNumPoints) = computeMeanAndSD(numCells, xDiff, yDiff, zDiff, spaceTimeCube)
  println("Mean : " + mean.toString)
  println("SD : " + sd.toString)
  println("Total number of points : " + totalNumPoints.toString)
  var zscore = 0.0
  var zScoreMap = collection.mutable.Map[String, Double]()
  for (i <- 0 until xDiff) {
    for (j <- 0 until yDiff) {
      for (k <- 0 until zDiff) {
        val (sm, cnt) = getSumAndCountOfNeighBours(i, j, k, xDiff, yDiff, zDiff, spaceTimeCube)
        zScoreMap += (formCoordString(i, j, k, minX, minY, minZ) -> calculateZScore(sm, cnt, mean, sd, totalNumPoints.toInt))
      }
    }
  }
  val zScoreMapSorted = ListMap(zScoreMap.toSeq.sortWith(_._2 > _._2):_*).take(50).keys.toList
  val finalOutput = zScoreMapSorted.map(x => (x.split(",")(0), x.split(",")(1), x.split(",")(2)))
//  for((k,v) <- zScoreMapSorted){
//    println(k + ',' + v.toString)
//  }

  val sc = spark.sparkContext
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  finalOutput.toDF()
}

  def formCoordString(i: Int, j: Int, k: Int, minX: Double, minY: Double, minZ: Int): String = {
    (i+minX).toInt.toString + ',' + (j+minY).toInt.toString + ',' + (k+minZ).toInt.toString
  }

  def getSumAndCountOfNeighBours(x: Int, y: Int, z: Int, xDiff: Int, yDiff: Int, zDiff: Int, spaceTimeCube: Array[Array[Array[Int]]]) = {
    var diffs = Array(-1, 0, 1)
    var sm = 0
    var cnt = 0
    for(i <- diffs){
      for(j <- diffs){
        for(k <- diffs){
            if (isInBounds(x + i, y + j, z + k, xDiff, yDiff, zDiff)) {
              sm += spaceTimeCube(x + i)(y + j)(z + k)
              cnt += 1
            }
        }
      }
    }
    (sm, cnt)
  }

  def isInBounds(x: Int, y: Int, z: Int, xDiff: Int, yDiff: Int, zDiff: Int): Boolean = {
    x >= 0 && x < xDiff && y >= 0 && y < yDiff && z >= 0 && z < zDiff
  }

  def computeMeanAndSD(numberOfCells: Double, xDiff: Int, yDiff: Int, zDiff: Int, spaceTimeCube: Array[Array[Array[Int]]]) = {
    var sm = 0.0
    var smSquare = 0.0
    for (i <- 0 until xDiff){
      for(j <- 0 until yDiff){
        for(k <- 0 until zDiff){
          sm += spaceTimeCube(i)(j)(k)
          smSquare += Math.pow(spaceTimeCube(i)(j)(k), 2)
        }
      }
    }
    val mean = sm/numberOfCells
    val sd = computeSD(numberOfCells, mean, smSquare)
    (mean, sd, sm)
  }

  def computeSD(numberOfCells: Double, mean: Double, smSquare: Double): Double = {
    Math.sqrt((smSquare/numberOfCells) - Math.pow(mean,2))
  }

  def calculateZScore(sm: Int, cnt: Int, mean: Double, sd: Double, totalNumPoints: Int): Double = {
    var num = 0.0
    var den = 0.0
    num = sm - mean*cnt
    den = sd*Math.sqrt((cnt*totalNumPoints - Math.pow(cnt, 2))/(totalNumPoints-1))
    num/den
  }
}
