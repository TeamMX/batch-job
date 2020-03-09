// scalastyle:off println

import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.types._
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

case class SensorStartEnd(sensorId: String, osmStartId: String, osmEndId: String)
case class Telemetry(sensorId: String, timestamp: Long, speedKph: Double)
case class SensorBucketWeightedSpeed(sensorId: String, bucket: Int, weight: Double, value: Double)
case class StartEndBucketWeightedSpeed(osmStartId: String, osmEndId: String, bucket: Int, weight: Double, value: Double)
case class MongoSpeed(_id: String, speedKph: Double)

object BatchJob {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.print("Usage: SparkPi <mongouri> <kafkaserver> <kafkatopic> <sensormap> <upserthalflife> <now>")
      System.exit(1)
    }
    val Array(mongouri, kafkaserver, kafkatopic, sensormap, halflifestr, nowstr) = args
    val upserthalflife = halflifestr.toInt

    val spark = SparkSession
      .builder
      .appName("Spark PI")
      .getOrCreate()
    import spark.implicits._

    val halfLife = spark.sparkContext.broadcast(halflifestr.toDouble)
    val now = spark.sparkContext.broadcast(nowstr.toLong)

    val startEndBySensorId = spark
      .read
      .csv(sensormap)
      .flatMap(row => {
        val sensorId = row.getString(0)
        val osmNodes = row
          .getString(1)
          .split(" ")
          .toList
        osmNodes
          .zip(osmNodes.drop(1))
          .map(link => new SensorStartEnd(
            sensorId,
            link._1,
            link._2))
      })

    val historicalKafkaTelemetry = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaserver)
      .option("subscribe", kafkatopic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map {
        case (key, value) => {
          val Array(idstr, tsstr, speedstr) = value.split(" ")
          new Telemetry(idstr, tsstr.toLong, speedstr.toDouble)
        }
      }

    val sensorBucketWeightedSpeeds = historicalKafkaTelemetry
      .map(telemetry => {
        val secondsInDay = 24 * 60 * 60
        val bucketSizeSeconds = 5 * 60
        val bucket = (telemetry.timestamp % secondsInDay).toInt / bucketSizeSeconds
        val weight = math.pow(0.5, (now.value - telemetry.timestamp) / halfLife.value)
        new SensorBucketWeightedSpeed(telemetry.sensorId, bucket, weight, telemetry.speedKph * weight)
      })
      .groupByKey(sensorSpeed => (sensorSpeed.sensorId, sensorSpeed.bucket))
      .reduceGroups((left, right) => new SensorBucketWeightedSpeed(
          left.sensorId,
          left.bucket,
          left.weight + right.weight,
          left.value + right.value)
      )
      .map(_._2)

    val mongoSpeeds = sensorBucketWeightedSpeeds
      .joinWith(
        startEndBySensorId,
        historicalKafkaTelemetry.col("sensorId") === startEndBySensorId.col("sensorId"))
      .map {
        case (telemetry, segment) => new StartEndBucketWeightedSpeed(
          segment.osmStartId,
          segment.osmEndId,
          telemetry.bucket,
          telemetry.weight,
          telemetry.value)
      }
      .groupByKey(s => (s.osmStartId, s.osmEndId, s.bucket))
      .reduceGroups((l, r) => new StartEndBucketWeightedSpeed(
        l.osmStartId,
        l.osmEndId,
        l.bucket,
        l.weight + r.weight,
        l.value + r.value)
      )
      .map {
        case (_, v) => {
          new MongoSpeed(s"${v.osmStartId} ${v.osmEndId} ${v.bucket}", v.value / v.weight)
        }
      }

    MongoSpark.save(mongoSpeeds, WriteConfig(Map("uri" -> mongouri)))
  }
}
// scalastyle:on println
