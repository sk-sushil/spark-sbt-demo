import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Demo {


  def main(args: Array[String]): Unit = {
    val KAFKA_TOPIC_NAME_CONS = "test-topic"
    val KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    System.setProperty("HADOOP_USER_NAME","hadoop")

    val spark = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .option("startingOffsets", "earliest") // From starting
      .load()
    df.printSchema()

    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    personStringDF.printSchema()

    personStringDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }


}
