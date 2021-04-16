package main



import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, Trigger}


object KafkaStreaming {

  def foreach_test1(batch_df: DataFrame, batchId: Long) : Unit= {
    val winSpec = Window.partitionBy("month").orderBy("month")
    val rankdf = batch_df.withColumn("rank", row_number().over(winSpec))
    println("hello world")
    rankdf.show()
  }

  def foreach_delta(transaction_detail: DataFrame, batchId: Long): Unit= {
    transaction_detail.write.format("delta").mode("append").save("/home/xs108-abhdas/IdeaProjects/kafkaTASK/files/delta/events")
  }


  def foreach_test(transaction_detail: DataFrame, batchId: Long) : Unit= {
    transaction_detail.persist()
    transaction_detail.write.format("csv").save("/home/xs108-abhdas/IdeaProjects/kafkaTASK/files/csv/") // location 1
    transaction_detail.write.format("json").save("/home/xs108-abhdas/IdeaProjects/kafkaTASK/files/json/") // location 2
    transaction_detail.unpersist()
  }

  // implicit val enc: Encoder[String] = Encoders.product[String]

  def main(args: Array[String]): Unit = {
    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "kafka-spark"
    val KAFKA_OUTPUT_TOPIC_NAME_CONS = "kafka-spark-out"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Structured Streaming with Kafka Demo")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "earliest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()


    val transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    println("Printing Schema of transaction_detail_df1: ")
    transaction_detail_df1.printSchema()

    // Define a schema for the transaction_detail data
    val transaction_detail_schema = ArrayType(StructType(Array(
      StructField("cust_id", IntegerType),
      StructField("month", IntegerType),
      StructField("expenses", FloatType)
    )))

    val transaction_detail_df2 = transaction_detail_df1
      .select(from_json(col("value"), transaction_detail_schema)
        .as("cust_detail"), col("timestamp"))
    println("Printing Schema of transaction_detail_df2: ")
    transaction_detail_df2.printSchema()


    val transaction_detail_df3 = transaction_detail_df2.withColumn("cust_detail_explode", explode(col("cust_detail")))
      .select("cust_detail_explode.*", "timestamp")
    println("Printing Schema of transaction_detail_df3: ")
    transaction_detail_df3.printSchema()

    val counts = transaction_detail_df3.withWatermark("timestamp", "10 minutes").groupBy(window(col("timestamp"), "10 minutes"), col("month")).count()
    counts.toDF()



    // Simple aggregate - find total_transaction_amount by grouping cust_id
    val transaction_detail_df4 = transaction_detail_df3.groupBy("cust_id", "month", "timestamp")
      .agg(sum(col("expenses")).as("total_expenses_amount"))

    println("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()


    val transaction_detail_df5 = transaction_detail_df4
      .withColumn("key", lit(100))
      .withColumn("value", concat(lit("{'month': '"),
        col("month"), lit("', 'cust_id: '"), col("cust_id"), lit("', 'total_expenses_amount: '"),
        col("total_expenses_amount")
          .cast("string"), lit("'}")))

    println("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()



      val win_func_stream = transaction_detail_df5.
      writeStream
        .trigger(Trigger.Once()).foreachBatch(foreach_test1 _)
        .outputMode("update")
        .start()


     //Write final window result into console for debugging purpose
     val window_detail_write_stream = counts
        .writeStream
        .trigger(Trigger.ProcessingTime("1 seconds"))
        .outputMode("update")
        .option("truncate","false").option("failOnDataLoss", "false")
        .format("console")
        .start()



    val trans_detail_write_stream = transaction_detail_df5
      .writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("truncate","false").option("failOnDataLoss", "false")
      .format("console")
      .start()


      // Write final result in the Kafka topic as key, value
      val trans_detail_write_stream_1 = transaction_detail_df5
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS)
        .trigger(Trigger.ProcessingTime("1 seconds"))
        .outputMode("update")
        .option("checkpointLocation", "/tmp/sparkCheckpoint/checkpoint")
        .option("failOnDataLoss", "false")
             .start()




    transaction_detail_df1
      .writeStream.trigger(Trigger.Once()).foreachBatch(foreach_test _)
      .start()
      .awaitTermination()

// write to delta


    transaction_detail_df5
      .writeStream.trigger(Trigger.Once()).foreachBatch(foreach_delta _)
      .outputMode("update")
      .start()
      .awaitTermination()



    val table = spark.read
      .format("delta")
      .load("/home/xs108-abhdas/IdeaProjects/kafkaTASK/files/delta/events")
      .createOrReplaceTempView("testTable")

    spark.sql("SELECT * FROM testTable where total_expenses_amount > 500").show(false)


        window_detail_write_stream.awaitTermination()
        win_func_stream.awaitTermination()
        trans_detail_write_stream.awaitTermination()
        trans_detail_write_stream_1.awaitTermination()

    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
}
