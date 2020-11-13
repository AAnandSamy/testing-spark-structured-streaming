package com.org.data.stream.test

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object DataStreamTester {

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.load(new FileInputStream("app-config.properties"))

    val sampleDta = prop.getProperty("value")
    val checkpointLocation = prop.getProperty("checkpointLocation")

    val spark = SparkSession
      .builder()
      .appName("Welcome to Spark 2.+++")
      .master("local[*]")
      .getOrCreate();

    import spark.implicits._
    implicit val ctx: SQLContext = spark.sqlContext

    val ds = spark.createDataset(s"""${sampleDta}""" :: Nil)
    val ds_schema = spark.read.json(ds).schema

    val stream = MemoryStream[String]
    stream.addData(sampleDta)
    val sDs = stream.toDS()

    val transDf = sDs.select(from_json($"value",ds_schema) as "dta")
      .select("dta.*")

    transDf
      .writeStream
      .format("console")
      .queryName("datastream")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointLocation)
      .start()
      .processAllAvailable()

  }


}
