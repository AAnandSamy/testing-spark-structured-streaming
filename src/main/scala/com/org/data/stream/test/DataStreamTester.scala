package com.org.data.stream.test

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object DataStreamTester extends App {

  // loading app config
  val prop = new Properties()
  prop.load(new FileInputStream("app-config.properties"))
  // app config setu
  val sampleDta = prop.getProperty("value")
  val checkpointLocation = prop.getProperty("checkpointLocation")
  val path = prop.getProperty("path")

  val spark = SparkSession
    .builder()
    .appName("Welcome to Spark 2.+++")
    .master("local[*]")
    .getOrCreate();

  import spark.implicits._

  implicit val ctx: SQLContext = spark.sqlContext
  // prepare schema based on given message
  val ds = spark.createDataset(s"""${sampleDta}""" :: Nil)
  val ds_schema = spark.read.json(ds).schema
  // read and transformations
  val kds = readFromKafka(sampleDta)
  val transDf = transform(kds)
  // write sink
  consoleSink(transDf, checkpointLocation)

  def readFromKafka(dta: String): Dataset[String] = {
    val stream = MemoryStream[String]
    stream.addData(dta)
    val sDs = stream.toDS()
    sDs
  }

  def transform(ds: Dataset[String]): DataFrame = {
    ds.select(from_json($"value", ds_schema) as "dta")
      .select("dta.*")
  }

  def consoleSink(df: DataFrame, checkpointLoc: String): Unit = {
    df
      .writeStream
      .format("console")
      .queryName("datastream")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointLoc)
      .start()
      .processAllAvailable()
  }

  def HDFSSink(df: DataFrame, checkpointLoc: String, path: String): Unit = {
    df
      .writeStream
      .format("parquet")
      .queryName("datastream")
      .outputMode(OutputMode.Append())
      .option("path", path)
      .option("checkpointLocation", checkpointLoc)
      .start()
      .processAllAvailable()
  }

}
