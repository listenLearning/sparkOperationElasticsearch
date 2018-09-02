package com.elastic.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql._

object SparkWriteEsWithDefault {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("local").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "master")
    conf.set("es.port", "9200")
    conf.set("es.http.timeout", "10m")
    conf.set("es.http.retries", "50")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val path = "src/main/data/ratings.csv"

    val df = session.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    df.saveToEs("rating/tag", Map(ConfigurationOptions.ES_MAPPING_ID -> "userId"))

    session.close()
  }
}
