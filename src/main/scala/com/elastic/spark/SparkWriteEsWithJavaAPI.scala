package com.elastic.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentFactory

object SparkWriteEsWithJavaAPI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("本地")
    val session = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val path = "src/main/data/ratings.csv"
    val client = ElasticearchUtils.getInstance().getClient
    @transient lazy val processor = ElasticearchUtils.getInstance().getProcessor

    val df = session.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    df.rdd.foreachPartition(iter => {
      iter.foreach(item => {
        processor.add(new IndexRequest("books", "tag", item.getInt(0).toString).source(XContentFactory.jsonBuilder()
          .startObject()
          .field("userId", item.getInt(0))
          .field("movieId", item.getInt(1))
          .field("rating", item.getDouble(2))
          .field("timestamp", item.getInt(3))
          .endObject()
        ))
      })
      processor.awaitClose(10, TimeUnit.MINUTES)
    })
    session.close()
  }
}
