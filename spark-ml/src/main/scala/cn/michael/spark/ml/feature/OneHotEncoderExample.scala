package cn.michael.spark.ml.feature

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by hufenggang on 2021/7/8.
 *
 * one-hot编码Demo
 */
object OneHotEncoderExample {
  private final val JOB_NAME: String = "OneHotEncoderExample"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.localShuffleReader", "true")
      .set("spark.sql.adaptive.skewedJoin.enabled", "true")
      .set("spark.sql.adaptive.join.enabled", "true")
      .set("spark.testing.memory","2147480000")
      .setAppName(JOB_NAME)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val dataFrame: DataFrame = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "name")

    val indexer: StringIndexer = new StringIndexer().setInputCol("name").setOutputCol("name_index")
    val frame: DataFrame = indexer.fit(dataFrame).transform(dataFrame)

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("name_index"))
      .setOutputCols(Array("name_vec"))
    val result: DataFrame = encoder.fit(frame).transform(frame)

    result.show(20)

    spark.stop()
  }

}
